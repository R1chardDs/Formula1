# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

Prm_Season_Year = 2004

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)

target_table = "Starting_Grid"

SCHEMA_GRID = StructType([
    StructField("grid_position", IntegerType(), True),
    StructField("car_number", IntegerType(), True),
    StructField("driver", StringType(), True),
    StructField("team", StringType(), True),
    StructField("time_or_note", StringType(), True),
    StructField("points", IntegerType(), True),
    StructField("driver_name", StringType(), True),
    StructField("driver_code", StringType(), True),
    StructField("source_url", StringType(), True),
    StructField("event_title", StringType(), True),
    StructField("scraped_at_utc", StringType(), True),
    StructField("race_id", IntegerType(), True),
    StructField("gp_slug", StringType(), True),
    StructField("season", IntegerType(), True),
])

target_lakehouse = "Lake_F1_Bronze"
target_workspace = "F1_Lab"
target_schema = "staging"

tgt_path = "abfss://" + target_workspace + "@onelake.dfs.fabric.microsoft.com/" + target_lakehouse + ".Lakehouse/Tables/" + target_schema + "/" + target_table

from delta.tables import *
import re, time, random, requests, pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urljoin
from pyspark.sql import functions as F
from io import StringIO
import pandas as pd

src_table = "All_Races"
src_workspace = target_workspace
src_lakehouse = target_lakehouse
src_shecma = "dbo"

src_path = "abfss://" + src_workspace + "@onelake.dfs.fabric.microsoft.com/" + src_lakehouse + ".Lakehouse/Tables/" + src_shecma + "/" + src_table

spark_races_all = spark.read.format("delta").load(src_path)
spark_races = spark_races_all.filter( (F.col("season") == Prm_Season_Year) & (F.col("bronze") == "N" ) )
races_pdf = spark_races.toPandas()

UDF_ITEM = "Fx - Fetchhtml"
fns = notebookutils.udf.getFunctions(UDF_ITEM)

def read_html_tables(html_or_tag):
    """Devuelve tablas de pandas desde HTML (string) o una etiqueta <table> de BS4."""
    if hasattr(html_or_tag, "name"):  # es un tag BeautifulSoup (e.g., <table>…)
        return pd.read_html(StringIO(str(html_or_tag)))
    return pd.read_html(StringIO(html_or_tag))

# --- Helper: normaliza tipos en pandas y crea Spark DF con schema (sin warnings de Arrow) ---
def to_spark(pdf: pd.DataFrame, schema: StructType):
    expected = [f.name for f in schema]
    # si viene vacío o None, crea vacío con columnas esperadas
    if pdf is None or pdf.empty:
        pdf2 = pd.DataFrame(columns=expected)
    else:
        pdf2 = pdf.copy()
        # asegura columnas y orden
        for c in expected:
            if c not in pdf2.columns:
                pdf2[c] = None
        pdf2 = pdf2[expected]

    # homogeneiza tipos para Arrow (strings y numéricos coherentes)
    for f in schema:
        col = f.name
        dt  = f.dataType
        if isinstance(dt, (IntegerType, DoubleType)):
            pdf2[col] = pd.to_numeric(pdf2[col], errors="coerce")
        else:
            # evita mezcla de bytes/float/None -> string estable
            pdf2[col] = pdf2[col].astype("string")

    # crea DF con schema explícito (Arrow no se queja)
    return spark.createDataFrame(pdf2, schema=schema)

# --- builder genérico para cualquier resultado ---
def build_result_url(race_result_url: str, kind: str, session_no: int | None = None) -> str:
    """
    kind: 'race-result', 'qualifying', 'starting-grid', 'pit-stop-summary',
          'fastest-laps', 'sprint-qualifying', 'sprint-grid', 'sprint-results',
          'practice' (requiere session_no 1/2/3)
    """
    if not race_result_url:
        raise ValueError("race_result_url is required")

    base = race_result_url.rsplit("/", 1)[0]  # quita el último segmento (p.ej. 'race-result')

    if kind == "practice":
        if session_no not in (1, 2, 3):
            raise ValueError("For 'practice', session_no must be 1, 2, or 3.")
        return f"{base}/practice/{session_no}"

    # endpoints simples
    return f"{base}/{kind}"

# --- wrappers de conveniencia (compatibilidad y legibilidad) ---
def with_suffix(race_result_url: str, suffix: str) -> str:
    return build_result_url(race_result_url, suffix)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def parse_starting_grid(url: str):
    html = fns.fetch_html(url)
    soup = BeautifulSoup(html, "lxml")

    title = soup.find(["h1","h2"])
    title_text = title.get_text(" ", strip=True) if title else None

    # localizar la tabla que tenga DRIVER y POS (y que NO sea de qualy con Q1/Q2/Q3)
    df = None
    for t in read_html_tables(html):
        cols_lc = [str(c).strip().lower() for c in t.columns]
        has_driver = any(c == "driver" for c in cols_lc)
        has_pos = ("pos." in cols_lc) or ("position" in cols_lc) or ("grid" in cols_lc)
        has_q = any(c in cols_lc for c in ["q1","q2","q3"])
        if has_driver and has_pos and not has_q:
            df = t.copy()
            break

    if df is None:
        return pd.DataFrame()

    # normalizar encabezados comunes
    df.columns = [c.strip().lower() for c in df.columns]
    rename_map = {
        "pos.": "grid_position",
        "position": "grid_position",
        "grid": "grid_position",
        "no.": "car_number",
        "pts.": "points",
        "time / retired": "time_or_note",   # por si apareciera
        "time": "time_or_note"
    }
    for k, v in rename_map.items():
        if k in df.columns:
            df.rename(columns={k: v}, inplace=True)

    # separar driver_name / driver_code (acepta 'Lando NorrisNOR')
    if "driver" in df.columns:
        def _split_dc(s: str):
            s = (s or "").strip()
            m = re.match(r"^(.*?)[\s]*([A-Z]{3})$", s)
            return ((m.group(1) or s).strip(), m.group(2)) if m else (s, None)
        df["driver_name"], df["driver_code"] = zip(*df["driver"].astype(str).map(_split_dc))

    # casts básicos
    for c in ["grid_position", "car_number", "points"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df["source_url"] = url
    df["event_title"] = title_text
    df["scraped_at_utc"] = datetime.utcnow().isoformat(timespec="seconds")
    return df


# --- usar el races_pdf existente para extraer starting-grid ---
all_grid_pdf, grid_errors = [], []

for race_url in races_pdf["url"].tolist():
    sg_url = with_suffix(race_url, "starting-grid")
    try:
        sg_df = parse_starting_grid(sg_url)
        if sg_df.empty:
            continue
        m = re.search(r"/races/(\d+)/([^/]+)/", race_url)
        sg_df["race_id"] = int(m.group(1)) if m else None
        sg_df["gp_slug"] = m.group(2) if m else None
        sg_df["season"]  = Prm_Season_Year
        all_grid_pdf.append(sg_df)
    except Exception as e:
        grid_errors.append({"url": sg_url, "error": str(e)})

starting_grid_pdf = pd.concat(all_grid_pdf, ignore_index=True) if all_grid_pdf else pd.DataFrame()

# garantizar schema mínimo si quedó vacío
min_cols_grid = [
    "grid_position","car_number","driver","team","time_or_note","points",
    "driver_name","driver_code","source_url","event_title","scraped_at_utc",
    "race_id","gp_slug","season"
]
if starting_grid_pdf.empty:
    starting_grid_pdf = pd.DataFrame(columns=min_cols_grid)
else:
    for c in min_cols_grid:
        if c not in starting_grid_pdf.columns:
            starting_grid_pdf[c] = None
    starting_grid_pdf = starting_grid_pdf[min_cols_grid]

spark_starting_grid = to_spark(starting_grid_pdf, SCHEMA_GRID)

#display(spark_starting_grid)
spark_starting_grid.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(tgt_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
