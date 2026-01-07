# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

Prm_Season_Year = 2017

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)

target_table = "Qualifying"

SCHEMA_QUALI = StructType([
    StructField("position", IntegerType(), True),
    StructField("car_number", IntegerType(), True),
    StructField("driver", StringType(), True),
    StructField("team", StringType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True),
    StructField("laps", IntegerType(), True),
    StructField("Q1_Segs", DoubleType(), True),
    StructField("Q2_Segs", DoubleType(), True),
    StructField("Q3_Segs", DoubleType(), True),
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

def parse_qualifying(url: str):
    html = fns.fetch_html(url)
    soup = BeautifulSoup(html, "lxml")

    title = soup.find(["h1","h2"])
    title_text = title.get_text(" ", strip=True) if title else None

    # localizar la tabla que tenga DRIVER y alguna de Q1/Q2/Q3
    df = None
    for t in read_html_tables(html):
        cols_lc = [str(c).strip().lower() for c in t.columns]
        has_driver = any(c == "driver" for c in cols_lc)
        has_q = any(c in cols_lc for c in ["q1","q2","q3"])
        if has_driver and has_q:
            df = t.copy()
            break
    if df is None:
        return pd.DataFrame()

    # normalizar encabezados
    df.columns = [c.strip().lower() for c in df.columns]
    df.rename(columns={
        "pos.": "position",
        "no.": "car_number",
        "pts.": "points",
    }, inplace=True)

    # separar driver_name / driver_code (permite 'Lando NorrisNOR')
    if "driver" in df.columns:
        def _split_dc(s: str):
            s = (s or "").strip()
            m = re.match(r"^(.*?)[\s]*([A-Z]{3})$", s)
            return ((m.group(1) or s).strip(), m.group(2)) if m else (s, None)
        df["driver_name"], df["driver_code"] = zip(*df["driver"].astype(str).map(_split_dc))

    # helper: texto de tiempo -> segundos (Q1/Q2/Q3)
    def _q_to_seconds(x):
        if x is None or (isinstance(x, float) and pd.isna(x)): return None
        s = str(x).strip().lower()
        if s in {"", "-", "—", "na", "n/a", "no time", "dns", "dnf"}: return None
        s = s.rstrip("s")  # a veces viene con 's'
        parts = s.split(":")
        try:
            if len(parts) == 2:  # M:SS.sss
                m = int(parts[0]); sec = float(parts[1])
                return m*60 + sec
            elif len(parts) == 1:  # SS.sss
                return float(parts[0])
            else:
                # H:MM:SS.sss (raro en qualy)
                h = int(parts[0]); m = int(parts[1]); sec = float(parts[2])
                return h*3600 + m*60 + sec
        except Exception:
            return None

    # crear columnas en segundos sin tocar las Q originales (texto)
    for qc in ["q1", "q2", "q3"]:
        if qc not in df.columns:
            df[qc] = None
        seg_col = f"{qc.upper()}_Segs"   # Q1_Segs, Q2_Segs, Q3_Segs
        df[seg_col] = df[qc].apply(_q_to_seconds)

    # casts básicos
    for c in ["position","car_number","laps","points"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df["source_url"] = url
    df["event_title"] = title_text
    df["scraped_at_utc"] = datetime.utcnow().isoformat(timespec="seconds")
    return df


# --- usar el races_pdf existente para extraer qualifying con columnas de segundos ---
all_quali_pdf, quali_errors = [], []

for race_url in races_pdf["url"].tolist():
    q_url = with_suffix(race_url, "qualifying")
    try:
        q_df = parse_qualifying(q_url)
        if q_df.empty:
            continue
        m = re.search(r"/races/(\d+)/([^/]+)/", race_url)
        q_df["race_id"] = int(m.group(1)) if m else None
        q_df["gp_slug"]  = m.group(2) if m else None
        q_df["season"]   = Prm_Season_Year
        all_quali_pdf.append(q_df)
    except Exception as e:
        quali_errors.append({"url": q_url, "error": str(e)})

qualifying_pdf = pd.concat(all_quali_pdf, ignore_index=True) if all_quali_pdf else pd.DataFrame()

# garantizar schema mínimo si quedó vacío
min_cols = [
    "position","car_number","driver","team","q1","q2","q3","laps",
    "Q1_Segs","Q2_Segs","Q3_Segs",
    "driver_name","driver_code","source_url","event_title","scraped_at_utc",
    "race_id","gp_slug","season"
]
if qualifying_pdf.empty:
    qualifying_pdf = pd.DataFrame(columns=min_cols)
else:
    for c in min_cols:
        if c not in qualifying_pdf.columns:
            qualifying_pdf[c] = None
    qualifying_pdf = qualifying_pdf[min_cols]

spark_qualifying = to_spark(qualifying_pdf, SCHEMA_QUALI)

#display(spark_qualifying)
spark_qualifying.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(tgt_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
