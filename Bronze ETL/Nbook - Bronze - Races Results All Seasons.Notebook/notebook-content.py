# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

Prm_Season_Year = 2021

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)

target_table = "Races_Results"

SCHEMA_RESULTS = StructType([
    StructField("position", IntegerType(), True),
    StructField("car_number", IntegerType(), True),
    StructField("driver", StringType(), True),
    StructField("team", StringType(), True),
    StructField("laps", IntegerType(), True),
    StructField("time_or_retired", StringType(), True),
    StructField("points", DoubleType(), True),
    StructField("driver_name", StringType(), True),
    StructField("driver_code", StringType(), True),
    StructField("status", StringType(), True),
    StructField("race_time_s", DoubleType(), True),
    StructField("gap_to_winner_s", DoubleType(), True),
    StructField("source_url", StringType(), True),
    StructField("event_title", StringType(), True),
    StructField("scraped_at_utc", StringType(), True),
    StructField("race_id", IntegerType(), True),
    StructField("season", IntegerType(), True),
    StructField("gp_slug", StringType(), True)
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

#display(spark_races)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DN_STATUSES = {"DNF","DNS","DSQ"}

def time_to_seconds(s: str):
    if not s or pd.isna(s): return None
    s = s.strip()
    if s in DN_STATUSES: return None
    is_gap = s.startswith("+")
    s = s[1:] if is_gap else s
    s = s.rstrip("s")
    parts = s.split(":")
    try:
        if len(parts) == 3:  # H:MM:SS.sss
            h = int(parts[0]); m = int(parts[1]); sec = float(parts[2])
            return h*3600 + m*60 + sec
        elif len(parts) == 2:  # M:SS.sss
            m = int(parts[0]); sec = float(parts[1])
            return m*60 + sec
        else:
            return float(parts[0])
    except Exception:
        return None

def split_driver_and_code(s: str):
    s = (s or "").strip()
    # captura los últimos 3 mayúsculos como código, haya o no espacio
    m = re.match(r"^(.*?)[\s]*([A-Z]{3})$", s)
    if m:
        name = m.group(1).strip()
        code = m.group(2).strip()
        # si el "name" quedó vacío (raro), devolvemos original
        return (name if name else s, code)
    return (s, None)

def parse_race_result(url: str):
    html = fns.fetch_html(url)
    soup = BeautifulSoup(html, "lxml")

    # Título (para contexto)
    title = soup.find(["h1","h2"])
    title_text = title.get_text(" ", strip=True) if title else None

    # Tabla principal (POS / NO / DRIVER / TEAM / LAPS / TIME / PTS)
    df = None
    for t in read_html_tables(html):
        cols = {c.strip().lower() for c in t.columns}
        if {"pos.", "no.", "driver", "team", "laps", "time / retired", "pts."}.issubset(cols):
            df = t.copy()
            break
    if df is None:
        raise RuntimeError(f"No encontré tabla de resultados en {url}")

    df.columns = [c.strip().lower() for c in df.columns]
    df.rename(columns={
        "pos.": "position",
        "no.": "car_number",
        "time / retired": "time_or_retired",
        "pts.": "points"
    }, inplace=True)

    # Driver + code
    names, codes = [], []
    for val in df["driver"].astype(str):
        n,c = split_driver_and_code(val); names.append(n); codes.append(c)
    df["driver_name"] = names
    df["driver_code"] = codes

    # Tiempos / gaps / estado
    df["status"] = df["time_or_retired"].apply(lambda s: s if str(s).strip() in DN_STATUSES else "Finished")
    df["race_time_s"] = None
    df["gap_to_winner_s"] = None

    # ganador = position == 1
    if (df["position"].astype(str) == "1").any():
        # ganador: tiempo de carrera (no +gap)
        w_idx = df.index[df["position"].astype(str)=="1"][0]
        df.loc[w_idx, "race_time_s"] = time_to_seconds(str(df.loc[w_idx,"time_or_retired"]))
        # resto: gaps en segundos (cuando empiece por '+')
        for i, v in enumerate(df["time_or_retired"].astype(str)):
            if i == w_idx: continue
            if v.strip().startswith("+"):
                df.loc[i, "gap_to_winner_s"] = time_to_seconds(v)

    # tipos
    for c in ["position","car_number","laps","points"]:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")

    df["source_url"] = url
    df["event_title"] = title_text
    df["scraped_at_utc"] = datetime.utcnow().isoformat(timespec="seconds")
    return df

# ---------- 3) Ejecutar: índice -> DF de carreras ----------

# ---------- 4) Loop por cada carrera y unión de resultados ----------
all_results_pdf = []
errors = []

for url in races_pdf["url"].tolist():
    try:
        df_one = parse_race_result(url)
        # agrega clave de carrera desde la url (race_id = número en la ruta)
        m = re.search(r"/races/(\d+)/", url)
        race_id = int(m.group(1)) if m else None
        df_one["race_id"] = race_id
        # gp (slug) y season
        df_one["season"] = Prm_Season_Year
        df_one["gp_slug"] = url.rstrip("/").split("/")[-2]
        all_results_pdf.append(df_one)
    except Exception as e:
        errors.append({"url": url, "error": str(e)})

results_pdf = pd.concat(all_results_pdf, ignore_index=True) if all_results_pdf else pd.DataFrame()

spark_results = to_spark(results_pdf, SCHEMA_RESULTS)

spark_results.write.format("delta").mode("append").option("overwriteSchema","true").save(tgt_path)

#display(spark_results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
