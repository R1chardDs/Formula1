# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

Prm_Season_Year = 2025

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)

target_table = "Pit_stop"

SCHEMA_PITS = StructType([
    StructField("stop", IntegerType(), True),
    StructField("car_number", IntegerType(), True),
    StructField("driver", StringType(), True),
    StructField("driver_name", StringType(), True),
    StructField("driver_code", StringType(), True),
    StructField("team", StringType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time_of_day", StringType(), True),
    StructField("pit_time", StringType(), True),
    StructField("pit_secs", DoubleType(), True),
    StructField("total_time", StringType(), True),
    StructField("total_secs", DoubleType(), True),
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

def parse_pit_stop_summary(url: str):
    html = fns.fetch_html(url)
    soup = BeautifulSoup(html, "lxml")

    title = soup.find(["h1","h2"])
    title_text = title.get_text(" ", strip=True) if title else None

    def _norm_txt(s):
        return re.sub(r"\s+", " ", str(s).replace("\xa0", " ")).strip().lower()

    def _flatten_cols(cols):
        flat = []
        for c in cols:
            if isinstance(c, tuple):
                parts = [p for p in c if p is not None and str(p).strip() != "" and not str(p).lower().startswith("unnamed")]
                name = " ".join(map(str, parts))
            else:
                name = str(c)
            flat.append(_norm_txt(name))
        return flat

    def _to_seconds(x):
        if x is None or (isinstance(x, float) and pd.isna(x)): return None
        s = _norm_txt(x).rstrip("s")
        if s in {"", "-", "—", "na", "n/a"}: return None
        parts = s.split(":")
        try:
            if len(parts) == 3:
                h = int(parts[0]); m = int(parts[1]); sec = float(parts[2])
                return h*3600 + m*60 + sec
            if len(parts) == 2:
                m = int(parts[0]); sec = float(parts[1])
                return m*60 + sec
            return float(parts[0])
        except Exception:
            return None

    df = None
    for t in read_html_tables(html):
        cols_lc = _flatten_cols(t.columns)
        # señales típicas de pit stops (flexibles)
        has_stop   = any(c in {"stop", "stops"} or c.startswith("stop") for c in cols_lc)
        has_driver = any("driver" in c for c in cols_lc)
        has_timey  = any(("time of day" in c) or (c == "time") or ("total" in c) for c in cols_lc)
        if has_stop and has_driver and has_timey:
            t.columns = cols_lc  # ya aplanadas/normalizadas
            df = t.copy()
            break

    if df is None:
        return pd.DataFrame()

    # renombres tolerantes (solo si existen)
    rename_map = {
        "no.": "car_number",
        "car no.": "car_number",
        "stop": "stop",
        "stops": "stop",
        "driver": "driver",
        "team": "team",
        "lap": "lap",
        "time of day": "time_of_day",
        "time": "pit_time",
        "total": "total_time",
        "total time": "total_time",
        "pts.": "points"
    }
    for k, v in rename_map.items():
        if k in df.columns:
            df.rename(columns={k: v}, inplace=True)

    # driver_name / driver_code
    if "driver" in df.columns:
        def _split_dc(s: str):
            s = (s or "").strip()
            m = re.match(r"^(.*?)[\s]*([A-Z]{3})$", s)
            return ((m.group(1) or s).strip(), m.group(2)) if m else (s, None)
        df["driver_name"], df["driver_code"] = zip(*df["driver"].astype(str).map(_split_dc))

    for c in ["stop","car_number","lap","points"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # tiempos en segundos
    if "pit_time" in df.columns:
        df["pit_secs"] = df["pit_time"].apply(_to_seconds)
    else:
        df["pit_time"] = None; df["pit_secs"] = None

    if "total_time" in df.columns:
        df["total_secs"] = df["total_time"].apply(_to_seconds)
    else:
        df["total_time"] = None; df["total_secs"] = None

    df["source_url"] = url
    df["event_title"] = title_text
    df["scraped_at_utc"] = datetime.utcnow().isoformat(timespec="seconds")
    return df

# --- usar el races_pdf existente para extraer pit-stop-summary ---
all_pits_pdf, pits_errors = [], []

for race_url in races_pdf["url"].tolist():
    pit_url = with_suffix(race_url, "pit-stop-summary")
    try:
        p_df = parse_pit_stop_summary(pit_url)
        if p_df.empty:
            continue
        m = re.search(r"/races/(\d+)/([^/]+)/", race_url)
        p_df["race_id"] = int(m.group(1)) if m else None
        p_df["gp_slug"] = m.group(2) if m else None
        p_df["season"]  = Prm_Season_Year
        all_pits_pdf.append(p_df)
    except Exception as e:
        pits_errors.append({"url": pit_url, "error": str(e)})

pitstop_pdf = pd.concat(all_pits_pdf, ignore_index=True) if all_pits_pdf else pd.DataFrame()

# schema mínimo (en caso de vacío) y orden de columnas
min_cols_pit = [
    "stop","car_number","driver","driver_name","driver_code","team","lap",
    "time_of_day","pit_time","pit_secs","total_time","total_secs",
    "source_url","event_title","scraped_at_utc","race_id","gp_slug","season"
]
if pitstop_pdf.empty:
    pitstop_pdf = pd.DataFrame(columns=min_cols_pit)
else:
    for c in min_cols_pit:
        if c not in pitstop_pdf.columns:
            pitstop_pdf[c] = None
    pitstop_pdf = pitstop_pdf[min_cols_pit]

spark_pitstops = to_spark(pitstop_pdf, SCHEMA_PITS)

spark_pitstops.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(tgt_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
