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
target_zone = "Test"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)

target_table_1 = "Practice_1"
target_table_2 = "Practice_2"
target_table_3 = "Practice_3"

SCHEMA_PRACTICE = StructType([
    StructField("position", IntegerType(), True),
    StructField("car_number", IntegerType(), True),
    StructField("driver", StringType(), True),
    StructField("team", StringType(), True),
    StructField("time", StringType(), True),         # mejor vuelta (texto)
    StructField("time_s", DoubleType(), True),       # mejor vuelta en segundos
    StructField("gap", StringType(), True),          # gap vs líder (texto)
    StructField("gap_s", DoubleType(), True),        # gap en segundos
    StructField("laps", IntegerType(), True),
    StructField("driver_name", StringType(), True),
    StructField("driver_code", StringType(), True),
    StructField("source_url", StringType(), True),
    StructField("event_title", StringType(), True),
    StructField("scraped_at_utc", StringType(), True),
    StructField("race_id", IntegerType(), True),
    StructField("gp_slug", StringType(), True),
    StructField("season", IntegerType(), True),
    StructField("practice_no", IntegerType(), True),
])


target_lakehouse = "Lake_F1_" + target_zone
target_workspace = "F1_Lab"
target_schema = "dbo"

tgt_path_1 = "abfss://" + target_workspace + "@onelake.dfs.fabric.microsoft.com/" + target_lakehouse + ".Lakehouse/Tables/" + target_schema + "/" + target_table_1
tgt_path_2 = "abfss://" + target_workspace + "@onelake.dfs.fabric.microsoft.com/" + target_lakehouse + ".Lakehouse/Tables/" + target_schema + "/" + target_table_2
tgt_path_3 = "abfss://" + target_workspace + "@onelake.dfs.fabric.microsoft.com/" + target_lakehouse + ".Lakehouse/Tables/" + target_schema + "/" + target_table_3

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
src_shecma = target_schema

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
def with_practice(race_result_url: str, n: int) -> str:
    return build_result_url(race_result_url, "practice", session_no=n)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def parse_practice(url: str, practice_no: int):
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

    def _time_to_seconds(x):
        if x is None or (isinstance(x, float) and pd.isna(x)): return None
        s = _norm_txt(x).lstrip("+").rstrip("s")
        if s in {"", "-", "—", "na", "n/a", "no time"}: return None
        parts = s.split(":")
        try:
            if len(parts) == 3:  # H:MM:SS.sss
                h = int(parts[0]); m = int(parts[1]); sec = float(parts[2]); return h*3600 + m*60 + sec
            if len(parts) == 2:  # M:SS.sss
                m = int(parts[0]); sec = float(parts[1]); return m*60 + sec
            return float(parts[0])  # SS.sss
        except Exception:
            return None

    def _split_dc(s: str):
        s = (s or "").strip()
        m = re.match(r"^(.*?)[\s]*([A-Z]{3})$", s)
        return ((m.group(1) or s).strip(), m.group(2)) if m else (s, None)

    # 1) Leer tablas directamente desde cada <table> (evita falsos positivos)
    tables = []
    for tbl in soup.find_all("table"):
        try:
            tables.extend(read_html_tables(tbl))
        except ValueError:
            pass
    if not tables:
        try:
            tables = read_html_tables(html)
        except ValueError:
            tables = []

    # 2) Detectar la tabla de práctica (admite "time / gap")
    df = None
    for t in tables:
        cols_lc = _flatten_cols(t.columns)
        has_driver = any(c == "driver" or "driver" in c for c in cols_lc)
        has_laps   = any(c == "laps" or "laps" in c for c in cols_lc)
        has_time_like = any(("time" in c) for c in cols_lc)  # cubre "time", "best time", "time / gap"
        if has_driver and has_laps and has_time_like:
            t.columns = cols_lc
            df = t.copy()
            break

    if df is None:
        return pd.DataFrame()

    # 3) Renombrar; manejar "time / gap" -> "time_or_gap"
    rename_map = {
        "pos.": "position",
        "position": "position",
        "no.": "car_number",
        "car no.": "car_number",
        "driver": "driver",
        "team": "team",
        "time / gap": "time_or_gap",
        "time": "time",               # si viniera separado
        "best time": "time",
        "lap time": "time",
        "gap": "gap",
        "interval": "interval",
        "laps": "laps",
    }
    for k, v in rename_map.items():
        if k in df.columns:
            df.rename(columns={k: v}, inplace=True)

    # 4) Si viene "time_or_gap" en una sola columna, sepárala en time/gap
    if "time_or_gap" in df.columns:
        # ganador tiene tiempo absoluto; el resto empieza con '+'
        df["time"] = df["time_or_gap"].where(~df["time_or_gap"].astype(str).str.strip().str.startswith("+"))
        df["gap"]  = df["time_or_gap"].where( df["time_or_gap"].astype(str).str.strip().str.startswith("+"))
    # columnas que podrían faltar
    for c in ["gap","interval","laps","car_number","time"]:
        if c not in df.columns:
            df[c] = None

    # 5) driver_name / driver_code
    if "driver" in df.columns:
        df["driver_name"], df["driver_code"] = zip(*df["driver"].astype(str).map(_split_dc))

    # 6) casts
    for c in ["position","car_number","laps"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # 7) derivados en segundos
    df["time_s"]     = df["time"].apply(_time_to_seconds) if "time" in df.columns else None
    df["gap_s"]      = df["gap"].apply(_time_to_seconds) if "gap" in df.columns else None
    df["interval_s"] = df["interval"].apply(_time_to_seconds) if "interval" in df.columns else None

    # (opcional) si quieres calcular tiempos absolutos para todos:
    # if df["time_s"].notna().any():
    #     leader = df.loc[df["position"]==1, "time_s"]
    #     if not leader.empty:
    #         lead = leader.iloc[0]
    #         df["time_s"] = df["time_s"].fillna(lead + df["gap_s"])

    df["source_url"]     = url
    df["event_title"]    = title_text
    df["scraped_at_utc"] = datetime.utcnow().isoformat(timespec="seconds")
    df["practice_no"]    = practice_no
    return df


# ------ Loop FP1 usando races_pdf existente y envío a Spark con tu helper to_spark ------
all_p1_pdf, p1_errors = [], []

for race_url in races_pdf["url"].tolist():
    p1_url = with_practice(race_url, 1)
    try:
        p1_df = parse_practice(p1_url, practice_no=1)
        if p1_df.empty:
            continue
        m = re.search(r"/races/(\d+)/([^/]+)/", race_url)
        p1_df["race_id"] = int(m.group(1)) if m else None
        p1_df["gp_slug"] = m.group(2) if m else None
        p1_df["season"]  = Prm_Season_Year
        all_p1_pdf.append(p1_df)
    except Exception as e:
        p1_errors.append({"url": p1_url, "error": str(e)})

practice1_pdf = pd.concat(all_p1_pdf, ignore_index=True) if all_p1_pdf else pd.DataFrame()
spark_practice1 = to_spark(practice1_pdf, SCHEMA_PRACTICE)

#display(spark_practice1)
spark_practice1.write.format("delta").mode("append").option("overwriteSchema","true").save(tgt_path_1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

races_no_sprint = (
    races_pdf.loc[
        races_pdf["sprint"].astype(str).str.upper().eq("N") & races_pdf["url"].notna()
    ]
    .copy()
)

# 2) Iterar y parsear FP2
all_p2_pdf, p2_errors = [], []

for race_url in races_no_sprint["url"].tolist():
    p2_url = with_practice(race_url, 2)  # .../practice/2
    try:
        p2_df = parse_practice(p2_url, practice_no=2)
        if p2_df.empty:
            continue
        m = re.search(r"/races/(\d+)/([^/]+)/", race_url)
        p2_df["race_id"] = int(m.group(1)) if m else None
        p2_df["gp_slug"] = m.group(2) if m else None
        p2_df["season"]  = Prm_Season_Year
        all_p2_pdf.append(p2_df)
    except Exception as e:
        p2_errors.append({"url": p2_url, "error": str(e)})

practice2_pdf = pd.concat(all_p2_pdf, ignore_index=True) if all_p2_pdf else pd.DataFrame()

spark_practice2 = to_spark(practice2_pdf, SCHEMA_PRACTICE)

#display(spark_practice2)
spark_practice2.write.format("delta").mode("append").option("overwriteSchema","true").save(tgt_path_2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2) Iterar y parsear FP3
all_p3_pdf, p3_errors = [], []

for race_url in races_no_sprint["url"].tolist():
    p3_url = with_practice(race_url, 3)  # .../practice/3
    try:
        p3_df = parse_practice(p3_url, practice_no=3)
        if p3_df.empty:
            continue
        m = re.search(r"/races/(\d+)/([^/]+)/", race_url)
        p3_df["race_id"] = int(m.group(1)) if m else None
        p3_df["gp_slug"] = m.group(2) if m else None
        p3_df["season"]  = Prm_Season_Year
        all_p3_pdf.append(p3_df)
    except Exception as e:
        p3_errors.append({"url": p3_url, "error": str(e)})

practice3_pdf = pd.concat(all_p3_pdf, ignore_index=True) if all_p3_pdf else pd.DataFrame()

# 3) A Spark con schema consistente
spark_practice3 = to_spark(practice3_pdf, SCHEMA_PRACTICE)

#display(spark_practice3)
spark_practice3.write.format("delta").mode("append").option("overwriteSchema","true").save(tgt_path_3)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
