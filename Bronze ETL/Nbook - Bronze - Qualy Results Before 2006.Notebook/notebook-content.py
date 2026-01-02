# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

Prm_Season_Year = 1975

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)

target_table = "Old_Qualifying"

SCHEMA_OLD_QUALI = StructType([
    StructField("position", IntegerType(), True),
    StructField("car_number", IntegerType(), True),
    StructField("driver", StringType(), True),
    StructField("team", StringType(), True),
    StructField("time", StringType(), True),     # tiempo en texto (p.ej. 1:21.434)
    StructField("time_s", DoubleType(), True),   # tiempo en segundos
    StructField("laps", IntegerType(), True),    # opcional (a veces no existe)
    StructField("points", IntegerType(), True),  # por si aparece en algunas temporadas
    StructField("driver_name", StringType(), True),
    StructField("driver_code", StringType(), True),
    StructField("source_url", StringType(), True),
    StructField("event_title", StringType(), True),
    StructField("scraped_at_utc", StringType(), True),
    StructField("race_id", IntegerType(), True),
    StructField("gp_slug", StringType(), True),
    StructField("season", IntegerType(), True),
])

target_lakehouse = "Lake_F1_" + target_zone
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

def parse_old_qualifying(url: str):
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
        if s in {"", "-", "—", "na", "n/a"}: return None
        parts = s.split(":")
        try:
            if len(parts) == 3:               # H:MM:SS.sss
                h = int(parts[0]); m = int(parts[1]); sec = float(parts[2])
                return h*3600 + m*60 + sec
            if len(parts) == 2:               # M:SS.sss
                m = int(parts[0]); sec = float(parts[1])
                return m*60 + sec
            return float(parts[0])            # SS.sss
        except Exception:
            return None

    def _split_dc(s: str):
        # En años viejos casi nunca hay 'driver_code' (ABC). Igual soportamos el formato si aparece.
        s = (s or "").strip()
        m = re.match(r"^(.*?)[\s]*([A-Z]{3})$", s)
        return ((m.group(1) or s).strip(), m.group(2)) if m else (s, None)

    # Leer tablas (robusto)
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

    # Detectar la tabla de "overall qualifying"
    df = None
    for t in tables:
        cols_lc = _flatten_cols(t.columns)
        # Buscamos: pos/no/driver/team/time (laps y/o points pueden faltar)
        has_pos    = any(c in {"pos.", "position"} for c in cols_lc)
        has_no     = any(c in {"no.", "car no.", "number"} for c in cols_lc)
        has_driver = any(c == "driver" for c in cols_lc)
        has_team   = any(c == "team" for c in cols_lc)
        has_time   = any(c in {"time", "best time"} for c in cols_lc)
        if has_pos and has_driver and has_team and has_time:
            t.columns = cols_lc
            df = t.copy()
            break

    if df is None:
        return pd.DataFrame()

    # Renombres flexibles
    rename_map = {
        "pos.": "position",
        "position": "position",
        "no.": "car_number",
        "car no.": "car_number",
        "number": "car_number",
        "driver": "driver",
        "team": "team",
        "time": "time",
        "best time": "time",
        "laps": "laps",
        "pts.": "points",
        "points": "points",
    }
    for k, v in rename_map.items():
        if k in df.columns:
            df.rename(columns={k: v}, inplace=True)

    # Columnas opcionales que podrían faltar
    for c in ["car_number", "laps", "points"]:
        if c not in df.columns:
            df[c] = None

    # Driver name / code
    if "driver" in df.columns:
        df["driver_name"], df["driver_code"] = zip(*df["driver"].astype(str).map(_split_dc))

    # Casts básicos
    for c in ["position", "car_number", "laps", "points"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Tiempo en segundos
    df["time_s"] = df["time"].apply(_time_to_seconds) if "time" in df.columns else None

    # Metadatos
    df["source_url"]     = url
    df["event_title"]    = title_text
    df["scraped_at_utc"] = datetime.utcnow().isoformat(timespec="seconds")

    return df

# --- Qualifying antiguo (overall) para toda la temporada cargada en races_pdf ---
all_oldq_pdf, oldq_errors = [], []

for race_url in races_pdf["url"].tolist():
    oq_url = with_suffix(race_url, "qualifying/0")   # <- sufijo con /0 (siempre es 0)
    try:
        oq_df = parse_old_qualifying(oq_url)
        if oq_df.empty:
            continue
        m = re.search(r"/races/(\d+)/([^/]+)/", race_url)
        oq_df["race_id"] = int(m.group(1)) if m else None
        oq_df["gp_slug"] = m.group(2) if m else None
        oq_df["season"]  = Prm_Season_Year
        all_oldq_pdf.append(oq_df)
    except Exception as e:
        oldq_errors.append({"url": oq_url, "error": str(e)})

old_qualifying_pdf = pd.concat(all_oldq_pdf, ignore_index=True) if all_oldq_pdf else pd.DataFrame()

# Asegurar columnas mínimas y orden para Spark
min_cols_oldq = [
    "position","car_number","driver","team","time","time_s","laps","points",
    "driver_name","driver_code","source_url","event_title","scraped_at_utc",
    "race_id","gp_slug","season"
]
if old_qualifying_pdf.empty:
    old_qualifying_pdf = pd.DataFrame(columns=min_cols_oldq)
else:
    for c in min_cols_oldq:
        if c not in old_qualifying_pdf.columns:
            old_qualifying_pdf[c] = None
    old_qualifying_pdf = old_qualifying_pdf[min_cols_oldq]

# → Spark (usa tu helper to_spark ya definido)
spark_old_qualifying = to_spark(old_qualifying_pdf, SCHEMA_OLD_QUALI)

#display(spark_old_qualifying.limit(50))
spark_old_qualifying.write.format("delta").mode("append").option("overwriteSchema","true").save(tgt_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
