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

target_table_race = "Sprint_Race_Results"
target_table_qualy = "Sprint_Qualifying"
target_table_Grid = "Sprint_Starting_Grid"

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

SCHEMA_RESULTS = StructType([
    StructField("position", IntegerType(), True),
    StructField("car_number", IntegerType(), True),
    StructField("driver", StringType(), True),
    StructField("team", StringType(), True),
    StructField("laps", IntegerType(), True),
    StructField("time_or_retired", StringType(), True),
    StructField("points", IntegerType(), True),
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
    StructField("gp_slug", StringType(), True),
])

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

target_lakehouse = "Lake_F1_" + target_zone
target_workspace = "F1_Lab"
target_schema = "dbo"

tgt_path_race = "abfss://" + target_workspace + "@onelake.dfs.fabric.microsoft.com/" + target_lakehouse + ".Lakehouse/Tables/" + target_schema + "/" + target_table_race
tgt_path_qualy = "abfss://" + target_workspace + "@onelake.dfs.fabric.microsoft.com/" + target_lakehouse + ".Lakehouse/Tables/" + target_schema + "/" + target_table_qualy
tgt_path_grid = "abfss://" + target_workspace + "@onelake.dfs.fabric.microsoft.com/" + target_lakehouse + ".Lakehouse/Tables/" + target_schema + "/" + target_table_Grid

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

races_sprint = (
    races_pdf.loc[
        races_pdf["sprint"].astype(str).str.upper().isin(["Y", "S"]) & races_pdf["url"].notna()
    ].copy()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===== Ejecutar SÓLO si la temporada tiene Sprint Qualifying en F1.com (>= 2023) =====
if int(Prm_Season_Year) >= 2023:
    # 2) Iterar y parsear Sprint Qualifying (misma estructura que qualifying)
    all_sq_pdf, sq_errors = [], []

    for race_url in races_sprint["url"].tolist():  # asume races_sprint ya filtrado por sprint == Y/S
        sq_url = with_suffix(race_url, "sprint-qualifying")
        try:
            sq_df = parse_qualifying(sq_url)  # reutiliza parser de qualifying
            if sq_df.empty:
                continue
            m = re.search(r"/races/(\d+)/([^/]+)/", race_url)
            sq_df["race_id"]    = int(m.group(1)) if m else None
            sq_df["gp_slug"]    = m.group(2) if m else None
            sq_df["season"]     = Prm_Season_Year
            sq_df["source_url"] = sq_url
            all_sq_pdf.append(sq_df)
        except Exception as e:
            sq_errors.append({"url": sq_url, "error": str(e)})

    sprint_qualifying_pdf = pd.concat(all_sq_pdf, ignore_index=True) if all_sq_pdf else pd.DataFrame()

    # 3) A Spark con schema consistente y escritura
    spark_sprint_qualifying = to_spark(sprint_qualifying_pdf, SCHEMA_QUALI)

    #display(spark_sprint_qualifying)
    spark_sprint_qualifying.write.format("delta").mode("append").option("overwriteSchema","true").save(tgt_path_qualy)

else:
    # Temporadas < 2023: F1.com no publica Sprint Qualifying. Definimos DF vacío con el schema esperado.
    min_cols_sq = [
        "position","car_number","driver","team","q1","q2","q3","laps",
        "Q1_Segs","Q2_Segs","Q3_Segs",
        "driver_name","driver_code","source_url","event_title","scraped_at_utc",
        "race_id","gp_slug","season"
    ]
    sprint_qualifying_pdf = pd.DataFrame(columns=min_cols_sq)
    spark_sprint_qualifying = to_spark(sprint_qualifying_pdf, SCHEMA_QUALI)
    print(f"Temporada {Prm_Season_Year}: no se extrae Sprint Qualifying (solo disponible desde 2023).")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===================== SPRINT GRID =====================
# Requiere: races_pdf, Prm_Season_Year, with_suffix, to_spark, SCHEMA_GRID ya definidos.

def parse_sprint_grid(url: str):
    html = fns.fetch_html(url)
    soup = BeautifulSoup(html, "lxml")

    title = soup.find(["h1","h2"])
    title_text = title.get_text(" ", strip=True) if title else None

    # leer tablas (future-proof con read_html_tables)
    try:
        tables = read_html_tables(html)
    except ValueError:
        tables = []

    df = None
    for t in tables:
        cols = [str(c).strip().lower() for c in t.columns]
        has_driver = any(c == "driver" for c in cols)
        has_pos    = any(c in {"pos.", "position", "grid"} for c in cols)
        has_qcols  = any(c in {"q1","q2","q3"} for c in cols)  # excluir tablas de quali
        if has_driver and has_pos and not has_qcols:
            df = t.copy()
            df.columns = cols
            break

    if df is None:
        return pd.DataFrame()

    # normalizar columnas (mismo schema que starting-grid: usa time_or_note)
    rename_map = {
        "pos.": "grid_position",
        "position": "grid_position",
        "grid": "grid_position",
        "no.": "car_number",
        "driver": "driver",
        "team": "team",
        "time": "time_or_note",          # sprint grid muestra 'TIME'
        "pts.": "points",
    }
    for k, v in rename_map.items():
        if k in df.columns:
            df.rename(columns={k: v}, inplace=True)

    # driver_name / driver_code (acepta 'Nombre ApellidoABC')
    def _split_dc(s: str):
        s = (s or "").strip()
        m = re.match(r"^(.*?)[\s]*([A-Z]{3})$", s)
        return ((m.group(1) or s).strip(), m.group(2)) if m else (s, None)

    if "driver" in df.columns:
        df["driver_name"], df["driver_code"] = zip(*df["driver"].astype(str).map(_split_dc))

    # casts básicos
    for c in ["grid_position","car_number","points"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df["source_url"] = url
    df["event_title"] = title_text
    df["scraped_at_utc"] = datetime.utcnow().isoformat(timespec="seconds")
    return df

# --- Iterar y armar Sprint Grid ---
all_sgrid_pdf, sgrid_errors = [], []

for race_url in races_sprint["url"].tolist():
    sg_url = with_suffix(race_url, "sprint-grid")
    try:
        sg_df = parse_sprint_grid(sg_url)
        if sg_df.empty:
            continue
        m = re.search(r"/races/(\d+)/([^/]+)/", race_url)
        sg_df["race_id"] = int(m.group(1)) if m else None
        sg_df["gp_slug"] = m.group(2) if m else None
        sg_df["season"]  = Prm_Season_Year
        all_sgrid_pdf.append(sg_df)
    except Exception as e:
        sgrid_errors.append({"url": sg_url, "error": str(e)})

sprint_grid_pdf = pd.concat(all_sgrid_pdf, ignore_index=True) if all_sgrid_pdf else pd.DataFrame()

# --- A Spark con el mismo esquema que starting-grid ---
spark_sprint_grid = to_spark(sprint_grid_pdf, SCHEMA_GRID)

#display(spark_sprint_grid)
spark_sprint_grid.write.format("delta").mode("append").option("overwriteSchema","true").save(tgt_path_grid)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#RESULTS OF RACES

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
# 2) Iterar y parsear sprint-results (misma estructura que race-result)
all_sr_pdf, sr_errors = [], []

for race_url in races_sprint["url"].tolist():
    sr_url = with_suffix(race_url, "sprint-results")
    try:
        sr_df = parse_race_result(sr_url)   # reutiliza tu parser de race result
        if sr_df.empty:
            continue
        m = re.search(r"/races/(\d+)/([^/]+)/", race_url)
        sr_df["race_id"]    = int(m.group(1)) if m else None
        sr_df["gp_slug"]    = m.group(2) if m else None
        sr_df["season"]     = Prm_Season_Year
        sr_df["source_url"] = sr_url  # asegura el URL de sprint
        all_sr_pdf.append(sr_df)
    except Exception as e:
        sr_errors.append({"url": sr_url, "error": str(e)})

sprint_results_pdf = pd.concat(all_sr_pdf, ignore_index=True) if all_sr_pdf else pd.DataFrame()

# 3) A Spark con el mismo schema de resultados
spark_sprint_results = to_spark(sprint_results_pdf, SCHEMA_RESULTS)

#display(spark_sprint_results)
spark_sprint_results.write.format("delta").mode("append").option("overwriteSchema","true").save(tgt_path_race)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
