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

target_table = "Season_Races"

SCHEMA_RACES = StructType([
    StructField("season", IntegerType(), True),
    StructField("gp", StringType(), True),
    StructField("date_text", StringType(), True),
    StructField("url", StringType(), True),
    StructField("sprint", StringType(), True),
    StructField("scraped_at_utc", StringType(), True),
    StructField("silver", StringType(), True),
    StructField("bronze", StringType(), True),
    StructField("race_id", IntegerType(), True)
])

RACE_ID_PAT = r"/races/(\d+)(?:/|$)"

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


def list_race_links(season: int = 2025):
    base_url  = "https://www.formula1.com"
    index_url = f"{base_url}/en/results/{season}/races"
    html = fns.fetch_html(index_url)
    soup = BeautifulSoup(html, "lxml")

    # localizar la tabla GRAND PRIX / DATE
    target_table = None
    for table in soup.find_all("table"):
        headers = [th.get_text(strip=True).upper() for th in table.find_all("th")]
        if "GRAND PRIX" in headers and "DATE" in headers:
            target_table = table
            break
    if target_table is None:
        return pd.DataFrame(columns=["season","gp","date_text","url","sprint","scraped_at_utc"])

    rows = target_table.find("tbody").find_all("tr")
    data = []

    # ---- detección robusta de sprint ----
    def _has_sprint(race_result_url: str) -> bool:
        if not race_result_url:
            return False
        base = race_result_url.rsplit("/", 1)[0]  # quita 'race-result'
        for suffix in ("sprint-results", "sprint-qualifying", "sprint-grid"):
            test_url = f"{base}/{suffix}"
            try:
                h = fns.fetch_html(test_url)
            except requests.RequestException:
                continue

            # Si la página dice "No results available", NO es sprint
            if re.search(r"no results available", h, flags=re.I):
                continue

            # Busca tablas reales de resultados: deben tener 'POS.' y 'DRIVER'
            try:
                tables = read_html_tables(h)
            except ValueError:
                tables = []

            for t in tables:
                cols_lc = [str(c).strip().lower() for c in t.columns]
                has_pos = "pos." in cols_lc or "position" in cols_lc
                has_driver = any(c == "driver" for c in cols_lc)
                has_resultish = any(k in cols_lc for k in ["time / retired","q1","q2","q3","laps","pts.","grid"])
                if not (has_pos and has_driver and has_resultish):
                    continue

                # Debe haber al menos una fila con DRIVER no vacío
                driver_col = next((c for c in t.columns if str(c).strip().lower() == "driver"), None)
                if driver_col is not None and t.shape[0] > 0 and t[driver_col].dropna().astype(str).str.strip().ne("").any():
                    return True
        return False
    # -------------------------------------

    for tr in rows:
        tds = tr.find_all(["td", "th"])
        if not tds:
            continue

        gp_cell = tds[0]
        for img in gp_cell.find_all("img"):
            img.decompose()
        # elimina cualquier nodo de texto tipo "Flag of ..."
        for txt in gp_cell.find_all(string=lambda s: isinstance(s, str) and "Flag of " in s):
            txt.extract()
        gp_text = gp_cell.get_text(" ", strip=True)

        date_text = tds[1].get_text(" ", strip=True) if len(tds) > 1 else None

        # hrefs relativos -> absolutos
        url = None
        cand_as = gp_cell.find_all("a", href=True) + tr.find_all("a", href=True)
        for a in cand_as:
            abs_url = urljoin(index_url, a["href"])
            if re.search(rf"/en/results/{season}/races/\d+/.+?/race-result/?$", abs_url):
                url = abs_url
                break
        if url is None:
            for a in cand_as:
                abs_url = urljoin(index_url, a["href"])
                m = re.search(rf"/en/results/{season}/races/(\d+)/([^/]+)/?$", abs_url)
                if m:
                    url = abs_url.rstrip("/") + "/race-result"
                    break

        sprint_flag = "Y" if _has_sprint(url) else "N"

        data.append({
            "season": season,
            "gp": gp_text,
            "date_text": date_text,
            "url": url,
            "sprint": sprint_flag,
            "scraped_at_utc": datetime.utcnow().isoformat(timespec="seconds"),
            "silver": "",
            "bronze": ""
        })

    return pd.DataFrame(data)

races_pdf = list_race_links(Prm_Season_Year)           # pandas DF con season/gp/date_text/url
spark_races_ = to_spark(races_pdf, SCHEMA_RACES)

spark_races = (
    spark_races_
    .withColumn("race_id_str", F.regexp_extract(F.col("url"), RACE_ID_PAT, 1))
    .withColumn(
        "race_id",
        F.when(F.col("race_id_str") == "", F.lit(None)).otherwise(F.col("race_id_str").cast("int"))
    )
    .drop("race_id_str")
)

#display(spark_races)
spark_races.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(tgt_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
