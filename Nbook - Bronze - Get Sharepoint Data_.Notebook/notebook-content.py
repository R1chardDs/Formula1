# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ##### _**Importaciones y Declaraciones Generales**_

# PARAMETERS CELL ********************

target_zone = "Bronze"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import io, msal, requests
import pandas as pd
import delta.tables

from urllib.parse import quote
from datetime import date
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DateType
)

TENANT_ID   = ""
CLIENT_ID   = ""
CLIENT_SEC  = ""
HOSTNAME    = "bidatasolutionsni.sharepoint.com"
SITE_PATH   = "sites/F1"

target_workspace = "F1_Lab"
target_lakehouse = "Lake_F1_" + target_zone
target_schema = "dbo"

# 1) Token app-only (Microsoft Graph)
authority = f"https://login.microsoftonline.com/{TENANT_ID}"
app = msal.ConfidentialClientApplication(CLIENT_ID, authority=authority,
                                         client_credential=CLIENT_SEC)
tok = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
if "access_token" not in tok:
    raise RuntimeError(tok.get("error_description"))
hdr = {"Authorization": f"Bearer {tok['access_token']}"}

# 2) Resolver siteId por path (o guarda el id compuesto si ya lo tenés)
url_site = f"https://graph.microsoft.com/v1.0/sites/{HOSTNAME}:/{SITE_PATH}?$select=id,webUrl"
site = requests.get(url_site, headers=hdr); site.raise_for_status()
site_id = site.json()["id"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### _**Dim_Drivers**_

# CELL ********************

# Imports


# 1) LEE la hoja "Hoja_Oficial"

File_Path = "/Dim_Drivers.xlsx"   # raíz de Documentos (NO pongas 'Shared Documents')
Target_Sheet = "Dim_Drivers_Oficial"

file_path_enc = quote(File_Path, safe="/")
url_file = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:{file_path_enc}:/content"
resp = requests.get(url_file, headers=hdr); resp.raise_for_status()

xls = pd.ExcelFile(io.BytesIO(resp.content), engine="openpyxl")
# resolver hoja con tolerancia (case/espacios)
sheet_lookup = {s.lower().strip(): s for s in xls.sheet_names}
resolved = sheet_lookup.get(Target_Sheet.lower().strip())
if not resolved:
    raise ValueError(f"La hoja '{Target_Sheet}' no existe. Hojas: {xls.sheet_names}")

pdf = pd.read_excel(xls, sheet_name=resolved)


# 2) NORMALIZA tipos para Spark (DOB y columnas int)
# Renombra columnas por si vienen con espacios o mayúsculas
rename_map = {
    "driver_name": "driver_name",
    "LastSeason": "LastSeason",
    "FirstSeason": "FirstSeason",
    "Nationality": "Nationality",
    "DOB": "DOB",
    "AGE_FS": "AGE_FS",
    "AGE_LS": "AGE_LS",
    "driver_name_clean": "driver_name_clean",
}
pdf = pdf.rename(columns=rename_map)

# Asegura que existan (si faltan, créalas vacías)
for c in rename_map.values():
    if c not in pdf.columns:
        pdf[c] = None

# DOB -> fecha (con arreglo de siglo si viniera mal interpretada)
dob = pd.to_datetime(pdf["DOB"], errors="coerce", dayfirst=True)
cur_year = pd.Timestamp.today().year
mask_century = dob.dt.year > (cur_year - 10)   # e.g., 2017 -> 1917
dob.loc[mask_century] = dob.loc[mask_century] - pd.DateOffset(years=100)
pdf["DOB"] = dob.dt.date  # objeto date (para encajar con DateType)

# Columnas numéricas -> intentar entero
for col in ["LastSeason", "FirstSeason", "AGE_FS", "AGE_LS"]:
    pdf[col] = pd.to_numeric(pdf[col], errors="coerce").astype("Int64")  # permite nulos

# Strings explícitos (evita mezclas raras)
for col in ["driver_name", "Nationality", "driver_name_clean"]:
    pdf[col] = pdf[col].astype("string")

# 3) CREA Spark DF e impone el SCHEMA final
# Primero deja que Spark lo infiera (Arrow rápido), luego castea a tu schema objetivo.
sdf0 = spark.createDataFrame(pdf)

sdf = (
    sdf0.select(
        F.col("driver_name").cast("string").alias("driver_name"),
        F.col("LastSeason").cast("int").alias("LastSeason"),
        F.col("FirstSeason").cast("int").alias("FirstSeason"),
        F.col("Nationality").cast("string").alias("Nationality"),
        F.to_date(F.col("DOB")).alias("DOB"),
        F.col("AGE_FS").cast("int").alias("AGE_FS"),
        F.col("AGE_LS").cast("int").alias("AGE_LS"),
        F.col("driver_name_clean").cast("string").alias("driver_name_clean"),
    )
)

target_table_DimDrivers = "Dim_Drivers"
tgt_path_DimDrivers = "abfss://" + target_workspace + "@onelake.dfs.fabric.microsoft.com/" + target_lakehouse + ".Lakehouse/Tables/" + target_schema + "/" + target_table_DimDrivers

#display(sdf)
sdf.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(tgt_path_DimDrivers)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### _**European_GP_WithKey**_

# CELL ********************

# --- Parámetros ---
File_Path_Europa = "/European_GP_WithKey.xlsx"     # ruta dentro del drive (raíz de Documentos)
Target_Sheet        = "Sheet1"           # nombre de la hoja que querés leer

# 3) Descargar el XLSX (encodear espacios en la ruta)
file_path_enc = quote(File_Path_Europa, safe="/")
url_file = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:{file_path_enc}:/content"
resp = requests.get(url_file, headers=hdr); resp.raise_for_status()

# 4) Abrir el workbook en memoria y resolver la hoja (robusto a may/min y espacios)
#    Si no tenés openpyxl instalado en el entorno, en Fabric normalmente ya viene.
xls = pd.ExcelFile(io.BytesIO(resp.content), engine="openpyxl")
sheet_lookup = {s.lower().strip(): s for s in xls.sheet_names}

resolved_sheet = sheet_lookup.get(Target_Sheet.lower().strip())
if not resolved_sheet:
    raise ValueError(
        f"La hoja '{Target_Sheet}' no existe. "
        f"Hojas disponibles: {xls.sheet_names}"
    )

# 5) Leer SOLO esa hoja a pandas
pdf_EuropaTraks = pd.read_excel(xls, sheet_name=resolved_sheet)

# 6) (Opcional) Pasar a Spark DF
sdf_EuropaTraks = spark.createDataFrame(pdf_EuropaTraks)

target_table_EuropaTraks = "Dim_Tracks_Europ"
tgt_path_EuropaTraks = "abfss://" + target_workspace + "@onelake.dfs.fabric.microsoft.com/" + target_lakehouse + ".Lakehouse/Tables/" + target_schema + "/" + target_table_EuropaTraks

#display(sdf_EuropaTraks)
sdf_EuropaTraks.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(tgt_path_EuropaTraks)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### _**Dim_Traks**_

# CELL ********************

# --- Parámetros ---
File_Path_Tracks = "/Dim_Tracks_WithKey.xlsx"     # ruta dentro del drive (raíz de Documentos)
Target_Sheet        = "Sheet1"           # nombre de la hoja que querés leer

# 3) Descargar el XLSX (encodear espacios en la ruta)
file_path_enc = quote(File_Path_Tracks, safe="/")
url_file = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:{file_path_enc}:/content"
resp = requests.get(url_file, headers=hdr); resp.raise_for_status()

# 4) Abrir el workbook en memoria y resolver la hoja (robusto a may/min y espacios)
#    Si no tenés openpyxl instalado en el entorno, en Fabric normalmente ya viene.
xls = pd.ExcelFile(io.BytesIO(resp.content), engine="openpyxl")
sheet_lookup = {s.lower().strip(): s for s in xls.sheet_names}

resolved_sheet = sheet_lookup.get(Target_Sheet.lower().strip())
if not resolved_sheet:
    raise ValueError(
        f"La hoja '{Target_Sheet}' no existe. "
        f"Hojas disponibles: {xls.sheet_names}"
    )

# 5) Leer SOLO esa hoja a pandas
pdf_Traks = pd.read_excel(xls, sheet_name=resolved_sheet)

# 6) (Opcional) Pasar a Spark DF
sdf_Traks = spark.createDataFrame(pdf_Traks)

target_table_DimTraks = "Dim_Tracks"
tgt_path_DimTraks = "abfss://" + target_workspace + "@onelake.dfs.fabric.microsoft.com/" + target_lakehouse + ".Lakehouse/Tables/" + target_schema + "/" + target_table_DimTraks

#display(sdf_Traks)
sdf_Traks.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(tgt_path_DimTraks)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### _**All_Races_ReviewPerSeason_WithKey**_

# CELL ********************

# --- Parámetros ---
File_Path_ReviewRaces = "/All_Races_ReviewPerSeason_WithKey.xlsx"     # ruta dentro del drive (raíz de Documentos)
Target_Sheet        = "Sheet1"           # nombre de la hoja que querés leer

# 3) Descargar el XLSX (encodear espacios en la ruta)
file_path_enc = quote(File_Path_ReviewRaces, safe="/")
url_file = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:{file_path_enc}:/content"
resp = requests.get(url_file, headers=hdr); resp.raise_for_status()

# 4) Abrir el workbook en memoria y resolver la hoja (robusto a may/min y espacios)
#    Si no tenés openpyxl instalado en el entorno, en Fabric normalmente ya viene.
xls = pd.ExcelFile(io.BytesIO(resp.content), engine="openpyxl")
sheet_lookup = {s.lower().strip(): s for s in xls.sheet_names}

resolved_sheet = sheet_lookup.get(Target_Sheet.lower().strip())
if not resolved_sheet:
    raise ValueError(
        f"La hoja '{Target_Sheet}' no existe. "
        f"Hojas disponibles: {xls.sheet_names}"
    )

# 5) Leer SOLO esa hoja a pandas
pdf_ReviewRaces = pd.read_excel(xls, sheet_name=resolved_sheet)

# 6) (Opcional) Pasar a Spark DF

sdf_ReviewRaces = spark.createDataFrame(pdf_ReviewRaces)

target_table_ReviewRaces = "Reviewed_Races_Per_Season"
tgt_path_ReviewRaces = "abfss://" + target_workspace + "@onelake.dfs.fabric.microsoft.com/" + target_lakehouse + ".Lakehouse/Tables/" + target_schema + "/" + target_table_ReviewRaces

#display(sdf_ReviewRaces)
sdf_ReviewRaces.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(tgt_path_ReviewRaces)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
