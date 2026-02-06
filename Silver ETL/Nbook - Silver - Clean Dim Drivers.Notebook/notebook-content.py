# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "895b8adc-96b6-4c1a-ae38-eba7a2566e4e",
# META       "default_lakehouse_name": "Lake_F1_Silver",
# META       "default_lakehouse_workspace_id": "265d92f7-0795-4b79-b272-0e06281c49c5",
# META       "known_lakehouses": [
# META         {
# META           "id": "895b8adc-96b6-4c1a-ae38-eba7a2566e4e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import pyspark.sql.functions as F
from pyspark.sql.types import *

SCHEMA_DIM_DRIVERS = StructType([
    StructField("Driver_Name", StringType(), True),
    StructField("Nationality", StringType(), True),
    StructField("LastSeason", IntegerType(), True),
    StructField("FirstSeason", IntegerType(), True),
    StructField("DOB", DateType(), True),
    StructField("Img_Url", StringType(), True),
])

Url_Default = "https://bidatasolutionsni-my.sharepoint.com/:i:/g/personal/rampie_bidatasolutionsni_onmicrosoft_com/IQAMN2z3fVUvQ4ezSYMpXWhHAVyHNlOJd_etC_0TlojYbok?download=1"

df_src = spark.sql( "SELECT * FROM Lake_F1_Silver.staging.Dim_Drivers" )

mappings = [
    ("driver_name_clean", "Driver_Name"),
    ("Nationality", "Nationality"),
    ("LastSeason", "LastSeason"),
    ("FirstSeason", "FirstSeason"),
    ("DOB", "DOB"),
    ("Img_Url","Img_Url")
]

df_out = df_src.select([F.col(src).alias(dst) for src, dst in mappings])

target_cols = [ F.col(f.name).cast(f.dataType).alias(f.name) for f in SCHEMA_DIM_DRIVERS.fields ]
df_final_src = df_out.select(*target_cols)

df_Final = df_final_src.withColumn(
    "URL_Download",
    F.when(F.col("Img_Url").isNull(), F.lit(Url_Default)) \
     .otherwise(F.regexp_replace(F.col("Img_Url"), r"\?.*", "?download=1"))
)

#display(df_Final)
df_Final.write.format("delta").mode("overwrite").option("overwriteschema","true").saveAsTable("Lake_F1_Silver.clean.Dim_Drivers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
