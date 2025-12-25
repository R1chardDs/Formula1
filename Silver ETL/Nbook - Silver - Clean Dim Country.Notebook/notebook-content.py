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

from pyspark.sql import functions as F

df_src = spark.sql("SELECT * FROM Lake_F1_Silver.src.Dim_Country")

df_Final = df_src.withColumn(
    "URL_Download",
    F.regexp_replace(F.col("Sharepoint_Image_URL"), r"\?.*", "?download=1")
)

#display(df_Final)
df_Final.write.format("delta").mode("overwrite").option("overwriteschema","true").saveAsTable("clean.Dim_Country")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
