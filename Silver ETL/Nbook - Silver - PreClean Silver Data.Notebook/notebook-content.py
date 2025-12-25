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
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **Races Results Scripts**

# CELL ********************

target_table = "Races_Results"
target_lakehouse = "Lake_F1_Silver"
target_workspace = "F1_Lab"
target_schema = "src"
tgt_path = "abfss://" + target_workspace + "@onelake.dfs.fabric.microsoft.com/" + target_lakehouse + ".Lakehouse/Tables/" + target_schema + "/" + target_table

df_RacesResults = spark.read.format("delta").load(tgt_path)
df_PreUpdate = df_RacesResults.filter( (F.col("status") == "Finished")  & (F.col("laps").isNull()) & (F.col("time_or_retired").isNull())  )

display(df_PreUpdate)

dt = DeltaTable.forPath(spark, tgt_path)

update_condition = (
    (F.col("status") == "Finished") &
    (F.col("laps").isNull()) &
    (F.col("time_or_retired").isNull())
)

dt.update(
    condition = update_condition,
    set       = {"time_or_retired": F.lit("SHC")}
)

df_RacesResults = spark.read.format("delta").load(tgt_path)
df_PostUpdate = df_RacesResults.filter( (F.col("status") == "Finished")  & (F.col("laps").isNull()) & (F.col("time_or_retired").isNull())  )

display(df_PostUpdate)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
