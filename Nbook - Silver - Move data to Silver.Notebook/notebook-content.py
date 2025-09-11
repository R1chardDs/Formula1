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
# META         },
# META         {
# META           "id": "cbf6d77c-d47c-4d87-97c6-3980282da182"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

bronze_table = "Races_Results"
src_zone = "Test"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pyspark.sql.functions as F
import delta.tables

src_workspace = "F1_Lab"
src_lakehouse = "Lake_F1_" + src_zone
src_schema = "dbo"
src_table_silver = bronze_table
src_table_races = "All_Races"

tgt_lakehouse = "Lake_F1_Silver"

src_path_silver = "abfss://" + src_workspace + "@onelake.dfs.fabric.microsoft.com/" + src_lakehouse + ".Lakehouse/Tables/" + src_schema + "/" + src_table_silver
src_path_races = "abfss://" + src_workspace + "@onelake.dfs.fabric.microsoft.com/" + src_lakehouse + ".Lakehouse/Tables/" + src_schema + "/" + src_table_races

df_src_silver = spark.read.format("delta").load(src_path_silver)
df_src_races = spark.read.format("delta").load(src_path_races)

tgt_path_silver = "abfss://" + src_workspace + "@onelake.dfs.fabric.microsoft.com/" + tgt_lakehouse + ".Lakehouse/Tables/" + src_schema + "/" + src_table_silver



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_new_races_id = df_src_races.select(F.col("race_id").alias("race_id_join")).filter(F.col("silver") == "N" ).distinct()

df_new_silverdata = (
    df_src_silver.alias("s")
    .join(df_new_races_id.alias("b"), df_new_races_id.race_id_join == df_src_silver.race_id, how = "left" )
    .drop("race_id_join")
)

display(df_new_silverdata)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
