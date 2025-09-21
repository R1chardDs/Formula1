# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "cbf6d77c-d47c-4d87-97c6-3980282da182",
# META       "default_lakehouse_name": "Lake_F1_Bronze",
# META       "default_lakehouse_workspace_id": "265d92f7-0795-4b79-b272-0e06281c49c5",
# META       "known_lakehouses": [
# META         {
# META           "id": "cbf6d77c-d47c-4d87-97c6-3980282da182"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit

target_table = "All_Races"
target_lakehouse = "Lake_F1_Bronze"
target_workspace = "F1_Lab"
target_schema = "dbo"
tgt_path = "abfss://" + target_workspace + "@onelake.dfs.fabric.microsoft.com/" + target_lakehouse + ".Lakehouse/Tables/" + target_schema + "/" + target_table

dt = DeltaTable.forPath(spark, tgt_path)

dt.update(
    set       = {"silver": lit("Y")}
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_table = "All_Races"
target_lakehouse = "Lake_F1_Silver"
target_workspace = "F1_Lab"
target_schema = "src"
tgt_path = "abfss://" + target_workspace + "@onelake.dfs.fabric.microsoft.com/" + target_lakehouse + ".Lakehouse/Tables/" + target_schema + "/" + target_table

dt = DeltaTable.forPath(spark, tgt_path)

dt.update(
    set       = {"silver": lit("Y")}
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
