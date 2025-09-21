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

# MAGIC %%sql
# MAGIC 
# MAGIC DESCRIBE HISTORY Lake_F1_Bronze.dbo.Races_Results

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import *

delta_path = "abfss://F1_Lab@onelake.dfs.fabric.microsoft.com/Lake_F1_Bronze.Lakehouse/Tables/dbo/Season_Races"

bk_Season_Races = spark.read.format("delta").option("versionAsOf", 75).load(delta_path)

bk_Season_Races.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(delta_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
