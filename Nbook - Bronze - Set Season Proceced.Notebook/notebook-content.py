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
target_zone = "Test"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit

target_table = "All_Races"
target_lakehouse = "Lake_F1_" + target_zone
target_workspace = "F1_Lab"
target_schema = "dbo"
tgt_path = "abfss://" + target_workspace + "@onelake.dfs.fabric.microsoft.com/" + target_lakehouse + ".Lakehouse/Tables/" + target_schema + "/" + target_table

dt = DeltaTable.forPath(spark, tgt_path)

dt.update(
    condition = col("season") == lit(Prm_Season_Year),
    set       = {"bronze": lit("Y")}
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
