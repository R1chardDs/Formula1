# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from pyspark.sql import functions as F
from delta.tables import *
import datetime as dt

target_table = "F1_Years"
target_lakehouse = "Lake_F1_Bronze"
target_workspace = "F1_Lab"
target_schema = "dbo"

_DateToday = dt.date.today()
_CurrentSeason = _DateToday.year

tgt_path = "abfss://" + target_workspace + "@onelake.dfs.fabric.microsoft.com/" + target_lakehouse + ".Lakehouse/Tables/" + target_schema + "/" + target_table

# Años 1950..2025 (76 filas)
df_years = spark.range(1950, _CurrentSeason + 1).select(F.col("id").cast("int").alias("year"))

df_years = df_years.withColumn("IsCurrentSeason", F.when( F.col("year") == _CurrentSeason, "Y" ).otherwise("N") ).orderBy(F.desc("year"))

df_years.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(tgt_path)

#display(df_years)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
