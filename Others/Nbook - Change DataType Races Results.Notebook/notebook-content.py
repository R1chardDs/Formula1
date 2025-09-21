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
# META     },
# META     "environment": {
# META       "environmentId": "e2c3c98f-b0bc-9a15-41d8-00e395eecd7c",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE TABLE Races_Results_BK
# MAGIC AS
# MAGIC SELECT * FROM Races_Results

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set('spark.sql.caseSensitive', True)

print(spark.conf.get('spark.sql.caseSensitive'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)

# 1) Definir el esquema correcto
SCHEMA_RESULTS = StructType([
    StructField("position", IntegerType(), True),
    StructField("car_number", IntegerType(), True),
    StructField("driver", StringType(), True),
    StructField("team", StringType(), True),
    StructField("laps", IntegerType(), True),
    StructField("time_or_retired", StringType(), True),
    StructField("points", DoubleType(), True),   # ← ahora Double
    StructField("driver_name", StringType(), True),
    StructField("driver_code", StringType(), True),
    StructField("status", StringType(), True),
    StructField("race_time_s", DoubleType(), True),
    StructField("gap_to_winner_s", DoubleType(), True),
    StructField("source_url", StringType(), True),
    StructField("event_title", StringType(), True),
    StructField("scraped_at_utc", StringType(), True),
    StructField("race_id", IntegerType(), True),
    StructField("season", IntegerType(), True),
    StructField("gp_slug", StringType(), True),
])

# 2) Crear DataFrame vacío con ese esquema
empty_df = spark.createDataFrame([], SCHEMA_RESULTS)

# 3) Guardar como tabla vacía en el Lakehouse (ajusta ruta/nombre según corresponda)
target_path = "abfss://F1_Lab@onelake.dfs.fabric.microsoft.com/Lake_F1_Bronze.Lakehouse/Tables/dbo/Races_Results"

(empty_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(target_path)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO Races_Results
# MAGIC SELECT * FROM Races_Results_BK

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
