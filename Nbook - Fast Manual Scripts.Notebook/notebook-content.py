# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from delta.tables import *

target_table = "Starting_Grid"
target_lakehouse = "Lake_F1_Test"
target_workspace = "F1_Lab"
target_schema = "dbo"

tgt_path = "abfss://" + target_workspace + "@onelake.dfs.fabric.microsoft.com/" + target_lakehouse + ".Lakehouse/Tables/" + target_schema + "/" + target_table

tgt_path_ = DeltaTable.forPath(spark,tgt_path)

print(tgt_path)
tgt_path_.delete()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import *

target_table = "Qualifying"
target_lakehouse = "Lake_F1_Test"
target_workspace = "F1_Lab"
target_schema = "dbo"


tgt_path = "abfss://" + target_workspace + "@onelake.dfs.fabric.microsoft.com/" + target_lakehouse + ".Lakehouse/Tables/" + target_schema + "/" + target_table

tgt_path_ = DeltaTable.forPath(spark,tgt_path)

print(tgt_path)
tgt_path_.delete()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import *
from pyspark.sql import functions as F

target_zone = "Bronze"
target_table = "All_Races"
target_lakehouse = "Lake_F1_" + target_zone
target_workspace = "F1_Lab"
target_schema = "dbo"

src_table = "Season_Races"
src_workspace = target_workspace
src_lakehouse = target_lakehouse
src_shecma = target_schema

src_path = "abfss://" + src_workspace + "@onelake.dfs.fabric.microsoft.com/" + src_lakehouse + ".Lakehouse/Tables/" + src_shecma + "/" + src_table

tgt_path = "abfss://" + target_workspace + "@onelake.dfs.fabric.microsoft.com/" + target_lakehouse + ".Lakehouse/Tables/" + target_schema + "/" + target_table

df_src = spark.read.format("delta").load(src_path)

df_target = df_src.withColumns({'silver' : F.lit("N"), "bronze" : F.lit("N")})

df_src = df_src.withColumns({'silver' : F.lit(""), "bronze" : F.lit("")}).filter(F.col("season") == 2025)

#display(df_src)
#display(df_target)

df_src.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(src_path)
#df_target.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(tgt_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ctx = notebookutils.runtime.context
user_name = ctx['userName']
user_id   = ctx['userId']
is_pipe   = ctx['isForPipeline']

print(user_name, user_id, is_pipe)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Nombre (y opcionalmente el Id) del notebook actual
ctx = notebookutils.runtime.context
nb_name = ctx['currentNotebookName']
nb_id   = ctx['currentNotebookId']
print(nb_name, nb_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_zone = "Test"
target_table = "All_Races"
target_lakehouse = "Lake_F1_" + target_zone
target_workspace = "F1_Lab"
target_schema = "dbo"

src_table = "Season_Races"
src_workspace = target_workspace
src_lakehouse = target_lakehouse
src_shecma = target_schema

src_path = "abfss://" + src_workspace + "@onelake.dfs.fabric.microsoft.com/" + src_lakehouse + ".Lakehouse/Tables/" + src_shecma + "/" + src_table

tgt_path = "abfss://" + target_workspace + "@onelake.dfs.fabric.microsoft.com/" + target_lakehouse + ".Lakehouse/Tables/" + target_schema + "/" + target_table

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Coment        = "Creando tabla del Log"
Action        = "Creando tabla del Log"

from datetime import datetime
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, BooleanType
)

# --- 1) Variables ---
DestinationTable = ""
Target_path      = ""
Src_Path         = ""

ctx = notebookutils.runtime.context
nb_name = ctx['currentNotebookName']
nb_id   = ctx['currentNotebookId']
user_name = ctx['userName']
is_pipe   = ctx['isForPipeline']

Datetime_Star = datetime.now()   # fecha/hora actual
Datetime_end  = datetime.now()   # fecha/hora actual
User_Name     = user_name
IsPipeline    = bool(is_pipe)
NotebookName  = nb_name
NotebookId    = nb_id

# --- 2) DataFrame (una fila) ---
schema = StructType([
    StructField("Datetime_Star",     TimestampType(), False),
    StructField("Datetime_end",      TimestampType(), False),
    StructField("User_Name",         StringType(),   True),
    StructField("IsPipeline",        BooleanType(),  True),
    StructField("Coment",            StringType(),   True),
    StructField("Action",            StringType(),   True),
    StructField("NotebookName",      StringType(),   True),
    StructField("NotebookId",        StringType(),   True),
    StructField("DestinationTable",  StringType(),   True),
    StructField("Target_path",       StringType(),   True),
    StructField("Src_Path",          StringType(),   True),
])

row = [(
    Datetime_Star, Datetime_end, User_Name, IsPipeline, Coment, Action,
    NotebookName, NotebookId, DestinationTable, Target_path, Src_Path
)]

df_log = spark.createDataFrame(row, schema=schema)

log_table_name = target_zone + "_Log"
log_path = "abfss://" + target_workspace + "@onelake.dfs.fabric.microsoft.com/" + target_lakehouse + ".Lakehouse/Tables/" + target_schema + "/" + log_table_name

df_log.write.format("delta").mode("append").option("overwriteSchema","true").save(log_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import *
from pyspark.sql import functions as F

src_zone = "Bronze"
src_table = "All_Races"
src_workspace = "F1_Lab"
src_lakehouse = "Lake_F1_" + src_zone
src_shecma = "dbo"

target_zone = "Test"
target_table = src_table
target_lakehouse = "Lake_F1_" + target_zone
target_workspace = src_workspace
target_schema = src_shecma

src_path = "abfss://" + src_workspace + "@onelake.dfs.fabric.microsoft.com/" + src_lakehouse + ".Lakehouse/Tables/" + src_shecma + "/" + src_table

tgt_path = "abfss://" + target_workspace + "@onelake.dfs.fabric.microsoft.com/" + target_lakehouse + ".Lakehouse/Tables/" + target_schema + "/" + target_table

years_test = [2021, 2017, 2004, 1990, 1994, 1975, 2025]

df_src = spark.read.format("delta").load(src_path)
df_target = df_src.filter(F.col("season").isin(years_test))

#print(src_path, tgt_path)

#print(tgt_path)

df_target.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(tgt_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Prm_Season_Year = 2025
target_zone = "Test"

from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit

target_table = "F1_Years"
target_lakehouse = "Lake_F1_" + target_zone
target_workspace = "F1_Lab"
target_schema = "dbo"
tgt_path = "abfss://" + target_workspace + "@onelake.dfs.fabric.microsoft.com/" + target_lakehouse + ".Lakehouse/Tables/" + target_schema + "/" + target_table

dt = DeltaTable.forPath(spark, tgt_path)

dt.update(
    condition = col("year") != lit(Prm_Season_Year),
    set       = {"IsCurrentSeason": lit("N")}
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Prm_Season_Year = 2025
target_zone = "Bronze"

from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit

target_table = "F1_Years"
target_lakehouse = "Lake_F1_" + target_zone
target_workspace = "F1_Lab"
target_schema = "dbo"
tgt_path = "abfss://" + target_workspace + "@onelake.dfs.fabric.microsoft.com/" + target_lakehouse + ".Lakehouse/Tables/" + target_schema + "/" + target_table

dt = DeltaTable.forPath(spark, tgt_path)

dt.update(
    condition = col("year") != lit(Prm_Season_Year),
    set       = {"IsCurrentSeason": lit("N")}
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Prm_Season_Year = 2025
target_zone = "Test"

from delta.tables import DeltaTable
import pyspark.sql.functions as F

target_table = "All_Races"
target_lakehouse = "Lake_F1_" + target_zone
target_workspace = "F1_Lab"
target_schema = "dbo"
tgt_path = "abfss://" + target_workspace + "@onelake.dfs.fabric.microsoft.com/" + target_lakehouse + ".Lakehouse/Tables/" + target_schema + "/" + target_table

dt = DeltaTable.forPath(spark, tgt_path)

dt.delete( (F.col("season") == 2025) & (F.col("gp") == "Netherlands" ) )



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_table = "Season_Races"
src_zone = "Bronze"

import pyspark.sql.functions as F
import delta.tables

src_workspace = "F1_Lab"
src_lakehouse = "Lake_F1_" + src_zone
src_schema = "dbo"
src_table = bronze_table

src_path = "abfss://" + src_workspace + "@onelake.dfs.fabric.microsoft.com/" + src_lakehouse + ".Lakehouse/Tables/" + src_schema + "/" + src_table

df_src = spark.read.format("delta").load(src_path)

RACE_ID_PAT = r"/races/(\d+)(?:/|$)"

df_src_ = (
    df_src
    .withColumn("race_id_str", F.regexp_extract(F.col("url"), RACE_ID_PAT, 1))
    .withColumn(
        "race_id",
        F.when(F.col("race_id_str") == "", F.lit(None)).otherwise(F.col("race_id_str").cast("int"))
    )
    .drop("race_id_str")
)

df_src_.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(src_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
