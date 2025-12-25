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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import *
import pyspark.sql.functions as F

target_table = "All_Races"
target_lakehouse = "Lake_F1_" + target_zone
target_workspace = "F1_Lab"
target_schema = "dbo"

tgt_path = "abfss://" + target_workspace + "@onelake.dfs.fabric.microsoft.com/" + target_lakehouse + ".Lakehouse/Tables/" + target_schema + "/" + target_table

src_table = "Season_Races"
src_workspace = target_workspace
src_lakehouse = target_lakehouse
src_shecma = target_schema

src_path = "abfss://" + src_workspace + "@onelake.dfs.fabric.microsoft.com/" + src_lakehouse + ".Lakehouse/Tables/" + src_shecma + "/" + src_table

df_season_races = spark.read.format("delta").load(src_path)
df_all_races = spark.read.format("delta").load(tgt_path)
df_all_races_to_eval = df_all_races.filter(F.col("season") == Prm_Season_Year )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_all_races_to_eval)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_bronze_status = (
    df_season_races.alias("s")
    .join(df_all_races_to_eval.alias("a"), (df_season_races.season == df_all_races_to_eval.season) & (df_season_races.gp == df_all_races_to_eval.gp) , how = "left")
    .select( "s.season", "s.gp", "s.date_text", "s.url", "s.sprint", "s.scraped_at_utc", "s.race_id", F.col("a.bronze").alias("_bronze") )
    .withColumns( {"silver": F.lit("N") , "bronze": F.when( F.col("_bronze") == "Y", "Y").otherwise("N") } )
)

df_season_races_new = df_bronze_status.select( "season", "gp", "date_text", "url", "sprint", "scraped_at_utc", "silver", "bronze", "race_id" )
df_new_races = df_season_races_new.filter(F.col("bronze") == "N" )
#display(df_season_races_new)
#display(df_new_races)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Ingestar df_season_races_new en Season_Races modo overwrite
# Ingestar df_new_races en All_Races modo append

df_season_races_new.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(src_path)
df_new_races.write.format("delta").mode("append").option("overwriteSchema","true").save(tgt_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
