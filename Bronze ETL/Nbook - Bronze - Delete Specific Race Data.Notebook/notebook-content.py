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

# PARAMETERS CELL ********************

Race_Id = 1272

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

query = f"""
    DELETE FROM Lake_F1_Bronze.dbo.Season_Races WHERE race_id = {Race_Id}
    DELETE FROM Lake_F1_Bronze.dbo.Old_Qualifying WHERE race_id = {Race_Id}
"""

spark.sql(query)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC DELETE FROM Lake_F1_Bronze.dbo.Old_Qualifying WHERE race_id = 1272;
# MAGIC DELETE FROM Lake_F1_Bronze.dbo.Pit_stop WHERE race_id = 1272;
# MAGIC DELETE FROM Lake_F1_Bronze.dbo.Practice_1 WHERE race_id = 1272;
# MAGIC DELETE FROM Lake_F1_Bronze.dbo.Practice_2 WHERE race_id = 1272;
# MAGIC DELETE FROM Lake_F1_Bronze.dbo.Practice_3 WHERE race_id = 1272;
# MAGIC DELETE FROM Lake_F1_Bronze.dbo.Qualifying WHERE race_id = 1272;
# MAGIC DELETE FROM Lake_F1_Bronze.dbo.Races_Results WHERE race_id = 1272;
# MAGIC DELETE FROM Lake_F1_Bronze.dbo.Sprint_Qualifying WHERE race_id = 1272;
# MAGIC DELETE FROM Lake_F1_Bronze.dbo.Sprint_Race_Results WHERE race_id = 1272;
# MAGIC DELETE FROM Lake_F1_Bronze.dbo.Sprint_Starting_Grid WHERE race_id = 1272;
# MAGIC DELETE FROM Lake_F1_Bronze.dbo.Starting_Grid WHERE race_id = 1272;
# MAGIC DELETE FROM Lake_F1_Bronze.dbo.Fast_Laps WHERE race_id = 1272

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC DELETE FROM Lake_F1_Bronze.dbo.All_Races WHERE race_id = 1276

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
