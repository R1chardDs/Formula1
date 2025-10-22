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

# MAGIC %%sql
# MAGIC 
# MAGIC DELETE FROM clean.Dim_Drivers;
# MAGIC DELETE FROM clean.Dim_Races;
# MAGIC DELETE FROM clean.Dim_Tracks;
# MAGIC DELETE FROM clean.Fast_Laps;
# MAGIC DELETE FROM clean.Old_Qualifying;
# MAGIC DELETE FROM clean.Pit_Stops;
# MAGIC DELETE FROM clean.Practices;
# MAGIC DELETE FROM clean.Qualifying;
# MAGIC DELETE FROM clean.Races_Notes;
# MAGIC DELETE FROM clean.Races_Results;
# MAGIC DELETE FROM clean.Sprint_Qualifying;
# MAGIC DELETE FROM clean.Sprint_Races_Results;
# MAGIC DELETE FROM clean.Sprint_Starting_Grid;
# MAGIC DELETE FROM clean.Starting_Grid

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC DELETE FROM src.All_Races;
# MAGIC DELETE FROM src.Dim_Drivers;
# MAGIC DELETE FROM src.Dim_Tracks;
# MAGIC DELETE FROM src.Fast_Laps;
# MAGIC DELETE FROM src.Old_Qualifying;
# MAGIC DELETE FROM src.Pit_Stop;
# MAGIC DELETE FROM src.Practices;
# MAGIC DELETE FROM src.Qualifying;
# MAGIC DELETE FROM src.Races_Results;
# MAGIC DELETE FROM src.Sprint_Qualifying;
# MAGIC DELETE FROM src.Sprint_Race_Results;
# MAGIC DELETE FROM src.Sprint_Starting_Grid;
# MAGIC DELETE FROM src.Sprint_Starting_Grid;
# MAGIC DELETE FROM src.Starting_Grid

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC UPDATE Lake_F1_Bronze.dbo.All_Races
# MAGIC SET silver = 'N'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
