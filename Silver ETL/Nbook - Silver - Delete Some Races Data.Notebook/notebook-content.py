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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC DELETE FROM Lake_F1_Silver.src.All_Races WHERE Race_Id = 1275;
# MAGIC DELETE FROM Lake_F1_Silver.src.Fast_Laps WHERE race_id = 1275;
# MAGIC DELETE FROM Lake_F1_Silver.src.Pit_Stop WHERE race_id = 1275;
# MAGIC DELETE FROM Lake_F1_Silver.src.Practices WHERE race_id = 1275;
# MAGIC DELETE FROM Lake_F1_Silver.src.Qualifying WHERE race_id = 1275;
# MAGIC DELETE FROM Lake_F1_Silver.src.Races_Results WHERE race_id = 1275;
# MAGIC DELETE FROM Lake_F1_Silver.src.Sprint_Qualifying WHERE race_id = 1275;
# MAGIC DELETE FROM Lake_F1_Silver.src.Sprint_Race_Results WHERE race_id = 1275;
# MAGIC DELETE FROM Lake_F1_Silver.src.Sprint_Starting_Grid WHERE race_id = 1275;
# MAGIC DELETE FROM Lake_F1_Silver.src.Starting_Grid WHERE race_id = 1275


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
