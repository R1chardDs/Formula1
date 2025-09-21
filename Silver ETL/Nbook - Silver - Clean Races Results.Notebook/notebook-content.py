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

# MARKDOWN ********************

# ###### _**Clean Races Results**_
# 
# - Remove in df_src all rows not related to the races results (add a column flag for identify this comments and save it in other table)
# - Reemplazar posiciones en NULL con NC
# - Eliminar columna driver
# - Limpiar Unicodes en columnas [team, driver_name,driver_code, status, event_title]
# - Convertir columna Car Number a Text
# - los valores en la columna time/retired que sean OK modificarlos por DNF
# - Add flag column [Fix_Points] for create separate dframes for manually fix it
# - Create a new df df_Fix_Points filtered by Fix_Points = "Y" and add a column named [New_Points] with the logic points per position
# - Create a new df df_NoFix_Points filtered by Fix_Points = "N"
# - Append df_Fix_Points & df_NoFix_Points, drop the New_Points column but keep the Fix_Points column in the end, for identify which columns changed
# - Save df in new Table in Lake_F1_Silver.clean.Races_Results

# CELL ********************

df_src = spark.sql(
    "SELECT R.* FROM Lake_F1_Silver.src.Races_Results R INNER JOIN Lake_F1_Silver.src.All_Races A ON A.Race_Id = R.race_id AND A.Silver_Clean = 'N'" 
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pyspark.sql.functions as F

display(df_src)#.filter(F.col("race_id") == 1222 ).orderBy(F.desc("points"),F.desc("laps")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
