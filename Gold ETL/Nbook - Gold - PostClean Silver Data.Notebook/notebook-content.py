# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0fb46a7a-f73d-4af7-ae4e-ec2a0a2daead",
# META       "default_lakehouse_name": "Lake_F1_Gold",
# META       "default_lakehouse_workspace_id": "265d92f7-0795-4b79-b272-0e06281c49c5",
# META       "known_lakehouses": [
# META         {
# META           "id": "0fb46a7a-f73d-4af7-ae4e-ec2a0a2daead"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Puntos de vuelta rapida que no se otorgaron al momento de recalcular puntos manualmente
# MAGIC 
# MAGIC UPDATE Lake_F1_Gold.dbo.Races_Results
# MAGIC SET Points = Points + 1
# MAGIC WHERE Race_Id = 1222 AND Position = '8';
# MAGIC 
# MAGIC UPDATE Lake_F1_Gold.dbo.Races_Results
# MAGIC SET Points = Points + 1
# MAGIC WHERE Race_Id = 1142 AND Position = '2'
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
