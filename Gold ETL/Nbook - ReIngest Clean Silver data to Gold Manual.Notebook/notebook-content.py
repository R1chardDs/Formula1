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
# META         },
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
# MAGIC DELETE FROM Lake_F1_Gold.dbo.Races_Results

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC INSERT INTO Lake_F1_Gold.dbo.Races_Results
# MAGIC SELECT * FROM Lake_F1_Silver.clean.Races_Results

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Lake_F1_Silver.clean.Sprint_Races_Results")
df.write.format("delta").mode("overwrite").option("overwriteschema","true").saveAsTable("Lake_F1_Gold.dbo.Sprint_Races_Result")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
