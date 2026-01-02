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

df_src = spark.sql( "SELECT TrackKey AS Track_Key, TrackName AS Track_Name, Country, YearBuilt AS Year_Built FROM Lake_F1_Silver.staging.Dim_Tracks" )

#display(df_src)
df_src.write.format("delta").option("overwriteschema","true").mode("overwrite").saveAsTable("Lake_F1_Silver.clean.Dim_Tracks")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
