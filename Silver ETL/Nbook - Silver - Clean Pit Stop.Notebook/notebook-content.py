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

import pyspark.sql.functions as F
from pyspark.sql.types import *

df_src = spark.sql(
    "SELECT * FROM Lake_F1_Silver.staging.Pit_Stop" 
)

SCHEMA_PITS = StructType([
    StructField("Stops", IntegerType(), True),
    StructField("Car_Number", IntegerType(), True),
    StructField("Driver_Name", StringType(), True),
    StructField("Driver_Code", StringType(), True),
    StructField("Team", StringType(), True),
    StructField("Lap", IntegerType(), True),
    StructField("Time_Of_Day", StringType(), True),
    StructField("Pit_Time", StringType(), True),
    StructField("Pit_Secs", DoubleType(), True),
    StructField("Total_Time", StringType(), True),
    StructField("Total_Secs", DoubleType(), True),
    StructField("Source_Url", StringType(), True),
    StructField("Event_Title", StringType(), True),
    StructField("Scraped_Timestamp", StringType(), True),
    StructField("Race_Id", IntegerType(), True),
    StructField("Gp_Slug", StringType(), True),
    StructField("Season", IntegerType(), True),
])

df_WithOut_Notes = df_src.filter(F.col("stop").isNotNull())

df_clean = (
    
    df_WithOut_Notes
        .withColumn("Clean_Driver_Name", F.trim(F.regexp_replace(F.col("driver_name"), " ", " ") ))
        .drop("driver")
)

mappings = [
("stop","Stops"),
("car_number","Car_Number"),
("Clean_Driver_Name","Driver_Name"),
("driver_code","Driver_Code"),
("team","Team"),
("lap","Lap"),
("time_of_day","Time_Of_Day"),
("pit_time","Pit_Time"),
("pit_secs","Pit_Secs"),
("total_time","Total_Time"),
("total_secs","Total_Secs"),
("source_url","Source_Url"),
("event_title","Event_Title"),
("scraped_at_utc","Scraped_Timestamp"),
("race_id","Race_Id"),
("gp_slug","Gp_Slug"),
("season","Season")
]

df_out = df_clean.select([F.col(src).alias(dst) for src, dst in mappings])

target_cols = [ F.col(f.name).cast(f.dataType).alias(f.name) for f in SCHEMA_PITS.fields ]
df_final = df_out.select(*target_cols)

#display(df_final)
df_final.write.format("delta").mode("overwrite").saveAsTable("Lake_F1_Silver.clean.Pit_Stops")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
