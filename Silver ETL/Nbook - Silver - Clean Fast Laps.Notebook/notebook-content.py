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
    "SELECT R.* FROM Lake_F1_Silver.src.Fast_Laps R INNER JOIN Lake_F1_Silver.src.All_Races A ON A.Race_Id = R.race_id AND A.Silver_Clean = 'N'" 
)

SCHEMA_FASTLAPS = StructType([
    StructField("Position", IntegerType(), True),
    StructField("Car_Number", IntegerType(), True),
    StructField("Team", StringType(), True),
    StructField("Lap", IntegerType(), True),
    StructField("Lap_Time", StringType(), True),
    StructField("Lap_Time_S", DoubleType(), True),
    StructField("Driver_Name", StringType(), True),
    StructField("Driver_Code", StringType(), True),
    StructField("Source_Url", StringType(), True),
    StructField("Event_Title", StringType(), True),
    StructField("Scraped_Timestamp", StringType(), True),
    StructField("Race_Id", IntegerType(), True),
    StructField("Gp_Slug", StringType(), True),
    StructField("Season", IntegerType(), True)
])

df_WithOut_Notes = df_src.filter(F.col("lap_time_s").isNotNull())

df_clean = (
    
    df_WithOut_Notes
        .withColumn("Clean_Driver_Name", F.trim(F.regexp_replace(F.col("driver_name"), " ", " ") ))
        .withColumn("Clean_Lap_Time_S", F.round("lap_time_s",3) )
        .drop("driver")
)

mappings = [
("position","Position"),
("car_number","Car_Number"),
("team","Team"),
("lap","Lap"),
("lap_time","Lap_Time"),
("Clean_Lap_Time_S","Lap_Time_S"),
("Clean_Driver_Name","Driver_Name"),
("driver_code","Driver_Code"),
("source_url","Source_Url"),
("event_title","Event_Title"),
("scraped_at_utc","Scraped_Timestamp"),
("race_id","Race_Id"),
("gp_slug","Gp_Slug"),
("season","Season")
]

df_out = df_clean.select([F.col(src).alias(dst) for src, dst in mappings])

target_cols = [ F.col(f.name).cast(f.dataType).alias(f.name) for f in SCHEMA_FASTLAPS.fields ]
df_final = df_out.select(*target_cols)

#display(df_final)
df_final.write.format("delta").mode("append").saveAsTable("Lake_F1_Silver.clean.Fast_Laps")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
