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

# ###### _**Clean Practices**_
# 
# - Remove in df_src all rows not related to the Practices results ( If laps is null so it's not race result )
# - Save Notes Data in df_WithNotes
# - Clean Notes Data (same data cleaning process than races )
# - Insert into Clean.Races_Notes the new data of the practices notes
# - Create Schema of the clean table
# - Create column [Practice] (Practice 1, Practice 2, Practice 3) derivated this new column from the column practice_no
# - Clean UNICODE in the driver_name column
# - create column [Real_Time_S]


# CELL ********************

import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

w = Window.partitionBy("race_id", "practice_no")

df_src = spark.sql(
    "SELECT R.* FROM Lake_F1_Silver.src.Practices R INNER JOIN Lake_F1_Silver.src.All_Races A ON A.Race_Id = R.race_id AND A.Silver_Clean = 'N'" 
)

SCHEMA_PRACTICE = StructType([
    StructField("Position", IntegerType(), True),
    StructField("Car_number", IntegerType(), True),
    StructField("Driver", StringType(), True),
    StructField("Team", StringType(), True),
    StructField("Time", StringType(), True),         # mejor vuelta (texto)
    StructField("Time_s", DoubleType(), True),       # mejor vuelta en segundos
    StructField("Gap", StringType(), True),          # gap vs líder (texto)
    StructField("Gap_s", DoubleType(), True),        # gap en segundos
    StructField("Base_time_s", DoubleType(), True), 
    StructField("Real_time_s", DoubleType(), True),
    StructField("Laps", IntegerType(), True),
    StructField("Driver_Name", StringType(), True),
    StructField("Driver_Code", StringType(), True),
    StructField("Source_Url", StringType(), True),
    StructField("Event_Title", StringType(), True),
    StructField("Scraped_Timestamp", StringType(), True),
    StructField("Race_Id", IntegerType(), True),
    StructField("Gp_Slug", StringType(), True),
    StructField("Season", IntegerType(), True),
    StructField("Practice_No", IntegerType(), True),
    StructField("Practice", StringType(), True)

])

df_WithOutNotes = df_src.filter( F.col("laps").isNotNull() )
df_With_Notes = df_src.filter( F.col("laps").isNull() )

df_CleanPractices = (
    df_WithOutNotes
        .withColumn("Practice", F.concat_ws(" ", F.lit("Practice"), F.col("practice_no").cast("string")))
        .withColumn("Clean_Driver_Name", F.trim(F.regexp_replace(F.col("driver_name"), " ", " ")) )
        .drop("interval","interval_s")
        .withColumn("base_time_s", F.first("time_s", ignorenulls=True).over(w))
        .withColumn(
            "real_time_s",
            F.when(F.col("gap_s").isNotNull(), F.col("base_time_s") + F.col("gap_s"))
            .otherwise(F.col("base_time_s"))
    )
        .withColumn("real_time_s", F.round("real_time_s", 3))
)

mappings = [
    ("position", "Position"),
    ("car_number", "Car_number"),
    ("driver", "Driver"),
    ("team", "Team"),
    ("time", "Time"),
    ("time_s", "Time_s"),
    ("gap", "Gap"),
    ("gap_s", "Gap_s"),
    ("base_time_s", "Base_time_s"),
    ("real_time_s", "Real_time_s"),
    ("laps", "Laps"),
    ("Clean_Driver_Name", "Driver_Name"),
    ("driver_code", "Driver_Code"),
    ("source_url", "Source_Url"),
    ("event_title", "Event_Title"),
    ("scraped_at_utc", "Scraped_Timestamp"),
    ("race_id", "Race_Id"),
    ("gp_slug", "Gp_Slug"),
    ("season", "Season"),
    ("practice_no", "Practice_No"),
    ("Practice", "Practice")
]

df_out = df_CleanPractices.select([F.col(src).alias(dst) for src, dst in mappings])

target_cols = [ F.col(f.name).cast(f.dataType).alias(f.name) for f in SCHEMA_PRACTICE.fields ]
df_final = df_out.select(*target_cols)

#display(df_final)
df_final.write.format("delta").mode("append").saveAsTable("Lake_F1_Silver.clean.Practices")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_Practices_Notes = (
    df_With_Notes
        .filter(F.col("driver") != "* Provisional results." )
        .withColumn("Note", F.trim(F.regexp_replace("driver_name", r"\*", "")))
        .withColumn("Event", F.lit("Practices"))
        .select( 
            F.col("Note"), 
            F.col("race_id").alias("Race_Id"),
            F.col("season").alias("Season"),
            F.col("gp_slug").alias("Gp_Slug"),
            F.col("Event")
        )
    )

#display(df_Practices_Notes)
df_Practices_Notes.write.format("delta").mode("append").option("overwriteschema","true").saveAsTable("Lake_F1_Silver.clean.Races_Notes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
