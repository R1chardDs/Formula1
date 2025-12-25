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
# - Remove in df_src all rows not related to the races results ( If Points is null so it's not race result )
# - Add Column [Clean_Position] 
#     - If position it's Null and Status = Finished so it's DQ (Disquilified)
#     - In [Clean_Position] column Replace the other Null Positions With NC
# - Add Column [Clean_Status] = If [Clean_Position] = DQ then DSQ else [Status]
#     - Modified the logic of status, before set DSQ evaluate if time_or_retired it's SHC, if it's status = SHC
# - Drop Driver column
# - Clean Unicodes In Columns [team, driver_name,driver_code, status, event_title]
# - In the column time/retired replace the "OK" values with DNF
# - Add flag column [Fix_Points] for create separate dframes for manually fix it
# - Create a new df df_Fix_Points filtered by Fix_Points = "Y" and add a column named [New_Points] with the logic points per position
# - Create a new df df_NoFix_Points filtered by Fix_Points = "N"
# - Create table Schema
# - Append df_Fix_Points & df_NoFix_Points, drop the New_Points column but keep the Fix_Points column in the end, for identify which columns changed
# - Save df in new Table in Lake_F1_Silver.clean.Races_Results


# MARKDOWN ********************

# **❗If you are running a full re-ingest, please run before: "Nbook - Silver - PreClean Silver Data" 
# and then run "Nbook - Silver - PostClean Silver Data" these are manual nbooks, run the snippets required/related to the table you are reprocessing**

# CELL ********************

import pyspark.sql.functions as F
from pyspark.sql.types import *

df_src = spark.sql(
    "SELECT R.* FROM Lake_F1_Silver.src.Races_Results R INNER JOIN Lake_F1_Silver.src.All_Races A ON A.Race_Id = R.race_id AND A.Silver_Clean = 'N'" 
)

TableSchema = StructType([
    StructField("Position", StringType(), True),
    StructField("Car_Number", IntegerType(), True),
    StructField("Team", StringType(), True),
    StructField("Laps", IntegerType(), True),
    StructField("Time_Or_Retired", StringType(), True),
    StructField("Points", DoubleType(), True),
    StructField("Driver_Name", StringType(), True),
    StructField("Driver_Code", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("Race_Time_S", DoubleType(), True),
    StructField("Gap_To_Winner_S", DoubleType(), True),
    StructField("Source_Url", StringType(), True),
    StructField("Event_Title", StringType(), True),
    StructField("Scraped_Timestamp", StringType(), True),
    StructField("Race_Id", IntegerType(), True),
    StructField("Season", IntegerType(), True),
    StructField("Gp_Slug", StringType(), True),
    StructField("Fix_Points", StringType(), True)
])

df_With_Notes = df_src.filter(F.col("points").isNull())
df_WithOut_Notes = df_src.filter(F.col("points").isNotNull())

CalcPoints = (
    F.when(F.col("position") == 1, 25)
     .when(F.col("position") == 2, 18)
     .when(F.col("position") == 3, 15)
     .when(F.col("position") == 4, 12)
     .when(F.col("position") == 5, 10)
     .when(F.col("position") == 6, 8)
     .when(F.col("position") == 7, 6)
     .when(F.col("position") == 8, 4)
     .when(F.col("position") == 9, 2)
     .when(F.col("position") == 10, 1)
     .otherwise(0)
)

df_CleanStatusPos = (

    df_WithOut_Notes
    
        .withColumn(
            "Clean_Position", 
                F.when( (F.col("position").isNull()) & (F.col("status") == "Finished" ) & (F.col("time_or_retired") != "SHC" ) , "DQ" )
                 .when( F.col("position").isNull(), "NC" )
                 .otherwise(F.col("position")) 
            )
        
        .withColumn(
            "Clean_Status", 
                F.when( F.col("Clean_Position") == "DQ", "DSQ" )
                 .when( (F.col("time_or_retired") == "SHC" ) & (F.col("laps").isNull() ), "SHC" )
                 .otherwise(F.col("status"))
            )

        .withColumn(
            "Clean_Time_or_Retired", 
                F.when( (F.col("time_or_retired") == "OK") & (F.col("Clean_Status") != "Finished"), F.col("Clean_Status") )
                .when( (F.col("time_or_retired") == "OK") & (F.col("Clean_Status") == "Finished"), F.col("gap_to_winner_s") )
                .otherwise(F.col("time_or_retired"))
            )
        
        .drop("driver")

        .withColumn(
            "Clean_Driver_Name", F.trim(F.regexp_replace(F.col("driver_name"), " ", " ") )
        )

        .withColumn(
            "Fix_Points",
            F.when( (F.col("points")  == 0 ) & (F.col("position") <= 10 ) & (F.col("season") >= 2010 ), "Y" )
            .otherwise("N")
        )

        .withColumn(
            "Clean_Points",
                F.when( F.col("Fix_Points") == "Y", CalcPoints )
                .otherwise(F.col("points"))
        )
)

mappings = [
    ("Clean_Position",        "Position"),
    ("car_number",            "Car_Number"),
    ("team",                  "Team"),
    ("laps",                  "Laps"),
    ("Clean_Time_or_Retired", "Time_Or_Retired"),
    ("Clean_Points",          "Points"),
    ("Clean_Driver_Name",     "Driver_Name"),
    ("driver_code",           "Driver_Code"),
    ("Clean_Status",          "Status"),
    ("race_time_s",           "Race_Time_S"),
    ("gap_to_winner_s",       "Gap_To_Winner_S"),
    ("source_url",            "Source_Url"),
    ("event_title",           "Event_Title"),
    ("scraped_at_utc",        "Scraped_Timestamp"),
    ("race_id",               "Race_Id"),
    ("season",                "Season"),
    ("gp_slug",               "Gp_Slug"),
    ("Fix_Points",            "Fix_Points"),
]

df_out = df_CleanStatusPos.select([F.col(src).alias(dst) for src, dst in mappings])

target_cols = [ F.col(f.name).cast(f.dataType).alias(f.name) for f in TableSchema.fields ]
df_final = df_out.select(*target_cols)


#print([(f.name, f.dataType.simpleString()) for f in df_final.schema])
#print([(f.name, f.dataType.simpleString()) for f in TableSchema])

#display(df_final)

df_final.write.format("delta").mode("append").option("overwriteschema","true").saveAsTable("Lake_F1_Silver.clean.Races_Results")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_Races_Notes = (
    df_With_Notes
        .filter(F.col("driver") != "* Provisional results." )
        .withColumn("Note", F.trim(F.regexp_replace("driver_name", r"\*", "")))
        .withColumn("Event", F.lit("Race"))
        .select( 
            F.col("Note"), 
            F.col("race_id").alias("Race_Id"),
            F.col("season").alias("Season"),
            F.col("gp_slug").alias("Gp_Slug"),
            F.col("Event")
        )
    )

#display(df_Races_Notes)
#df_Races_Notes.write.format("delta").mode("append").option("overwriteschema","true").saveAsTable("Lake_F1_Silver.clean.Races_Notes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
