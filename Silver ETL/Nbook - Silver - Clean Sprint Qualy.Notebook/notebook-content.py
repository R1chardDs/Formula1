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
# - Drop Driver Column
# - Add Column [Clean_Position] Replace the Null Positions With NC
# - Clean Unicodes in driver name
# - Replace Null with zero in Q1,Q2,Q3 segs

# CELL ********************

import pyspark.sql.functions as F
from pyspark.sql.types import *

df_src = spark.sql(
    "SELECT R.* FROM Lake_F1_Silver.src.Sprint_Qualifying R INNER JOIN Lake_F1_Silver.src.All_Races A ON A.Race_Id = R.race_id AND A.Silver_Clean = 'N'" 
)


SCHEMA_QUALI = StructType([
    StructField("Position", StringType(), True),
    StructField("Car_number", IntegerType(), True),
    StructField("Team", StringType(), True),
    StructField("Q1", StringType(), True),
    StructField("Q2", StringType(), True),
    StructField("Q3", StringType(), True),
    StructField("Laps", IntegerType(), True),
    StructField("Q1_Segs", DoubleType(), True),
    StructField("Q2_Segs", DoubleType(), True),
    StructField("Q3_Segs", DoubleType(), True),
    StructField("Driver_Name", StringType(), True),
    StructField("Driver_Code", StringType(), True),
    StructField("Source_Url", StringType(), True),
    StructField("Event_Title", StringType(), True),
    StructField("Scraped_Timestamp", StringType(), True),
    StructField("Race_Id", IntegerType(), True),
    StructField("Gp_Slug", StringType(), True),
    StructField("Season", IntegerType(), True),
])

df_With_Notes = df_src.filter(F.col("laps").isNull())
df_WithOut_Notes = df_src.filter(F.col("laps").isNotNull())

df_clean = (
    
    df_WithOut_Notes
        .withColumn("Clean_Position", F.when( F.col("position").isNull(), "NC" ).otherwise(F.col("position")))
        .withColumn("Clean_Driver_Name", F.trim(F.regexp_replace(F.col("driver_name"), " ", " ") ))
        .drop("driver")
        .withColumns({
            "Q1_Segs_Clean": F.when( F.col("Q1_Segs").isNull(), 0.00 ).otherwise(F.round("Q1_Segs",3)),
            "Q2_Segs_Clean": F.when( F.col("Q2_Segs").isNull(), 0.00 ).otherwise(F.round("Q2_Segs",3)),
            "Q3_Segs_Clean": F.when( F.col("Q3_Segs").isNull(), 0.00 ).otherwise(F.round("Q3_Segs",3)),
            })
)

mappings = [
("Clean_Position","Position"),
("car_number","Car_number"),
("team","Team"),
("q1","Q1"),
("q2","Q2"),
("q3","Q3"),
("laps","Laps"),
("Q1_Segs_Clean","Q1_Segs"),
("Q2_Segs_Clean","Q2_Segs"),
("Q3_Segs_Clean","Q3_Segs"),
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

target_cols = [ F.col(f.name).cast(f.dataType).alias(f.name) for f in SCHEMA_QUALI.fields ]
df_final = df_out.select(*target_cols)

#display(df_final)
df_final.write.format("delta").mode("append").saveAsTable("Lake_F1_Silver.clean.Sprint_Qualifying")

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
        .withColumn("Event", F.lit("Sprint_Qualifying"))
        .select( 
            F.col("Note"), 
            F.col("race_id").alias("Race_Id"),
            F.col("season").alias("Season"),
            F.col("gp_slug").alias("Gp_Slug"),
            F.col("Event")
        )
    )

#display(df_Races_Notes)
df_Races_Notes.write.format("delta").mode("append").option("overwriteschema","true").saveAsTable("Lake_F1_Silver.clean.Races_Notes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
