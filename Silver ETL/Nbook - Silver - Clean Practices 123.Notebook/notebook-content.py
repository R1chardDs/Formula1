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

df_src = spark.sql(
    "SELECT R.* FROM Lake_F1_Silver.src.Practices R INNER JOIN Lake_F1_Silver.src.All_Races A ON A.Race_Id = R.race_id AND A.Silver_Clean = 'N'" 
)

display(df_src)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    df_src
        .filter(
            (F.col("race_id") == 1267) &
            (F.col("practice_no") == 1)
            )
        .orderBy(F.col("position"))
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Definir ventana por cada carrera y práctica
w = Window.partitionBy("race_id", "practice_no")

df_out = (
    df_src
    # 1. Propagar el tiempo base de P1 a todos en la práctica
    .withColumn("base_time_s", F.first("time_s", ignorenulls=True).over(w))
    # 2. Calcular el tiempo absoluto sumando gap_s
    .withColumn(
        "real_time_s",
        F.when(F.col("gap_s").isNotNull(), F.col("base_time_s") + F.col("gap_s"))
         .otherwise(F.col("base_time_s"))
    )
    # Opcional: redondear para consistencia
    .withColumn("real_time_s", F.round("real_time_s", 3))
)

display(df_out)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
