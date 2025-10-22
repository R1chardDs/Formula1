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
    "SELECT A.* FROM Lake_F1_Silver.src.All_Races A WHERE A.Silver_Clean = 'N'" 
)

# 1) Mapa de abreviaturas -> número de mes (ENG + ESP)
month_map = {
    "JAN":1, "FEB":2, "MAR":3, "APR":4, "MAY":5, "JUN":6,
    "JUL":7, "AUG":8, "SEP":9, "OCT":10, "NOV":11, "DEC":12,
    "ENE":1, "ABR":4, "AGO":8, "DIC":12
}
map_expr = F.create_map([F.lit(k) for kv in month_map.items() for k in kv])

df_out = (
    df_src
    # Últimos 3 chars -> abreviatura de mes
    .withColumn("Month", F.upper(F.expr("right(trim(Date_text), 3)")))
    .withColumn("Id_Month", map_expr[F.col("Month")].cast("int"))

    # Primeros chars antes del espacio -> día (1–2 dígitos)
    .withColumn("Id_Day", F.regexp_extract(F.trim(F.col("Date_text")), r'^(\d{1,2})', 1).cast("int"))

    # Fecha completa (Season = año)
    .withColumn("Date", F.make_date(F.col("Season").cast("int"),
                                    F.col("Id_Month"),
                                    F.col("Id_Day")))
    .withColumn("Gold", F.lit("N") )
)

#display(df_out)
df_out.write.format("delta").mode("append").option("overwriteSchema", "true").saveAsTable("Lake_F1_Silver.clean.Dim_Races")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
