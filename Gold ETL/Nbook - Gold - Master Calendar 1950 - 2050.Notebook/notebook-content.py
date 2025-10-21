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

from pyspark.sql import functions as F

# ====== Parámetros de rango ======
start_date = "1950-01-01"
end_date   = "2050-12-31"

# ====== Base de fechas ======
df_dates = (
    spark.createDataFrame([(1,)], ["dummy"])
    .select(
        F.explode(
            F.sequence(
                F.to_date(F.lit(start_date)),
                F.to_date(F.lit(end_date)),
                F.expr("interval 1 day")
            )
        ).alias("Date_dt")
    )
)

# ====== Derivados básicos ======
df = (
    df_dates
    .withColumn("Id_Month", F.month("Date_dt"))
    .withColumn("Year", F.year("Date_dt"))
    .withColumn("Id_Quarter", F.quarter("Date_dt"))
    # dayofweek: 1=Sunday..7=Saturday  → remapeamos a Monday=1..Sunday=7
    .withColumn("dow_sun1", F.dayofweek("Date_dt"))
    .withColumn("Id_Day", ((F.col("dow_sun1") + F.lit(5)) % F.lit(7)) + F.lit(1))
    .drop("dow_sun1")
    .withColumn("YearWeek", F.weekofyear("Date_dt"))
)

# ====== Nombres en inglés ======
months_long_arr  = F.array([F.lit(m) for m in [
    "January","February","March","April","May","June",
    "July","August","September","October","November","December"
]])
months_short_arr = F.array([F.lit(m) for m in [
    "Jan","Feb","Mar","Apr","May","Jun",
    "Jul","Aug","Sep","Oct","Nov","Dec"
]])
days_long_arr    = F.array([F.lit(d) for d in [
    "Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"
]])
days_short_arr   = F.array([F.lit(d) for d in [
    "Mon","Tue","Wed","Thu","Fri","Sat","Sun"
]])

df = (
    df
    .withColumn("Month", F.element_at(months_long_arr, F.col("Id_Month")))
    .withColumn("Mth",   F.element_at(months_short_arr, F.col("Id_Month")))
    .withColumn("Day_Name",       F.element_at(days_long_arr,  F.col("Id_Day")))
    .withColumn("Day_Name_Short", F.element_at(days_short_arr, F.col("Id_Day")))
)

# ====== MonthWeek (semana dentro del mes, lunes=1) ======
df = (
    df
    .withColumn("FirstDayMonth", F.trunc("Date_dt", "MM"))
    .withColumn("fdw_sun1", F.dayofweek("FirstDayMonth"))
    .withColumn("FirstDayMonth_ISO", ((F.col("fdw_sun1") + F.lit(5)) % F.lit(7)) + F.lit(1))
    .withColumn("OffsetMon", (F.col("FirstDayMonth_ISO") - F.lit(1)))
    .withColumn("DayOfMonth", F.dayofmonth("Date_dt"))
    .withColumn(
        "MonthWeek",
        (F.floor((F.col("DayOfMonth") + F.col("OffsetMon") - F.lit(1)) / F.lit(7)) + F.lit(1)).cast("int")
    )
    .drop("FirstDayMonth","fdw_sun1","FirstDayMonth_ISO","OffsetMon","DayOfMonth")
)

df = (
    df
    .withColumn("Date_StarOfMonth", F.trunc("Date_dt", "MM"))
    .withColumn("Date_EndOfMonth",  F.last_day("Date_dt"))
    # lunes=1..domingo=7 (Id_Day) → inicio y fin de semana
    .withColumn("Date_StarOfWeek", F.date_sub(F.col("Date_dt"), F.col("Id_Day") - F.lit(1)))
    .withColumn("Date_EndOfWeek",  F.date_add(F.col("Date_StarOfWeek"), F.lit(6)))
    .withColumn("IdPeriod", (F.col("Year") * F.lit(1000) + F.col("Id_Month")).cast("int"))
)

df = df.withColumnRenamed("Date_dt", "Date")

df_calendar = df.select(
    "Date",
    "Id_Month",
    "Month",
    "Mth",
    "Year",
    "Id_Quarter",
    "Id_Day",
    "Day_Name",
    "Day_Name_Short",
    "MonthWeek",
    "YearWeek",
    "Date_StarOfMonth",
    "Date_EndOfMonth",
    "Date_StarOfWeek",
    "Date_EndOfWeek",
    "IdPeriod"
)

#display(df_calendar)
df_calendar.write.format("delta").mode("overwrite").saveAsTable("Lake_F1_Gold.dbo.Master_Calendar")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
