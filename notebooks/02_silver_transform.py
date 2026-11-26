# Databricks notebook source
from pyspark.sql import functions as F, Window

START, END = "2025-01-01", "2025-03-31"
def save(df, name):
    df.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"workspace.silver.{name}")
    print(name, spark.table(f"workspace.silver.{name}").count())

# COMMAND ----------

t = spark.table("workspace.bronze.taxi_trips")
taxi = (t
    .select(
        F.col("VendorID").cast("int").alias("vendor_id"),
        F.col("tpep_pickup_datetime").cast("timestamp").alias("pickup_ts"),
        F.col("tpep_dropoff_datetime").cast("timestamp").alias("dropoff_ts"),
        F.col("passenger_count").cast("int"),
        F.col("trip_distance").cast("double"),
        F.col("PULocationID").cast("int").alias("pu_location_id"),
        F.col("DOLocationID").cast("int").alias("do_location_id"),
        F.col("payment_type").cast("int"),
        F.col("fare_amount").cast("double"),
        F.col("tip_amount").cast("double"),
        F.col("total_amount").cast("double"))
    .dropDuplicates()
    .withColumn("trip_minutes", (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60)
    .filter(F.col("pickup_ts").between(START, f"{END} 23:59:59"))
    .filter("trip_distance > 0 AND trip_distance < 100")
    .filter("total_amount > 0 AND total_amount < 1000 AND fare_amount >= 0")
    .filter("trip_minutes > 0 AND trip_minutes < 360")
    .withColumn("pickup_date", F.to_date("pickup_ts"))
    .withColumn("pickup_hour", F.hour("pickup_ts")))
save(taxi, "taxi_trips")

# COMMAND ----------

zones = (spark.table("workspace.bronze.taxi_zones")
    .select(F.col("LocationID").cast("int").alias("location_id"),
            F.col("Borough").alias("borough"), F.col("Zone").alias("zone"),
            F.col("service_zone")))
save(zones, "taxi_zones")

fx_raw = (spark.table("workspace.bronze.fx_rates")
    .select(F.to_date("TIME_PERIOD").alias("date"), F.col("OBS_VALUE").cast("double").alias("usd_per_eur"))
    .filter(F.col("usd_per_eur").isNotNull()))
calendar = spark.sql("SELECT explode(sequence(DATE'2015-01-01', current_date(), INTERVAL 1 DAY)) AS date")
w = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
fx = (calendar.join(fx_raw, "date", "left")
      .withColumn("is_ecb_day", F.col("usd_per_eur").isNotNull())
      .withColumn("usd_per_eur", F.last("usd_per_eur", ignorenulls=True).over(w))
      .filter(F.col("usd_per_eur").isNotNull()))
save(fx, "fx_rates")

gdp = (spark.table("workspace.bronze.gdp")
    .select(F.col("`country.value`").alias("country"), F.col("countryiso3code").alias("country_code"),
            F.col("date").cast("int").alias("year"), F.col("value").cast("double").alias("gdp_usd"))
    .filter(F.col("gdp_usd").isNotNull()))
save(gdp, "gdp")

# COMMAND ----------

loc = (spark.table("workspace.bronze.openaq_locations")
    .select(F.col("id").cast("int").alias("location_id"), F.col("name").alias("location_name"),
            F.col("`coordinates.latitude`").cast("double").alias("lat"),
            F.col("`coordinates.longitude`").cast("double").alias("lon")))
save(loc, "air_locations")

air = (spark.table("workspace.bronze.openaq_daily")
    .select(F.col("location_id").cast("int"), F.col("sensor_id").cast("int"),
            F.col("`parameter.name`").alias("parameter"), F.col("`parameter.units`").alias("units"),
            F.to_date(F.substring(F.col("`period.datetimeFrom.local`"), 1, 10)).alias("date"),
            F.col("value").cast("double"),
            F.col("`coverage.percentComplete`").cast("double").alias("coverage_pct"))
    .filter(F.col("date").between(START, END))
    .filter(~F.isnan("value") & (F.col("value") >= 0))
    .dropDuplicates(["sensor_id", "date"]))
save(air, "air_quality_daily")

# COMMAND ----------

