# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.gold.dim_date AS
# MAGIC SELECT CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key,
# MAGIC        d                                       AS date,
# MAGIC        year(d)                                 AS year,
# MAGIC        month(d)                                AS month,
# MAGIC        date_format(d, 'MMMM')                  AS month_name,
# MAGIC        day(d)                                  AS day,
# MAGIC        dayofweek(d)                            AS day_of_week,
# MAGIC        date_format(d, 'EEEE')                  AS day_name,
# MAGIC        dayofweek(d) IN (1, 7)                  AS is_weekend
# MAGIC FROM (SELECT explode(sequence(DATE'2025-01-01', DATE'2025-03-31', INTERVAL 1 DAY)) AS d);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.gold.dim_zone AS
# MAGIC SELECT location_id AS zone_key, borough, zone, service_zone
# MAGIC FROM workspace.silver.taxi_zones;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE workspace.gold.dim_fx AS
# MAGIC SELECT CAST(date_format(date, 'yyyyMMdd') AS INT) AS date_key, usd_per_eur, is_ecb_day
# MAGIC FROM workspace.silver.fx_rates
# MAGIC WHERE date BETWEEN '2025-01-01' AND '2025-03-31';
# MAGIC
# MAGIC CREATE OR REPLACE TABLE workspace.gold.dim_gdp AS
# MAGIC SELECT year, country, country_code, gdp_usd
# MAGIC FROM workspace.silver.gdp;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE workspace.gold.dim_air_location AS
# MAGIC SELECT location_id, location_name, lat, lon
# MAGIC FROM workspace.silver.air_locations;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.gold.fact_taxi_daily AS
# MAGIC SELECT CAST(date_format(t.pickup_date, 'yyyyMMdd') AS INT) AS date_key,
# MAGIC        t.pu_location_id                    AS zone_key,
# MAGIC        count(*)                            AS trips,
# MAGIC        sum(t.passenger_count)              AS passengers,
# MAGIC        round(sum(t.trip_distance), 2)      AS distance_miles,
# MAGIC        round(sum(t.total_amount), 2)       AS revenue_usd,
# MAGIC        round(sum(t.total_amount) / max(fx.usd_per_eur), 2) AS revenue_eur,
# MAGIC        round(avg(t.total_amount), 2)       AS avg_fare_usd,
# MAGIC        round(avg(t.trip_minutes), 1)       AS avg_trip_minutes
# MAGIC FROM workspace.silver.taxi_trips t
# MAGIC JOIN workspace.silver.fx_rates fx ON fx.date = t.pickup_date
# MAGIC GROUP BY 1, 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.gold.fact_taxi_hourly AS
# MAGIC SELECT CAST(date_format(pickup_date, 'yyyyMMdd') AS INT) AS date_key,
# MAGIC        pickup_hour AS hour,
# MAGIC        pu_location_id AS zone_key,
# MAGIC        count(*) AS trips,
# MAGIC        round(sum(total_amount), 2) AS revenue_usd
# MAGIC FROM workspace.silver.taxi_trips
# MAGIC GROUP BY 1, 2, 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.gold.fact_air_quality_daily AS
# MAGIC SELECT CAST(date_format(date, 'yyyyMMdd') AS INT) AS date_key,
# MAGIC        location_id, sensor_id, parameter, units,
# MAGIC        value, coverage_pct
# MAGIC FROM workspace.silver.air_quality_daily;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH trips AS (
# MAGIC   SELECT date_key, sum(trips) AS trips FROM workspace.gold.fact_taxi_daily GROUP BY date_key),
# MAGIC air AS (
# MAGIC   SELECT date_key, parameter, avg(value) AS avg_value
# MAGIC   FROM workspace.gold.fact_air_quality_daily GROUP BY date_key, parameter)
# MAGIC SELECT a.parameter,
# MAGIC        round(corr(t.trips, a.avg_value), 3) AS correlation,
# MAGIC        count(*) AS days
# MAGIC FROM trips t JOIN air a USING (date_key)
# MAGIC GROUP BY a.parameter;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT parameter, count(*) AS rows, count_if(isnan(value)) AS nan_rows
# MAGIC FROM workspace.gold.fact_air_quality_daily
# MAGIC GROUP BY parameter;

# COMMAND ----------

