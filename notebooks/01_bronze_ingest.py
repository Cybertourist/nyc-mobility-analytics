# Databricks notebook source
import os, json, requests

RAW = "/Volumes/workspace/bronze/raw"
MONTHS = ["2025-01", "2025-02", "2025-03"]

def download(url, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if os.path.exists(path):
        print("already there:", path)
        return
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_content(8 * 1024 * 1024):
                f.write(chunk)
    print("saved:", path, os.path.getsize(path) // 1024, "KB")

# COMMAND ----------

BASE = "https://d37ci6vzurychx.cloudfront.net"
for m in MONTHS:
    download(f"{BASE}/trip-data/yellow_tripdata_{m}.parquet", f"{RAW}/taxi/yellow_tripdata_{m}.parquet")
download(f"{BASE}/misc/taxi_zone_lookup.csv", f"{RAW}/taxi_zones/taxi_zone_lookup.csv")

# COMMAND ----------

download("https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?format=csvdata&startPeriod=2015-01-01",
         f"{RAW}/fx/ecb_usd_eur.csv")
download("https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.MKTP.CD?format=json&date=2000:2025&per_page=100",
         f"{RAW}/gdp/worldbank_usa_gdp.json")

# COMMAND ----------

import pandas as pd

spark.read.parquet(f"{RAW}/taxi/").write.mode("overwrite").saveAsTable("workspace.bronze.taxi_trips")

spark.read.option("header", True).csv(f"{RAW}/taxi_zones/").write.mode("overwrite").saveAsTable("workspace.bronze.taxi_zones")

spark.read.option("header", True).csv(f"{RAW}/fx/").write.mode("overwrite").saveAsTable("workspace.bronze.fx_rates")

with open(f"{RAW}/gdp/worldbank_usa_gdp.json") as f:
    meta, rows = json.load(f)
gdp = pd.json_normalize(rows).astype(str)
spark.createDataFrame(gdp).write.mode("overwrite").saveAsTable("workspace.bronze.gdp")

# COMMAND ----------

for t in ["taxi_trips", "taxi_zones", "fx_rates", "gdp"]:
    print(t, spark.table(f"workspace.bronze.{t}").count())

# COMMAND ----------

dbutils.widgets.text("openaq_key", "")

# COMMAND ----------

import time

API = "https://api.openaq.org/v3"
HEADERS = {"X-API-Key": dbutils.widgets.get("openaq_key")}

def get(path, **params):
    for attempt in range(5):
        r = requests.get(f"{API}{path}", headers=HEADERS, params=params, timeout=60)
        if r.status_code == 429:
            time.sleep(10)
            continue
        r.raise_for_status()
        time.sleep(1.1)
        return r.json()
    raise RuntimeError(f"rate limit on {path}")

locations = get("/locations", coordinates="40.7128,-74.0060", radius=25000, limit=1000)["results"]
os.makedirs(f"{RAW}/openaq", exist_ok=True)
with open(f"{RAW}/openaq/locations.json", "w") as f:
    json.dump(locations, f)

sensors = [(loc["id"], s["id"], s["parameter"]["name"])
           for loc in locations for s in loc["sensors"]
           if s["parameter"]["name"] in ("pm25", "no2")]
print(len(locations), "locations,", len(sensors), "sensors with pm25/no2")

# COMMAND ----------

daily = []
for loc_id, sensor_id, param in sensors:
    page = 1
    while True:
        res = get(f"/sensors/{sensor_id}/days", datetime_from="2025-01-01", datetime_to="2025-04-01",
                  limit=1000, page=page)["results"]
        for r in res:
            r["location_id"], r["sensor_id"] = loc_id, sensor_id
        daily += res
        if len(res) < 1000:
            break
        page += 1

with open(f"{RAW}/openaq/sensor_days.json", "w") as f:
    json.dump(daily, f)
print(len(daily), "daily rows")

# COMMAND ----------

spark.createDataFrame(pd.json_normalize(locations).astype(str)).write.mode("overwrite") \
    .option("overwriteSchema", True).saveAsTable("workspace.bronze.openaq_locations")
spark.createDataFrame(pd.json_normalize(daily).astype(str)).write.mode("overwrite") \
    .option("overwriteSchema", True).saveAsTable("workspace.bronze.openaq_daily")
print(spark.table("workspace.bronze.openaq_daily").count())

# COMMAND ----------

