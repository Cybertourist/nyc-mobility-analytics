# NYC Mobility, Air Quality & Economy Analytics

Course project (Itransition, Data Engineering): a medallion data platform that combines
NYC Taxi trips, OpenAQ air quality, World Bank GDP and ECB USD/EUR rates.

Microsoft Fabric trial is not available for new tenants, so the same architecture
is implemented on Databricks Free Edition.

| Fabric | Databricks (this project) |
|---|---|
| Lakehouse | Unity Catalog schemas `bronze`, `silver` |
| OneLake Files | Volume `bronze.raw` |
| Notebook (PySpark) | Notebook (PySpark) |
| Warehouse | schema `gold` |
| Data Pipeline | Jobs |
| Power BI | Databricks Dashboard |

## Layers
- **Bronze** (`01_bronze_ingest`): raw files from APIs → Volume, then Delta tables as-is.
- **Silver** (`02_silver_transform`): types, duplicates, invalid trips, period filter, FX for every day.
- **Gold** (`03_gold_model`): star schema — `fact_taxi_daily`, `fact_taxi_hourly`,
  `fact_air_quality_daily`, `dim_date`, `dim_zone`, `dim_fx`, `dim_gdp`, `dim_air_location`.

Period: January–March 2025.