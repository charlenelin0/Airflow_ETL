# Databricks ETL Layer

This folder contains Databricks platform ETL assets for the weather data pipeline.

Airflow only lands raw JSON files into Databricks. The Databricks platform owns
the notebook execution from bronze to silver and gold.

## Flow

```text
Airflow
  -> Databricks Volume bronze JSON landing
  -> Databricks notebook: bronze to silver
  -> Databricks SQL notebook: silver to gold
```

## Folder Layout

```text
databricks/
  notebooks/
    weather_bronze_to_silver.py
    weather_silver_to_gold.sql
```

## Execution

Databricks Workflows / Jobs are configured directly in the Databricks UI.

They are not versioned in this repository for now. This keeps platform-side
experimentation simple while the ETL design is still evolving.

Recommended task order:

```text
weather_bronze_to_silver.py
  -> weather_silver_to_gold.sql
```

Use one shared workflow parameter:

```text
batch_date
```

Example task parameters:

```text
weather_bronze_to_silver.py
  batch_date = {{job.parameters.batch_date}}
  source_base_path = /Volumes/workspace/default/airflow_json_landing/weather/bronze
  target_schema = silver

weather_silver_to_gold.sql
  batch_date = {{job.parameters.batch_date}}
  temperature_source_table = silver.weather_temperature_hourly
  rain_source_table = silver.weather_rain_hourly
  target_schema = gold
```

## Airflow Boundary

The Airflow side should only upload raw JSON to the Databricks Volume landing
path using the existing connection id `databricks_conn`.

Do not store Databricks credentials in this repository.
