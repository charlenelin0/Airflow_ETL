# Databricks Spark Transform Layer

This folder contains the planned Databricks assets for the weather data pipeline.

The initial goal is to add Databricks as a Spark transform layer without changing
the existing Airflow, Postgres, BigQuery, or dbt flow.

## Planned Flow

```text
Airflow scrape tasks
  -> bronze weather files
  -> Databricks Spark transform
  -> Delta silver/gold tables
  -> downstream validation or warehouse loading
```

## Folder Layout

```text
databricks/
  notebooks/
    weather_bronze_to_silver.py
    weather_silver_to_gold.py
  jobs/
    weather_transform_job.json
```

## MVP Scope

1. Trigger a Databricks job from Airflow.
2. Pass `batch_date` and `batch_time` as job parameters.
3. Read weather bronze files from a Databricks-readable location.
4. Transform records with Spark.
5. Write Delta tables for silver and gold layers.

## Airflow Connection

The Airflow side should use the existing Databricks connection id:

```text
databricks_conn
```

The connection should contain the Databricks workspace host and token/password.
Do not store Databricks credentials in this repository.
