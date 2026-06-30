# Databricks Parallel Bronze Landing SA

## 1. Project Goal

This phase adds a parallel Databricks bronze landing path to the existing
Airflow weather pipeline.

The current pipeline remains unchanged:

```text
API
  -> MinIO bronze
  -> local Spark / pandas transform
  -> Postgres staging
  -> BigQuery staging
  -> dbt
```

The new parallel path is:

```text
API
  -> Databricks Volume bronze landing
  -> Databricks platform bronze-to-silver processing
```

Airflow will continue to call the API once. The same raw API response will be
written to both MinIO and Databricks.

## 2. Design Principles

- Airflow is responsible only for API calls, MinIO bronze writes, and
  Databricks bronze landing uploads.
- Databricks is responsible for reading bronze JSON, running Spark transforms,
  and writing silver Delta tables.
- The existing pipeline must not be blocked by Databricks landing failures in
  this first version.
- Databricks credentials must be read from the Airflow connection
  `databricks_conn`.
- No Databricks credentials should be committed to the repository.

## 3. Selected Options

| Area | Decision |
| --- | --- |
| Upload method | Databricks REST API / Files API |
| Bronze file layout | Date partition with object-type subfolders |
| Upload failure behavior | Log warning and continue the existing pipeline |
| Upload location in Airflow | Inside `scrape_weatherforecast()` |
| Path configuration | `dags/include/config/constant.py` |
| Bronze-to-silver trigger | Not triggered by Airflow in this version |
| Credential source | Airflow connection `databricks_conn` |
| File content | Raw API response string |

## 4. Databricks Volume Landing Path

The Databricks Volume path is:

```text
/Volumes/workspace/default/airflow_json_landing/weather/bronze/date={{ ds }}/
```

At runtime, Airflow renders `{{ ds }}` as the DAG run date:

```text
/Volumes/workspace/default/airflow_json_landing/weather/bronze/date=2026-06-30/
```

The target file layout is:

```text
/Volumes/workspace/default/airflow_json_landing/weather/bronze/date=2026-06-30/
  temperature/
    country=Taiwan.json
    country=Japan.json
  rain/
    country=Taiwan.json
    country=Japan.json
```

The path format is:

```text
{base_path}/date={batch_date}/{object_name}/country={safe_country}.json
```

The configured base path should be:

```text
/Volumes/workspace/default/airflow_json_landing/weather/bronze
```

## 5. Planned Airflow Changes

Add a new helper file:

```text
dags/include/databricks/upload_json.py
```

The helper should expose:

```python
upload_json_to_databricks_volume(
    batch_date: str,
    object_name: str,
    country: str,
    data: str,
) -> str
```

The helper responsibilities are:

- Read Databricks host and token/password from `databricks_conn`.
- Upload raw JSON through the Databricks Files API.
- Write files into the Databricks Volume bronze landing path.
- Return the uploaded Volume path.
- Log a warning on upload failure without raising an exception.

Update:

```text
dags/include/config/constant.py
```

Add:

```python
databricks_volume_weather_bronze_path = (
    "/Volumes/workspace/default/airflow_json_landing/weather/bronze"
)
```

Update:

```text
dags/weather_data_pipeline.py
```

Inside `scrape_weatherforecast()`, after the MinIO upload, also upload the same
raw API response to Databricks:

```text
api_data = get_weather_info(...)

upload_json_to_minio(...)

upload_json_to_databricks_volume(
    batch_date=batch_date,
    object_name=object_name,
    country=city.country,
    data=api_data,
)
```

The existing return value should stay unchanged:

```python
return {"objectName": object_name}
```

## 6. DAG Dependency Design

This version does not add a Databricks transform task to the DAG.

Reason:

- Airflow only lands raw data in Databricks.
- Bronze-to-silver processing is owned by the Databricks platform.
- Databricks failures should not block the current Airflow pipeline in the first
  version.

The existing DAG dependencies remain unchanged.

The scrape task behavior becomes:

```text
scrape_weatherforecast
  -> write MinIO bronze
  -> write Databricks Volume bronze
  -> return objectName
```

The existing downstream path remains:

```text
trans_minio_silver_forecast
  -> ins_postgres_staging
  -> ins_bigquery_staging
  -> dbt
```

The Databricks platform path is independent:

```text
Databricks job / notebook
  -> read /Volumes/.../weather/bronze/date=...
  -> transform bronze to silver
```

## 7. Databricks Files API Design

The upload helper should use the Airflow Databricks connection:

```text
databricks_conn
```

Conceptual flow:

```text
Airflow task
  -> BaseHook.get_connection("databricks_conn")
  -> read host and token/password
  -> call Databricks Files API
  -> write raw JSON to Volume path
```

The helper should avoid hard-coded credentials.

Implementation notes:

- Use the connection password as the token when available.
- Optionally support token values from connection extras later.
- Strip trailing `/` from the host before building API URLs.
- Sanitize `country` before using it in the output filename.

## 8. Upload Failure Policy

The first version should use this behavior:

```text
Databricks upload failure -> log warning -> continue existing pipeline
```

Reason:

- Databricks is a parallel transform track in this phase.
- The current pipeline should keep working while the Databricks path stabilizes.
- Once stable, the policy can be changed to fail the DAG or alert explicitly.

## 9. Databricks Platform Responsibility

Databricks notebooks or jobs should handle:

1. Read the date partition:

   ```text
   /Volumes/workspace/default/airflow_json_landing/weather/bronze/date=YYYY-MM-DD/
   ```

2. Parse object-type folders:

   ```text
   temperature/
   rain/
   ```

3. Read raw JSON files.
4. Transform data with Spark.
5. Add metadata columns:

   ```text
   batch_date
   processed_at
   source_file
   ```

6. Write silver Delta tables:

   ```text
   silver.weather_temperature_hourly
   silver.weather_rain_hourly
   ```

## 10. Success Criteria

This phase is complete when:

- The existing Airflow DAG main flow still works.
- API responses are still written to MinIO.
- The same API responses are written to Databricks Volume.
- Databricks upload failures log warnings and do not block the current pipeline.
- Databricks Volume contains files under:

  ```text
  /Volumes/workspace/default/airflow_json_landing/weather/bronze/date=YYYY-MM-DD/temperature/
  /Volumes/workspace/default/airflow_json_landing/weather/bronze/date=YYYY-MM-DD/rain/
  ```

- Databricks notebooks can read the raw JSON from the Volume path.

## 11. Out of Scope

This version does not include:

- Adding `DatabricksSubmitRunOperator` to the DAG.
- Triggering Databricks notebooks from Airflow.
- Running silver transforms in Airflow.
- Changing dbt.
- Changing Postgres staging.
- Changing BigQuery loading.
- Removing MinIO.
- Changing existing DAG dependencies.
- Committing Databricks credentials.

## 12. Future Evolution

After bronze landing is stable, the project can add:

- Databricks bronze-to-silver notebooks.
- Databricks job scheduling.
- Row-count validation.
- Schema validation.
- Comparison between local silver and Databricks silver output.
- Optional Airflow-triggered Databricks jobs.
- Optional dbt-databricks integration.
- Optional migration from local transform output to Databricks Delta output.

## 13. Planned Implementation Checklist

When approved, implement:

- Add `databricks_volume_weather_bronze_path` to
  `dags/include/config/constant.py`.
- Add `dags/include/databricks/upload_json.py`.
- Implement upload using `databricks_conn` and the Databricks Files API.
- Log warning and continue when upload fails.
- Import the upload helper in `dags/weather_data_pipeline.py`.
- Add Databricks bronze upload inside `scrape_weatherforecast()`.
- Keep the existing return value and DAG dependencies unchanged.

