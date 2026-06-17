# weather_db Data Governance Documentation

Generated from PostgreSQL metadata retrieved from the `weather_db` MCP server.

## Scope

This document includes database objects actively used by this repository and present in PostgreSQL.

In-scope object sources:

| Source | In-scope Objects |
| --- | --- |
| dbt source configuration | `public.staging_forecast`, `public.staging_alert` |
| dbt active models and model references | `public.stg_weather_forecast`, `public.stg_weather_alert`, `public.dim_city`, `public.dim_area`, `public.fact_weather_forecast`, `public.fact_weather_alert` |
| Airflow SQL references | `public.staging_forecast`, `public.staging_alert`, `public.etl_pipeline_state` |

Excluded from this document:

| Object | Exclusion Reason |
| --- | --- |
| PostgreSQL system schemas | Excluded by skill scope |
| `public.orders` | Not referenced by repository dbt models, Airflow DAGs, repository SQL files, or dbt source configuration |
| `public.dag_runs` | Not referenced by repository dbt models, Airflow DAGs, repository SQL files, or dbt source configuration |
| `dbt/weather_data_platform/models/retired/dim_city.sql` | Retired dbt model path is disabled in `dbt_project.yml` |

## Database Overview

| Item | Value |
| --- | --- |
| MCP server | `weather_db` |
| PostgreSQL database | `test` |
| Metadata source | `information_schema` via `weather_db` MCP server |
| In-scope schemas | 1 |
| In-scope database objects | 9 |
| In-scope base tables | 7 |
| In-scope views | 2 |

## Schema Inventory

| Schema | In-scope Objects | Base Tables | Views |
| --- | ---: | ---: | ---: |
| `public` | 9 | 7 | 2 |

## Table Inventory

| Schema | Object | Type | Repository Evidence |
| --- | --- | --- | --- |
| `public` | `dim_area` | BASE TABLE | Active dbt mart model |
| `public` | `dim_city` | BASE TABLE | dbt seed and active dbt model references |
| `public` | `etl_pipeline_state` | BASE TABLE | Airflow pipeline state SQL |
| `public` | `fact_weather_alert` | BASE TABLE | Active dbt mart model |
| `public` | `fact_weather_forecast` | BASE TABLE | Active dbt mart model |
| `public` | `staging_alert` | BASE TABLE | dbt source and Airflow staging SQL |
| `public` | `staging_forecast` | BASE TABLE | dbt source and Airflow staging SQL |
| `public` | `stg_weather_alert` | VIEW | Active dbt staging model |
| `public` | `stg_weather_forecast` | VIEW | Active dbt staging model |

## Data Lineage Summary

| Data Product | Upstream Objects | Transformation Evidence |
| --- | --- | --- |
| `public.stg_weather_forecast` | `public.staging_forecast` | `models/staging/stg_weather_forecast.sql` |
| `public.stg_weather_alert` | `public.staging_alert` | `models/staging/stg_weather_alert.sql` |
| `public.dim_area` | `public.stg_weather_alert` | `models/marts/dim_area.sql` |
| `public.fact_weather_forecast` | `public.stg_weather_forecast`, `public.dim_city` | `models/intermediate/eph_weather_forecast.sql`, `models/marts/fact_weather_forecast.sql` |
| `public.fact_weather_alert` | `public.stg_weather_alert`, `public.dim_city`, `public.dim_area` | `models/intermediate/eph_weather_alert.sql`, `models/marts/fact_weather_alert.sql` |
| `public.etl_pipeline_state` | Airflow DAG execution state | `dags/include/storage/pipeline_state.py` |

## Data Dictionary

### `public.dim_area`

| Column | Data Type | Nullable |
| --- | --- | --- |
| `area_key` | text | YES |
| `city` | text | YES |
| `area` | text | YES |

### `public.dim_city`

| Column | Data Type | Nullable |
| --- | --- | --- |
| `city_key` | text | YES |
| `city` | text | YES |

### `public.etl_pipeline_state`

| Column | Data Type | Nullable |
| --- | --- | --- |
| `pipeline_name` | character varying | NO |
| `last_processed_at` | timestamp with time zone | YES |
| `last_run_at` | timestamp with time zone | YES |
| `status` | character varying | YES |
| `row_count` | bigint | YES |
| `created_at` | timestamp with time zone | YES |
| `updated_at` | timestamp with time zone | YES |

### `public.fact_weather_alert`

| Column | Data Type | Nullable |
| --- | --- | --- |
| `city_key` | text | YES |
| `area_key` | text | YES |
| `weather_date` | date | YES |
| `event` | text | YES |
| `level` | text | YES |

### `public.fact_weather_forecast`

| Column | Data Type | Nullable |
| --- | --- | --- |
| `city_key` | text | YES |
| `weather_date` | date | YES |
| `avg_lowesttemp` | numeric | YES |
| `avg_highesttemp` | numeric | YES |
| `flag_highesttemp` | text | YES |

### `public.staging_alert`

| Column | Data Type | Nullable |
| --- | --- | --- |
| `event` | text | YES |
| `level` | text | NO |
| `area` | text | NO |
| `start_time` | timestamp without time zone | NO |
| `end_time` | timestamp without time zone | NO |
| `batch_time` | timestamp with time zone | NO |
| `update_time` | timestamp with time zone | YES |

### `public.staging_forecast`

| Column | Data Type | Nullable |
| --- | --- | --- |
| `city` | text | NO |
| `type` | text | NO |
| `start_time` | timestamp without time zone | NO |
| `end_time` | timestamp without time zone | NO |
| `temp` | integer | YES |
| `unit` | text | YES |
| `batch_time` | timestamp with time zone | NO |
| `update_time` | timestamp with time zone | YES |

### `public.stg_weather_alert`

| Column | Data Type | Nullable |
| --- | --- | --- |
| `event` | text | YES |
| `level` | text | YES |
| `area` | text | YES |
| `weather_date` | date | YES |

### `public.stg_weather_forecast`

| Column | Data Type | Nullable |
| --- | --- | --- |
| `city` | text | YES |
| `type` | text | YES |
| `weather_date` | date | YES |
| `temp` | integer | YES |

## Governance Notes

| Area | Repository Evidence |
| --- | --- |
| Data ownership | Airflow DAG owner is `charlenelin0` in `dags/weather_data_pipeline.py` |
| Data quality | dbt YAML defines selected `not_null` and `unique` tests for staging, dimension, and fact models |
| Pipeline state monitoring | `public.etl_pipeline_state` stores pipeline status, row count, last processed timestamp, and run timestamps |
| Batch control | Staging loaders delete existing rows for the current `batch_time` before appending replacement rows |
| Access controls | No repository-defined database grants or role policies were found in the in-scope files |
| Retention controls | No repository-defined PostgreSQL retention policy was found in the in-scope files |

## Known Gaps

| Gap | Evidence |
| --- | --- |
| Alert pipeline wiring should be verified | `dags/weather_data_pipeline.py` imports alert staging and BigQuery load functions, but current task wiring calls forecast staging/load functions for both temperature and rain paths |
| `public.dim_city` metadata differs from the dbt seed definition | PostgreSQL metadata currently shows `city_key` and `city`; `seeds/dim_city.csv` defines `city_id`, `city_name`, `country`, `latitude`, `longitude`, `timezone`, and `is_active` |
| Notion publication requires approval | The target Notion page `Airflow_docker Documentation` was found, but this document has not been appended to Notion |

## Notion Update Status

The Notion MCP server is available, and a page named `Airflow_docker Documentation` was found.

No Notion content has been created or modified. Per the skill workflow, publish this documentation to Notion only after user approval.
