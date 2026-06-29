# Databricks Airflow Helpers

This package is reserved for small helper functions used by Airflow tasks that
trigger Databricks jobs.

Current scope:

- Build Databricks job parameters.
- Use Databricks settings from `include.config.constant`.
- Keep Databricks-specific Airflow helper code separate from transform logic.
- Avoid changing the existing weather DAG until the Databricks connection and
  job are ready.

## Airflow Usage

The project expects an Airflow Databricks connection with this id:

```text
databricks_conn
```

The actual value is defined in `include.config.constant.databricks_conn_id`.

Example DAG usage:

```python
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

from include.databricks.job_params import (
    DATABRICKS_CONN_ID,
    build_weather_submit_run_payload,
)

databricks_transform = DatabricksSubmitRunOperator(
    task_id="databricks_weather_transform",
    databricks_conn_id=DATABRICKS_CONN_ID,
    json=build_weather_submit_run_payload(
        batch_date="{{ ds }}",
        batch_time="{{ ts }}",
        source_path="{{ var.value.weather_databricks_source_path }}",
    ),
)
```
