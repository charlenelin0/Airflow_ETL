from __future__ import annotations

from include.config.constant import (
    databricks_bronze_to_silver_notebook,
    databricks_bronze_to_silver_task_key,
    databricks_conn_id,
    databricks_gold_schema,
    databricks_node_type_id,
    databricks_num_workers,
    databricks_run_name,
    databricks_silver_schema,
    databricks_silver_table,
    databricks_silver_to_gold_notebook,
    databricks_silver_to_gold_task_key,
    databricks_spark_version,
)


DATABRICKS_CONN_ID = databricks_conn_id


def build_weather_transform_params(
    batch_date: str,
    batch_time: str,
    source_path: str,
) -> dict[str, str]:
    return {
        "batch_date": batch_date,
        "batch_time": batch_time,
        "source_path": source_path,
    }


def build_weather_submit_run_payload(
    batch_date: str,
    batch_time: str,
    source_path: str,
) -> dict:
    params = build_weather_transform_params(
        batch_date=batch_date,
        batch_time=batch_time,
        source_path=source_path,
    )

    cluster = {
        "spark_version": databricks_spark_version,
        "node_type_id": databricks_node_type_id,
        "num_workers": databricks_num_workers,
    }

    return {
        "run_name": databricks_run_name,
        "tasks": [
            {
                "task_key": databricks_bronze_to_silver_task_key,
                "new_cluster": cluster,
                "notebook_task": {
                    "notebook_path": databricks_bronze_to_silver_notebook,
                    "base_parameters": {
                        **params,
                        "target_schema": databricks_silver_schema,
                    },
                },
            },
            {
                "task_key": databricks_silver_to_gold_task_key,
                "depends_on": [
                    {"task_key": databricks_bronze_to_silver_task_key}
                ],
                "new_cluster": cluster,
                "notebook_task": {
                    "notebook_path": databricks_silver_to_gold_notebook,
                    "base_parameters": {
                        "batch_date": batch_date,
                        "source_table": databricks_silver_table,
                        "target_schema": databricks_gold_schema,
                    },
                },
            },
        ],
    }
