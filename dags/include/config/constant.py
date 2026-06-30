# MinIO
minio_conn_id = "minio_conn"
bronze_bucket_name = "bronze"
silver_bucket_name = "silver"

# Postgres
postgres_conn_id = "postgres_localhost"

# BigQuery
bigquery_json = "/opt/airflow/config/airflow-bigquery.json"
bigquery_project = "bq-test-499005"
bigquery_dataset = "weather_platform"

# Databricks
databricks_conn_id = "databricks_conn"
databricks_spark_version = "15.4.x-scala2.12"
databricks_node_type_id = "i3.xlarge"
databricks_num_workers = 1
databricks_run_name = "weather_spark_transform"
databricks_bronze_to_silver_task_key = "bronze_to_silver"
databricks_silver_to_gold_task_key = "silver_to_gold"
databricks_bronze_to_silver_notebook = "/Repos/weather/weather_bronze_to_silver"
databricks_silver_to_gold_notebook = "/Repos/weather/weather_silver_to_gold"
databricks_silver_schema = "silver"
databricks_gold_schema = "gold"
databricks_silver_table = "silver.weather_bronze_to_silver"
databricks_volume_weather_bronze_path = (
    "/Volumes/workspace/default/airflow_json_landing/weather/bronze"
)
