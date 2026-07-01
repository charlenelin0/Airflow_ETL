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
databricks_volume_weather_bronze_path = (
    "/Volumes/workspace/default/airflow_json_landing/weather/bronze"
)
