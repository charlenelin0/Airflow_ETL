
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

from airflow.sdk import get_current_context
from airflow.decorators import dag, task
from airflow.providers.smtp.notifications.smtp import send_smtp_notification

from include.api.weather_forecast.api_meteo import get_city_coordinates, get_weather_info

from include.storage.minio_bronze import get_bronze_json, upload_json_to_minio, delete_json_from_minio
from include.storage.minio_silver import get_parquet_from_minio, upload_parquet_to_minio, delete_parquet_from_minio
from include.storage.pipeline_state import update_pipeline_state, get_row_count

from include.transform.transform_temperature import trans_to_df as transform_temperature
from include.transform.transform_rain import trans_to_df as transform_rain
from include.transform.extract_temp_staging import insert_into_local_staging as extract_temp_staging
from include.transform.extract_temp_alert_staging import insert_into_local_staging as extract_temp_alert_staging
from include.transform.load_bigquery_alert import load_bigquery_alert as load_alert_bigquery
from include.transform.load_bigquery_forecast import load_bigquery_forecast as load_temp_bigquery

import subprocess
import shutil

def update_pipeline_failed(context) -> None:
    dag_id = context['dag'].dag_id
    update_pipeline_state(
        pipeline_name = dag_id,
        status = 'FAILED'
    )

default_args = {
    'owner': 'charlenelin0',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

failure_email = send_smtp_notification(
            from_email='example1@gmail.com',
            to = ['example2@gmail.com', 'example3@gmail.com'],
            subject = "[Airflow] DAG {{dag.dag_id}} Failed",
            html_content = """
            DAG: {{dag.dag_id}}<br>
            Task: {{ti.task_id}}<br>
            Run ID: {{run_id}}<br>
            Time: {{ ts }}<br>
            URL: <a href="{{ ti.log_url }}">Log</a>
            """
        )

@dag(
    dag_id = 'weather_data_pipeline',
    default_args = default_args,
    start_date = datetime(2026, 5, 24),
    schedule = '30 20-22 * * *',
    catchup = False,
    on_failure_callback = update_pipeline_failed
)
def weather_data_pipeline() -> None:

    @task()
    def update_pipeline_running() -> None:
        dag_id = get_current_context()['dag'].dag_id
        update_pipeline_state(
            pipeline_name = dag_id,
            status = 'RUNNING'
        )

    @task(
        multiple_outputs = True, 
        on_failure_callback = [failure_email]
    )
    def scrape_weatherforecast(variable) -> dict[str, str]:

        weather_variable = variable['weather_variable']

        object_name = variable['file_name_prefix']

        batch_date = get_current_context()['ds']

        # get data and push into bronze layer
        cities = get_city_coordinates()

        for city in cities:

            api_data = get_weather_info(
                latitude = city.latitude,
                longitude = city.longitude,
                weather_variable = weather_variable
            )
        
            file_name = upload_json_to_minio(
                batch_datetime = batch_date, 
                data = api_data, 
                object_name = object_name,
                country = city.country
            )

        return {'objectName': f'{object_name}'}

    @task()
    def trans_minio_silver_forecast(bronze) -> dict[str]:

        batch_date = get_current_context()['ds']

        object_name = bronze['objectName']

        # get file from bronze
        bronze_json = get_bronze_json(
            batch_date = batch_date,
            object_name = object_name
        )

        # transform
        TRANSFORM_MAPPING = {
            "temperature": transform_temperature,
            "rain": transform_rain
        }
        transform_func = TRANSFORM_MAPPING[object_name]
        # spark
        spark = (
            SparkSession
            .builder
            .appName('silver')
            .getOrCreate()
        )
        df = spark.createDataFrame(bronze_json)
        df = transform_func(df)

        # upload to minio silver
        filename = upload_parquet_to_minio(
            df = df,
            batch_date = batch_date, 
            object_name = object_name
        )

        return {'silverFile': f'{filename}'}

    @task()
    def ins_postgres_staging_forecast(silver) -> None:

        silver_filename = silver['silverFile']

        tmp_dir = get_parquet_from_minio(
            object_name = silver_filename
        )

        batch_datetime = get_current_context()['ts']

        try:
            extract_temp_staging(tmp_dir, batch_datetime)
        finally:
            shutil.rmtree(tmp_dir)

    @task()
    def ins_bigquery_staging_forecast() -> None:
        batch_datetime = get_current_context()['ts']
        load_temp_bigquery(batch_time = batch_datetime)

    @task(on_failure_callback=[failure_email])
    def run_dbt() -> None:
        subprocess.run(
            [
                "dbt",
                "run",
                "--project-dir",
                "/opt/airflow/dbt/weather_data_platform",
                "--profiles-dir",
                "/opt/airflow/dbt/profiles"
            ],
            check=True
        )
    
    @task()
    def update_pipeline_success() -> None:

        dag_id = get_current_context()['dag'].dag_id
        batch_datetime = get_current_context()['ts']

        row_count_fcst = get_row_count('staging_forecast', batch_datetime)
        row_count_alert = get_row_count('staging_alert', batch_datetime)

        update_pipeline_state(
            pipeline_name = dag_id,
            status = 'SUCCESS',
            row_count = (row_count_alert + row_count_fcst),
            last_processed_at = batch_datetime
        )

    @task()
    def cleanup_bronze_files() -> None:
        delete_json_from_minio()

    @task()
    def cleanup_silver_files() -> None:
        delete_parquet_from_minio()

    running = update_pipeline_running()

    # temperature
    bronzeForecast = scrape_weatherforecast(
        {
            'weather_variable': 'temperature_2m',
            'file_name_prefix': 'temperature'
        }
    )
    silverForecast = trans_minio_silver_forecast(bronzeForecast)
    stagingForecast = ins_postgres_staging_forecast(silverForecast)

    # rain
    bronzeRain = scrape_weatherforecast(
        {
            'weather_variable': 'rain',
            'file_name_prefix': 'rain'
        }
    )
    silverRain = trans_minio_silver_forecast(bronzeRain)
    stagingRain = ins_postgres_staging_forecast(silverRain)

    # bigquery
    bigqueryForecast = ins_bigquery_staging_forecast()
    bigqueryAlert = ins_bigquery_staging_forecast()

    dbt_job = run_dbt()

    success = update_pipeline_success()
    
    clean_bronze = cleanup_bronze_files()
    clean_silver = cleanup_silver_files()

    # start flow
    running >> [bronzeForecast, bronzeRain]

    # bronze -> silver -> postgres (staging)

    # postgres (staging) -> bigquery (warehouse)
    stagingForecast >> bigqueryForecast
    stagingRain >> bigqueryAlert
    [bigqueryForecast, bigqueryAlert] >> dbt_job

    # success & cleaing + reset
    dbt_job >> success
    success >> [clean_bronze, clean_silver]

weather_data_pipeline()