from datetime import datetime, timedelta

from airflow.operators.python import get_current_context
from airflow.decorators import dag, task
from airflow.providers.smtp.notifications.smtp import send_smtp_notification

from include.api.weather_forecast.api_temp import request_api as get_temp_api
from include.api.weather_forecast.api_temp_alert import request_api as get_temp_alert_api

from include.storage.minio_bronze import get_json_from_minio, upload_json_to_minio
from include.storage.minio_silver import get_parquet_from_minio, upload_parquet_to_minio

from include.transform.transform_temp import trans_to_df as trans_temp
from include.transform.transform_temp_alert import trans_to_df as trans_temp_alert
from include.transform.extract_temp_staging import insert_into_local_staging as extract_temp_staging
from include.transform.extract_temp_alert_staging import insert_into_local_staging as extract_temp_alert_staging

import json
import subprocess
import shutil

default_args = {
    'owner': 'charlenelin0',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

failure_email = send_smtp_notification(
            from_email='dog062106@gmail.com',
            to = ['clin0621@gmail.com', 'linjpcharlene@gmail.com'],
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
    schedule = '30 20-22 * * *'
)

def weather_data_pipeline():

    @task(multiple_outputs = True, on_failure_callback=[failure_email])
    def scrape_weatherforecast():

        # call api get data
        api_data = get_temp_api()
        
        # get current flow time 
        context = get_current_context()
        ts = context['ts']

        # upload to minio bronze
        filename = upload_json_to_minio(ts, api_data, "temperature")

        return {
            'bronzeFile': f'{filename}',
            'batchDatetime': f'{ts}'
            }

    @task()
    def trans_minio_silver_forecast(bronze):

        bronze_filename = bronze['bronzeFile']
        batch_datetime = bronze['batchDatetime']

        # get file from bronze
        json_str = get_json_from_minio(
            object_name = bronze_filename
        )

        # convert into python dict
        data = json.loads(json_str)

        # trans json into dataframe
        df = trans_temp(data)

        # upload to minio silver
        filename = upload_parquet_to_minio(
            df = df,
            batch_datetime = batch_datetime, 
            object_name = "temperature"
        )

        return {
            'silverFile': f'{filename}',
            'batchDatetime': f'{batch_datetime}'
        }

    @task()
    def ins_postgres_staging_forecast(silver):

        silver_filename = silver['silverFile']
        batch_datetime = silver['batchDatetime']

        tmp_dir = get_parquet_from_minio(
            object_name = silver_filename
        )

        try:
            extract_temp_staging(tmp_dir, batch_datetime)
        finally:
            shutil.rmtree(tmp_dir)

        return {
            'batchDatetime': f'{batch_datetime}'
        }
        
    @task(multiple_outputs = True, on_failure_callback=[failure_email])
    def scrape_hightempalert():

        # call api get data
        api_data = get_temp_alert_api()
        
        # get current flow time 
        context = get_current_context()
        ts = context['ts']

        # upload to minio bronze
        filename = upload_json_to_minio(ts, api_data, "temperature_alert")

        return {
            'bronzeFile': f'{filename}',
            'batchDatetime': f'{ts}'
            }

    @task()
    def trans_minio_silver_alert(bronze):

        bronze_filename = bronze['bronzeFile']
        batch_datetime = bronze['batchDatetime']

        # get file from bronze
        json_str = get_json_from_minio(
            object_name = bronze_filename
        )

        # convert into python dict
        data = json.loads(json_str)

        # trans json into dataframe
        df = trans_temp_alert(data)

        # upload to minio silver
        filename = upload_parquet_to_minio(
            df = df,
            batch_datetime = batch_datetime, 
            object_name = "temperature_alert"
        )

        return {
            'silverFile': f'{filename}',
            'batchDatetime': f'{batch_datetime}'
        }

    @task()
    def ins_postgres_staging_alert(silver):

        silver_filename = silver['silverFile']
        batch_datetime = silver['batchDatetime']

        tmp_dir = get_parquet_from_minio(
            object_name = silver_filename
        )

        try:
            extract_temp_alert_staging(tmp_dir, batch_datetime)
        finally:
            shutil.rmtree(tmp_dir)

        return {
            'batchDatetime': f'{batch_datetime}'
        }

    @task(on_failure_callback=[failure_email])
    def run_dbt():

        subprocess.run(
            [
                "dbt",
                "run",
                "--project-dir",
                "/opt/airflow/dbt/jaffle_shop",
                "--profiles-dir",
                "/opt/airflow/dbt/profiles"
            ],
            check=True
        )

        return "dbt run successfully"

    bronzeForecast = scrape_weatherforecast()
    silverForecast = trans_minio_silver_forecast(bronzeForecast)
    stagingForecast = ins_postgres_staging_forecast(silverForecast)

    bronzeAlert = scrape_hightempalert()
    silverAlert = trans_minio_silver_alert(bronzeAlert)
    stagingAlert = ins_postgres_staging_alert(silverAlert)

    dbt_job = run_dbt()

    [stagingAlert, stagingForecast] >> dbt_job

weather_data_pipeline()