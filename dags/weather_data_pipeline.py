from datetime import datetime, timedelta

from airflow.operators.python import get_current_context
from airflow.decorators import dag, task
from airflow.providers.smtp.notifications.smtp import send_smtp_notification

from include.api.weather_forecast.api_temp import request_api as get_temp_api
from include.api.weather_forecast.api_temp_alert import request_api as get_temp_alert_api

from include.storage.minio_bronze import get_json_from_minio, upload_json_to_minio
from include.storage.minio_silver import get_parquet_from_minio, upload_parquet_to_minio
from include.storage.pipeline_state import update_pipeline_state, get_row_count

from include.transform.transform_temp import trans_to_df as trans_temp
from include.transform.transform_temp_alert import trans_to_df as trans_temp_alert
from include.transform.extract_temp_staging import insert_into_local_staging as extract_temp_staging
from include.transform.extract_temp_alert_staging import insert_into_local_staging as extract_temp_alert_staging

import json
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
        context = get_current_context()
        dag_id = context['dag'].dag_id
        update_pipeline_state(
            pipeline_name = dag_id,
            status = 'RUNNING'
        )

    @task(
        multiple_outputs = True, 
        on_failure_callback = [failure_email]
    )
    def scrape_weatherforecast() -> dict[str, str]:

        object_name = "temperature"

        # call api get data
        api_data = get_temp_api()
        
        # get current flow time 
        context = get_current_context()
        batch_datetime = context['ts']

        # upload to minio bronze
        filename = upload_json_to_minio(
            batch_datetime = batch_datetime, 
            data = api_data, 
            object_name = object_name
        )

        return {
            'bronzeFile': f'{filename}',
            'objectName': f'{object_name}',
        }

    @task()
    def trans_minio_silver_forecast(bronze) -> dict[str]:

        bronze_filename = bronze['bronzeFile']

        # get current flow time 
        context = get_current_context()
        batch_datetime = context['ts']

        # get file from bronze
        json_str = get_json_from_minio(
            object_name = bronze_filename
        )

        # convert into python dict
        data = json.loads(json_str)

        # trans json into dataframe
        df = trans_temp(data)

        # upload to minio silver
        object_name = bronze['objectName']
        filename = upload_parquet_to_minio(
            df = df,
            batch_datetime = batch_datetime, 
            object_name = object_name
        )

        return {'silverFile': f'{filename}'}

    @task()
    def ins_postgres_staging_forecast(silver) -> None:

        silver_filename = silver['silverFile']

        tmp_dir = get_parquet_from_minio(
            object_name = silver_filename
        )

        # get current flow time 
        context = get_current_context()
        batch_datetime = context['ts']

        try:
            extract_temp_staging(tmp_dir, batch_datetime)
        finally:
            shutil.rmtree(tmp_dir)
        
    @task(multiple_outputs = True)
    def scrape_hightempalert() -> dict[str, str]:

        object_name = "temperature_alert"

        # call api get data
        api_data = get_temp_alert_api()
        
        # get flow time 
        context = get_current_context()
        batch_datetime = context['ts']

        # upload to minio bronze
        filename = upload_json_to_minio(
            batch_datetime = batch_datetime,
            data = api_data, 
            object_name = object_name
        )

        return {
            'bronzeFile': f'{filename}', 
            'objectName': f'{object_name}'
        }

    @task()
    def trans_minio_silver_alert(bronze) -> dict[str]:

        bronze_filename = bronze['bronzeFile']

        # get flow time
        context = get_current_context()
        batch_datetime = context['ts']

        # get file from bronze
        json_str = get_json_from_minio(
            object_name = bronze_filename
        )

        # convert into python dict
        data = json.loads(json_str)

        # trans json into dataframe
        df = trans_temp_alert(data)

        # upload to minio silver
        objectName = bronze['objectName']
        filename = upload_parquet_to_minio(
            df = df,
            batch_datetime = batch_datetime, 
            object_name = objectName
        )

        return {'silverFile': f'{filename}'}

    @task()
    def ins_postgres_staging_alert(silver) -> None:

        silver_filename = silver['silverFile']

        tmp_dir = get_parquet_from_minio(
            object_name = silver_filename
        )

        # get flow time
        context = get_current_context()
        batch_datetime = context['ts']

        try:
            extract_temp_alert_staging(tmp_dir, batch_datetime)
        finally:
            shutil.rmtree(tmp_dir)

    @task(on_failure_callback=[failure_email])
    def run_dbt() -> None:
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
    
    @task()
    def update_pipeline_success() -> None:
        context = get_current_context()
        dag_id = context['dag'].dag_id
        batch_datetime = context['ts']

        row_count_fcst = get_row_count('staging_forecast', batch_datetime)
        row_count_alert = get_row_count('staging_alert', batch_datetime)

        update_pipeline_state(
            pipeline_name = dag_id,
            status = 'SUCCESS',
            row_count = (row_count_alert + row_count_fcst),
            last_processed_at = batch_datetime
        )

    running = update_pipeline_running()

    # weather forecast
    bronzeForecast = scrape_weatherforecast()
    silverForecast = trans_minio_silver_forecast(bronzeForecast)
    stagingForecast = ins_postgres_staging_forecast(silverForecast)

    # high temperature alert
    bronzeAlert = scrape_hightempalert()
    silverAlert = trans_minio_silver_alert(bronzeAlert)
    stagingAlert = ins_postgres_staging_alert(silverAlert)

    dbt_job = run_dbt()

    success = update_pipeline_success()

    running >> [bronzeForecast, bronzeAlert]

    [stagingAlert, stagingForecast] >> dbt_job >> success

weather_data_pipeline()