
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from airflow.providers.postgres.hooks.postgres import PostgresHook
from include.config.constant import bigquery_json, bigquery_project, bigquery_dataset

def load_bigquery_forecast(batch_time: str) -> None:

    table_name = 'staging_forecast'

    hook = PostgresHook(postgres_conn_id = 'postgres_localhost')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT city, type, start_time, end_time, temp, unit, batch_time
          FROM public.{table_name}
         WHERE batch_time = %s
        """,
        (batch_time,)
    )
    rows = cursor.fetchall()

    df = pd.DataFrame(
        data=rows,
        columns=[
            "city",
            "type",
            "start_time",
            "end_time",
            "temp",
            "unit",
            "batch_time"
        ]
    )
    df["update_time"] = datetime.now()
    
    client = bigquery.Client.from_service_account_json(bigquery_json)
    bigquery_table_id = f"{bigquery_project}.{bigquery_dataset}.{table_name}"
    errors = client.load_table_from_dataframe(
        df,
        bigquery_table_id
    )

    if errors:
        print(errors)
    else:
        print(f"Inserted {len(df)} rows")

if __name__ == "__main__":
    load_bigquery_forecast(batch_time='2026-06-01 17:56:20.000')