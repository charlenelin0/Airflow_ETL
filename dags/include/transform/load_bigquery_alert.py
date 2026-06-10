
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from airflow.providers.postgres.hooks.postgres import PostgresHook
from include.config.constant import bigquery_json, bigquery_project, bigquery_dataset

def load_bigquery_alert(batch_time: str) -> None:

    table_name = 'staging_alert'

    hook = PostgresHook(postgres_conn_id = 'postgres_localhost')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT event, level, area, start_time, end_time, batch_time
          FROM public.{table_name}
         WHERE batch_time = %s
        """,
        (batch_time,)
    )
    rows = cursor.fetchall()

    df = pd.DataFrame(
        data=rows,
        columns=[
            "event",
            "level",
            "area",
            "start_time",
            "end_time",
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
    load_bigquery_alert(batch_time='2026-06-01 17:56:20.000')