
import pandas as pd
from datetime import datetime
from include.config.constant import bigquery_project, bigquery_dataset
from include.storage.postgres_storage import PostgresStorage
from include.storage.bigquery_storage import BigQueryStorage

postgres_storage = PostgresStorage()
bigquery_storage = BigQueryStorage(
    project = bigquery_project,
    dataset = bigquery_dataset
)

def load_bigquery_alert(batch_time: str) -> None:

    table_name = 'staging_alert'

    rows = postgres_storage.get_records(
        f"""
        SELECT event, level, area, start_time, end_time, batch_time
          FROM public.{table_name}
         WHERE batch_time = %s
        """,
        (batch_time,)
    )

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

    bigquery_storage.load_data(
        table_name,
        df
    )

if __name__ == "__main__":
    load_bigquery_alert(batch_time='2026-06-01 17:56:20.000')