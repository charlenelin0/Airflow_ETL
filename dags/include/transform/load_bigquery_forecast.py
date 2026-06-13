
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

def load_bigquery_forecast(batch_time: str) -> None:

    table_name = 'staging_forecast'

    rows = postgres_storage.get_records(
        f"""
        SELECT city, type, start_time, end_time, temp, unit, batch_time
          FROM public.{table_name}
         WHERE batch_time = %s
        """,
        (batch_time,)
    )

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
    
    bigquery_storage.load_data(
        table_name,
        df
    )

if __name__ == "__main__":
    load_bigquery_forecast(batch_time='2026-06-01 17:56:20.000')