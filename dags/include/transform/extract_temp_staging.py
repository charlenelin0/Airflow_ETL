
import pandas as pd
import logging

from pathlib import Path
from datetime import datetime
from include.storage.postgres_storage import PostgresStorage
from include.utils.file_utils import resolve_parquet_file

postgres_storage = PostgresStorage()

def insert_into_local_staging(tmp_dir: Path, batch_time: str) -> None:

    table_name = 'staging_forecast'

    # step 1: check postgressql db
    postgres_storage.execute(
        f"""
        CREATE TABLE IF NOT EXISTS public.{table_name} 
        (
            city TEXT,
            type TEXT,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            temp INT,
            unit TEXT,
            batch_time TIMESTAMPTZ,
            update_time TIMESTAMPTZ,
            PRIMARY KEY (city, type, start_time, end_time, batch_time)
        );
        """
    )
    logging.info(f"Check Table public.{table_name} exists.")

    # step 2: reset data
    postgres_storage.execute(
        f"""
        DELETE FROM public.{table_name}
        WHERE batch_time = %s
        """,
        (batch_time,)
    )
    logging.info(f"Reset public.{table_name}, batch_time: {batch_time}.")

    parquet_file = resolve_parquet_file(tmp_dir)
    df = pd.read_parquet(parquet_file)
    df.columns = [
        "city",
        "type",
        "start_time",
        "end_time",
        "temp",
        "unit"
    ]
    df['update_time'] = datetime.now()
    df['batch_time'] = pd.to_datetime(batch_time)

    engine = postgres_storage.get_sqlalchemy_engine()
    df.to_sql(
        table_name,
        con = engine,
        if_exists = "append",
        index = False,
        chunksize = 2000
    )
    logging.info(f"Insert data into public.{table_name}, batch_time: {batch_time}.")

if __name__ == "__main__":
    result = insert_into_local_staging()
    print("Finish")