
import pandas as pd
import logging

from pathlib import Path
from datetime import datetime
from include.storage.postgres_storage import PostgresStorage
from include.utils.file_utils import resolve_parquet_file

postgres_storage = PostgresStorage()

def insert_into_local_staging(tmp_dir: Path, batch_time: str) -> None:

    table_name = 'staging_alert'

    postgres_storage.execute(
        f"""
        CREATE TABLE IF NOT EXISTS public.{table_name}
        (
            event TEXT,
            level TEXT,
            area TEXT,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            batch_time TIMESTAMPTZ,
            update_time TIMESTAMPTZ,
            PRIMARY KEY (area, level, start_time, end_time, batch_time)
        );
        """
    )
    logging.info(f"Check Table public.{table_name} exists.")

    # reset
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
        "event",
        "level",
        "area",
        "start_time",
        "end_time"
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