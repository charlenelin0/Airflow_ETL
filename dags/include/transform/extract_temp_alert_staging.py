
import pandas as pd
import logging
from datetime import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook

def insert_into_local_staging(file_path: str, batch_time: str) -> None:

    table_name = 'staging_alert'

    # step 1: check postgressql db
    hook = PostgresHook(postgres_conn_id = 'postgres_localhost')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(
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
    conn.commit()
    logging.info(f"Check Table public.{table_name} exists.")

    # step 2: reset data
    cursor.execute(
        f"""
        DELETE FROM public.{table_name}
        WHERE batch_time = %s
        """,
        (batch_time,)
    )
    conn.commit()
    logging.info(f"Reset public.{table_name}, batch_time: {batch_time}.")

    df = pd.read_parquet(file_path)
    df.columns = [
        "event",
        "level",
        "area",
        "start_time",
        "end_time"
    ]
    df['update_time'] = datetime.now()
    df['batch_time'] = pd.to_datetime(batch_time)

    engine = hook.get_sqlalchemy_engine()

    df.to_sql(
        table_name,
        con = engine,
        if_exists = "append",
        index = False,
        chunksize = 2000
    )
    logging.info(f"Insert data into public.{table_name}, batch_time: {batch_time}.")

    cursor.close()
    conn.close()

    return None

if __name__ == "__main__":
    result = insert_into_local_staging()
    print("Finish")