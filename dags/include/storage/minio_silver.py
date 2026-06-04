
from datetime import datetime, timedelta, timezone
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from tempfile import NamedTemporaryFile, TemporaryDirectory, mkdtemp
from include.config.constant import minio_conn_id, silver_bucket_name

import logging
import pandas as pd

def get_parquet_from_minio(object_name: str) -> str:

    tmp_dir = mkdtemp()

    s3_hook = S3Hook(aws_conn_id = minio_conn_id)
    s3_hook.download_file(
        key = object_name,
        bucket_name = silver_bucket_name,
        local_path = tmp_dir
    )
        
    return tmp_dir

def upload_parquet_to_minio(df: pd.DataFrame, batch_datetime: str, object_name: str) -> str:

    # create temp file & save into minio
    with NamedTemporaryFile(mode = 'wb', prefix = f'{batch_datetime}_', suffix = '.parquet') as f:

        # 0. get parameters
        temp_filename = f.name
        minio_filename = f'weather/etl_{object_name}_{batch_datetime}.parquet'

        # 1. create temp file
        df.to_parquet(f, engine = 'pyarrow')
        f.flush()
        logging.info('Save data into parquet file: %s', temp_filename)

        # 2. save into minio
        s3_hook = S3Hook(aws_conn_id = minio_conn_id)
        s3_hook.load_file(
            filename = temp_filename,
            key = minio_filename,
            bucket_name = silver_bucket_name,
            replace = True
        )
        logging.info('Parquet file %s has been pushed into S3', minio_filename)
        
        return minio_filename

def delete_parquet_from_minio() -> None:

    delete_files = []

    s3_hook = S3Hook(aws_conn_id = minio_conn_id)
    keys = s3_hook.list_keys(
        bucket_name = silver_bucket_name
    )

    cutoff_date = (
        datetime.now(timezone.utc) 
        - timedelta(days=2)
    )

    if keys is None:
        return

    for file in keys:
        time_stamp = file[-33:-8]
        time_df = datetime.fromisoformat(time_stamp)
        if time_df < cutoff_date:
            delete_files.append(file)

    if len(delete_files) > 0:
        s3_hook.delete_objects(
            bucket = silver_bucket_name,
            keys = delete_files
        )