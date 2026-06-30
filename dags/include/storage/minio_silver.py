
from datetime import datetime, timedelta, timezone
from tempfile import NamedTemporaryFile, mkdtemp
from include.config.constant import minio_conn_id, silver_bucket_name
from include.storage.minio_storage import MinioStorage
from pathlib import Path

import logging
import pandas as pd

silver_storage = MinioStorage(
    bucket_name = silver_bucket_name,
    aws_conn_id = minio_conn_id
)

def get_parquet_from_minio(object_name: str) -> Path:

    tmp_dir = Path(mkdtemp())
    silver_storage.download_file(
        object_name,
        str(tmp_dir)
    )

    return tmp_dir

def upload_parquet_to_minio(df: pd.DataFrame, batch_date: str, object_name: str) -> str:

    # create temp file & save into minio
    with NamedTemporaryFile(mode = 'wb', prefix = f'{batch_date}_', suffix = '.parquet') as f:

        # 0. get parameters
        temp_filename = f.name
        minio_filename = f'weather/etl_{object_name}_{batch_date}.parquet'

        # 1. create temp file
        df.to_parquet(f, engine = 'pyarrow')
        f.flush()
        logging.info('Save data into parquet file: %s', temp_filename)

        # 2. save into minio
        silver_storage.upload_file(
            temp_filename,
            minio_filename
        )
        
        return minio_filename

def delete_parquet_from_minio() -> None:

    delete_files = []

    keys = silver_storage.list_files()

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
        silver_storage.delete_files(delete_files)