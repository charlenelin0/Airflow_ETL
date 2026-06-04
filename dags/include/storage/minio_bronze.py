
from datetime import datetime, timedelta, timezone
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from tempfile import NamedTemporaryFile
from include.config.constant import minio_conn_id, bronze_bucket_name

import logging
import json

def get_json_from_minio(object_name: str) -> str:

    s3_hook = S3Hook(aws_conn_id = minio_conn_id)
    json_str = s3_hook.read_key(
        key = object_name,
        bucket_name = bronze_bucket_name
    )
    
    return json_str

def upload_json_to_minio(batch_datetime: str, data: dict, object_name: str) -> str:

    # create temp file & save into minio
    with NamedTemporaryFile(mode='w', prefix = f'{batch_datetime}_', suffix = '.json') as f:

        # 0. filenames
        temp_filename = f.name
        minio_filename = f'weather/{object_name}_{batch_datetime}.json'

        # 1. create temp file
        json.dump(data, f, indent=4)
        f.flush()
        logging.info('Save data into json file: %s', temp_filename)

        # 2. save into minio
        s3_hook = S3Hook(aws_conn_id = minio_conn_id)
        s3_hook.load_file(
            filename = temp_filename,
            key = minio_filename,
            bucket_name = bronze_bucket_name,
            replace = True
        )
        logging.info('Json file %s has been pushed into S3', minio_filename)
        
        return minio_filename

def delete_json_from_minio() -> None:

    delete_files = []

    s3_hook = S3Hook(aws_conn_id = minio_conn_id)
    keys = s3_hook.list_keys(
        bucket_name = bronze_bucket_name
    )

    cutoff_date = (
        datetime.now(timezone.utc) 
        - timedelta(days=2)
    )

    if keys is None:
        return

    for file in keys:
        time_stamp = file[-30:-5]
        time_df = datetime.fromisoformat(time_stamp)
        if time_df < cutoff_date:
            delete_files.append(file)

    if len(delete_files) > 0:
        s3_hook.delete_objects(
            bucket = bronze_bucket_name,
            keys = delete_files
        )