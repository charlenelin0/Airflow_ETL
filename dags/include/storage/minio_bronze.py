
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from tempfile import NamedTemporaryFile

import logging
import json

def get_json_from_minio(object_name: str) -> str:

    s3_hook = S3Hook(aws_conn_id = 'minio_conn')
    json_str = s3_hook.read_key(
        key = object_name,
        bucket_name = 'bronze'
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
        s3_hook = S3Hook(aws_conn_id = 'minio_conn')
        s3_hook.load_file(
            filename = temp_filename,
            key = minio_filename,
            bucket_name = 'bronze',
            replace = True
        )
        logging.info('Json file %s has been pushed into S3', minio_filename)
        
        return minio_filename