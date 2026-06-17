
from datetime import datetime, timedelta, timezone
from tempfile import NamedTemporaryFile
from include.config.constant import minio_conn_id, bronze_bucket_name
from include.storage.minio_storage import MinioStorage

import logging
import json

bronze_storage = MinioStorage(
    bucket_name = bronze_bucket_name,
    aws_conn_id = minio_conn_id
)

def get_bronze_json(batch_date: str, object_name: str) -> list[dict]:
    
    files = bronze_storage.list_files(
        prefix = f'bronze/weather/{object_name}/date={batch_date}/',
        recursive = True
    )

    json_context = []
    for file_name in files:
        country = (
            file_name
            .split('/')[-1]
            .replace('country=', '')
            .replace('.json', '')
        )
        context = json.loads(get_json_file_context(file_name))
        context['country'] = country
        json_context.append(context)
    
    return json_context

def get_json_file_context(object_name: str) -> str:
    return bronze_storage.read_text(object_name)

def upload_json_to_minio(batch_date: str, data: dict, object_name: str, country: str) -> str:

    # create temp file & save into minio
    with NamedTemporaryFile(mode='w', prefix = f'{batch_date}_', suffix = '.json') as f:

        # 0. filenames
        temp_filename = f.name
        minio_filename = f'bronze/weather/{object_name}/date={batch_date}/country={country}.json'

        # 1. create temp file
        json.dump(data, f, indent=4)
        f.flush()
        logging.info('Save data into json file: %s', temp_filename)

        # 2. save into minio
        bronze_storage.upload_file(
            temp_filename,
            minio_filename
        )
        
        return minio_filename

def delete_json_from_minio() -> None:

    delete_files = []

    keys = bronze_storage.list_files()

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
        bronze_storage.delete_files(delete_files)