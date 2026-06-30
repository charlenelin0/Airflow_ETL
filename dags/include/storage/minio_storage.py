
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import logging

class MinioStorage:

    def __init__(
        self,
        bucket_name: str,
        aws_conn_id: str
    ):
        self._bucket_name = bucket_name
        self._s3_hook = S3Hook(aws_conn_id = aws_conn_id)

    def read_text(
        self,
        object_name: str
    ) -> str:

        return self._s3_hook.read_key(
            key = object_name,
            bucket_name = self._bucket_name
        )

    def download_file(
        self,
        object_name: str,
        local_path: str
    ) -> None:
        
        self._s3_hook.download_file(
            key = object_name,
            bucket_name = self._bucket_name,
            local_path = local_path
        )

        logging.info(
            'File %s has been downloaded from MinIO S3 - bucket %s', 
            object_name,
            self._bucket_name
        )

    def upload_file(
        self,
        local_file: str,
        object_name: str,
        overwrite: bool = True
    ) -> None:

        self._s3_hook.load_file(
            filename = local_file,
            key = object_name,
            bucket_name = self._bucket_name,
            replace = overwrite
        )

        logging.info(
            'File %s has been uploaded to MinIO S3 - bucket %s', 
            object_name,
            self._bucket_name
        )

    def list_files(
        self,
        prefix: str | None = None
    ) -> list[str]:

        return self._s3_hook.list_keys(
            bucket_name = self._bucket_name,
            prefix = prefix
        ) or []

    def delete_files(
        self,
        object_names: list[str]
    ) -> None:
        
        self._s3_hook.delete_objects(
            bucket = self._bucket_name,
            keys = object_names
        )

        logging.info(
            'Deleted %s file(s) from MinIO S3 - bucket %s', 
            len(object_names),
            self._bucket_name
        )