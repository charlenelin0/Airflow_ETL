
from google.cloud import bigquery
from include.config.constant import bigquery_json

import pandas as pd
import logging

class BigQueryStorage:

    def __init__(
        self,
        project: str,
        dataset: str
    ):
        self._client = bigquery.Client.from_service_account_json(bigquery_json)
        self._project = project
        self._dataset = dataset

    def load_data(
        self,
        table: str,
        dataframe: pd.DataFrame
    ) -> None:
        
        table_id = f"{self._project}.{self._dataset}.{table}"

        load_job = self._client.load_table_from_dataframe(
            dataframe,
            table_id
        )

        load_job.result()

        logging.info(
            "Loaded %s rows into Bigquery - %s",
            len(dataframe),
            table_id
        )