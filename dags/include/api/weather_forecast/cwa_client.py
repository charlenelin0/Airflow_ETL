
import requests
from airflow.models import Variable

class CwaApiClient:

    def __init__(self):
        self._api_key = Variable.get("CWA_API_KEY")
        self._base_url = 'https://opendata.cwa.gov.tw/api/v1/rest/datastore'

    def fetch(
        self,
        endpoint: str,
        extra_params: dict | None = None
    ) -> dict:

        url = f'{self._base_url}/{endpoint}'

        request_params = {
            'Authorization': self._api_key,
            'format': 'JSON'
        }
        if extra_params:
            request_params.update(extra_params)

        res = requests.get(
            url,
            params = request_params,
            timeout = 30,
            verify = False
        )
        res.raise_for_status()

        return res.json()