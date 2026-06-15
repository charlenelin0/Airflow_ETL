
from include.api.weather_forecast.meteo_client import MeteoApiClient
from include.storage.bigquery_storage import BigQueryStorage
from include.constant import bigquery_project, bigquery_dataset

import panadas as pd 

meteoApi = MeteoApiClient()
bigquery = BigQueryStorage(
    bigquery_project,
    bigquery_dataset
)

def get_weather_info() -> None:

    sql = f"""
    SELECT latitude, longitude
      FROM dim_city
     WHERE is_active = 1
    """

    weather = []

    result = bigquery.get_date(sql)

    for row in result.itertuples(index=False):
        res_json = meteoApi.fetch(
            row.latitude,
            row.longitude
        )