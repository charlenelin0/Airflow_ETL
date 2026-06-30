
from include.api.weather_forecast.meteo_client import MeteoApiClient
from include.storage.bigquery_storage import BigQueryStorage
from include.config.constant import bigquery_project, bigquery_dataset
from include.models.city_coordinate import City

import pandas as pd 

meteoApi = MeteoApiClient()
bigquery = BigQueryStorage(
    bigquery_project,
    bigquery_dataset
)

def get_city_coordinates() -> list[City]:
    
    sql = f"""
    SELECT country, latitude, longitude
      FROM `{bigquery_project}.{bigquery_dataset}.dim_city`
     WHERE is_active = 1
    """
    
    df = bigquery.get_data(sql)

    return [
        City(
            country = row.country,
            latitude = row.latitude,
            longitude = row.longitude
        )
        for row in df.itertuples()
    ]

def get_weather_info(latitude: float, longitude: float, weather_variable: str, batch_date: str) -> str:
    return meteoApi.fetch(
        latitude,
        longitude,
        weather_variable,
        batch_date
    )