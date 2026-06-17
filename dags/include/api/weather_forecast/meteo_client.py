
import requests

class MeteoApiClient:

    def __init__(self):
        self._base_url = "https://api.open-meteo.com/v1/forecast"

    def fetch(
        self,
        latitude: float,
        longitude: float,
        weather_variable: str,
        fcst_date: str
    ) -> dict:

        url = f'{self._base_url}?latitude={latitude}&={longitude}&hourly={weather_variable}&timezone=Asia%2FTokyo&start_date={fcst_date}&end_date={fcst_date}'
        response = requests.get(
            url,
            timeout = 60
        )
        response.raise_for_status()
        
        return request.json()