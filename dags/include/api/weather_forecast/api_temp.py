
from include.api.weather_forecast.cwa_client import CwaApiClient

api_cwa = CwaApiClient()

def request_api() -> dict:
    return api_cwa.fetch(
        endpoint = "F-C0032-001"
    )

if __name__ == "__main__":
    result = request_api()
    print(result)