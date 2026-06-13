
from include.api.weather_forecast.cwa_client import CwaApiClient

api_cwa = CwaApiClient()

def request_api() -> dict:
    return api_cwa.fetch(
        endpoint = "W-C0033-005",
        extra_params = {
            'expires': True
        }
    )

if __name__ == "__main__":
    result = request_api()
    print(result)