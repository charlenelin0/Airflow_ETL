
import requests
from airflow.models import Variable

def request_api() -> dict:
        
        api_code = "W-C0033-005"
        api_key = Variable.get("CWA_API_KEY")
        url = f'https://opendata.cwa.gov.tw/api/v1/rest/datastore/{api_code}'
        params = {
            'Authorization': api_key,
            'format': 'JSON',
            'expires': True
        }
        res = requests.get(url, params = params, verify = False)

        if res.status_code != 200:
            print(f'Unable to reach API. Status code: {res.status_code}')
            return None
        else:
            api_data = res.json()
            
        return api_data

if __name__ == "__main__":
    result = request_api()
    print(result)