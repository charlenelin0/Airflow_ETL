
from include.api.weather_forecast.api_temp_alert import request_api

import pandas as pd
import json

def trans_to_df(data: dict) -> pd.DataFrame:

    rows = []

    # get info & save into csv
    for item in data['records']['info']:

        start_time = item['onset']
        end_time = item['expires']
        event = item['event']

        parameter = item['parameter']
        for element in parameter:
            if element['valueName'] == 'alert_criteria':
                level = element['value']
        
        weather = item['area']
        for element in weather:
            if element is not None:
                rows.append({
                    'event': event,
                    'level': level,
                    'area': element['areaDesc'],
                    'start_time': start_time,
                    'end_time': end_time
                })

    df = pd.DataFrame(rows)
    
    return df

if __name__ == "__main__":
    sample_data = request_api()
    print(sample_data)
    result = trans_to_df(sample_data)
    print(result)