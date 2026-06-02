
from include.api.weather_forecast.api_temp import request_api

import pandas as pd
import json

def trans_to_df(data: dict) -> pd.DataFrame:

    rows = []
    mapping = {
        'MinT': 'LowestTemp',
        'MaxT': 'HighestTemp'
    }

    # get info & save into csv
    for item in data['records']['location']:
        loc = item['locationName']
        weather = item['weatherElement']
        for element in weather:
            flag = mapping.get(
                element['elementName']
            )
            if flag is not None:
                for i in element['time']:
                    rows.append({
                        'City': loc,
                        'Type': flag,
                        'startTime': i['startTime'],
                        'endTime': i['endTime'],
                        'Temp': i['parameter']['parameterName'],
                        'Unit': i['parameter']['parameterUnit']
                    })

    df = pd.DataFrame(rows)
    
    return df

if __name__ == "__main__":
    sample_data = request_api()
    result = trans_json_to_df(sample_data)
    print(result)