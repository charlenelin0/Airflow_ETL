
import pandas as pd

def trans_to_df(raw_data: str) -> pd.DataFrame:

    df = pd.DataFrame(raw_data)

    df['hourly_time'] = df['hourly'].apply(lambda x: x['time'])
    df['hourly_temp'] = df['hourly'].apply(lambda x: x['temperature_2m'])

    df_filtered = df[['latitude', 'longitude', 'country', 'hourly_time', 'hourly_temp']]
    df_final = df_filtered.explode(['hourly_time', 'hourly_temp']).reset_index(drop=True)

    return df_final