
from pyspark.sql import (
    DataFrame, 
    arrays_zip, 
    explode,
    col
)

def trans_to_df(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn(
            "weather",
            explode(
                arrays_zip(
                    "hourly.time",
                    "hourly.temperature_2m"
                )
            )
        ).select(
            "country",
            "latitude",
            "longitude",
            col('hourly_units.temperature_2m').alias('unit'),
            col("weather.time").alias('datetime'),
            col("weather.temperature_2m").alias('temperature'),
        )
    )