
from pyspark.sql import (
    DataFrame
)

from pyspark.sql.functions import(
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
                    "hourly.rain"
                )
            )
        ).select(
            "country",
            "latitude",
            "longitude",
            col('hourly_units.rain').alias('rain_unit'),
            col("weather.time").alias('datetime'),
            col("weather.rain").alias('rain'),
        )
    )