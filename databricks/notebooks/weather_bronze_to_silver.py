# Databricks notebook source

from pyspark.sql import functions as F


dbutils.widgets.text("batch_date", "")
dbutils.widgets.text(
    "source_base_path",
    "/Volumes/workspace/default/airflow_json_landing/weather/bronze",
)
dbutils.widgets.text("target_schema", "silver")

batch_date = dbutils.widgets.get("batch_date")
source_base_path = dbutils.widgets.get("source_base_path").rstrip("/")
target_schema = dbutils.widgets.get("target_schema")


if not batch_date:
    raise ValueError("batch_date is required")

if not source_base_path:
    raise ValueError("source_base_path is required")


temperature_source_path = f"{source_base_path}/date={batch_date}/temperature/*.json"
rain_source_path = f"{source_base_path}/date={batch_date}/rain/*.json"

temperature_table = f"{target_schema}.weather_temperature_hourly"
rain_table = f"{target_schema}.weather_rain_hourly"


spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")


def read_bronze_json(source_path: str):
    return (
        spark.read
        .option("multiLine", "true")
        .json(source_path)
        .withColumn("source_file", F.input_file_name())
    )


def build_temperature_silver(source_path: str):
    raw_df = read_bronze_json(source_path)

    return (
        raw_df
        .withColumn(
            "hourly_metric",
            F.explode(
                F.arrays_zip(
                    F.col("hourly.time"),
                    F.col("hourly.temperature_2m"),
                )
            ),
        )
        .select(
            F.col("latitude").cast("double").alias("latitude"),
            F.col("longitude").cast("double").alias("longitude"),
            F.col("country").cast("string").alias("country"),
            F.to_timestamp(F.col("hourly_metric.time")).alias("weather_time"),
            F.col("hourly_metric.temperature_2m").cast("double").alias("temperature"),
            F.lit(batch_date).cast("date").alias("batch_date"),
            F.col("source_file"),
            F.current_timestamp().alias("processed_at"),
        )
    )


def build_rain_silver(source_path: str):
    raw_df = read_bronze_json(source_path)

    return (
        raw_df
        .withColumn(
            "hourly_metric",
            F.explode(
                F.arrays_zip(
                    F.col("hourly.time"),
                    F.col("hourly.rain"),
                )
            ),
        )
        .select(
            F.col("latitude").cast("double").alias("latitude"),
            F.col("longitude").cast("double").alias("longitude"),
            F.col("country").cast("string").alias("country"),
            F.to_timestamp(F.col("hourly_metric.time")).alias("weather_time"),
            F.col("hourly_metric.rain").cast("double").alias("rain"),
            F.lit(batch_date).cast("date").alias("batch_date"),
            F.col("source_file"),
            F.current_timestamp().alias("processed_at"),
        )
    )


def overwrite_batch(df, table_name: str) -> int:
    row_count = df.count()

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .option("replaceWhere", f"batch_date = '{batch_date}'")
        .saveAsTable(table_name)
    )

    return row_count


temperature_df = build_temperature_silver(temperature_source_path)
rain_df = build_rain_silver(rain_source_path)

temperature_row_count = overwrite_batch(temperature_df, temperature_table)
rain_row_count = overwrite_batch(rain_df, rain_table)


dbutils.notebook.exit(
    {
        "batch_date": batch_date,
        "temperature": {
            "source_path": temperature_source_path,
            "target_table": temperature_table,
            "row_count": temperature_row_count,
        },
        "rain": {
            "source_path": rain_source_path,
            "target_table": rain_table,
            "row_count": rain_row_count,
        },
    }
)
