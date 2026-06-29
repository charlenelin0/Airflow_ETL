# Databricks notebook source

from pyspark.sql import functions as F


dbutils.widgets.text("batch_date", "")
dbutils.widgets.text("batch_time", "")
dbutils.widgets.text("source_path", "")
dbutils.widgets.text("target_schema", "silver")

batch_date = dbutils.widgets.get("batch_date")
batch_time = dbutils.widgets.get("batch_time")
source_path = dbutils.widgets.get("source_path")
target_schema = dbutils.widgets.get("target_schema")


if not batch_date:
    raise ValueError("batch_date is required")

if not source_path:
    raise ValueError("source_path is required")


raw_df = spark.read.json(source_path)

silver_df = (
    raw_df
    .withColumn("batch_date", F.lit(batch_date))
    .withColumn("batch_time", F.to_timestamp(F.lit(batch_time)))
    .withColumn("processed_at", F.current_timestamp())
)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")

(
    silver_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(f"{target_schema}.weather_bronze_to_silver")
)

row_count = silver_df.count()

dbutils.notebook.exit(
    {
        "target_table": f"{target_schema}.weather_bronze_to_silver",
        "row_count": row_count,
        "batch_date": batch_date,
    }
)

