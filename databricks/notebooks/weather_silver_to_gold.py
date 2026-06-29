# Databricks notebook source

from pyspark.sql import functions as F


dbutils.widgets.text("batch_date", "")
dbutils.widgets.text("source_table", "silver.weather_bronze_to_silver")
dbutils.widgets.text("target_schema", "gold")

batch_date = dbutils.widgets.get("batch_date")
source_table = dbutils.widgets.get("source_table")
target_schema = dbutils.widgets.get("target_schema")


if not batch_date:
    raise ValueError("batch_date is required")


silver_df = spark.table(source_table).where(F.col("batch_date") == batch_date)

gold_df = (
    silver_df
    .groupBy("batch_date")
    .agg(F.count(F.lit(1)).alias("record_count"))
    .withColumn("processed_at", F.current_timestamp())
)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")

(
    gold_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(f"{target_schema}.weather_daily_summary")
)

row_count = gold_df.count()

dbutils.notebook.exit(
    {
        "target_table": f"{target_schema}.weather_daily_summary",
        "row_count": row_count,
        "batch_date": batch_date,
    }
)

