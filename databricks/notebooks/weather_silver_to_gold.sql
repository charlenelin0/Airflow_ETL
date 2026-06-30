-- Databricks notebook source

CREATE WIDGET TEXT batch_date DEFAULT "";
CREATE WIDGET TEXT temperature_source_table DEFAULT "silver.weather_temperature_hourly";
CREATE WIDGET TEXT rain_source_table DEFAULT "silver.weather_rain_hourly";
CREATE WIDGET TEXT target_schema DEFAULT "gold";

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS ${target_schema};

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ${target_schema}.weather_daily_summary (
  country STRING,
  weather_date DATE,
  avg_temperature DOUBLE,
  min_temperature DOUBLE,
  max_temperature DOUBLE,
  total_rain DOUBLE,
  temperature_record_count BIGINT,
  rain_record_count BIGINT,
  weather_level STRING,
  batch_date DATE,
  processed_at TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

DELETE FROM ${target_schema}.weather_daily_summary
WHERE batch_date = TRY_CAST('$batch_date' AS DATE);

-- COMMAND ----------

INSERT INTO ${target_schema}.weather_daily_summary
WITH temperature_daily AS (
  SELECT
    country,
    DATE(weather_time) AS weather_date,
    batch_date,
    AVG(temperature) AS avg_temperature,
    MIN(temperature) AS min_temperature,
    MAX(temperature) AS max_temperature,
    COUNT(*) AS temperature_record_count
  FROM ${temperature_source_table}
  WHERE batch_date = TRY_CAST('$batch_date' AS DATE)
  GROUP BY country, DATE(weather_time), batch_date
),
rain_daily AS (
  SELECT
    country,
    DATE(weather_time) AS weather_date,
    batch_date,
    SUM(rain) AS total_rain,
    COUNT(*) AS rain_record_count
  FROM ${rain_source_table}
  WHERE batch_date = TRY_CAST('$batch_date' AS DATE)
  GROUP BY country, DATE(weather_time), batch_date
),
joined_daily AS (
  SELECT
    COALESCE(t.country, r.country) AS country,
    COALESCE(t.weather_date, r.weather_date) AS weather_date,
    t.avg_temperature,
    t.min_temperature,
    t.max_temperature,
    r.total_rain,
    COALESCE(t.temperature_record_count, 0) AS temperature_record_count,
    COALESCE(r.rain_record_count, 0) AS rain_record_count,
    COALESCE(t.batch_date, r.batch_date) AS batch_date
  FROM temperature_daily t
  FULL OUTER JOIN rain_daily r
    ON t.country = r.country
   AND t.weather_date = r.weather_date
   AND t.batch_date = r.batch_date
)
SELECT
  country,
  weather_date,
  avg_temperature,
  min_temperature,
  max_temperature,
  total_rain,
  temperature_record_count,
  rain_record_count,
  CASE
    WHEN max_temperature >= 36 THEN 'Extreme'
    WHEN max_temperature >= 32 THEN 'Hot'
    WHEN max_temperature >= 28 THEN 'Warm'
    ELSE 'Normal'
  END AS weather_level,
  batch_date,
  CURRENT_TIMESTAMP() AS processed_at
FROM joined_daily;

-- COMMAND ----------

SELECT
  '$batch_date' AS batch_date,
  '${target_schema}.weather_daily_summary' AS target_table,
  COUNT(*) AS row_count
FROM ${target_schema}.weather_daily_summary
WHERE batch_date = TRY_CAST('$batch_date' AS DATE);
