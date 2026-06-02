-- models/staging/stg_weather.sql

{{ config(materialized='view') }}

select city, type, start_time::date as weather_date, temp
  from {{ source('raw_data', 'staging') }}