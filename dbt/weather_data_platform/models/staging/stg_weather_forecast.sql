-- models/staging/stg_weather_forecast.sql

{{ 
  config(materialized='view') 
}}

select 
    city, 
    type, 
    {% if target.name == 'bigquery' %}
        DATE(start_time) as weather_date,
    {% else %}
        start_time::date as weather_date,
    {% endif %}
    temp
  from {{ source('raw_data', 'staging_forecast') }}