-- models/intermediate/eph_weather_forecast.sql

{{ 
     config(materialized='ephemeral') 
}}

select event, level, area, weather_date,
       substr(area, 1, 3) as city, 
       substr(area, 4) as district
  from {{ ref('stg_weather_alert') }}