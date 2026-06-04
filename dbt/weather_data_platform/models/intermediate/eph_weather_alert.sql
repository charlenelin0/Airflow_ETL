-- models/intermediate/eph_weather_forecast.sql

{{ 
     config(materialized='ephemeral') 
}}

select event, level, area, weather_date,
       left(area, 3) as city, 
       substring(area from 4) as district
  from {{ ref('stg_weather_alert') }}