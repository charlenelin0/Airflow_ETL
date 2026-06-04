-- models/marts/fact_weather_forecast.sql

{{ 
     config(materialized='table') 
}}

select 
       city_key,
       weather_date,
       avg_LowestTemp,
       avg_HighestTemp,
       {{weather_level('avg_HighestTemp')}} flag_HighestTemp
  from {{ ref('eph_weather_forecast') }}