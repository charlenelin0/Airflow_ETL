-- models/intermediate/eph_weather_forecast.sql

{{ 
     config(materialized='ephemeral') 
}}

select 
       d.city_key,
       s.weather_date,
       avg(case when type = 'LowestTemp' then temp end) avg_LowestTemp,
       avg(case when type = 'HighestTemp' then temp end) avg_HighestTemp
  from {{ ref('stg_weather_forecast') }} s
  join {{ ref('dim_city') }} d
    on s.city = d.city
 group by d.city_key, s.weather_date