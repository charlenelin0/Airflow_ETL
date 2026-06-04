-- models/marts/fact_weather_alert.sql

{{
    config(materialized="table")
}}

select d.city_key,
       a.area_key,
       s.weather_date,
       s.event,
       s.level
  from {{ ref('eph_weather_alert') }} s
  join {{ ref('dim_city') }} d
    on s.city = d.city
  join {{ ref('dim_area') }} a
    on s.city = a.city
   and s.district = a.area