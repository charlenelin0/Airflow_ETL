-- models/marts/dim_city.sql

{{
    config(
        materialized='incremental'
    )
}}

select md5(city) as city_key, city
  from (
select distinct city
  from {{ref('stg_weather_forecast')}}
  union
select distinct left(area,3) as city
  from {{ref('stg_weather_alert')}}
)

{% if is_incremental() %}
where city not in (
    select city
      from {{ this }}
)
{% endif %}