-- models/marts/dim_city.sql

{{
    config(
        materialized='incremental'
    )
}}

select md5(city || area) as area_key, city, area
  from (
select distinct left(area, 3) as city, substring(area from 4) as area
  from {{ref('stg_weather_alert')}}
)

{% if is_incremental() %}
where md5(city || area) not in (
    select area_key
      from {{ this }}
)
{% endif %}