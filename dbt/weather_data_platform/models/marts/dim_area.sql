-- models/marts/dim_city.sql

{% if target.name == 'bigquery' %}

{{
    config(
        materialized='table'
    )
}}

with city_list as (
  select distinct substr(area, 1, 3) as city, substr(area, 4) as area
  from {{ref('stg_weather_alert')}}
)

select to_hex(md5(city || area)) as area_key, city, area
  from city_list

{% else %}

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

{% endif %}

{% if is_incremental() %}
where md5(city || area) not in (
    select area_key
      from {{ this }}
)
{% endif %}