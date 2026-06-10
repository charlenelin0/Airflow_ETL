-- models/marts/dim_city.sql

{% if target.name == 'bigquery' %}

{{
    config(
        materialized='table'
    )
}}

with cte_city_list as (
  select city
    from {{ref('stg_weather_forecast')}}
  union distinct
  select substr(area,1,3) as city
    from {{ref('stg_weather_alert')}}
)

select to_hex(md5(city)) as city_key, city
  from cte_city_list

{% else %}

{{
    config(
        materialized='incremental'
    )
}}

with cte_city_list as (
  select distinct city
    from {{ref('stg_weather_forecast')}}
  union
  select distinct left(area,3) as city
    from {{ref('stg_weather_alert')}}
)

select md5(city) as city_key, city
  from cte_city_list

{% endif %}

{% if is_incremental() %}
where city not in (
    select city
      from {{ this }}
)
{% endif %}