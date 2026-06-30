-- models/marts/fact_weather_alert.sql

{{
    config(materialized="table")
}}

select to_hex(md5(city || area || weather_date || event)) alert_key,
       to_hex(md5(city || area)) area_key,
       weather_date,
       event,
       level
  from {{ ref('eph_weather_alert') }}