{{
    config(materialized="view")
}}

select event, level, area, start_time::date as weather_date
  from {{ source('raw_data', 'staging_alert') }}