-- models/staging/stg_weather_alert.sql

{{
    config(materialized="view")
}}

select 
    event, 
    level, 
    area,

    {% if target.name == 'bigquery' %}
        DATE(start_time) as weather_date
    {% else %}
        start_time::date as weather_date
    {% endif %}

  from {{ source('raw_data', 'staging_alert') }}