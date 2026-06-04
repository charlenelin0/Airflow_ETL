# Weather dbt Project

This dbt project is responsible for transforming weather data stored in PostgreSQL staging tables into analytical data models.

## Model Structure

### Staging Layer

* stg_weather_forecast
* stg_weather_alert

### Intermediate Layer

* eph_weather_forecast

### Mart Layer

* dim_city
* dim_area
* fact_weather_forecast
* fact_weather_alert