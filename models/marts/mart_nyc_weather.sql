{{ config(schema="marts") }}

SELECT *
FROM {{ref("int_weather_stats_day")}}