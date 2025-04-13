{{ config(schema="marts") }}

SELECT *
FROM {{ref("stg_weather")}}