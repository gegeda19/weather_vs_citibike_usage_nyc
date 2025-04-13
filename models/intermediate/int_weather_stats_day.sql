{{ config(schema="intermediate") }}

SELECT *
FROM {{ref('stg_weather')}}