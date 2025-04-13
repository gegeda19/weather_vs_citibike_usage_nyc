{{ config(schema="marts") }}

SELECT *
FROM {{ref("int_citibike_usage_stats_day")}}