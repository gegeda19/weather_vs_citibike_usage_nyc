{{ config(schema="marts") }}

SELECT *
FROM {{ref("int_citibike_demo_profile")}}