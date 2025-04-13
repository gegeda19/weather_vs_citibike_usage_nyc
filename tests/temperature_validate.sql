SELECT *
FROM {{ref("mart_nyc_weather")}}
WHERE 
    c_temp > 60 AND c_temp < -30
