{{ config(schema='staging') }}

SELECT 
    CITY_NAME,
    COUNTRY,
    LATITUDE,
    LONGITUDE,
    WEATHER_MAIN,
    WEATHER_DESC,
    CASE
        WHEN LOWER(WEATHER_DESC) LIKE '%light%' THEN 'light'
        WHEN LOWER(WEATHER_DESC) LIKE '%moderate%' THEN 'moderate'
        WHEN LOWER(WEATHER_DESC) LIKE '%heavy%' THEN 'heavy'
        WHEN LOWER(WEATHER_DESC) LIKE '%shower%' THEN 'shower'
        ELSE NULL
    END AS weather_level,
    WEATHER_ICON,
    WEATHER_ID,
    TEMP AS F_TEMP,
    TEMP_MIN AS F_TEMP_MIN,
    TEMP_MAX AS F_TEMP_MAX,
    ROUND(TEMP - 273.15) AS C_TEMP,
    ROUND(TEMP_MIN - 273.15) AS C_TEMP_MIN,
    ROUND(TEMP_MAX - 273.15) AS C_TEMP_MAX,
    PRESSURE,
    HUMIDITY,
    WIND_SPEED,
    WIND_DEG,
    CLOUDINESS,
    TIMESTAMP,
    CAST(TIMESTAMP AS DATE) AS DATE,
    TO_CHAR(DATE, 'YYYY-MM') AS month,
    CASE 
        WHEN MONTH(DATE) BETWEEN 3 AND 5 THEN 'Spring'
        WHEN MONTH(DATE) BETWEEN 6 AND 8 THEN 'Summer'
        WHEN MONTH(DATE) BETWEEN 9 AND 11 THEN 'Fall'
        ELSE 'Winter'  -- Covers Dec, Jan, Feb
    END AS season
FROM {{source('STG_SOURCE','RAW_WEATHER_INFO')}}

