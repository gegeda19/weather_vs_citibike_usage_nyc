{{ config(schema="staging") }}

with
    raw_weather as (
        select
            city_name,
            country,
            latitude,
            longitude,
            weather_main,
            weather_desc,
            case
                when lower(weather_desc) like '%light%'
                then 'light'
                when lower(weather_desc) like '%moderate%'
                then 'moderate'
                when lower(weather_desc) like '%heavy%'
                then 'heavy'
                when lower(weather_desc) like '%shower%'
                then 'shower'
                else null
            end as weather_level,
            weather_icon,
            weather_id,
            temp as f_temp,
            temp_min as f_temp_min,
            temp_max as f_temp_max,
            round(temp - 273.15) as c_temp,
            round(temp_min - 273.15) as c_temp_min,
            round(temp_max - 273.15) as c_temp_max,
            pressure,
            humidity,
            wind_speed,
            wind_deg,
            cloudiness,
            timestamp,
            cast(timestamp as date) as date,
            to_char(date, 'YYYY-MM') as month,
            to_char(date, 'YYYY') as year,
            case
                when month(date) between 3 and 5
                then 'Spring'
                when month(date) between 6 and 8
                then 'Summer'
                when month(date) between 9 and 11
                then 'Fall'
                else 'Winter'  -- Covers Dec, Jan, Feb
            end as season
        from {{ source("STG_SOURCE", "RAW_WEATHER_INFO") }}
    )

select *
from raw_weather
