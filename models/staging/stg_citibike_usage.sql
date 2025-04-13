{{ config(schema="staging") }}

with
    raw_citikie_usage as (
        select
            tripduration as trip_duration,
            starttime as start_time,
            stoptime as stop_time,
            start_station_id,
            start_station_name,
            start_station_latitude,
            start_station_longitude,
            end_station_id,
            end_station_name,
            end_station_latitude,
            end_station_longitude,
            bikeid,
            rider_plan,
            usertype,
            birth_year,
            gender as gender_code,
            cast(starttime as date) as start_date,
            round(tripduration / 60.0, 0) as trip_minutes,
            to_char(start_date, 'YYYY-MM') as month,
            cast(to_char(start_date, 'YYYY') as number) as year,
            case
                when birth_year = '' then 0 else cast(year - birth_year as int)
            end as age,
            case
                when month(start_date) between 3 and 5
                then 'Spring'
                when month(start_date) between 6 and 8
                then 'Summer'
                when month(start_date) between 9 and 11
                then 'Fall'
                else 'Winter'  -- Covers Dec, Jan, Feb
            end as season
        from {{ source("STG_SOURCE", "RAW_CITIBIKE") }}
    )

select *
from raw_citikie_usage
