{{ config(schema="intermediate") }}

with
    stg_citibike as (select * from {{ ref("stg_citibike_usage") }}),

    int_stats_citibike_usage as (
        select
            count(*) as total_trips,
            round(avg(trip_duration) / 60.0, 0) as avg_duration_minutes,
            round(max(trip_duration) / 60.0, 0) as max_duration_minutes,
            round(min(trip_duration) / 60.0, 0) as min_duration_minutes,
            count(distinct start_station_id) as start_station_count_per_day,
            count(distinct end_station_id) as end_station_count_per_day,
            count(distinct bikeid) as used_bikes_per_day,
            start_date,
            month,
            season
        from stg_citibike
        group by start_date, month, season
    )

select *
from int_stats_citibike_usage
order by start_date
