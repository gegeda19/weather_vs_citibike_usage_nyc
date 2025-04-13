{{ config(schema="intermediate") }}

with
    init_citibike as (
        select
            *,
            case
                when gender_code = 1
                then 'Male'
                when gender_code = 2
                then 'Female'
                else 'Unknown'
            end as gender
        from {{ ref("stg_citibike_usage") }}
    ),
    total_trips_per_day as (
        select start_date, count(*) as total_trips_all_genders
        from init_citibike
        group by start_date
    ),
    known_gender_stats as (
        select
            ic.gender,
            count(*) as total_trips,
            round(avg(age), 0) as avg_age,
            round(avg(trip_duration) / 60.0, 0) as avg_duration_minutes,
            round(max(trip_duration) / 60.0, 0) as max_duration_minutes,
            round(min(trip_duration) / 60.0, 0) as min_duration_minutes,
            count(distinct start_station_id) as start_station_count_per_day,
            count(distinct end_station_id) as end_station_count_per_day,
            count(distinct bikeid) as used_bikes_per_day,
            ic.start_date,
            ic.month,
            ic.season,
            round(
                count(*) * 100.0 / t.total_trips_all_genders, 2
            ) as gender_trip_percentage
        from init_citibike ic
        join total_trips_per_day t on ic.start_date = t.start_date
        where ic.gender = 'Female' OR ic.gender = 'Male'
        group by
            ic.gender, ic.start_date, ic.month, ic.season, t.total_trips_all_genders
    ),
    unknown_gender_stats as (
        select
            ic.gender,
            count(*) as total_trips,
            0 as avg_age,
            round(avg(trip_duration) / 60.0, 0) as avg_duration_minutes,
            round(max(trip_duration) / 60.0, 0) as max_duration_minutes,
            round(min(trip_duration) / 60.0, 0) as min_duration_minutes,
            count(distinct start_station_id) as start_station_count_per_day,
            count(distinct end_station_id) as end_station_count_per_day,
            count(distinct bikeid) as used_bikes_per_day,
            ic.start_date,
            ic.month,
            ic.season,
            round(
                count(*) * 100.0 / t.total_trips_all_genders, 2
            ) as gender_trip_percentage
        from init_citibike ic
        join total_trips_per_day t on ic.start_date = t.start_date
        where ic.gender = 'Unknown'
        group by
            ic.gender, ic.start_date, ic.month, ic.season, t.total_trips_all_genders
    )

select *
from known_gender_stats
union all 
select *
from unknown_gender_stats
order by start_date, gender
