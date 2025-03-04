{{ config(materialized='table') }}

with fhv_trips_data as (
    select * from {{ ref('dim_fhv_trips') }}
)
    select 
    -- Grouping
    pickup_datetime,
    year,
    month,
    pickup_zone,
    pulocationid,
    dropoff_zone,
    dolocationid,
    TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS trip_duration,
    -- Compute percentiles
    PERCENTILE_CONT( TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND), 0.9) OVER (
        PARTITION BY year, month, pulocationid, dolocationid
    ) AS p90_trip_duration
    
    from fhv_trips_data