{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fact_trips') }}
)
    select 
    -- Grouping
    service_type,
    year,
    month,
    fare_amount,

    -- Compute percentiles
    PERCENTILE_CONT(fare_amount, 0.9) OVER (
        PARTITION BY service_type, year, month
    ) AS p90_fare_amount,
    PERCENTILE_CONT(fare_amount, 0.95) OVER (
        PARTITION BY service_type, year, month
    ) AS p95_fare_amount,
    PERCENTILE_CONT(fare_amount, 0.97) OVER (
        PARTITION BY service_type, year, month
    ) AS p97_fare_amount
    
    from trips_data
    where fare_amount > 0
    and payment_type_description in ('Cash', 'Credit card')
    and trip_distance > 0