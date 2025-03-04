{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fact_trips') }}
)
    select 
    -- Revenue grouping 
    year,
    quarter,
    service_type,

    -- Revenue calculation 
    sum(total_amount) as revenue_quarter_total_amount,
    
    from trips_data
    group by 1,2,3