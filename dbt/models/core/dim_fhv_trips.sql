{{
    config(
        materialized='table'
    )
}}

with dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    dispatching_base_num,
    pickup_datetime,
    dropoff_datetime,
    t1.pulocationid,    
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone,
    t1.dolocationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    sr_flag,
    affiliated_base_number,
    extract(year from t1.pickup_datetime) as year,
    extract(month from t1.pickup_datetime) as month,
    extract(quarter from t1.pickup_datetime) as quarter
from {{ ref("stg_fhv_tripdata")}} t1
inner join dim_zones as pickup_zone
on t1.pulocationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on t1.dolocationid = dropoff_zone.locationid