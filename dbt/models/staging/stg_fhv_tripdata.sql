{{
    config(
        materialized='view'
    )
}}

select 	
    dispatching_base_num,
    pickup_datetime,
    dropOff_datetime as dropoff_datetime,
    PUlocationID as pulocationid,
    DOlocationID as dolocationid,
    SR_Flag as sr_flag,
    Affiliated_base_number as affiliated_base_number
from {{ source('staging','fhv_tripdata') }}
where dispatching_base_num is not null 


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}