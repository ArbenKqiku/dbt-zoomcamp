{{
    config(
        materialized='table'
    )
}}

with fhv as (
    select
        *
    from 
        {{ref('hw_fhv')}}
),

dim_zones as (
    select
        *
    from
        {{ref('dim_zones')}}
    where
        borough != "Unknown"
)

select
    fhv.dispatching_base_num as dispatching_base_num,
    fhv.pickup_datetime as pickup_datetime,
    fhv.dropoff_datetime as dropoff_datetime,
    fhv.pickup_locationid as pickup_locationid,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    fhv.dropoff_locationid as dropoff_locationid,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    fhv.sr_flag as sr_flag,
    fhv.affiliated_base_number as affiliated_base_number
from
    fhv
inner join
    dim_zones as pickup_zone
    on fhv.pickup_locationid = pickup_zone.locationid
inner join
    dim_zones as dropoff_zone
    on fhv.dropoff_locationid = dropoff_zone.locationid 
