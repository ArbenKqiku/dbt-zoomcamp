{{
    config(
        materialized='view'
    )
}}


with fhv as (select
    *,
    extract(year from pickup_datetime) as pickup_year
from
    {{ source('homework_staging', 'fhv_trip_data')}}
)

select
    dispatching_base_num,
    pickup_datetime,
    dropOff_datetime as dropoff_datetime,
    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer"))}} as pickup_locationid,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer"))}} as dropoff_locationid,
    SR_Flag as sr_flag,
    Affiliated_base_number as affiliated_base_number
from
    fhv
where
    pickup_year = 2019