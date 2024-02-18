**dbt Homework**

**Data Ingestion**

***add green and yellow taxi data to BigQuery***

```python
import io
import pandas as pd
import requests
import pyarrow.parquet as pq
import itertools
from google.cloud import bigquery # pip install google-cloud-bigquery and pyarrow as a dependency
from google.oauth2 import service_account

services = ["green", "yellow"]
years = ["2019", "2020"]
months = list(i for i in range(1, 13))

credentials = service_account.Credentials.from_service_account_file(
    'week_4_data_ingestion/my_creds.json', scopes=['https://www.googleapis.com/auth/cloud-platform'],
)
client = bigquery.Client(project=credentials.project_id, credentials=credentials)

job_config = bigquery.LoadJobConfig()

for service, year, month in itertools.product(services, years, months):
    print(f"Now processing:\nService: {service}, Year: {year}, Month: {month}")
    month = f"{month:02d}" # Pad leading zeros if neccessary
    file_name = f"{service}_tripdata_{year}-{month}.parquet"
    request_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
    print(f"request url: {request_url}")
    
    try:
        response = requests.get(request_url)
        response.raise_for_status()
        data = io.BytesIO(response.content)
        
        new_df = pq.read_table(data).to_pandas()
        print(f"Parquet loaded:\n{file_name}\nDataFrame shape:\n{new_df.shape}")
        if service == 'green':
            table_name = 'green_tripdata'
        else: 
            table_name = 'yellow_tripdata'

        table_id = '{0}.{1}.{2}'.format(credentials.project_id, "data_ingestion", table_name)
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

        # Upload new set incrementally:
        # ! This method requires pyarrow to be installed:
        job = client.load_table_from_dataframe(
            new_df, table_id, job_config=job_config
        )

    except requests.HTTPError as e:
        print(f"HTPP Error: {e}")

    
```

***add fhv data to bigquery***

```python
import io
import pandas as pd
import requests
import pyarrow.parquet as pq
import itertools
from google.cloud import bigquery # pip install google-cloud-bigquery and pyarrow as a dependency
from google.oauth2 import service_account

services = ["fhv"]
years = [2019]
months = list(i for i in range(1, 13))

credentials = service_account.Credentials.from_service_account_file(
    'week_4_data_ingestion/my_creds.json', scopes=['https://www.googleapis.com/auth/cloud-platform'],
)
client = bigquery.Client(project=credentials.project_id, credentials=credentials)

job_config = bigquery.LoadJobConfig()

for service, year, month in itertools.product(services, years, months):
    print(f"Now processing:\nService: {service}, Year: {year}, Month: {month}")
    month = f"{month:02d}" # Pad leading zeros if neccessary
    file_name = f"{service}_tripdata_{year}-{month}.parquet"
    request_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
    print(f"request url: {request_url}")
    
    try:
        response = requests.get(request_url)
        response.raise_for_status()
        data = io.BytesIO(response.content)
        
        new_df = pq.read_table(data).to_pandas()
        print(f"Parquet loaded:\n{file_name}\nDataFrame shape:\n{new_df.shape}")
        
        table_name = 'fhv_trip_data'

        table_id = '{0}.{1}.{2}'.format(credentials.project_id, "data_ingestion", table_name)
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

        # Upload new set incrementally:
        # ! This method requires pyarrow to be installed:
        job = client.load_table_from_dataframe(
            new_df, table_id, job_config=job_config
        )

    except requests.HTTPError as e:
        print(f"HTPP Error: {e}")

    
```
***Created yml file where I defined my sources***

```yml
version: 2

sources:
  - name: homework_staging
    database: terraform-412318 # BigQuery project id
    schema: data_ingestion # BigQuery data set

    tables:
      - name: yellow_tripdata
      - name: green_tripdata
      - name: fhv_trip_data
```
***created view of yellow taxi trip data. I created a surrogate key, wrote column names in lowercase, and made sure columns have the right data type***

```sql
{{
    config(
        materialized='view'
    )
}}

with tripdata as (

    select
        *,
        row_number() over (partition by VendorID, tpep_pickup_datetime) as rn,

    from 
        {{ source('homework_staging', 'yellow_tripdata')}}

    where
        VendorID is not null

)

select
    {{ dbt_utils.generate_surrogate_key(['VendorID', 'tpep_pickup_datetime']) }} as tripid,
    VendorID as vendorid,
    {{ dbt.safe_cast("RatecodeID", api.Column.translate_type("integer")) }} as ratecodeid,
    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as dropoff_locationid,
    
    -- timestamps
    cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    {{ dbt.safe_cast("passenger_count", api.Column.translate_type("integer")) }} as passenger_count,
    cast(trip_distance as numeric) as trip_distance,

    -- payment info
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(airport_fee as numeric) as airport_fee,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    coalesce({{ dbt.safe_cast("payment_type", api.Column.translate_type("integer")) }},0) as payment_type,
    {{ get_payment_type_description("payment_type") }} as payment_type_description

from
    tripdata

where
    rn = 1
```

***created view of green taxi trip data. I created a surrogate key, wrote column names in lowercase, and made sure columns have the right data type***

```sql
{{
    config(
        materialized='view'
    )
}}

with tripdata as (

    select
        *,
        row_number() over (partition by VendorID, lpep_pickup_datetime) as rn,

    from 
        {{ source('homework_staging', 'green_tripdata')}}

    where
        VendorID is not null

)

select
    {{ dbt_utils.generate_surrogate_key(['VendorID', 'lpep_pickup_datetime']) }} as tripid,
    VendorID as vendorid,
    {{ dbt.safe_cast("RatecodeID", api.Column.translate_type("integer")) }} as ratecodeid,
    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as dropoff_locationid,

    -- timestamps
    cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    {{ dbt.safe_cast("passenger_count", api.Column.translate_type("integer")) }} as passenger_count,
    cast(trip_distance as numeric) as trip_distance,

    -- payment info
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(ehail_fee as numeric) as ehail_fee,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    coalesce({{ dbt.safe_cast("payment_type", api.Column.translate_type("integer")) }},0) as payment_type,
    {{ get_payment_type_description("payment_type") }} as payment_type_description

from
    tripdata

where
    rn = 1
```

***Created view of fhv taxi trip data. I made sure column names are in lowercase, and only selected the data fro 2019***

```sql
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
```

***Created my fact trips table for green and yellow taxi trips. I unioned green and yellow taxi trips, then I inner joined with dim_zones based on pickup_locationid and dropoff_location id. Therefore, all the taxi trips that don't have a valid pickup_locationid or dropofflocation_id are not present***

```sql
{{
    config(
        materialized='table'
    )
}}

-- green taxis
with green_trips as (
    select
        *,
        "green" as service_type
    from
        {{ ref('hw_staging_green') }}
),

-- yellow taxis
yellow_trips as (
    select
        *,
        "yellow" as service_type
    from
        {{ ref('hw_staging_yellow') }}
),

-- union tables
trips_unioned as (
    select 
        *
    from
        green_trips

    union all

    select
        *
    from
        yellow_trips
),

-- add zones
dim_zones as (
    select
        *
    from
        {{ ref('dim_zones') }}
    where
        borough != "Unknown"
)

select
    trips_unioned.vendorid, 
    trips_unioned.service_type,
    trips_unioned.ratecodeid, 
    trips_unioned.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    trips_unioned.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    trips_unioned.pickup_datetime, 
    trips_unioned.dropoff_datetime, 
    trips_unioned.store_and_fwd_flag, 
    trips_unioned.passenger_count, 
    trips_unioned.trip_distance, 
    trips_unioned.fare_amount, 
    trips_unioned.extra, 
    trips_unioned.mta_tax, 
    trips_unioned.tip_amount, 
    trips_unioned.tolls_amount, 
    trips_unioned.ehail_fee, 
    trips_unioned.improvement_surcharge, 
    trips_unioned.total_amount, 
    trips_unioned.payment_type, 
    trips_unioned.payment_type_description
from
    trips_unioned
inner join
    dim_zones as pickup_zone
    on trips_unioned.pickup_locationid = pickup_zone.locationid
inner join
    dim_zones as dropoff_zone
    on trips_unioned.dropoff_locationid = dropoff_zone.locationid
```

*** I created a fact trips table for fhv trips. I inner joined fhv trips with dim zones based on pickup or dropoff location id. Therefore, all taxi trips without a valid pickup or dropoff location id are dropped. ***

```sql
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
```

***Question 3: based on fhv fact trips, calculate the number of records***

```sql
SELECT count(*) FROM `terraform-412318.dbt_arbenkqiku.hw_fhv_fact_trips` LIMIT 1000
```

***Question 4: Based on the service, calculate how many trips there have been for 2019 in July. Yellow has 3,238,564 trips, green 415,384 trips***

```sql
with prep_1 as (
select
  concat(vendorid, pickup_datetime) as trips,
  service_type,
  extract(year from pickup_datetime) as year,
  extract(month from pickup_datetime) as month
from 
  `terraform-412318.dbt_arbenkqiku.hw_fact_trips`
)

select
  service_type,
  count(trips) as trips
from
  prep_1
where
  year = 2019 and
  month = 7
group by
  service_type
```

***Question 4: For fhv, Calculate how many trips there have been for 2019 in July. 290,682 trips***

```sql
with prep_1 as (
select
  concat(dispatching_base_num, pickup_datetime) as trips,
  extract(year from pickup_datetime) as year,
  extract(month from pickup_datetime) as month
from
  `terraform-412318.dbt_arbenkqiku.hw_fhv_fact_trips`
)

select
  count(trips)
from
  prep_1
where
  year = 2019 and
  month = 7
```