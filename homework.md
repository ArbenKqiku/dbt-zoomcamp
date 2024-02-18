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