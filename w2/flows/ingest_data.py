import pandas as pd 
import os
from time import time
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector
from datetime import timedelta

@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url) -> pd.DataFrame:
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    # read data by chunk
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100_000)
    return next(df_iter)

@task
def transfrom_data(df_chunk):
    print(f"pre: missing passenger count: {(df_chunk.passenger_count==0).sum()}")
    new_df_chunk = df_chunk[df_chunk.passenger_count!=0].copy(deep=True)
    print(f"post: missing passenger count: {(new_df_chunk.passenger_count==0).sum()}")
    if "tpep_pickup_datetime" in new_df_chunk.columns: 
        new_df_chunk.tpep_pickup_datetime = pd.to_datetime(new_df_chunk.tpep_pickup_datetime)
        new_df_chunk.tpep_dropoff_datetime = pd.to_datetime(new_df_chunk.tpep_dropoff_datetime)
    elif "lpep_pickup_datetime" in df_chunk.columns: 
        new_df_chunk.lpep_pickup_datetime = pd.to_datetime(new_df_chunk.lpep_pickup_datetime)
        new_df_chunk.lpep_dropoff_datetime = pd.to_datetime(new_df_chunk.lpep_dropoff_datetime)
    return new_df_chunk 

@task(retries=3)
def ingest_data(df_chunk, table_name):
    # load the prefect block 
    connection_block = SqlAlchemyConnector.load("postgres-connector")
    # connect to docker postgres
    with connection_block.get_connection(begin=False) as engine:
        # Inject data into database
        df_chunk.to_sql(name=table_name, con=engine, if_exists='append')

@flow
def log_subflow(table_name):
    print(f"Logging subflow for: {table_name}")

@flow(name="Ingest flow")
def main(table_name: str, csv_url: str):
    log_subflow(table_name)
    df_chunk = extract_data(csv_url)
    t_start = time()
    new_df_chunk = transfrom_data(df_chunk)
    ingest_data(new_df_chunk, table_name)
    t_end = time()
    print('inserted another chunk, took %.3f second' % (t_end - t_start))


if __name__ == "__main__":
    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
    main(table_name="yellow_taxi_data", csv_url=url)

