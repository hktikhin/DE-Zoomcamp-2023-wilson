import pandas as pd 
import os

from pathlib import Path 
from prefect import flow, task 
from prefect_gcp.cloud_storage import GcsBucket, cloud_storage_upload_blob_from_file
# from prefect.tasks import task_input_hash
# from datetime import timedelta

@task(retries=3) # , cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas Dataframe"""
    df = pd.read_csv(dataset_url)
    return df 

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issue"""
    df = df.copy(deep=True)
    if "tpep_pickup_datetime" in df.columns: 
        df["tpep_pickup_datetime"] = pd.to_datetime(df.tpep_pickup_datetime)
        df["tpep_dropoff_datetime"] = pd.to_datetime(df.tpep_dropoff_datetime)
    elif "lpep_pickup_datetime" in df.columns: 
        df["lpep_pickup_datetime"] = pd.to_datetime(df.lpep_pickup_datetime)
        df["lpep_dropoff_datetime"] = pd.to_datetime(df.lpep_dropoff_datetime)
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"row: {len(df)}")
    return df 

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write dataframe out locally as parquet file"""
    path = Path(f"data/{color}/{dataset_file}.parquet.gz")
    if not os.path.exists(path.parent):
        os.makedirs(path.parent)
    df.to_parquet(path, compression="gzip")
    return path


def write_gcs(path:Path) -> None: 
    """Upload local parquet file to gcs"""
    gcs_block = GcsBucket.load("zoom-gcs")
    # upload data by 10 MB chunk
    cloud_storage_upload_blob_from_file(
        path, gcs_block.bucket, path.as_posix(), gcs_block.gcp_credentials, chunk_size = 10_485_760
    )

@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """the main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"  
    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

@flow(log_prints=True)
def etl_parent_flow(
    year: int = 2021,
    months: list[int] = [1, 2],
    color: str = "yellow"
):
    print(f"The script is running on {os.getcwd()}")
    for month in months:
        etl_web_to_gcs(year, month, color)

if __name__ == "__main__":
    color = "yellow"
    year = 2019
    months = [3]
    etl_parent_flow(year, months, color)
