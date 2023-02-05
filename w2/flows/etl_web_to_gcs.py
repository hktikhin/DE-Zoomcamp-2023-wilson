import pandas as pd 
import os

from pathlib import Path 
from prefect import flow, task 
from prefect_gcp.cloud_storage import GcsBucket

@task(retries=3)
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
    path = Path(f"data/{color}/{dataset_file}.parquet")
    if not os.path.exists(path.parent):
        os.makedirs(path.parent)
    df.to_parquet(path, compression="gzip")
    return path

@task()
def write_gcs(path:Path) -> None: 
    """Upload local parquet file to gcs"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path.as_posix())

@flow()
def etl_web_to_gcs():
    """the main ETL function"""
    color = "yellow"
    year = 2021
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"  
    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

if __name__ == "__main__":
    etl_web_to_gcs()
