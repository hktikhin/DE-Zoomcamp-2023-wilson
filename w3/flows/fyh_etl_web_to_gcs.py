import pandas as pd 
import os

from pathlib import Path 
from prefect import flow, task 
from prefect_gcp.cloud_storage import GcsBucket, cloud_storage_upload_blob_from_file

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read fyh taxi data from web into pandas Dataframe"""
    df = pd.read_csv(dataset_url)
    return df 

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    '''Fix data type issue'''
    df = df.copy(deep=True)
    df["pickup_datetime"] = pd.to_datetime(df.pickup_datetime)
    df["dropOff_datetime"] = pd.to_datetime(df.dropOff_datetime)
    df["PUlocationID"] = df.PUlocationID.astype("Int64")
    df["DOlocationID"] = df.DOlocationID.astype("Int64")
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"row: {len(df)}")
    return df 

@task()
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write dataframe out locally as parquet file"""
    path = Path(f"data/fhv/{dataset_file}.parquet")
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
def etl_web_to_gcs(year: int, month: int) -> None:
    """the main ETL function"""
    # https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-01.csv.gz
    dataset_file = f"fhv_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"  
    df = fetch(dataset_url)
    cleaned_df = clean(df)
    path = write_local(cleaned_df, dataset_file)
    write_gcs(path)
    os.remove(path)

@flow(log_prints=True)
def etl_parent_flow(
    year: int = 2021,
    months: list[int] = [1, 2]
):
    print(f"The script is running on {os.getcwd()}")
    for month in months:
        etl_web_to_gcs(year, month)

if __name__ == "__main__":
    year = 2019
    months = [i for i in range(1, 13)]
    etl_parent_flow(year, months)
