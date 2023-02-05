import pandas as pd 
import os

from pathlib import Path 
from prefect import flow, task 
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task()
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet.gz"
    gcs_block = GcsBucket.load("zoom-gcs")
    local_path = Path(gcs_path)
    # download entire content of base path into local path 
    gcs_block.get_directory(from_path=gcs_path, local_path="./")
    return local_path

@task(log_prints=True)
def read_data(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"# of rows processed: {len(df)}")
    return df 

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write dataframe to bigquery"""
    gcp_cred_block = GcpCredentials.load("zoom-gcp-creds")
    
    df.to_gbq(
        destination_table="trips_data_all.rides",
        project_id="dtc-de-376314",
        credentials=gcp_cred_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )

@flow()
def etl_gcs_to_bq(year: int, month: int, color: str):
    """the main ETL function"""
    path = extract_from_gcs(color, year, month)
    df = read_data(path)
    write_bq(df)

@flow()
def parent_etl_gcs_to_bq(year: int = 2019, months: list[int] = [2, 3], color: str = "yellow"):
    """Run mutiple etl to big query"""
    for month in months:
        etl_gcs_to_bq(year, month, color)

if __name__ == "__main__":
    parent_etl_gcs_to_bq()
