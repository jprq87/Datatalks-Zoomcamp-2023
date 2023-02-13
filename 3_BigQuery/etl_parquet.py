import glob
import pandas as pd
import os
from datetime import timedelta
from google.cloud import storage
from pathlib import Path

def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url, encoding='utf-8', engine='pyarrow')
    print(f"{df.dtypes}")
    print(f"Number of rows: {len(df)}")
    return df

def fix_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df_fixed = df.astype({"dispatching_base_num":"string", 
                "pickup_datetime":"datetime64",
                "dropOff_datetime":"datetime64",
                "PUlocationID":"Int64",
                "DOlocationID":"Int64",
                "SR_Flag":"Int64",
                "Affiliated_base_number":"string"})
    print(f"{df_fixed.dtypes}")
    print(f"Number of rows: {len(df_fixed)}")
    return df_fixed

def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out locally as CSV file"""
    path = Path(f"{dataset_file}.parquet")
    # Path(path).parents[0].mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, compression="gzip")
    return path

def etl_web_to_gcs(year: int, month: int) -> None:
    """The main ETL function"""
    dataset_file = f"fhv_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = fix_dtypes(df)
    path = write_local(df_clean, dataset_file)
    print(path)

def etl_parent_flow(
    months: list[int] = [1, 2], year: int = 2021
):
    for month in months:
        etl_web_to_gcs(year, month)

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )

def get_parquet_files(folder_path):
    return glob.glob(f"{folder_path}/*.parquet")

if __name__ == "__main__":
    months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    year = 2019
    etl_parent_flow(months, year)
    folder_path = os.path.dirname(os.path.abspath(__file__))
    parquet_files = get_parquet_files(folder_path)
    for parquet in parquet_files:
        upload_blob(
            bucket_name='de-zc-jprq-bucket',
            source_file_name=f'{os.path.basename(parquet)}',
            destination_blob_name=f'fhv_tripdata/{os.path.basename(parquet)}'
        )