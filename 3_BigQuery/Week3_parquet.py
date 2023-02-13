from pathlib import Path
import pandas as pd
from datetime import timedelta

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

if __name__ == "__main__":
    months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    year = 2019
    etl_parent_flow(months, year)