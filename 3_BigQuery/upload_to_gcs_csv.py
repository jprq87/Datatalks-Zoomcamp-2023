import sys
import glob
import os
from google.cloud import storage

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

def get_csv_files(folder_path):
    return glob.glob(f"{folder_path}/*.csv.gz")

if __name__ == "__main__":
    folder_path = os.path.dirname(os.path.abspath(__file__))
    csv_files = get_csv_files(folder_path)
    for csv in csv_files:
        upload_blob(
            bucket_name='de-zc-jprq-bucket',
            source_file_name=f'{os.path.basename(csv)}',
            destination_blob_name=f'fhv_tripdata\{os.path.basename(csv)}'
        )