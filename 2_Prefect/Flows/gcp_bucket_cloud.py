from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

bucket_block = GcsBucket(
gcp_credentials=GcpCredentials.load("zoom-gcp-creds"),
bucket="de-zc-jprq-bucket"
)

bucket_block.save("zoom-gcs", overwrite=True)