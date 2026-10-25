import boto3
import os
from botocore.config import Config

from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id=os.environ["RUSTFS_ACCESS_KEY"],
    aws_secret_access_key=os.environ["RUSTFS_SECRET_KEY"],
    region_name="us-east-1",
    config=Config(
        signature_version="s3v4",
        s3={"addressing_style": "path"},
    ),
)

s3.create_bucket(Bucket="mini-data-lake")

print("bucket created")

s3.upload_file(
    "data/orders.csv",
    "mini-data-lake",
    "raw/orders.csv",
)

print("file uploaded")

response = s3.list_objects_v2(Bucket="mini-data-lake")

for obj in response.get("Contents", []):
    print(obj["Key"])

s3.download_file(
    "mini-data-lake",
    "raw/orders.csv",
    "data/orders_downloaded.csv",
)

print("file downloaded")

response = s3.list_buckets()

for bucket in response["Buckets"]:
    print(bucket["Name"])
