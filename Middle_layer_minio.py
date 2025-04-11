import boto3
from botocore.client import Config
from datetime import datetime
import os
import shutil

today = datetime.now()
timestamp = today.strftime("%Y-%m-%d")
print(timestamp)

# Create client to open communication with minIO
s3 = boto3.client(
    's3',
    endpoint_url = 'http://localhost:9000',
    aws_access_key_id = 'minioadmin',
    aws_secret_access_key = 'minioadmin',
    config = Config(signature_version = 's3v4'), # Signature Version 4 requested by minIO
    region_name = 'us-east-1'
)

# Create bucket name
bucket_name = "analytics"

# if bucket doesn't exist yet -> create it
buckets = s3.list_buckets()
if bucket_name not in [b["Name"] for b in buckets["Buckets"]]:
    s3.create_bucket(Bucket=bucket_name)

local_dir = f"data/warehouse_ready/partition_{timestamp}"
for file in os.listdir(local_dir):
    retrive_path = os.path.join(local_dir, file)
    s3_key = f"partition_{timestamp}/{file}"
    s3.upload_file(retrive_path, bucket_name, s3_key)
    print(f"\nUploaded {retrive_path}")

# Clean local repo
shutil.rmtree(local_dir)
print("\nExt:Removed daily partition from local.")