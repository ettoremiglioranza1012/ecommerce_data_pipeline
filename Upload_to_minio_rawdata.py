import os
import boto3
from botocore.client import Config

"""
Create an S3-compatible client to interact with MinIO, 
(acting as an S3 server) over HTTP using the S3 REST API.
"""
s3 = boto3.client(
    's3',
    endpoint_url = 'http://localhost:9000',
    aws_access_key_id = 'minioadmin',
    aws_secret_access_key = 'minioadmin',
    config = Config(signature_version = 's3v4'), # Signature Version 4 requested by minIO
    region_name = 'us-east-1'
)

# Assign name to bucket
bucket_name = "raw-data"

# Check if the bucket exists otherwise create it
buckets = s3.list_buckets()
if bucket_name not in [b["Name"] for b in buckets["Buckets"]]:
    s3.create_bucket(Bucket=bucket_name)

# Get all newly ingested files in data/raw/
local_dir = "data/raw/"

"""
We use a Depth first search to visit all files in all directories with topdown approach to upload:

data/raw/
├── users/
│   └── week=2025_15/
│       └── users.json
└── products/
    └── week=2025_15/
        └── products.json

The we use a bottom up approach to delete all empty directories.
"""

for root, dirs, files in os.walk(local_dir):
    for filename in files:
        # local path
        local_path = os.path.join(root, filename)
        
        # get they s3_key from relative path from local directory data/raw/
        relative_path = os.path.relpath(local_path, local_dir)
        s3_key = relative_path.replace("\\", "/")

        # load partitioned data
        s3.upload_file(local_path, bucket_name, s3_key)
        print(f"\nUploaded file {local_path} to s3://{bucket_name}/{s3_key}")

        os.remove(local_path)
        print(f"\nRemoved file {local_path} from local")

# Cleaning from directories
for root, dirs, files in os.walk(local_dir, topdown=False):
    for dir_name in dirs:
        dir_path = os.path.join(root, dir_name)
        if not os.listdir(dir_path):
            print("\nWarning: empty directory detected!")
            print("\nDeleting directory...")
            os.rmdir(dir_path)
            print(f"\nDeleted empty folder {dir_path}")    

