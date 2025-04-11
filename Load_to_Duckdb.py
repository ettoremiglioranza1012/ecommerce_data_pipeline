import os
import boto3
from datetime import datetime
from botocore.client import Config

# Create the client to comunicate with minIO with S3 API
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    config=Config(signature_version='s3v4'),
    region_name='ue-est-1'
)

# Get timestamp for partition
today = datetime.now()
timestamp = today.strftime('%Y-%m-%d')

# Create local directory transitorial data 
local_path = f'data/to_duckdb/partition_{timestamp}'
os.makedirs(local_path, exist_ok=True)

# Get data from minIO
analytics = s3.list_objects_v2(Bucket='analytics')['Contents']
for obj in analytics:
    s3.download_file('analytics', f"{obj['Key']}", local_path)