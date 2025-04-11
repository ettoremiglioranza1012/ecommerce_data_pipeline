import os
import boto3
import duckdb
from datetime import datetime
from botocore.client import Config

# Configura il percorso locale per i file scaricati
local_download_path = 'data/downloaded_parquets'
os.makedirs(local_download_path, exist_ok=True)

# Connessione al database DuckDB locale
con = duckdb.connect("data/MyDataWarehouse/analytics.duckdb")

# Configura il client boto3 per MinIO
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

# Ottieni la lista degli oggetti nel bucket
objects = s3.list_objects_v2(Bucket='analytics').get('Contents', [])

for obj in objects:
    key = obj['Key']
    if not key.endswith('.parquet'):
        continue

    # Scarica il file Parquet in locale
    local_file_path = os.path.join(local_download_path, os.path.basename(key))
    s3.download_file('analytics', key, local_file_path)

    table_name = os.path.splitext(os.path.basename(key))[0]

    # Crea la tabella se non esiste
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} AS 
        SELECT CURRENT_DATE AS date, *
        FROM read_parquet('{local_file_path}') LIMIT 0
    """)

    # Inserisci i dati nella tabella
    con.execute(f"""
        INSERT INTO {table_name}
        SELECT CURRENT_DATE AS date, *
        FROM read_parquet('{local_file_path}')
    """)

    print(f"✅ Caricato: {table_name} da {local_file_path}")



