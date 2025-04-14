import os
import boto3
import shutil
import duckdb
from datetime import datetime
from botocore.client import Config

# Create database
local_path = "data/MyDataWarehouse"
os.makedirs(local_path, exist_ok=True)
con = duckdb.connect(f"{local_path}/Ecommerce_analytics.duckdb")

# Create client to interact with minIO through S3 API
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    config = Config(signature_version = 's3v4'),
    region_name='us-east-1'
)

# Get the time info for partition reference
today = datetime.now()
timestamp = today.strftime('%Y-%m-%d')

# Get list of objects from specific time parition folder
prefix = f'partition_{timestamp}'
response = s3.list_objects_v2(Bucket='analytics', Prefix=prefix)

# Create local path to store transitory data 
to_duckdb_path = f'data/to_duckdb'

# Data flow to extract the object and store it locally
for obj_dict in response['Contents']:
    # allocate memory and save objects
    os.makedirs(f'data/to_duckdb/partition_{timestamp}', exist_ok=True)
    save_file_path = os.path.join(to_duckdb_path, obj_dict['Key'])
    s3.download_file(f'analytics', obj_dict['Key'], save_file_path)

    file_path = f"data/to_duckdb/{obj_dict['Key']}"

    # Create table if not exist
    file_name = obj_dict['Key'].split("/")[1]
    temp_table_name = file_name.replace(".parquet", "")
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {temp_table_name} AS
        SELECT '{timestamp}' || '_' || row_number() OVER() AS date_id,
        '{timestamp}' AS static_date,
        *
        FROM read_parquet('{file_path}')
    """)

    # Load data into table, use CTE to avoid duplicates when re-running.
    # This is useful if in the Daily DAG data flow something goes wrong and we need
    # to re-run the scripts. Maybe for a project like this it's an overkill,
    # but since this is done to learn stuff I think it is a valuable thing to add.
    # This best practice is called Idempotency.
    """
        1. 'WITH new_rows AS...' is a Common Table Expression;
            Used to generate the table with the newly added data;
            Create a unique date_id for each observation (some sort of primary key);
            To do this, use:
                
                - row_number() <- numerates the row progressively,
                - OVER() <- without parameter, continous indexing over the rows.
                  *Notes*: OVER() can be used for:
                    - row_number() OVER (ORDER BY prezzo DESC) <- self explanatory;
                    - row_number() OVER (PARTITION BY category ORDER BY prezzo DESC) <- same;
                
            **Eventually this part can be improved with an ash function**;
            Create a static type for date.
        
        2. 'INSERT INTO {temp_...}...' expression to insert new record only;
            The logical order of execution is:
            - FROM / JOIN;
            - WHERE;
            - SELECT;
            - INSERT;
            - ORDER BY (default for row_number() OVER());
            
            2.1 -> LEFT JOIN my_table AS already_existing_table, we basically create
                   a table:

                   new_rows.date_id    date	         price   ...	my_table.date_id
                   2025-04-14_1        2025-04-14	 10	     ...    NULL
                   2025-04-14_2	       2025-04-14	 20	     ...    2025-04-14_2
                   2025-04-14_3	       2025-04-14	 5       ...    NULL
                
                   We now have a reference of where we have duplicates values.
                   If date_id already exists, we have the date_id value in the existing_id 
                   column, otherwise we have NULL.

            2.2 -> With WHERE clause we filter to keep only NULL values;

            2.3 -> SELECT all columns of the filtered rows set;

            2.4 -> We INSERT the unique observations;

            2.5 -> Ordered by the row_number() assigned;

    """
    con.execute(f"""
        WITH new_rows AS(
            SELECT '{timestamp}' || '_' || row_number() OVER() AS date_id,
            '{timestamp}' AS static_date,
            *
            FROM read_parquet('{file_path}')
            )
        INSERT INTO {temp_table_name}
        SELECT new_rows.*
        FROM new_rows 
        LEFT JOIN {temp_table_name} AS existing
            ON existing.date_id = new_rows.date_id
        WHERE existing.date_id IS NULL 
    """)
# Clean local from data
shutil.rmtree(f"data/to_duckdb/partition_{timestamp}")
