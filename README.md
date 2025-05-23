# 🛒 E-Commerce Data Pipeline (Local → Cloud Ready)

This project simulates a real-world data engineering pipeline using **Python**, **PySpark**, and other state-of-the-art tools. It includes:

- 📦 **Data ingestion** from the Fake Store API:
  - Products and Users are fetched weekly (every Monday or with `--force_sync`)
  - Orders (carts) are fetched daily

- 🐋 **Docker setup** for running MinIO locally

- 🗂️ **Partitioned file storage**:
  - Products and Users are saved in folders like `products/week=YYYY_WW/` and `users/week=YYYY_WW/`
  - Orders are saved daily with full timestamps in filenames (`carts_YYYY-MM-DD.json`)

- ☁️ **Cloud-ready storage simulation**:
  - All files are uploaded to **MinIO**, a local S3-compatible object store
  - Folder structures mimic real cloud storage for seamless migration later

- 🧠 **Data Processing with PySpark**
  - Reads raw data dynamically based on current week/date from MinIO using Spark + `s3a://`
  - Parses, flattens, and **enriches cart data** by joining with user and product attributes
  - Caches intermediate result for fast re-use
  - Computes 12+ analytics metrics including:
    - Total revenues per category, city, user
    - Top-selling products
    - Weighted average ratings
    - Average order value per user
    - Quantity sold per product
  - Writes metrics in `Parquet` format with partitioning by date

- 🧼 **Post-Processing Layer: Warehouse Ready**
  - Output Parquet files are stored as Spark folders (with `part-xxxx.parquet`)
  - A **cleanup job**:
    - Extracts the `.parquet` file from each folder
    - Renames it using the logical table name (e.g., `avg_ordval_per_user.parquet`)
    - Saves it to `data/warehouse_ready/partition_<YYYY-MM-DD>/`
    - Uploads the clean output to `s3://analytics/partition_<YYYY-MM-DD>/...`

- 🧱 Data Warehousing with DuckDB
  - The cleaned and renamed `.parquet` files are downloaded from MinIO and loaded into a local **DuckDB** database, simulating a lightweight analytical warehouse.
  - For each analytics table:
    - If the table doesn't exist, it's created automatically.
    - New records are inserted **only if they are not already present**, ensuring idempotency (i.e., safe re-runs in case of failures).
    - A `date_id` field is added using a combination of the execution date and row number to create a pseudo primary key.
    - This approach supports easy debugging and time-based tracking of ingested data.

---

## 🧰 Tools Used

- **Python**: For data generation and orchestration.
- **MinIO**: Local S3-compatible object storage used to simulate AWS S3, storing raw data (like `customers.csv`) and processed data.
- **PySpark**: Used for data transformation and cleaning at scale.
- **Docker**: For containerizing MinIO, simulating an S3-like object storage system on a single node.
- **boto3**: Python library to interact with MinIO (S3-compatible API).
- **Duckdb**: Lightweight analytical database engine used to store and query cleaned `.parquet` files locally, enabling fast OLAP-style analytics without the need for a full-fledged data warehouse.


---

### ⚠️ Limitations & Future Improvements

- Due to limitations in **DuckDB's native S3 support**, it was **not possible to query MinIO directly**. As a workaround, the `.parquet` files are temporarily downloaded to local memory before ingestion.
- Currently, we use a `date_id` string (e.g., `2025-04-14_1`) to identify unique rows. This works for small volumes, but a **hash-based strategy** (e.g., SHA256 over selected fields) would be more robust for production-grade uniqueness.

---

## 🚀 How to Run

1. **Set up a Python virtual environment** (if not already done):
   ```bash
    python -m venv .venv
    source .venv/bin/activate   # On Windows use .venv\Scripts\activate
    ```
    
2. **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```
    
3. **Start MinIO locally using Docker**:
    Install docker desktop, then to simulate object storage, run
    ```bash
    docker-compose up -d
    ```
    MinIO will be accessible at http://localhost:9000, and the web UI can be accessed at http://localhost:9001.
    Login to MinIO with Username: minioadmin, Password: minioadmin.

4. **Generate fake data**:
    Run the ingest_data.py script to generate and save fake data locally
    ```bash
    python scripts/ingest_data.py
    ```
    
5. **Upload the generated data to MinIO**:
    ```bash
    python scripts/upload_to_minio.py
    ```

6. **Run the main Spark job**: 
    This script reads raw JSON data from MinIO, transforms it, aggregates metrics, and generates Parquet files.
    ```bash
    python Spark_Job.py
    ```

7. **Upload warehouse-ready analytics to MinIO**: 
    This script will move the coalesced .parquet files into a clean structure, then push them to the analytics bucket.
    ```bash
    python Middle_layer_minio.py
    ```
8. **Upload to duckdb database cleaned parquet as table of ready-to-use metrics**: 
    This script will locally download the cleaned parquet files from the analytics bucket in minIO, then store each objects in a ligthweigth analytical database in the referenced table.
    ```bash
    Load_to_duckdb.py
    ```

---

## ⚙️ Setup Notes & Troubleshooting

This project requires Java 8+, Hadoop 3.4.1, and a Spark-compatible S3 endpoint (e.g., MinIO). 

Setting up PySpark in a local development environment on **Windows** presented several challenges:

- Missing `winutils.exe` required by Hadoop to run on Windows
- `ClassNotFoundException` errors due to missing AWS credential provider classes
- Incompatibility between PySpark, Hadoop, and AWS SDK versions
- Manual configuration of environment variables (`JAVA_HOME`, `HADOOP_HOME`, etc.)
- Ensuring connectivity and compatibility with local MinIO via the `s3a://` protocol

➡️ See [docs/Windows_setup_notes.md](docs/Windows_setup_notes.md) for a detailed breakdown of the setup process on windows and how the issues were resolved.

A few days later I tried initializing the project on Linux after dual booting with Linux/Windows:
- The setup process was significantly easier.
- No issues with winutils.exe, environment variables, or compatibility between PySpark, Hadoop, and AWS SDK versions.
- The whole installation and configuration, including Hadoop, Spark, and MinIO, was much quicker and more streamlined.

It is highly recommended to run this project on a Unix-like system, such as Linux or macOS.

> TLDR: Running Spark locally on Windows required non-trivial configuration, but it provided valuable insights into the full data engineering workflow — from local setup to cloud-ready infrastructure.



 

