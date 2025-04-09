from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime

"""
We build the SparkSession:
- Sets the name of the Spark application (for logs/UI)
- Adds the required libraries (jars) to support reading from S3 (or MinIO)
  JARs are packages for the Java/Scale environment.
  With Spark, they are used to enable external functionality, such as reading from MinIO or working with cloud systems.
- Defines the endpoint URL for your MinIO instance
- Sets MinIO credentials (default ones in this case)
- Enables path-style access (instead of virtual-hosted style that is not supported by MinIO)
  i.e., "http://localhost:9000/bucket/file" instead of "bucket.s3a.amazonaws.com"
- Specifies the credentials provider class — needed to authenticate with your MinIO server. 
  In particular, we force "access.key" and "secret.key"
- Finally, with .getOrCreate we actually creates the Spark session
"""

spark = SparkSession.builder \
    .appName("E-commerce analysis") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

# Ignoring some Spark's log
spark.sparkContext.setLogLevel("ERROR")

# Retrive current date, timestamps, week 
today = datetime.now()
timestamp = today.strftime("%Y-%m-%d")
year, week_num, _ = today.isocalendar() # ISO standard week number
partition_folder = f"week_{year}_{week_num}"

# load data on spark
try:
    users_df = spark.read.json(f"s3a://raw-data/users/{partition_folder}/users.json", multiLine = True)
except Exception as e:
    print(f"⚠️ Could not load users: {e}")

try:
    products_df = spark.read.json(f"s3a://raw-data/products/{partition_folder}/products.json", multiLine = True)
except Exception as e:
    print(f"⚠️ Could not load users: {e}")

try:
    carts_df = spark.read.json(f"s3a://raw-data/carts_{timestamp}.json", multiLine = True)
except Exception as e:
    print(f"⚠️ Could not load users: {e}")

# Parse json data to dataframe
users_df_flat = users_df.select(
    col("__V"),
    col("address.city"),
    col("address.geolocation.lat"),
    col("address.geolocation.long"),
    col("address.number"),
    col("address.street"),
    col("address.zipcode"),
    col("email"),
    col("id"),
    col("name.firstname"),
    col("name.lastname"),
    col("password"),
    col("phone"),
    col("username")
)

products_df_flat = products_df.select(
    col("category"),
    col("description"),
    col("id"),
    col("image"),
    col("price"),
    col("rating.count"),
    col("rating.rate"),
    col("title")
)

carts_df_exploded = carts_df.withColumn("product", explode(col("products")))
carts_df_flat = carts_df_exploded.select(
    "__V",
    "date",
    "id",
    col("product.productID").alias("productID"),
    col("product.quantity").alias("quantity")
)

# Build enriched_orders