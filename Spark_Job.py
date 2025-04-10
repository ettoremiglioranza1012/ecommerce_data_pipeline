from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime

# Utility Functions
def trigger_cache(df):
    """
    Caches the given DataFrame and triggers the execution plan without saving any data.
    This is useful when the DataFrame will be reused multiple times, allowing Spark to keep it in memory.
    The 'noop' format simulates a write without performing any I/O operations.
    """
    df.cache()
    df.write.format("noop").mode("overwrite").save()

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

# Clarify some column names
products_df_flat = products_df_flat.withColumnRenamed("id", "Product_ID")
users_df_flat = users_df_flat.withColumnRenamed("id", "User_ID")
carts_df_flat = carts_df_flat.withColumnRenamed("id", "User_ID")

# Build enriched carts facts data sets with static data from users and products
enriched_df = carts_df_flat.join(
    products_df_flat,
    products_df_flat.Product_ID == carts_df_flat.product_ID,
    "left_outer"
).select(
    products_df_flat["category"], 
    products_df_flat["price"],
    products_df_flat["count"], 
    products_df_flat["rate"],
    carts_df_flat["User_ID"],
    carts_df_flat["product_ID"],
    carts_df_flat["quantity"]
)

enriched_df = enriched_df.join(
    users_df_flat,
    users_df_flat.User_ID == enriched_df.User_ID,
    "left_outer" 
).select(
    enriched_df["*"],
    users_df_flat["city"],
    users_df_flat["email"],
    users_df_flat["firstname"],
    users_df_flat["lastname"]
)

# We cache the data frame because we will re-use this several times to group by category and compute metrics
# Caching the dataframe in RAM is useful because it is a daily partion that is not going to be very heavy
trigger_cache(enriched_df)

# Revenues grouped by categories
revenue_by_category = enriched_df.groupBy("category").agg(
    sum(col("price") * col("quantity")).alias("total_revenue_byCat")
)

# Revenues grouped by users 
revenue_by_users_ID = enriched_df.groupBy("User_ID", "firstname","lastname", "email").agg(
    sum(col("price") * col("quantity")).alias("total_revenue_byUser")
).orderBy(col("total_revenue_byUser").desc())

# Total daily revenues
total_day_revenue = enriched_df.agg(
    round(sum(col("price") * col("quantity")),2).alias("total_day_revenue")
)

# Total quantities sold grouped by products
total_quantity_per_prod = enriched_df.groupBy("Product_ID", "category").agg(
    sum(col("quantity")).alias("quantity_per_productID")
).orderBy(col("quantity_per_productID").desc())

# Average quantities sold grouped by products
avg_ordval_per_user = enriched_df.groupBy("User_ID", "firstname", "lastname", "email").agg(
    round(avg(col("price")), 2).alias("avg_ordval_per_UserID")
).orderBy(col("avg_ordval_per_UserID").desc())

# Weighted average ratings grouped by categories
avg_rat_per_cat = enriched_df.groupBy("category").agg(
    round((sum(col("rate") * col("count")) / sum(col("count"))),1).alias("Weighted_avg_rating_per_category")
).orderBy(col("Weighted_avg_rating_per_category").desc())

# Total revenues grouped by cities
total_revenue_per_city = enriched_df.groupBy("city").agg(
    round(sum(col("quantity") * col("price")),2).alias("Total_revenue_city")
).orderBy(col("Total_revenue_city").desc())

# Number of distinct products sold
num_of_prod_sold = enriched_df.groupBy("product_ID").agg(
    count("product_ID").alias("Num_prod_sold")
).orderBy(col("Num_prod_sold").desc())

# Revenue grouped by Products per categories
rev_prod_cat = enriched_df.groupBy("category", "product_ID").agg(
    sum(col("price") * col("quantity")).alias("revenue_prod_byCat")
).orderBy(col("revenue_prod_byCat").desc())

# Top User by Revenue
top_user_rev = enriched_df.groupBy("user_ID", "firstname", "lastname", "email").agg(
    sum(col("price") * col("quantity")).alias("Top_user_revenue")
).orderBy(col("Top_user_revenue").desc()).limit(1)

# Top Products by Revenue 
top_prod_rev = enriched_df.groupBy("product_ID", "category").agg(
    sum(col("price") * col("quantity")).alias("Top_product_revenue")
).orderBy(col("Top_product_revenue").desc()).limit(1)

# Number of orders grouped by cities
ord_bycity = enriched_df.groupBy("city").agg(
    sum(col("quantity")).alias("Orders_by_city")
).orderBy(col("Orders_by_city").desc())
