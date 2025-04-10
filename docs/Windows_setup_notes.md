# ⚙️ Local Setup Notes (Windows - Java, Hadoop, PySpark)

This document summarizes the key steps and challenges encountered when setting up a local Spark environment on **Windows**. The environment simulates a cloud-based pipeline locally using MinIO (S3-compatible object storage).

---

## ✅ Requirements

- **Java 8** (JDK)
- **Hadoop 3.4.1** (Windows binary + `winutils.exe`)
- **PySpark** (compatible with Java and Hadoop versions)
- **MinIO** via Docker (S3 simulation)
- **boto3** (for S3 access from Python)

---

## 🔧 Installation Steps

### 1. Install Java (JDK 8)

- Download from [Adoptium](https://adoptium.net/) or Oracle.
- After installation, configure environment variables:

```powershell
setx JAVA_HOME "C:\Program Files\Java\jdk1.8.0_xx"
setx PATH "%JAVA_HOME%\bin;%PATH%"
```

Confirm with:

```cmd
java -version
```

---

### 2. Install Hadoop (with `winutils.exe`)

- Download Hadoop 3.4.1 binaries (no need for HDFS setup).
- Download a **compatible `winutils.exe`** for Hadoop 3.3.x (e.g., from: https://github.com/steveloughran/winutils).
- Place `winutils.exe` in:  
  `C:\hadoop\bin\winutils.exe`

Set environment variables:

```powershell
setx HADOOP_HOME "C:\hadoop"
setx PATH "%HADOOP_HOME%\bin;%PATH%"
```

⚠️ **Important**: Without `winutils.exe`, Spark will fail to start on Windows with errors related to permissions or native libraries.

---

### 3. Install PySpark

```bash
pip install pyspark
```

Make sure PySpark uses the correct Java and Hadoop installation by checking paths and logs at startup.

---

## 🧨 Common Issues & Fixes

### Missing AWS Credential Provider

Error:
```
java.lang.ClassNotFoundException: Class com.amazonaws.auth.InstanceProfileCredentialsProvider not found
```

✅ Fix: This means Spark is trying to use the default AWS credentials provider chain, but the required SDK class is not on the classpath.

To resolve this, force Spark to use basic access key authentication:

```python
spark._jsc.hadoopConfiguration().set(
    "fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
)
```

Then provide your credentials directly in code or via environment variables:

```python
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "your_access_key")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "your_secret_key")
```

---

### Classpath or AWS SDK errors

If interacting with S3/MinIO leads to `ClassNotFoundException`, add:

```python
spark._jsc.hadoopConfiguration().set(
    "fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
)
```

Make sure your `spark-defaults.conf` or job script includes any needed `.jar` files if you use custom SDK versions.

---

### Environment variable issues

Verify all key variables are correctly set:

- `JAVA_HOME` → JDK path
- `HADOOP_HOME` → Hadoop directory
- `PATH` includes both `%JAVA_HOME%\bin` and `%HADOOP_HOME%\bin`

We done with this, if using vsc, close and re-open the IDE. 

---

### ✅ Example: Working SparkSession configuration (Windows + MinIO)

Below is a minimal, working example used to read a CSV file from MinIO (running locally) using PySpark on Windows:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MinIO test") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

df = spark.read.csv("s3a://yourbucket/yourfile.csv", header=True, inferSchema=True)
df.show()
```

In this script we build the SparkSession:
- Sets the name of the Spark application (for logs/UI)
- Adds the required libraries (jars) to support reading from S3 (or MinIO)
- Defines the endpoint URL for your MinIO instance
- Sets MinIO credentials (default ones in this case)
- Enables path-style access (instead of virtual-hosted style that is not supported by MinIO)
  i.e., "http://localhost:9000/bucket/file" instead of "bucket.s3a.amazonaws.com"
- Specifies the credentials provider class — needed to authenticate with your MinIO server. 
  In particular, we force "access.key" and "secret.key"
- Finally, with .getOrCreate we actually creates the Spark session

Make sure:
- MinIO is running and reachable at `localhost:9000`
- The `yourbucket` bucket exists and contains `yourfile.csv`
- The `spark.jars.packages` versions match your Hadoop distribution

Notes: JARs are packages for the Java/Scale environment. With Spark, they are used to enable external functionality, such as reading from MinIO or working with cloud systems.

---

## ✅ Final Test

You should be able to:

1. Launch PySpark (`pyspark` in terminal or via script).
2. Read from local CSVs.
3. Connect to MinIO using the `s3a://` protocol.

If all three work, Spark is correctly set up on Windows.

---

## 📌 Notes

This setup was tested on **Windows 11**. It should also work on Windows 10 with the same steps. Admin permissions may be required when setting environment variables or running Docker.
