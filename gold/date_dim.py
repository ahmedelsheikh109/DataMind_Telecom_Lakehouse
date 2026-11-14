"""
DataMind Telecom Lakehouse — Gold Layer Date Dimension Generator
================================================================
Generates an algorithmic calendar dimension table spanning 2020 to 2030,
enriched with year, quarter, month, day, and weekend flags.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, quarter, dayofweek, date_format, expr

# ==============================================================================
# 1. Spark Session Initialization
# ==============================================================================
print("=" * 70)
print("  TELECOM LAKEHOUSE: GOLD LAYER DATE DIMENSION GENERATOR")
print("=" * 70)

spark = SparkSession.builder \
    .appName("Telecom_Date_Dimension") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ==============================================================================
# 2. Algorithmic Date Sequence Generation (2020 to 2030)
# ==============================================================================
start_date = "2020-01-01"
end_date = "2030-12-31"

df_date = spark.sql(f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) as Date")

df_dim_date = (
    df_date
    .withColumn("date_id", col("Date"))
    .withColumn("year", year(col("Date")))
    .withColumn("quarter", quarter(col("Date")))
    .withColumn("month", month(col("Date")))
    .withColumn("month_name", date_format(col("Date"), "MMMM"))
    .withColumn("day_of_month", dayofmonth(col("Date")))
    .withColumn("day_of_week", dayofweek(col("Date")))
    .withColumn("day_name", date_format(col("Date"), "EEEE"))
    .withColumn(
        "is_weekend",
        expr("CASE WHEN dayofweek(Date) IN (6, 7) THEN True ELSE False END")
    )
)

# ==============================================================================
# 3. Delta Lake Materialization
# ==============================================================================
target_path = "s3a://gold/dim_date"
print(f"Writing Date Dimension to: {target_path}")

(
    df_dim_date.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .save(target_path)
)

print(f"Dim_Date materialized successfully! Total days generated: {df_dim_date.count():,}")
print("=" * 70)
