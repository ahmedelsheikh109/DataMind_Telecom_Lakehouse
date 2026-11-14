"""
DataMind Telecom Lakehouse — Bronze Layer Ingestion
===================================================
Ingests raw streaming/batch Call Detail Records (CDRs) and network telemetry
into the Bronze Delta Lake with schema enforcement and audit metadata.
"""

import os
import uuid
import random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, lit, current_timestamp, input_file_name

# ==============================================================================
# 1. Spark Session Initialization
# ==============================================================================
print("=" * 70)
print("  TELECOM LAKEHOUSE: BRONZE LAYER INGESTION")
print("=" * 70)

spark = SparkSession.builder \
    .appName("Telecom_Bronze_Ingestion") \
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
# 2. Configuration & Paths
# ==============================================================================
bronze_target_path = "s3a://bronze/telecom_cdrs_raw"
checkpoint_path = "s3a://bronze/checkpoints/bronze_cp/"

print(f"Target Bronze Table: {bronze_target_path}")

# ==============================================================================
# 3. Raw Telecom CDR Stream / Batch Generation (Correlated Physics)
# ==============================================================================
print("\nGenerating correlated telecom CDR telemetry...")
cdr_events = []
base_time = datetime.now() - timedelta(hours=3)

for i in range(1500):
    event_time = base_time + timedelta(seconds=i * 6)
    event_type = random.choice(["voice", "voice", "data", "data", "data"])
    is_congested = (random.random() < 0.25) # 25% congested cells
    tower_id = f"TOWER_{random.randint(1, 50)}"
    customer_num = f"010{10000000 + random.randint(1, 100)}"
    
    if is_congested:
        signal_dbm = random.randint(-115, -90) # Weak RF signal
        latency_ms = random.randint(150, 380)  # High round-trip latency
        network_cond = "Congested"
        call_status = "dropped" if random.random() < 0.35 else "completed"
    else:
        signal_dbm = random.randint(-85, -55)  # Good RF signal
        latency_ms = random.randint(20, 75)    # Low latency
        network_cond = "Normal"
        call_status = "dropped" if random.random() < 0.02 else "completed"

    if event_type == "voice":
        duration = random.randint(15, 450)
        data_mb = 0
    else:
        duration = 0
        data_mb = random.randint(5, 350)
        call_status = "completed"

    cdr_events.append((
        str(uuid.uuid4())[:8],
        customer_num,
        tower_id,
        event_type,
        duration,
        data_mb,
        call_status,
        latency_ms,
        signal_dbm,
        network_cond,
        event_time.strftime("%Y-%m-%d %H:%M:%S")
    ))

schema = StructType([
    StructField("call_id", StringType(), True),
    StructField("customer_number", StringType(), True),
    StructField("tower_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("duration_seconds", IntegerType(), True),
    StructField("data_consumed_mb", IntegerType(), True),
    StructField("call_status", StringType(), True),
    StructField("latency_ms", IntegerType(), True),
    StructField("signal_strength_dbm", IntegerType(), True),
    StructField("network_condition", StringType(), True),
    StructField("timestamp", StringType(), True)
])

df_raw = spark.createDataFrame(cdr_events, schema)

# ==============================================================================
# 4. Ingestion Metadata & Delta Lake Write
# ==============================================================================
df_bronze = df_raw \
    .withColumn("bronze_ingested_at", current_timestamp()) \
    .withColumn("source_stream", lit("telecom.kafka.cdr_events"))

(
    df_bronze.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .save(bronze_target_path)
)

print(f"Bronze Layer Materialized Successfully! Total rows: {df_bronze.count():,}")
print("=" * 70)
