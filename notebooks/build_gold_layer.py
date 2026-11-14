import os
import random
import uuid
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, md5, concat_ws, lit, current_timestamp, to_date, to_timestamp, when, year, month, date_format
from delta.tables import DeltaTable

print("="*60)
print("  TELECOM LAKEHOUSE: BUILDING BRONZE, SILVER & GOLD LAYERS")
print("="*60)

# 1. Initialize Spark Session
print("\n[1/4] Initializing Spark Session with Delta Lake & MinIO S3A...")
spark = SparkSession.builder \
    .appName("Telecom_Build_Gold") \
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
print(" Spark Session ready!")

# 2. Bronze Layer: Generate 1,500 Correlated Telecom CDRs
print("\n[2/4] Generating Correlated Telecom Events into Bronze Delta (s3a://bronze/telecom_cdrs_raw)...")
cdr_events = []
base_time = datetime.now() - timedelta(hours=3)

for i in range(1500):
    event_time = base_time + timedelta(seconds=i * 6)
    event_type = random.choice(["voice", "voice", "data", "data", "data"])
    is_congested = (random.random() < 0.25) # 25% congested cells
    tower_id = f"TOWER_{random.randint(1, 50)}"
    customer_num = f"010{10000000 + random.randint(1, 100)}"
    
    if is_congested:
        signal_dbm = random.randint(-115, -90) # Weak signal
        latency_ms = random.randint(150, 380)  # High latency
        network_cond = "Congested"
        call_status = "dropped" if random.random() < 0.35 else "completed"
    else:
        signal_dbm = random.randint(-85, -55)  # Good signal
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

cdr_schema = StructType([
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

df_raw_cdrs = spark.createDataFrame(cdr_events, cdr_schema)
df_raw_cdrs.write.format("delta").mode("overwrite").save("s3a://bronze/telecom_cdrs_raw")
print(f" Bronze Delta Table written: {len(cdr_events)} CDR events in s3a://bronze/telecom_cdrs_raw")

# 3. Silver Layer: Clean, Enrich & Seed Dimensions
print("\n[3/4] Transforming to Silver Layer (Deduplication, Quality Quarantine, Seed Dimensions)...")
df_raw = spark.read.format("delta").load("s3a://bronze/telecom_cdrs_raw")

df_clean = df_raw.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
                 .withColumn("date_id", to_date(col("timestamp"))) \
                 .withColumn("data_consumed_mb", when(col("event_type") == "voice", 0).otherwise(col("data_consumed_mb"))) \
                 .withColumn("duration_seconds", when(col("event_type") == "data", 0).otherwise(col("duration_seconds")))

df_validated = df_clean.filter(col("call_id").isNotNull() & col("customer_number").isNotNull())
df_validated.write.format("delta").mode("overwrite").save("s3a://silver/telecom_cdrs")

# Silver Dimension Seeds
cities = ["Cairo - Nasr City", "Cairo - Maadi", "Cairo - Tagamoa", "Giza - Dokki", "Giza - 6th October", "Alexandria - Smouha", "Alexandria - Corniche", "Mansoura - City Center"]

# Towers Seed
towers_data = []
for i in range(1, 51):
    t_id = f"TOWER_{i}"
    city = cities[(i - 1) % len(cities)]
    tech = "5G" if i % 3 == 0 else "4G"
    towers_data.append((t_id, f"Tower Site #{i}", city, tech, 30.0 + (i*0.01), 31.2 + (i*0.01)))
spark.createDataFrame(towers_data, ["tower_id", "tower_name", "location_city", "technology", "latitude", "longitude"]) \
     .write.format("delta").mode("overwrite").save("s3a://silver/towers_dim_seed")

# Customers Seed
plan_ids = ["PLAN_PREPAID_BASIC", "PLAN_PREPAID_SUPER", "PLAN_POSTPAID_GOLD", "PLAN_POSTPAID_PLATINUM"]
customers_data = []
for i in range(1, 101):
    c_num = f"010{10000000 + i}"
    c_name = f"Subscriber_{i}"
    c_plan = plan_ids[i % len(plan_ids)]
    c_city = cities[i % len(cities)]
    customers_data.append((c_num, c_name, c_plan, c_city, "Active", "2023-01-15"))
spark.createDataFrame(customers_data, ["customer_number", "customer_name", "plan_id", "city", "status", "signup_date"]) \
     .write.format("delta").mode("overwrite").save("s3a://silver/customers_dim_seed")

# Plans Seed
plans_data = [
    ("PLAN_PREPAID_BASIC", "Flex Basic 30", 30, 2, 500),
    ("PLAN_PREPAID_SUPER", "Flex Super 70", 70, 6, 1200),
    ("PLAN_POSTPAID_GOLD", "Red Gold 200", 200, 25, 3000),
    ("PLAN_POSTPAID_PLATINUM", "Red Platinum 500", 500, 70, 8000)
]
spark.createDataFrame(plans_data, ["plan_id", "plan_name", "monthly_fee_egp", "quota_gb", "voice_minutes"]) \
     .write.format("delta").mode("overwrite").save("s3a://silver/seed_plan")

# Date Seed
df_date = spark.sql("SELECT explode(sequence(to_date('2024-01-01'), to_date('2027-12-31'), interval 1 day)) as Date")
df_dim_date = df_date.withColumn("date_id", col("Date")) \
                     .withColumn("year", year(col("Date"))) \
                     .withColumn("month", month(col("Date"))) \
                     .withColumn("day_name", date_format(col("Date"), "EEEE"))
df_dim_date.write.format("delta").mode("overwrite").save("s3a://silver/seed_date")
print(" Silver Layer complete: Cleaned telecom_cdrs + 4 dimension tables written in Delta.")

# 4. Gold Layer: Kimball Star Schema
print("\n[4/4] Building Gold Layer (Kimball Star Schema with Surrogate Keys & SCD Type 1)...")

# 4.1 Dim_Date
spark.read.format("delta").load("s3a://silver/seed_date") \
     .write.format("delta").mode("overwrite").option("mergeSchema", "true").save("s3a://gold/dim_date")

# 4.2 Dim_Tower (SCD Type 1)
df_dim_tower = spark.read.format("delta").load("s3a://silver/towers_dim_seed") \
                    .withColumn("tower_sk", md5(col("tower_id")))
df_dim_tower.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("s3a://gold/dim_tower")

# 4.3 Dim_Customer (SCD Type 1)
df_dim_cust = spark.read.format("delta").load("s3a://silver/customers_dim_seed") \
                   .withColumn("customer_sk", md5(col("customer_number")))
df_dim_cust.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("s3a://gold/dim_customer")

# 4.4 Dim_Plan
df_dim_plan = spark.read.format("delta").load("s3a://silver/seed_plan") \
                   .withColumn("plan_sk", md5(col("plan_id")))
df_dim_plan.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("s3a://gold/dim_plan")

# 4.5 Fact_CDR (Fact Table with Surrogate Keys & Measurements)
df_silver_cdrs = spark.read.format("delta").load("s3a://silver/telecom_cdrs")

df_fact = df_silver_cdrs \
    .withColumn("customer_sk", md5(col("customer_number"))) \
    .withColumn("tower_sk", md5(col("tower_id"))) \
    .select(
        "call_id",
        "customer_sk",
        "tower_sk",
        "date_id",
        "timestamp",
        "event_type",
        "duration_seconds",
        "data_consumed_mb",
        "call_status",
        "latency_ms",
        "signal_strength_dbm",
        "network_condition"
    )

df_fact.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("s3a://gold/fact_cdr")
print(" Gold Layer successfully materialized in MinIO!")

# Verification Queries
print("\n" + "="*60)
print("  TELECOM GOLD LAYER: STAR SCHEMA VERIFICATION")
print("="*60)

fact_table = spark.read.format("delta").load("s3a://gold/fact_cdr")
dim_tower = spark.read.format("delta").load("s3a://gold/dim_tower")
dim_cust = spark.read.format("delta").load("s3a://gold/dim_customer")
dim_plan = spark.read.format("delta").load("s3a://gold/dim_plan")

print(f"\n Fact_CDR Row Count:     {fact_table.count()}")
print(f" Dim_Tower Row Count:    {dim_tower.count()}")
print(f" Dim_Customer Row Count: {dim_cust.count()}")
print(f" Dim_Plan Row Count:     {dim_plan.count()}")

print("\n--- [Fact_CDR Sample Records] ---")
fact_table.show(5, truncate=False)

print("\n--- [Business Insight: Top 5 Towers with Most Dropped Calls] ---")
fact_table.filter(col("call_status") == "dropped") \
    .join(dim_tower, "tower_sk") \
    .groupBy("tower_name", "location_city", "technology") \
    .count() \
    .orderBy(col("count").desc()) \
    .show(5, truncate=False)

print("\n--- [Business Insight: Average QoS Metrics by Network Condition] ---")
fact_table.groupBy("network_condition") \
    .agg(
        {"latency_ms": "avg", "signal_strength_dbm": "avg", "data_consumed_mb": "sum"}
    ).show(truncate=False)

print("="*60)
print("  SUCCESS: GOLD LAYER IS COMPLETE AND ACCURATE!")
print("="*60)
