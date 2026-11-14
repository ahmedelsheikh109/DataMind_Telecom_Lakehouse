"""
DataMind Telecom Lakehouse — Silver Layer Cleansing & Conforming
================================================================
Performs incremental state management, window-based deduplication,
domain-specific QoS cleaning, quarantine routing for corrupt records,
and CRM dimension staging in Delta Lake.
"""

import uuid
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, to_timestamp, to_date, trim, regexp_replace,
    initcap, lower, current_timestamp, row_number, max
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ==============================================================================
# 1. Spark Session Initialization
# ==============================================================================
print("=" * 70)
print("  TELECOM LAKEHOUSE: SILVER LAYER CLEANSING & CONFORMING")
print("=" * 70)

spark = SparkSession.builder \
    .appName("Telecom_Silver_Cleansing") \
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
# 2. Configurations & State Variables
# ==============================================================================
bronze_table = "s3a://bronze/telecom_cdrs_raw"
silver_table = "s3a://silver/telecom_cdrs"
quarantine_table = "s3a://silver/telecom_quarantine"
control_table = "s3a://silver/processing_control"

silver_run_id = str(uuid.uuid4())
PRIMARY_KEY = "call_id"

print(f"Silver Run ID: {silver_run_id}")
print(f"Source Bronze: {bronze_table}")
print(f"Target Silver: {silver_table}")

# ==============================================================================
# 3. State Management & Incremental Processing
# ==============================================================================
def get_last_processed_ts():
    try:
        if DeltaTable.isDeltaTable(spark, control_table):
            df_ctrl = spark.read.format("delta").load(control_table)
            last_ts = df_ctrl.filter(col("entity_name") == "telecom_cdrs") \
                             .select(max("last_bronze_ingested_at")).collect()[0][0]
            return last_ts
    except Exception:
        return None
    return None

last_processed_ts = get_last_processed_ts()
df_bronze = spark.read.format("delta").load(bronze_table)

if last_processed_ts:
    print(f"Incremental Fetch: Loading records ingested after {last_processed_ts}")
    df_new = df_bronze.filter(col("bronze_ingested_at") > lit(last_processed_ts))
else:
    print("Full Initial Ingestion: Loading all available records from Bronze...")
    df_new = df_bronze

new_records_count = df_new.count()
print(f"Records to process: {new_records_count:,}")

if new_records_count > 0:
    # ==========================================================================
    # 4. Window-Based Deduplication (Preserve Latest Ingestion)
    # ==========================================================================
    dedup_window = Window.partitionBy(PRIMARY_KEY).orderBy(col("bronze_ingested_at").desc())
    df_dedup = df_new \
        .withColumn("row_rank", row_number().over(dedup_window)) \
        .filter(col("row_rank") == 1) \
        .drop("row_rank")

    # ==========================================================================
    # 5. Telecom Domain Business Cleansing
    # ==========================================================================
    df_clean = df_dedup \
        .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("date_id", to_date(col("timestamp"))) \
        .withColumn("call_id", trim(regexp_replace(col("call_id"), r'\s+', ''))) \
        .withColumn("customer_number", trim(regexp_replace(col("customer_number"), r'\s+', ''))) \
        .withColumn("tower_id", trim(col("tower_id"))) \
        .withColumn("event_type", lower(trim(col("event_type")))) \
        .withColumn("call_status", lower(trim(col("call_status")))) \
        .withColumn("network_condition", initcap(trim(col("network_condition"))))

    # Business Rule Enforcement:
    # Voice calls do not consume data, and data sessions have 0 call duration
    df_clean = df_clean \
        .withColumn("data_consumed_mb", when(col("event_type") == "voice", 0).otherwise(col("data_consumed_mb"))) \
        .withColumn("duration_seconds", when(col("event_type") == "data", 0).otherwise(col("duration_seconds")))

    # RF Physical Invariants Sanitization
    df_clean = df_clean \
        .withColumn("latency_ms", when(col("latency_ms") <= 0, 10).otherwise(col("latency_ms"))) \
        .withColumn("signal_strength_dbm", when(col("signal_strength_dbm") > -30, -50).when(col("signal_strength_dbm") < -140, -120).otherwise(col("signal_strength_dbm"))) \
        .withColumn("silver_run_id", lit(silver_run_id)) \
        .withColumn("silver_processed_at", current_timestamp())

    # ==========================================================================
    # 6. Data Quality Quarantine Pattern
    # ==========================================================================
    df_valid = df_clean.filter(
        col("call_id").isNotNull() & 
        col("customer_number").isNotNull() & 
        col("tower_id").isNotNull()
    )

    df_quarantine = df_clean.filter(
        col("call_id").isNull() | 
        col("customer_number").isNull() | 
        col("tower_id").isNull()
    ).withColumn("quarantine_ts", current_timestamp()) \
     .withColumn("quarantine_reason", lit("Missing Primary or Foreign Business Key"))

    # Upsert Clean Records into Silver Delta Table
    if not DeltaTable.isDeltaTable(spark, silver_table):
        df_valid.write.format("delta").mode("overwrite").save(silver_table)
    else:
        silver_dt = DeltaTable.forPath(spark, silver_table)
        silver_dt.alias("t").merge(
            df_valid.alias("s"),
            f"t.{PRIMARY_KEY} = s.{PRIMARY_KEY}"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

    # Route Invalid Records to Quarantine
    if df_quarantine.count() > 0:
        df_quarantine.write.format("delta").mode("append").option("mergeSchema", "true").save(quarantine_table)
        print(f"Quarantined records: {df_quarantine.count()}")

    print(f"Cleaned records written to Silver: {df_valid.count():,}")

    # ==========================================================================
    # 7. Seed CRM Dimension Staging Tables
    # ==========================================================================
    print("Staging CRM Dimension Tables in Silver...")
    cities = [
        "Cairo - Nasr City", "Cairo - Maadi", "Cairo - Tagamoa", 
        "Giza - Dokki", "Giza - 6th October", "Alexandria - Smouha", 
        "Alexandria - Corniche", "Mansoura - City Center"
    ]

    # 50 Cell Towers
    towers_data = []
    for i in range(1, 51):
        t_id = f"TOWER_{i}"
        city = cities[(i - 1) % len(cities)]
        tech = "5G" if i % 3 == 0 else "4G"
        towers_data.append((t_id, f"Tower Site #{i}", city, tech, 30.0 + (i * 0.01), 31.2 + (i * 0.01)))
    
    spark.createDataFrame(towers_data, ["tower_id", "tower_name", "location_city", "technology", "latitude", "longitude"]) \
         .write.format("delta").mode("overwrite").save("s3a://silver/towers_dim_seed")

    # 100 Subscribers
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

    # 4 Commercial Plans
    plans_data = [
        ("PLAN_PREPAID_BASIC", "Flex Basic 30", 30, 2, 500),
        ("PLAN_PREPAID_SUPER", "Flex Super 70", 70, 6, 1200),
        ("PLAN_POSTPAID_GOLD", "Red Gold 200", 200, 25, 3000),
        ("PLAN_POSTPAID_PLATINUM", "Red Platinum 500", 500, 70, 8000)
    ]
    spark.createDataFrame(plans_data, ["plan_id", "plan_name", "monthly_fee_egp", "quota_gb", "voice_minutes"]) \
         .write.format("delta").mode("overwrite").save("s3a://silver/seed_plan")

    # ==========================================================================
    # 8. Update Control Table (High-Water Mark)
    # ==========================================================================
    max_bronze_ts = df_new.select(max("bronze_ingested_at")).collect()[0][0]
    control_data = [(
        "silver",
        "telecom_cdrs",
        max_bronze_ts,
        df_valid.count(),
        "SUCCESS",
        silver_run_id,
        datetime.now()
    )]
    control_schema = """
        layer STRING,
        entity_name STRING,
        last_bronze_ingested_at TIMESTAMP,
        rows_merged BIGINT,
        run_status STRING,
        silver_run_id STRING,
        updated_at TIMESTAMP
    """
    df_ctrl_new = spark.createDataFrame(control_data, schema=control_schema)
    
    if not DeltaTable.isDeltaTable(spark, control_table):
        df_ctrl_new.write.format("delta").mode("overwrite").save(control_table)
    else:
        dt_ctrl = DeltaTable.forPath(spark, control_table)
        dt_ctrl.alias("t").merge(
            df_ctrl_new.alias("s"),
            "t.layer = s.layer AND t.entity_name = s.entity_name"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    print(f"Control Table updated successfully! High-water mark set to: {max_bronze_ts}")

print("=" * 70)
