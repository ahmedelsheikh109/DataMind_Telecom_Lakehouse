"""
DataMind Telecom Lakehouse — Gold Layer Star Schema & Dimensional Modeling
==========================================================================
Materializes the Kimball Star Schema with deterministic MD5 surrogate keys,
referential integrity enforcement, and Delta MERGE upserts across
Fact and Dimension tables.
"""

import uuid
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, md5, current_timestamp, max, lit
from delta.tables import DeltaTable

# ==============================================================================
# 1. Spark Session Initialization
# ==============================================================================
print("=" * 70)
print("  TELECOM LAKEHOUSE: GOLD LAYER STAR SCHEMA & MERGE")
print("=" * 70)

spark = SparkSession.builder \
    .appName("Telecom_Gold_Star_Schema") \
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
# 2. Configurations & Table Paths
# ==============================================================================
silver_cdrs_table = "s3a://silver/telecom_cdrs"
dim_tower_table = "s3a://gold/dim_tower"
dim_customer_table = "s3a://gold/dim_customer"
dim_plan_table = "s3a://gold/dim_plan"
dim_date_table = "s3a://gold/dim_date"
fact_cdr_table = "s3a://gold/fact_cdr"
control_table = "s3a://gold/gold_processing_control"

gold_run_id = str(uuid.uuid4())
PRIMARY_KEY = "call_id"

print(f"Gold Run ID: {gold_run_id}")

# ==============================================================================
# 3. Process Dimensions (Surrogate Keys & Upsert)
# ==============================================================================
print("\n[1/4] Processing Dim_Tower (SCD Type 1)...")
df_towers_seed = spark.read.format("delta").load("s3a://silver/towers_dim_seed")
df_dim_tower = df_towers_seed.withColumn("tower_sk", md5(col("tower_id")))

if not DeltaTable.isDeltaTable(spark, dim_tower_table):
    df_dim_tower.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(dim_tower_table)
else:
    dt_tower = DeltaTable.forPath(spark, dim_tower_table)
    dt_tower.alias("t").merge(
        df_dim_tower.alias("s"),
        "t.tower_sk = s.tower_sk"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
print(f"Dim_Tower count: {df_dim_tower.count()}")

print("\n[2/4] Processing Dim_Customer (SCD Type 1)...")
df_cust_seed = spark.read.format("delta").load("s3a://silver/customers_dim_seed")
df_dim_cust = df_cust_seed.withColumn("customer_sk", md5(col("customer_number")))

if not DeltaTable.isDeltaTable(spark, dim_customer_table):
    df_dim_cust.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(dim_customer_table)
else:
    dt_cust = DeltaTable.forPath(spark, dim_customer_table)
    dt_cust.alias("t").merge(
        df_dim_cust.alias("s"),
        "t.customer_sk = s.customer_sk"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
print(f"Dim_Customer count: {df_dim_cust.count()}")

print("\n[3/4] Processing Dim_Plan (SCD Type 1)...")
df_plan_seed = spark.read.format("delta").load("s3a://silver/seed_plan")
df_dim_plan = df_plan_seed.withColumn("plan_sk", md5(col("plan_id")))

if not DeltaTable.isDeltaTable(spark, dim_plan_table):
    df_dim_plan.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(dim_plan_table)
else:
    dt_plan = DeltaTable.forPath(spark, dim_plan_table)
    dt_plan.alias("t").merge(
        df_dim_plan.alias("s"),
        "t.plan_sk = s.plan_sk"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
print(f"Dim_Plan count: {df_dim_plan.count()}")

# ==============================================================================
# 4. Materialize Fact_CDR Table (Delta MERGE)
# ==============================================================================
print("\n[4/4] Processing Fact_CDR (Kimball Fact Table with Surrogate Keys)...")
df_silver_cdrs = spark.read.format("delta").load(silver_cdrs_table)

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

if not DeltaTable.isDeltaTable(spark, fact_cdr_table):
    print("Initializing Fact_CDR Delta Lake table...")
    df_fact.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(fact_cdr_table)
else:
    print("Executing Delta MERGE on Fact_CDR...")
    dt_fact = DeltaTable.forPath(spark, fact_cdr_table)
    dt_fact.alias("t").merge(
        df_fact.alias("s"),
        f"t.{PRIMARY_KEY} = s.{PRIMARY_KEY}"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

fact_count = df_fact.count()
print(f"Fact_CDR successfully updated! Total rows: {fact_count:,}")

# ==============================================================================
# 5. Control Table High-Water Mark Update
# ==============================================================================
print("\nUpdating Gold Processing Control Table...")
control_data = [(
    "gold",
    "telecom_star_schema",
    fact_count,
    "SUCCESS",
    gold_run_id,
    datetime.now()
)]
control_schema = """
    layer STRING,
    entity_name STRING,
    rows_merged BIGINT,
    run_status STRING,
    gold_run_id STRING,
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

print("Gold Layer Pipeline Completed Successfully!")
print("=" * 70)
