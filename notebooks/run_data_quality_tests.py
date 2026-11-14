from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct

spark = SparkSession.builder \
    .appName("Telecom_DQ_Tests") \
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

print("="*65)
print("  SENIOR DATA ENGINEER: AUTOMATED DATA QUALITY & INTEGRITY SUITE")
print("="*65)

fact = spark.read.format("delta").load("s3a://gold/fact_cdr")
dim_cust = spark.read.format("delta").load("s3a://gold/dim_customer")
dim_tower = spark.read.format("delta").load("s3a://gold/dim_tower")
dim_plan = spark.read.format("delta").load("s3a://gold/dim_plan")

total_tests = 0
passed_tests = 0

def assert_test(name, condition, details=""):
    global total_tests, passed_tests
    total_tests += 1
    if condition:
        passed_tests += 1
        print(f" [PASS] Test #{total_tests}: {name}")
    else:
        print(f"❌ [FAIL] Test #{total_tests}: {name} - Details: {details}")

# Test 1: Primary Key Uniqueness on Fact Table
fact_total = fact.count()
fact_distinct_pk = fact.select("call_id").distinct().count()
assert_test("Fact_CDR Primary Key (call_id) Uniqueness", fact_total == fact_distinct_pk, f"Total: {fact_total}, Distinct: {fact_distinct_pk}")

# Test 2: Surrogate Key Uniqueness on Dim_Customer
cust_total = dim_cust.count()
cust_distinct = dim_cust.select("customer_sk").distinct().count()
assert_test("Dim_Customer Surrogate Key (customer_sk) Uniqueness", cust_total == cust_distinct, f"Total: {cust_total}, Distinct: {cust_distinct}")

# Test 3: Surrogate Key Uniqueness on Dim_Tower
tower_total = dim_tower.count()
tower_distinct = dim_tower.select("tower_sk").distinct().count()
assert_test("Dim_Tower Surrogate Key (tower_sk) Uniqueness", tower_total == tower_distinct, f"Total: {tower_total}, Distinct: {tower_distinct}")

# Test 4: Referential Integrity (No Orphan Customers in Fact)
orphan_customers = fact.join(dim_cust, "customer_sk", "left_anti").count()
assert_test("Referential Integrity: No Orphan Customer Keys in Fact_CDR", orphan_customers == 0, f"Orphans found: {orphan_customers}")

# Test 5: Referential Integrity (No Orphan Towers in Fact)
orphan_towers = fact.join(dim_tower, "tower_sk", "left_anti").count()
assert_test("Referential Integrity: No Orphan Tower Keys in Fact_CDR", orphan_towers == 0, f"Orphans found: {orphan_towers}")

# Test 6: Business Domain Rule - Voice Calls Must Have 0 MB Data Usage
voice_with_data = fact.filter((col("event_type") == "voice") & (col("data_consumed_mb") > 0)).count()
assert_test("Business Rule: Voice calls have data_consumed_mb == 0", voice_with_data == 0, f"Violations: {voice_with_data}")

# Test 7: Business Domain Rule - Data Sessions Must Have 0 Call Duration
data_with_duration = fact.filter((col("event_type") == "data") & (col("duration_seconds") > 0)).count()
assert_test("Business Rule: Data sessions have duration_seconds == 0", data_with_duration == 0, f"Violations: {data_with_duration}")

# Test 8: QoS Sanity Check - Latency must be strictly positive
invalid_latency = fact.filter(col("latency_ms") <= 0).count()
assert_test("QoS Sanity: latency_ms > 0", invalid_latency == 0, f"Violations: {invalid_latency}")

# Test 9: QoS Sanity Check - Signal Strength dBm must be in realistic negative range (-140 to -30 dBm)
invalid_signal = fact.filter((col("signal_strength_dbm") > -30) | (col("signal_strength_dbm") < -140)).count()
assert_test("QoS Sanity: signal_strength_dbm in range [-140, -30]", invalid_signal == 0, f"Violations: {invalid_signal}")

print("="*65)
print(f"  TOTAL TESTS RUN: {total_tests} | PASSED: {passed_tests} | FAILED: {total_tests - passed_tests}")
print("="*65)
