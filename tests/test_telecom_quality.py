"""
DataMind Telecom Lakehouse - Automated QA & Integrity Suite (PyTest)
===================================================================
Executes 9 production-grade assertions across Trino and Delta Lake:
1. Fact Table Primary Key Uniqueness
2. Dimension Customer Surrogate Key Uniqueness
3. Dimension Tower Surrogate Key Uniqueness
4. Referential Integrity (Zero Orphan Customer Keys)
5. Referential Integrity (Zero Orphan Tower Keys)
6. Telecom Domain Rule (Voice calls data_consumed_mb == 0)
7. Telecom Domain Rule (Data sessions duration_seconds == 0)
8. QoS Sanity (latency_ms > 0)
9. RF Physics Sanity (signal_strength_dbm between -140 and -30 dBm)
"""

import pytest
import trino

@pytest.fixture(scope="module")
def trino_cursor():
    conn = trino.dbapi.connect(
        host="localhost",
        port=8085,
        user="admin",
        catalog="delta",
        schema="telecom"
    )
    cur = conn.cursor()
    yield cur
    conn.close()

def test_fact_cdr_pk_uniqueness(trino_cursor):
    """Test 1: Fact_CDR Primary Key (call_id) must have 0 duplicates"""
    trino_cursor.execute("SELECT COUNT(call_id) - COUNT(DISTINCT call_id) FROM delta.telecom.fact_cdr")
    dups = trino_cursor.fetchone()[0]
    assert dups == 0, f"Found {dups} duplicate call_id values in fact_cdr"

def test_dim_customer_sk_uniqueness(trino_cursor):
    """Test 2: Dim_Customer Surrogate Key (customer_sk) must have 0 duplicates"""
    trino_cursor.execute("SELECT COUNT(customer_sk) - COUNT(DISTINCT customer_sk) FROM delta.telecom.dim_customer")
    dups = trino_cursor.fetchone()[0]
    assert dups == 0, f"Found {dups} duplicate customer_sk in dim_customer"

def test_dim_tower_sk_uniqueness(trino_cursor):
    """Test 3: Dim_Tower Surrogate Key (tower_sk) must have 0 duplicates"""
    trino_cursor.execute("SELECT COUNT(tower_sk) - COUNT(DISTINCT tower_sk) FROM delta.telecom.dim_tower")
    dups = trino_cursor.fetchone()[0]
    assert dups == 0, f"Found {dups} duplicate tower_sk in dim_tower"

def test_referential_integrity_customers(trino_cursor):
    """Test 4: 0 orphan customers in fact_cdr"""
    sql = """
    SELECT COUNT(f.customer_sk) 
    FROM delta.telecom.fact_cdr f 
    LEFT JOIN delta.telecom.dim_customer c ON f.customer_sk = c.customer_sk 
    WHERE c.customer_sk IS NULL
    """
    trino_cursor.execute(sql)
    orphans = trino_cursor.fetchone()[0]
    assert orphans == 0, f"Found {orphans} orphan customer keys in fact_cdr"

def test_referential_integrity_towers(trino_cursor):
    """Test 5: 0 orphan towers in fact_cdr"""
    sql = """
    SELECT COUNT(f.tower_sk) 
    FROM delta.telecom.fact_cdr f 
    LEFT JOIN delta.telecom.dim_tower t ON f.tower_sk = t.tower_sk 
    WHERE t.tower_sk IS NULL
    """
    trino_cursor.execute(sql)
    orphans = trino_cursor.fetchone()[0]
    assert orphans == 0, f"Found {orphans} orphan tower keys in fact_cdr"

def test_voice_calls_zero_data(trino_cursor):
    """Test 6: Voice calls must strictly have 0 MB data consumed"""
    trino_cursor.execute("SELECT COUNT(*) FROM delta.telecom.fact_cdr WHERE event_type = 'voice' AND data_consumed_mb > 0")
    violations = trino_cursor.fetchone()[0]
    assert violations == 0, f"Found {violations} voice calls with data consumption"

def test_data_sessions_zero_duration(trino_cursor):
    """Test 7: Data sessions must strictly have 0 seconds call duration"""
    trino_cursor.execute("SELECT COUNT(*) FROM delta.telecom.fact_cdr WHERE event_type = 'data' AND duration_seconds > 0")
    violations = trino_cursor.fetchone()[0]
    assert violations == 0, f"Found {violations} data sessions with call duration"

def test_qos_latency_positive(trino_cursor):
    """Test 8: Round-trip latency must be strictly positive"""
    trino_cursor.execute("SELECT COUNT(*) FROM delta.telecom.fact_cdr WHERE latency_ms <= 0")
    violations = trino_cursor.fetchone()[0]
    assert violations == 0, f"Found {violations} records with invalid latency"

def test_rf_signal_strength_bounds(trino_cursor):
    """Test 9: Signal strength dBm must be in realistic physical range [-140, -30]"""
    trino_cursor.execute("SELECT COUNT(*) FROM delta.telecom.fact_cdr WHERE signal_strength_dbm > -30 OR signal_strength_dbm < -140")
    violations = trino_cursor.fetchone()[0]
    assert violations == 0, f"Found {violations} records outside RF physical range"
