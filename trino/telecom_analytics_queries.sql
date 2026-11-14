-- =====================================================================
-- TELECOM LAKEHOUSE ANALYTICS (TRINO SQL ENGINE)
-- Querying Gold Layer Star Schema (Delta Lake on MinIO S3)
-- =====================================================================

-- 1. Check all materialized tables in Delta Lake
SHOW TABLES FROM delta.telecom;

-- 2. Inspect Fact Table schema and sample records
SELECT * 
FROM delta.telecom.fact_cdr 
LIMIT 10;

-- 3. Business KPI: Top 5 Cell Towers with Highest Dropped Calls (Cell Congestion)
SELECT 
    t.tower_name,
    t.location_city,
    t.technology,
    COUNT(f.call_id) AS total_events,
    SUM(CASE WHEN f.call_status = 'dropped' THEN 1 ELSE 0 END) AS dropped_calls,
    ROUND(CAST(SUM(CASE WHEN f.call_status = 'dropped' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(f.call_id) * 100, 2) AS drop_rate_pct,
    ROUND(AVG(f.latency_ms), 2) AS avg_latency_ms,
    ROUND(AVG(f.signal_strength_dbm), 2) AS avg_signal_dbm
FROM delta.telecom.fact_cdr f
JOIN delta.telecom.dim_tower t ON f.tower_sk = t.tower_sk
GROUP BY t.tower_name, t.location_city, t.technology
ORDER BY dropped_calls DESC
LIMIT 5;

-- 4. Commercial Analysis: Revenue & Usage by Subscription Plan
SELECT 
    p.plan_name,
    p.monthly_fee_egp,
    COUNT(DISTINCT c.customer_number) AS total_subscribers,
    ROUND(SUM(f.data_consumed_mb) / 1024.0, 2) AS total_gb_consumed,
    ROUND(SUM(f.duration_seconds) / 60.0, 2) AS total_voice_minutes,
    COUNT(DISTINCT c.customer_number) * p.monthly_fee_egp AS estimated_monthly_revenue_egp
FROM delta.telecom.fact_cdr f
JOIN delta.telecom.dim_customer c ON f.customer_sk = c.customer_sk
JOIN delta.telecom.dim_plan p ON c.plan_id = p.plan_id
GROUP BY p.plan_name, p.monthly_fee_egp
ORDER BY total_gb_consumed DESC;

-- 5. Network Engineering: QoS Breakdown by Technology (4G vs 5G)
SELECT 
    t.technology,
    COUNT(f.call_id) AS total_traffic_events,
    ROUND(AVG(f.latency_ms), 2) AS avg_latency_ms,
    ROUND(AVG(f.signal_strength_dbm), 2) AS avg_signal_strength_dbm,
    SUM(f.data_consumed_mb) AS total_mb_transferred
FROM delta.telecom.fact_cdr f
JOIN delta.telecom.dim_tower t ON f.tower_sk = t.tower_sk
GROUP BY t.technology;
