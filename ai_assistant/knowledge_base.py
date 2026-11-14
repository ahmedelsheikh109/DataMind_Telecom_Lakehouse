"""
Telecom Domain Knowledge Base & RAG Context
Provides semantic schema context, domain glossaries, and SLA rules for Text-to-SQL.
"""

TELECOM_SCHEMA_CONTEXT = """
Database Catalog: delta
Database Schema: telecom

Tables and Column Descriptions:
1. Table: delta.telecom.fact_cdr (Call Detail Records & Data Traffic Events)
   - call_id (VARCHAR): Unique identifier for each event
   - customer_sk (VARCHAR): Surrogate key joining to dim_customer (MD5 hash)
   - tower_sk (VARCHAR): Surrogate key joining to dim_tower (MD5 hash)
   - date_id (DATE): Event date (YYYY-MM-DD), joins to dim_date
   - timestamp (TIMESTAMP): Full event timestamp
   - event_type (VARCHAR): 'voice' for voice calls, 'data' for internet sessions
   - duration_seconds (INTEGER): Call duration in seconds (0 for data events)
   - data_consumed_mb (INTEGER): Data consumed in Megabytes (0 for voice calls)
   - call_status (VARCHAR): 'completed' or 'dropped' (only voice calls can be dropped)
   - latency_ms (INTEGER): Network round-trip latency in milliseconds
   - signal_strength_dbm (INTEGER): Mobile signal strength in dBm (-120 dBm is weak, -50 dBm is excellent)
   - network_condition (VARCHAR): 'Normal' or 'Congested'

2. Table: delta.telecom.dim_tower (Cell Towers Dimension)
   - tower_sk (VARCHAR): Primary surrogate key (MD5 hash)
   - tower_id (VARCHAR): Business key (e.g. 'TOWER_1')
   - tower_name (VARCHAR): Display name (e.g. 'Tower Site #1')
   - location_city (VARCHAR): City and district (e.g. 'Cairo - Nasr City', 'Cairo - Maadi', 'Alexandria - Smouha', 'Mansoura - City Center')
   - technology (VARCHAR): Cellular generation ('4G' or '5G')
   - latitude (DOUBLE): Tower latitude coordinate
   - longitude (DOUBLE): Tower longitude coordinate

3. Table: delta.telecom.dim_customer (Subscriber Dimension)
   - customer_sk (VARCHAR): Primary surrogate key (MD5 hash)
   - customer_number (VARCHAR): MSISDN (e.g. '01010000001')
   - customer_name (VARCHAR): Subscriber name
   - plan_id (VARCHAR): Subscribed plan code (joins to dim_plan)
   - city (VARCHAR): Customer home city
   - status (VARCHAR): Account status ('Active')
   - signup_date (VARCHAR): Subscription start date

4. Table: delta.telecom.dim_plan (Billing & Tariffs Dimension)
   - plan_sk (VARCHAR): Primary surrogate key (MD5 hash)
   - plan_id (VARCHAR): Business key ('PLAN_PREPAID_BASIC', 'PLAN_PREPAID_SUPER', 'PLAN_POSTPAID_GOLD', 'PLAN_POSTPAID_PLATINUM')
   - plan_name (VARCHAR): Plan commercial name ('Flex Basic 30', 'Flex Super 70', 'Red Gold 200', 'Red Platinum 500')
   - monthly_fee_egp (INTEGER): Price in Egyptian Pounds (EGP)
   - quota_gb (INTEGER): Internet bundle quota in Gigabytes
   - voice_minutes (INTEGER): Included voice call minutes

5. Table: delta.telecom.dim_date (Calendar Dimension)
   - date_id (DATE): Calendar date (PK)
   - year (INTEGER): Year
   - month (INTEGER): Month number (1-12)
   - day_name (VARCHAR): Day of the week (e.g. 'Monday')
"""

TELECOM_BUSINESS_RULES = """
Telecom Domain Business Rules & Engineering KPIs:
- Call Drop Rate (CDR %): Calculated as:
    ROUND(CAST(SUM(CASE WHEN call_status = 'dropped' THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(call_id), 0) * 100, 2)
  * Healthy SLA: < 2.0%
  * Warning: 2.0% - 5.0%
  * Critical Anomaly / Outage: > 5.0%
- Congestion Criteria:
  * Network condition is 'Congested' when signal_strength_dbm <= -90 dBm and latency_ms > 150 ms.
- Data Conversion:
  * 1 Gigabyte (GB) = 1024 Megabytes (MB).
  * 1 Minute = 60 Seconds.
- Surrogate Keys:
  * Always JOIN fact_cdr to dim_tower ON fact_cdr.tower_sk = dim_tower.tower_sk
  * Always JOIN fact_cdr to dim_customer ON fact_cdr.customer_sk = dim_customer.customer_sk
  * Always JOIN dim_customer to dim_plan ON dim_customer.plan_id = dim_plan.plan_id
  * Always JOIN fact_cdr to dim_date ON fact_cdr.date_id = dim_date.date_id
"""

def get_rag_context(user_query: str) -> str:
    """
    Retrieves the relevant schema and business definitions matching the user query.
    """
    return f"{TELECOM_SCHEMA_CONTEXT}\n\n{TELECOM_BUSINESS_RULES}"
