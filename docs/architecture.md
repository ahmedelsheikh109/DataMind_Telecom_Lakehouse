# DataMind Telecom Lakehouse — System Architecture & Design

## 1. High-Level Architecture

The platform implements an end-to-end **Medallion Lakehouse Architecture** on top of **Delta Lake** and **MinIO S3**, queried via **Trino (Distributed SQL Engine)**, and interfaced through a **GenAI Text-to-SQL + Domain RAG Assistant** powered by **Google Gemini**.

```mermaid
flowchart TD
    subgraph Ingestion["1. Ingestion Layer"]
        K[Kafka / CDR Stream] -->|PySpark Streaming| B[(Bronze Layer: s3a://bronze/telecom_cdrs_raw)]
    end

    subgraph Processing["2. Medallion Refinement Layer"]
        B -->|Cleaning & Quality Quarantine| S[(Silver Layer: s3a://silver/telecom_cdrs)]
        S -->|Kimball Star Schema & MD5 Surrogate Keys| G[(Gold Layer: s3a://gold/)]
        G --> F[fact_cdr]
        G --> C[dim_customer]
        G --> T[dim_tower]
        G --> P[dim_plan]
        G --> D[dim_date]
    end

    subgraph Serving["3. Serving & Query Engine"]
        F & C & T & P & D --> TR[Trino Coordinator Port 8085]
        TR --> CAT[Catalog: delta.telecom]
    end

    subgraph AI["4. Agentic AI & RAG Layer"]
        U[User Question] --> LLM[Google Gemini 2.5 Flash]
        KB[(Telecom Domain Knowledge Base)] --> LLM
        LLM -->|ANSI SQL Query| TR
        TR -->|DataFrame Results| LLM
        LLM -->|Telecom SLA Analysis & Direct Answer| ST[Streamlit Dashboard Port 8501]
    end
```

---

## 2. Kimball Dimensional Model (Star Schema)

The Gold layer is strictly modeled using Kimball dimensional design principles:

```mermaid
erDiagram
    fact_cdr }|..|| dim_customer : "customer_sk"
    fact_cdr }|..|| dim_tower : "tower_sk"
    fact_cdr }|..|| dim_date : "date_id"
    dim_customer }|..|| dim_plan : "plan_id"

    fact_cdr {
        string call_id PK "Unique Call Identifier"
        string customer_sk FK "MD5 Hash of customer_number"
        string tower_sk FK "MD5 Hash of tower_id"
        date date_id FK "Calendar Date (YYYY-MM-DD)"
        timestamp timestamp "Full Call Timestamp"
        string event_type "'voice' or 'data'"
        int duration_seconds "Call Duration in Seconds"
        int data_consumed_mb "Data Consumed in Megabytes"
        string call_status "'completed' or 'dropped'"
        int latency_ms "Round-Trip Latency (ms)"
        int signal_strength_dbm "RF Signal Strength (dBm)"
        string network_condition "'Normal' or 'Congested'"
    }

    dim_customer {
        string customer_sk PK "MD5 Hash (Surrogate Key)"
        string customer_number "MSISDN (e.g. 01010000001)"
        string customer_name "Subscriber Name"
        string plan_id "Foreign Key to dim_plan"
        string city "Subscriber Residence City"
        string status "Account Status ('Active')"
        string signup_date "Subscription Start Date"
    }

    dim_tower {
        string tower_sk PK "MD5 Hash (Surrogate Key)"
        string tower_id "Business Key (e.g. TOWER_1)"
        string tower_name "Site Name (e.g. Tower Site #1)"
        string location_city "City & District"
        string technology "'4G' or '5G'"
        double latitude "Tower Latitude Coordinate"
        double longitude "Tower Longitude Coordinate"
    }

    dim_plan {
        string plan_sk PK "MD5 Hash (Surrogate Key)"
        string plan_id "Business Key (e.g. PLAN_PREPAID_BASIC)"
        string plan_name "Plan Name (Flex Basic 30, Red Platinum 500)"
        int monthly_fee_egp "Price in Egyptian Pounds (EGP)"
        int quota_gb "Internet Bundle Quota (GB)"
        int voice_minutes "Included Voice Minutes"
    }

    dim_date {
        date date_id PK "Calendar Date (YYYY-MM-DD)"
        int year "Calendar Year"
        int month "Month Number (1-12)"
        string day_name "Day of Week ('Monday', etc.)"
    }
```

---

## 3. Telecom Domain & RF Physical Rules

The lakehouse data reflects real-world cellular RF propagation physics:
1. **Signal Strength ($\text{dBm}$):**
   * Realistic mobile range: $-120 \text{ dBm}$ (cell edge / extreme attenuation) to $-50 \text{ dBm}$ (direct line-of-sight).
2. **Network Congestion:**
   * A cell is classified as `Congested` when $\text{signal\_strength\_dbm} \le -90 \text{ dBm}$ and $\text{latency\_ms} > 150 \text{ ms}$.
3. **Call Drop Rate (CDR%):**
   $$\text{CDR\%} = \frac{\sum \text{Dropped Voice Calls}}{\sum \text{Total Voice Calls}} \times 100$$
   * $\text{CDR\%} < 2.0\%$: Healthy SLA.
   * $2.0\% \le \text{CDR\%} \le 5.0\%$: Warning threshold.
   * $\text{CDR\%} > 5.0\%$: Critical network outage.
4. **Physical Invariants:**
   * $\text{Poor Signal Drop Rate (31.47\%)} \gg \text{Good Signal Drop Rate (1.32\%)}$.
   * Voice events strictly have $\text{data\_consumed\_mb} = 0$.
   * Data sessions strictly have $\text{duration\_seconds} = 0$.
