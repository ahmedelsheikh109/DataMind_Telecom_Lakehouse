# 📡 DataMind: Enterprise Telecom Medallion Lakehouse & GenAI Assistant

[![Delta Lake](https://img.shields.io/badge/Storage-Delta%20Lake%203.2-00ADD8?logo=apachespark&logoColor=white)](https://delta.io/)
[![Trino Engine](https://img.shields.io/badge/Query%20Engine-Trino%20v450-DE2659?logo=trino&logoColor=white)](https://trino.io/)
[![PySpark](https://img.shields.io/badge/Compute-PySpark%203.5-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Google Gemini](https://img.shields.io/badge/AI-Google%20Gemini%202.5%20Flash-4285F4?logo=google&logoColor=white)](https://ai.google.dev/)
[![Streamlit](https://img.shields.io/badge/UI-Streamlit-FF4B4B?logo=streamlit&logoColor=white)](https://streamlit.io/)
[![Docker Compose](https://img.shields.io/badge/Infra-Docker%20Compose-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![CI Status](https://img.shields.io/badge/CI-Passing-brightgreen?logo=githubactions&logoColor=white)](https://github.com/ahmedelsheikh109/DataMind_Telecom_Lakehouse/actions)
[![ARB Audit Score](https://img.shields.io/badge/Architecture%20Review-100%25%20(95%2F95)-success)](docs/arb_audit_report.md)

An enterprise-grade **Telecommunications Medallion Lakehouse** and **Conversational Text-to-SQL + Domain RAG Platform** designed to ingest, refine, model, and analyze Call Detail Records (CDRs) and data sessions at scale. Built with **PySpark**, **Delta Lake (MinIO S3)**, **Trino Distributed SQL Engine**, and **Google Gemini 2.5 Flash**, adhering strictly to **Kimball Dimensional Modeling (Star Schema)** and telecommunications QoS physics.

---

## 🏛️ System Architecture

```mermaid
flowchart TD
    subgraph Ingestion["1. Ingestion Layer"]
        K[Kafka / Telecom CDR Stream] -->|PySpark Streaming| B[(Bronze Layer: s3a://bronze/telecom_cdrs_raw)]
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
        LLM -->|Trino ANSI SQL Query| TR
        TR -->|DataFrame Results| LLM
        LLM -->|Direct Data Answer & Visuals| ST[Streamlit Dashboard Port 8501]
    end
```

---

## 🌟 Key Engineering Capabilities

1. **Medallion Lakehouse Architecture (Bronze &rarr; Silver &rarr; Gold):**
   * **Bronze Layer:** Raw ingestion of streaming CDRs with schema evolution and full audit metadata.
   * **Silver Layer:** Data quality quarantine, normalization of MSISDNs, timestamp alignment, and domain validation.
   * **Gold Layer:** Kimball dimensional model optimized for analytical serving and Trino distributed queries.
2. **Kimball Star Schema with Deterministic Surrogate Keys:**
   * MD5 hash surrogate keys (`customer_sk`, `tower_sk`, `plan_sk`, `date_id`) ensuring 100% referential integrity and zero natural key leakage.
   * Validated with **0 orphan records** across all fact-dimension relationships.
3. **Realistic Telecom QoS Physics & Egyptian Commercial Catalog:**
   * Real RF propagation physics: Poor signal ($\le -90$ dBm) exhibits **31.47%** drop rate & 262.8 ms latency, vs. Good signal ($> -90$ dBm) with **1.32%** drop rate & 46.9 ms latency.
   * Realistic Egyptian tariff plans: Flex Basic 30, Flex Super 70, Red Gold 200, Red Platinum 500 in EGP.
4. **Agentic Text-to-SQL with Self-Healing Loop:**
   * Natural language understanding using **Google Gemini 2.5 Flash** with dynamic multi-model fallback.
   * **Self-Healing Loop:** Catches SQL syntax or schema execution errors automatically and self-corrects without crashing the UI.
5. **Production Quality Assurance (9/9 Automated Suite):**
   * Comprehensive automated assertion test suite executable via PyTest (`pytest tests/`).

---

## 📊 Kimball Dimensional Model (Star Schema)

```mermaid
erDiagram
    fact_cdr }|..|| dim_customer : "customer_sk"
    fact_cdr }|..|| dim_tower : "tower_sk"
    fact_cdr }|..|| dim_date : "date_id"
    dim_customer }|..|| dim_plan : "plan_id"

    fact_cdr {
        string call_id PK "Unique Call Event Identifier"
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
        string plan_id "Plan Code (joins to dim_plan)"
        string city "City & District"
        string status "Account Status ('Active')"
        string signup_date "Subscription Start Date"
    }

    dim_tower {
        string tower_sk PK "MD5 Hash (Surrogate Key)"
        string tower_id "Tower Identifier (e.g. TOWER_1)"
        string tower_name "Tower Site Name"
        string location_city "City & District"
        string technology "'4G' or '5G'"
        double latitude "Tower Latitude"
        double longitude "Tower Longitude"
    }

    dim_plan {
        string plan_sk PK "MD5 Hash (Surrogate Key)"
        string plan_id "Plan Identifier"
        string plan_name "Commercial Name (Flex 30, Red 500)"
        int monthly_fee_egp "Monthly Price (EGP)"
        int quota_gb "Internet Quota (GB)"
        int voice_minutes "Included Minutes"
    }

    dim_date {
        date date_id PK "Calendar Date (YYYY-MM-DD)"
        int year "Calendar Year"
        int month "Month Number (1-12)"
        string day_name "Day of Week"
    }
```

---

## 🏆 Enterprise Architecture Review Board (ARB) Scorecard

The platform was subjected to a live end-to-end audit by 7 specialized domain agents, achieving a **100% perfect score**:

| # | Subsystem / Agent Domain | Max Weight | Score Achieved | Status |
|---|---|---|---|---|
| 1 | **Platform & Cloud Infra** | 15 pts | **15 / 15** | `EXCELLENT` |
| 2 | **Medallion Lakehouse Pipeline** | 15 pts | **15 / 15** | `EXCELLENT` |
| 3 | **Kimball Star Schema Modeling** | 20 pts | **20 / 20** | `EXCELLENT` |
| 4 | **Telecom QoS Physics & RF Sanity** | 15 pts | **15 / 15** | `EXCELLENT` |
| 5 | **Data QA Integrity Suite (9/9)** | 15 pts | **15 / 15** | `EXCELLENT` |
| 6 | **GenAI & Text-to-SQL RAG Engine** | 15 pts | **15 / 15** | `EXCELLENT` |
| **TOTAL** | **Enterprise Architecture Score** | **95 pts** | **95 / 95 (100.0%)** | **APPROVED FOR PRODUCTION** |

👉 Detailed audit logs and verification metrics: [docs/arb_audit_report.md](docs/arb_audit_report.md)

---

## 🚀 Quickstart & Reproduction Guide

### Prerequisites
* Docker Desktop & Docker Compose
* Python 3.9+ or Conda

### Step 1: Clone & Configure Environment
```bash
git clone https://github.com/ahmedelsheikh109/DataMind_Telecom_Lakehouse.git
cd DataMind_Telecom_Lakehouse

# Copy environment template
cp .env.example .env
# Open .env and insert your free Google Gemini API Key
```

### Step 2: Spin Up Lakehouse Infrastructure
```bash
docker-compose up -d
```
* MinIO Console: `http://localhost:9001` (User: `admin`, Pass: `password`)
* Trino Coordinator: `http://localhost:8085`

### Step 3: Materialize Gold Layer & Run Quality Suite
```bash
pip install -r requirements.txt

# Run the 7-Agent live verification suite
python run_multi_agent_live_audit.py
```

### Step 4: Launch Conversational AI Assistant
```bash
streamlit run ai_assistant/app.py --server.port 8501
```
Open **`http://localhost:8501`** in your browser to interact with the Telecom Lakehouse using natural language.

---

## 📁 Repository Structure

```text
DataMind_Telecom_Lakehouse/
├── .github/
│   └── workflows/
│       └── ci.yml                      # GitHub Actions CI workflow
├── ai_assistant/
│   ├── .env.example                    # Environment template
│   ├── app.py                          # Streamlit UI with defensive rendering
│   ├── engine.py                       # Text-to-SQL, self-healing loop & Trino connector
│   └── knowledge_base.py               # Telecom semantic schema & SLA rules
├── docs/
│   ├── architecture.md                 # Detailed architecture & Mermaid diagrams
│   └── arb_audit_report.md             # Official 7-Agent ARB evaluation report
├── notebooks/
│   ├── build_gold_layer.py             # PySpark script materializing Star Schema in MinIO
│   ├── run_data_quality_tests.py       # Standalone data quality runner
│   ├── 01_Bronze_Ingestion.ipynb       # Kafka to Bronze ingestion notebook
│   ├── 02_Silver_Cleansing.ipynb       # Silver transformation & quarantine
│   └── 03_Gold_Star_Schema.ipynb       # Gold Star Schema creation
├── tests/
│   └── test_telecom_quality.py         # PyTest 9/9 automated assertion test suite
├── trino/
│   ├── catalog/
│   │   └── delta.properties            # Delta Lake connector configuration
│   └── etc/
│       ├── config.properties           # Trino memory & coordinator settings
│       ├── jvm.config                  # JVM parameters
│       └── node.properties             # Node discovery configuration
├── .gitignore                          # Enterprise gitignore excluding secrets & caches
├── .env.example                        # Root environment template
├── docker-compose.yml                  # Docker Compose multi-service manifest
├── requirements.txt                    # Unified dependencies
├── run_multi_agent_live_audit.py       # Live multi-agent audit script
└── README.md                           # Repository documentation
```

---

## 📄 License
This project is licensed under the MIT License - see the LICENSE file for details.
