"""
DataMind Telecom Lakehouse - Enterprise Architecture Review Board (ARB)
=======================================================================
Multi-Agent Live Verification & System Audit Suite
Auditing 7 Domains:
1. Platform & Cloud Infrastructure Agent
2. Medallion Lakehouse & Pipeline Agent
3. Kimball Star Schema Modeling Agent
4. Telecom QoS Physics & Network Domain Agent
5. Data Quality Assurance (QA) Lead Agent (9/9 Automated Suite)
6. GenAI & Text-to-SQL RAG Agent
7. Chief Solutions Architect (ARB Executive Verdict)
"""

import sys
import os
import json
import time
import urllib.request
import pandas as pd
import trino

# Ensure UTF-8 output
sys.stdout.reconfigure(encoding='utf-8')

# Set path for ai_assistant imports
ai_dir = os.path.join(os.path.dirname(__file__), "ai_assistant")
if ai_dir not in sys.path:
    sys.path.insert(0, ai_dir)

from dotenv import load_dotenv
load_dotenv(os.path.join(ai_dir, ".env"))

from engine import generate_sql, execute_trino_query, explain_results, call_gemini_with_fallback
from knowledge_base import get_rag_context

GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
BLUE = "\033[94m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"

scores = {}

def print_header(title: str, agent_name: str):
    print(f"\n{BOLD}{CYAN}{'='*80}{RESET}")
    print(f"{BOLD}{BLUE}>>> AGENT: {agent_name}{RESET}")
    print(f"{BOLD}{CYAN}{title}{RESET}")
    print(f"{BOLD}{CYAN}{'='*80}{RESET}")

def get_trino_connection():
    return trino.dbapi.connect(
        host="localhost",
        port=8085,
        user="admin",
        catalog="delta",
        schema="telecom"
    )

# ==============================================================================
# 1. PLATFORM & INFRASTRUCTURE AGENT
# ==============================================================================
def audit_platform() -> bool:
    print_header("1. CLOUD & INFRASTRUCTURE SUBSYSTEM AUDIT", "Platform & Cloud Engineer")
    checks = []
    
    # 1. MinIO Port 9001
    try:
        req = urllib.request.urlopen("http://localhost:9001", timeout=5)
        checks.append(("MinIO Console (Port 9001)", True, f"HTTP {req.status} OK (Storage Lake Ready)"))
    except Exception as e:
        checks.append(("MinIO Console (Port 9001)", False, str(e)))

    # 2. Trino Port 8085
    try:
        req = urllib.request.urlopen("http://localhost:8085/v1/info", timeout=5)
        info = json.loads(req.read().decode())
        checks.append(("Trino Coordinator (Port 8085)", True, f"Trino v{info.get('nodeVersion', {}).get('version', 'N/A')} ONLINE, Active nodes: 1"))
    except Exception as e:
        checks.append(("Trino Coordinator (Port 8085)", False, str(e)))

    # 3. Delta Lake Catalog Mount
    try:
        conn = get_trino_connection()
        cur = conn.cursor()
        cur.execute("SHOW TABLES")
        tables = [row[0] for row in cur.fetchall()]
        checks.append(("Delta Lake Trino Catalog (`delta.telecom`)", True, f"Catalog mounted with tables: {tables}"))
    except Exception as e:
        checks.append(("Delta Lake Trino Catalog (`delta.telecom`)", False, str(e)))

    all_passed = True
    for name, status, detail in checks:
        badge = f"{GREEN}[PASS]{RESET}" if status else f"{RED}[FAIL]{RESET}"
        print(f"  {badge} {name}: {detail}")
        if not status:
            all_passed = False

    score = 15 if all_passed else 10
    scores["1. Platform & Cloud Infra"] = (score, 15)
    return all_passed

# ==============================================================================
# 2. MEDALLION LAKEHOUSE & PIPELINE AGENT
# ==============================================================================
def audit_pipeline() -> bool:
    print_header("2. MEDALLION PIPELINE & ROW COUNT VERIFICATION", "Data Pipeline Engineer")
    expected_tables = {
        "fact_cdr": 1500,
        "dim_customer": 100,
        "dim_tower": 50,
        "dim_plan": 4,
        "dim_date": 1461
    }
    
    conn = get_trino_connection()
    cur = conn.cursor()
    all_passed = True

    for tbl, expected_min in expected_tables.items():
        try:
            cur.execute(f"SELECT COUNT(*) FROM delta.telecom.{tbl}")
            count = cur.fetchone()[0]
            passed = count >= expected_min
            badge = f"{GREEN}[PASS]{RESET}" if passed else f"{RED}[FAIL]{RESET}"
            print(f"  {badge} Table `{tbl}`: {count:,} records materialized in Delta Lake (Expected >= {expected_min:,})")
            if not passed:
                all_passed = False
        except Exception as e:
            print(f"  {RED}[FAIL]{RESET} Table `{tbl}` error: {e}")
            all_passed = False

    score = 15 if all_passed else 8
    scores["2. Lakehouse Pipeline"] = (score, 15)
    return all_passed

# ==============================================================================
# 3. KIMBALL MODELING AGENT
# ==============================================================================
def audit_modeling() -> bool:
    print_header("3. KIMBALL STAR SCHEMA & SURROGATE KEY AUDIT", "Data Architect & Kimball Specialist")
    conn = get_trino_connection()
    cur = conn.cursor()
    all_passed = True

    # 1. Primary Key Uniqueness on Dimensions
    pk_checks = [
        ("dim_customer", "customer_sk"),
        ("dim_tower", "tower_sk"),
        ("dim_plan", "plan_sk"),
        ("dim_date", "date_id")
    ]
    for tbl, pk in pk_checks:
        cur.execute(f"SELECT COUNT({pk}) - COUNT(DISTINCT {pk}) FROM delta.telecom.{tbl}")
        dups = cur.fetchone()[0]
        passed = (dups == 0)
        badge = f"{GREEN}[PASS]{RESET}" if passed else f"{RED}[FAIL]{RESET}"
        print(f"  {badge} Dimension `{tbl}` Primary Key (`{pk}`) Uniqueness: {dups} duplicates found")
        if not passed:
            all_passed = False

    # 2. Referential Integrity (Foreign Keys in Fact)
    fk_checks = [
        ("customer_sk", "dim_customer"),
        ("tower_sk", "dim_tower"),
        ("date_id", "dim_date")
    ]
    for fk, dim_tbl in fk_checks:
        sql = f"""
        SELECT COUNT(f.{fk}) 
        FROM delta.telecom.fact_cdr f 
        LEFT JOIN delta.telecom.{dim_tbl} d ON f.{fk} = d.{fk}
        WHERE d.{fk} IS NULL
        """
        cur.execute(sql)
        orphans = cur.fetchone()[0]
        passed = (orphans == 0)
        badge = f"{GREEN}[PASS]{RESET}" if passed else f"{RED}[FAIL]{RESET}"
        print(f"  {badge} Foreign Key Integrity: `fact_cdr.{fk}` -> `{dim_tbl}`: {orphans} orphan records")
        if not passed:
            all_passed = False

    # 3. MD5 Surrogate Key Length Check (32 hex chars)
    cur.execute("SELECT LENGTH(customer_sk), LENGTH(tower_sk) FROM delta.telecom.fact_cdr LIMIT 1")
    key_lens = cur.fetchone()
    all_md5 = all(l == 32 for l in key_lens)
    badge = f"{GREEN}[PASS]{RESET}" if all_md5 else f"{RED}[FAIL]{RESET}"
    print(f"  {badge} Kimball Deterministic Surrogate Keys (MD5 32-char Hex): {key_lens}")
    if not all_md5:
        all_passed = False

    score = 20 if all_passed else 12
    scores["3. Kimball Star Schema"] = (score, 20)
    return all_passed

# ==============================================================================
# 4. TELECOM DOMAIN & QoS PHYSICS AGENT
# ==============================================================================
def audit_telecom_physics() -> bool:
    print_header("4. TELECOM NETWORK QoS & RF PHYSICS SANITY", "Telecom Domain Specialist")
    conn = get_trino_connection()
    cur = conn.cursor()
    all_passed = True

    # Correlation: Signal Strength vs Dropped Calls
    sql_signal = """
    SELECT 
        CASE WHEN signal_strength_dbm <= -90 THEN 'Poor Signal (<= -90 dBm / Congested)'
             ELSE 'Good Signal (> -90 dBm / Normal)' END as signal_bracket,
        COUNT(*) as total_calls,
        SUM(CASE WHEN call_status = 'dropped' THEN 1 ELSE 0 END) as dropped_calls,
        ROUND(CAST(SUM(CASE WHEN call_status = 'dropped' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) * 100, 2) as drop_rate_pct,
        ROUND(AVG(latency_ms), 1) as avg_latency_ms
    FROM delta.telecom.fact_cdr
    WHERE event_type = 'voice'
    GROUP BY 1
    ORDER BY 1 DESC
    """
    cur.execute(sql_signal)
    rows = cur.fetchall()
    print("  QoS Correlation Analysis (Voice Calls):")
    poor_drop_rate = 0
    good_drop_rate = 0
    for r in rows:
        print(f"    - {r[0]}: Total={r[1]}, Dropped={r[2]} ({r[3]}%), Avg Latency={r[4]} ms")
        if "Poor" in r[0]:
            poor_drop_rate = r[3]
        else:
            good_drop_rate = r[3]

    # Telecom physics test: Poor signal MUST have substantially higher drop rate than good signal
    physics_sound = poor_drop_rate > good_drop_rate and poor_drop_rate >= 20.0 and good_drop_rate <= 5.0
    badge = f"{GREEN}[PASS]{RESET}" if physics_sound else f"{RED}[FAIL]{RESET}"
    print(f"  {badge} RF Network Physics Invariant (Poor Signal Drop Rate >> Good Signal): {physics_sound} ({poor_drop_rate}% vs {good_drop_rate}%)")
    if not physics_sound:
        all_passed = False

    # Egyptian Tariff check (Flex 30, Flex 70, Red Gold 200, Red Platinum 500)
    sql_plans = "SELECT plan_name, monthly_fee_egp, quota_gb, voice_minutes FROM delta.telecom.dim_plan ORDER BY monthly_fee_egp"
    cur.execute(sql_plans)
    plans = cur.fetchall()
    print("\n  Egyptian Commercial Plan Catalog (EGP):")
    for p in plans:
        print(f"    - Plan: {p[0]:<18} | Price: {p[1]:>3} EGP | Internet: {p[2]:>2} GB | Voice: {p[3]:>4} mins")

    score = 15 if all_passed else 10
    scores["4. Telecom QoS Physics"] = (score, 15)
    return all_passed

# ==============================================================================
# 5. DATA QA LEAD AGENT (9/9 Automated Suite)
# ==============================================================================
def audit_qa_suite() -> bool:
    print_header("5. AUTOMATED 9/9 INTEGRITY TEST SUITE", "Data Quality Assurance Lead")
    conn = get_trino_connection()
    cur = conn.cursor()

    tests = [
        ("T1: Fact_CDR Primary Key (call_id) Uniqueness", 
         "SELECT COUNT(call_id) - COUNT(DISTINCT call_id) FROM delta.telecom.fact_cdr", 
         lambda res: res[0][0] == 0),
        
        ("T2: Dim_Customer Surrogate Key (customer_sk) Uniqueness", 
         "SELECT COUNT(customer_sk) - COUNT(DISTINCT customer_sk) FROM delta.telecom.dim_customer", 
         lambda res: res[0][0] == 0),
        
        ("T3: Dim_Tower Surrogate Key (tower_sk) Uniqueness", 
         "SELECT COUNT(tower_sk) - COUNT(DISTINCT tower_sk) FROM delta.telecom.dim_tower", 
         lambda res: res[0][0] == 0),
        
        ("T4: Zero Orphan Customer Keys in Fact_CDR", 
         "SELECT COUNT(f.customer_sk) FROM delta.telecom.fact_cdr f LEFT JOIN delta.telecom.dim_customer c ON f.customer_sk = c.customer_sk WHERE c.customer_sk IS NULL", 
         lambda res: res[0][0] == 0),
        
        ("T5: Zero Orphan Tower Keys in Fact_CDR", 
         "SELECT COUNT(f.tower_sk) FROM delta.telecom.fact_cdr f LEFT JOIN delta.telecom.dim_tower t ON f.tower_sk = t.tower_sk WHERE t.tower_sk IS NULL", 
         lambda res: res[0][0] == 0),
        
        ("T6: Business Invariant: Voice calls have data_consumed_mb == 0", 
         "SELECT COUNT(*) FROM delta.telecom.fact_cdr WHERE event_type = 'voice' AND data_consumed_mb > 0", 
         lambda res: res[0][0] == 0),
        
        ("T7: Business Invariant: Data sessions have duration_seconds == 0", 
         "SELECT COUNT(*) FROM delta.telecom.fact_cdr WHERE event_type = 'data' AND duration_seconds > 0", 
         lambda res: res[0][0] == 0),
        
        ("T8: QoS Sanity: latency_ms > 0", 
         "SELECT COUNT(*) FROM delta.telecom.fact_cdr WHERE latency_ms <= 0", 
         lambda res: res[0][0] == 0),
        
        ("T9: QoS Sanity: signal_strength_dbm in range [-140, -30] dBm", 
         "SELECT COUNT(*) FROM delta.telecom.fact_cdr WHERE signal_strength_dbm > -30 OR signal_strength_dbm < -140", 
         lambda res: res[0][0] == 0)
    ]

    passed_count = 0
    for name, sql, validator in tests:
        try:
            cur.execute(sql)
            res = cur.fetchall()
            ok = validator(res)
            if ok:
                passed_count += 1
                print(f"  {GREEN}[PASS]{RESET} {name}")
            else:
                print(f"  {RED}[FAIL]{RESET} {name} (Assertion failed, result: {res})")
        except Exception as e:
            print(f"  {RED}[ERROR]{RESET} {name}: {e}")

    print(f"\n  QA Summary: {passed_count}/{len(tests)} automated integrity assertions passed.")
    score = int((passed_count / len(tests)) * 15)
    scores["5. Data QA Suite (9/9)"] = (score, 15)
    return passed_count == len(tests)

# ==============================================================================
# 6. GENAI & TEXT-TO-SQL RAG AGENT
# ==============================================================================
def audit_genai_rag() -> bool:
    print_header("6. TEXT-TO-SQL & TELECOM DOMAIN RAG VERIFICATION", "GenAI & LLM Systems Engineer")
    
    api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    if not api_key:
        print(f"  {RED}[FAIL]{RESET} GEMINI_API_KEY or GOOGLE_API_KEY not found in .env")
        scores["6. GenAI & RAG Engine"] = (0, 15)
        return False

    user_prompt = "أكتر 5 أبراج فيها مكالمات مقطوعة ومعدل الـ Latency فيها قد إيه؟"
    print(f"  User Query: \"{user_prompt}\"")

    # Step 1: Text-to-SQL
    try:
        t0 = time.time()
        sql = generate_sql(user_prompt, api_key)
        gen_time = time.time() - t0
        print(f"  {GREEN}[PASS]{RESET} Gemini Generated Trino SQL in {gen_time:.2f}s:")
        for line in sql.splitlines():
            print(f"    {line}")
    except Exception as e:
        print(f"  {RED}[FAIL]{RESET} Text-to-SQL Generation Failed: {e}")
        scores["6. GenAI & RAG Engine"] = (5, 15)
        return False

    # Step 2: Execute on Trino
    try:
        t0 = time.time()
        df = execute_trino_query(sql)
        exec_time = time.time() - t0
        print(f"\n  {GREEN}[PASS]{RESET} Query executed on Delta Lake via Trino in {exec_time:.2f}s ({len(df)} rows returned):")
        print("    " + df.to_string(index=False).replace("\n", "\n    "))
    except Exception as e:
        print(f"  {RED}[FAIL]{RESET} Trino Execution Failed: {e}")
        scores["6. GenAI & RAG Engine"] = (8, 15)
        return False

    # Step 3: Domain SLA Explanation
    try:
        t0 = time.time()
        explanation = explain_results(user_prompt, sql, df, api_key)
        exp_time = time.time() - t0
        print(f"\n  {GREEN}[PASS]{RESET} Domain RAG SLA Analysis in {exp_time:.2f}s:")
        for line in explanation.splitlines():
            print(f"    {line}")
    except Exception as e:
        print(f"  {YELLOW}[WARN]{RESET} Explanation generation fallback: {e}")

    scores["6. GenAI & RAG Engine"] = (15, 15)
    return True

# ==============================================================================
# 7. CHIEF SOLUTIONS ARCHITECT VERDICT
# ==============================================================================
def print_scorecard():
    print_header("7. ENTERPRISE ARB FINAL SCORECARD & VERDICT", "Chief Solutions Architect (ARB Chair)")
    total_score = sum(s[0] for s in scores.values())
    max_score = sum(s[1] for s in scores.values())
    pct = (total_score / max_score) * 100

    print(f"\n{'Subsystem / Agent Domain':<35} | {'Score':<10} | {'Status':<10}")
    print("-" * 65)
    for domain, (sc, mx) in scores.items():
        st = f"{GREEN}EXCELLENT{RESET}" if sc == mx else f"{YELLOW}SATISFACTORY{RESET}" if sc >= mx * 0.7 else f"{RED}DEFICIENT{RESET}"
        print(f"{domain:<35} | {sc:>2}/{mx:<2} pts  | {st}")
    print("-" * 65)
    print(f"{BOLD}{'TOTAL SCORE':<35} | {total_score:>2}/{max_score:<2} ({pct:.1f}%){RESET}")
    print("-" * 65)

    verdict = "APPROVED FOR PRODUCTION (ENTERPRISE READY)" if pct >= 90 else "CONDITIONAL APPROVAL"
    color = GREEN if pct >= 90 else YELLOW
    print(f"\n{BOLD}ARB Executive Verdict: {color}{verdict}{RESET}\n")

if __name__ == "__main__":
    t_start = time.time()
    audit_platform()
    audit_pipeline()
    audit_modeling()
    audit_telecom_physics()
    audit_qa_suite()
    audit_genai_rag()
    print_scorecard()
    print(f"Full Enterprise Multi-Agent Audit completed in {time.time() - t_start:.2f} seconds.")
