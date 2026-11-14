import re
import os
import pandas as pd
import trino
from google import genai
from knowledge_base import get_rag_context

FALLBACK_MODELS = [
    "gemini-2.5-flash",
    "gemini-2.5-flash-lite",
    "gemini-flash-latest",
    "gemini-2.5-pro"
]

def call_gemini_with_fallback(client, prompt: str) -> str:
    """
    Calls Gemini API with automatic fallback across models in case of high demand (503) or rate limits.
    """
    last_err = None
    for model_name in FALLBACK_MODELS:
        try:
            res = client.models.generate_content(
                model=model_name,
                contents=prompt
            )
            if res and res.text:
                return res.text
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"All Gemini models failed. Last error: {str(last_err)}")

def execute_trino_query(sql_query: str, host: str = "localhost", port: int = 8085):
    """
    Executes a SQL query against Trino and returns the result as a pandas DataFrame.
    """
    conn = trino.dbapi.connect(
        host=host,
        port=port,
        user="admin",
        catalog="delta",
        schema="telecom"
    )
    cursor = conn.cursor()
    cursor.execute(sql_query)
    columns = [col[0] for col in cursor.description] if cursor.description else []
    rows = cursor.fetchall() if cursor.description else []
    conn.close()
    return pd.DataFrame(rows, columns=columns)

def clean_sql(raw_sql: str) -> str:
    """
    Extracts clean SQL from markdown code fences and fixes common schema typos.
    """
    clean = re.sub(r"```sql", "", raw_sql, flags=re.IGNORECASE)
    clean = re.sub(r"```", "", clean).strip()
    # Correct any schema typos like delta.teleam -> delta.telecom
    clean = re.sub(r"\bdelta\.tele[a-z]*\.", "delta.telecom.", clean, flags=re.IGNORECASE)
    return clean

def generate_sql(user_question: str, api_key: str) -> str:
    """
    Uses Gemini to generate Trino ANSI SQL based on RAG context and user question.
    """
    client = genai.Client(api_key=api_key)
    rag_context = get_rag_context(user_question)
    
    prompt = f"""
You are an expert Telecom Data Engineer specializing in Trino SQL and Star Schema analytics.
Given the Lakehouse schema and domain business rules below, generate a valid, optimized Trino ANSI SQL query to answer the user's question.

### Domain Context & Schema:
{rag_context}

### Strict Rules:
1. Return ONLY the raw SQL query. Do NOT include markdown code fences, comments, or explanations.
2. Always qualify table names using the EXACT schema: delta.telecom.<table_name> (e.g. delta.telecom.fact_cdr, delta.telecom.dim_customer, delta.telecom.dim_tower, delta.telecom.dim_plan, delta.telecom.dim_date). Never misspell 'telecom'.
3. Do NOT invent columns. Use ONLY the columns listed in the schema.
4. Always use appropriate joins using surrogate keys:
   - fact_cdr.customer_sk = dim_customer.customer_sk
   - fact_cdr.tower_sk = dim_tower.tower_sk
   - dim_customer.plan_id = dim_plan.plan_id
   - fact_cdr.date_id = dim_date.date_id
5. If aggregating, always use GROUP BY for all non-aggregated select columns.
6. Limit results to 20 rows unless the user specifies otherwise.

User Question: {user_question}
"""
    raw_response = call_gemini_with_fallback(client, prompt)
    return clean_sql(raw_response)

def fix_sql(user_question: str, failed_sql: str, error_message: str, api_key: str) -> str:
    """
    Self-healing loop: asks Gemini to inspect the failed SQL and Trino error message and return a corrected query.
    """
    client = genai.Client(api_key=api_key)
    rag_context = get_rag_context(user_question)
    
    prompt = f"""
You are an expert Trino SQL debugger for a Telecom Lakehouse.
The following SQL query was generated for the user question, but it failed during execution on Trino.

### Lakehouse Context & Schema:
{rag_context}

### User Question:
{user_question}

### Failed SQL Query:
{failed_sql}

### Trino Error Message:
{error_message}

### Instructions:
1. Analyze why the query failed (e.g. schema typo like 'teleam' instead of 'telecom', incorrect column name, or missing group by).
2. Fix the error completely. The schema name is strictly `delta.telecom`.
3. Return ONLY the raw corrected Trino ANSI SQL query. No markdown code fences, comments, or explanations.
"""
    raw_response = call_gemini_with_fallback(client, prompt)
    return clean_sql(raw_response)

def explain_results(user_question: str, sql_query: str, df_results: pd.DataFrame, api_key: str) -> str:
    """
    Generates a concise, direct answer based strictly on the query results from the Telecom Lakehouse / DWH.
    Matches the user's language and does NOT provide unsolicited advice or recommendations unless requested.
    """
    client = genai.Client(api_key=api_key)
    rag_context = get_rag_context(user_question)
    
    table_preview = df_results.head(15).to_markdown(index=False) if not df_results.empty else "No data returned."
    
    prompt = f"""
You are an expert Data Warehouse & Telecom Analytics Assistant.
Answer the user's question directly, accurately, and concisely based ONLY on the SQL query results from our Telecom Lakehouse.

### Lakehouse Context & Schema Rules:
{rag_context}

### User Question:
{user_question}

### Executed SQL Query:
{sql_query}

### Query Results (from DWH):
{table_preview}

### Strict Instructions:
1. **Language Matching:** Reply ONLY in the same language the user asked in. If the question is in Arabic, reply ONLY in concise Arabic. If in English, reply ONLY in English. Do NOT provide dual/bilingual sections or summaries.
2. **Direct Answer:** Deliver the numbers and facts directly from the query results. Use concise bullet points and bold formatting for numbers and entities (e.g. برج، أرقام، قيم).
3. **Be Concise & Focused:** Avoid lengthy intros (e.g. "سيدي الفاضل بصفتي استشاري..."), avoid filler text, and keep the answer focused on what was asked.
4. **NO Unsolicited Recommendations:** Do NOT provide recommendations, suggestions, engineering action plans, or proposals UNLESS the user explicitly asked for recommendations (e.g. "ما هي توصياتك؟" or "what do you recommend?").
"""
    return call_gemini_with_fallback(client, prompt)
