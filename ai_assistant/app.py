import os
import streamlit as st
import pandas as pd
import plotly.express as px
from dotenv import load_dotenv
from engine import generate_sql, execute_trino_query, explain_results, fix_sql

load_dotenv()

st.set_page_config(
    page_title="Telecom Lakehouse AI Assistant",
    page_icon="📡",
    layout="wide"
)

# Custom Styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.2rem;
        font-weight: 700;
        color: #1E3A8A;
        margin-bottom: 0.2rem;
    }
    .sub-header {
        font-size: 1.1rem;
        color: #4B5563;
        margin-bottom: 1.5rem;
    }
    .metric-card {
        background-color: #F3F4F6;
        border-radius: 10px;
        padding: 15px;
        border-left: 5px solid #2563EB;
    }
</style>
""", unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    st.image("https://img.icons8.com/color/96/000000/cell-tower.png", width=70)
    st.title("⚙️ AI Configuration")
    
    env_api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY", "")
    api_key = st.text_input("🔑 Google Gemini API Key", value=env_api_key, type="password")
    
    st.markdown("---")
    st.markdown("### 🔌 Lakehouse Infrastructure")
    st.success("🟢 **Trino Engine:** Connected (localhost:8085)")
    st.success("🟢 **Storage:** MinIO Delta Lake (S3)")
    st.info("📦 **Catalog:** `delta.telecom` (Star Schema)")
    
    st.markdown("---")
    st.markdown("### 💡 Quick Example Prompts")
    example_prompts = [
        "أكثر 5 أبراج فيها مكالمات مقطوعة في مصر؟",
        "مقارنة بين أداء وتأخير الـ 4G مقابل 5G",
        "استهلاك الداتا ومعدل الإيرادات لكل باقة اشتراك",
        "توزيع استهلاك الداتا على المدن المختلفة",
        "What is the average latency for congested towers?"
    ]
    for prompt in example_prompts:
        if st.button(prompt, key=prompt):
            st.session_state["user_query"] = prompt

def safe_dataframe(df):
    try:
        st.dataframe(df, use_container_width=True)
    except TypeError:
        st.dataframe(df)

def safe_plotly(fig):
    try:
        st.plotly_chart(fig, use_container_width=True)
    except TypeError:
        st.plotly_chart(fig)

# Main Area
st.markdown('<div class="main-header">📡 Telecom Lakehouse AI Assistant</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">Conversational Text-to-SQL & Domain RAG over Delta Lake & Trino</div>', unsafe_allow_html=True)

default_query = st.session_state.get("user_query", "أكثر 5 أبراج فيها مكالمات مقطوعة في مصر؟")
user_input = st.text_input("💬 Ask any business or network question about your Telecom Lakehouse:", value=default_query)

if st.button("🚀 Analyze & Generate Insights"):
    if not api_key:
        st.error("⚠️ يرجى إدخال Google Gemini API Key في القائمة الجانبية (Sidebar) للمتابعة.")
    else:
        with st.spinner("🤖 يقوم الـ AI بفهم السؤال وقواعد الشبكة وكتابة استعلام Trino SQL..."):
            try:
                # 1. Text-to-SQL
                generated_sql = generate_sql(user_input, api_key)
                
                # 2. Execution on Trino with Auto Self-Healing Loop
                with st.spinner("⚡ تنفيذ الاستعلام على Trino وسحب البيانات من MinIO Delta Lake..."):
                    try:
                        df_results = execute_trino_query(generated_sql)
                    except Exception as exec_err:
                        with st.spinner("🔧 تم رصد خطأ صياغي، جاري التصحيح الذاتي التلقائي للاستعلام (Auto Self-Healing)..."):
                            generated_sql = fix_sql(user_input, generated_sql, str(exec_err), api_key)
                            df_results = execute_trino_query(generated_sql)
                
                # 3. RAG Explanation
                with st.spinner("📊 صياغة التقرير التحليلي والتوصيات الفنية..."):
                    explanation = explain_results(user_input, generated_sql, df_results, api_key)
                
                # Display Results
                st.markdown("---")
                
                # Tabbed display
                tab_insights, tab_data, tab_sql, tab_viz = st.tabs([
                    "📝 Executive Insights", 
                    "📋 Query Results", 
                    "⚡ Generated Trino SQL",
                    "📈 Visualization"
                ])
                
                with tab_insights:
                    st.markdown(explanation)
                
                with tab_data:
                    safe_dataframe(df_results)
                    csv = df_results.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        "📥 Download Results as CSV",
                        data=csv,
                        file_name="telecom_lakehouse_query_results.csv",
                        mime="text/csv"
                    )
                
                with tab_sql:
                    st.code(generated_sql, language="sql")
                
                with tab_viz:
                    if len(df_results) > 0 and len(df_results.columns) >= 2:
                        string_cols = df_results.select_dtypes(include=['object', 'string']).columns.tolist()
                        numeric_cols = df_results.select_dtypes(include=['int64', 'float64', 'int32']).columns.tolist()
                        
                        if string_cols and numeric_cols:
                            x_col = string_cols[0]
                            y_col = numeric_cols[0]
                            fig = px.bar(
                                df_results, 
                                x=x_col, 
                                y=y_col, 
                                title=f"{y_col} across {x_col}",
                                color=y_col,
                                color_continuous_scale="Viridis"
                            )
                            safe_plotly(fig)
                        else:
                            st.info("No categorical and numeric column pair found for automatic plotting.")
                    else:
                        st.info("No data available to visualize.")
                        
            except Exception as e:
                st.error(f"❌ Error during processing: {str(e)}")
