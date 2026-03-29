import os
import re
import time
import random
import requests
import streamlit as st
import pandas as pd
import plotly.express as px

from pages_src.bq import get_client, query

PROJECT_ID = "nem-data-491304"
DATASET = "nem_data_store"

# Environment variable for the model
DEFAULT_GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-1.5-flash")

# ── Schema context for Gemini ────────────────────────────────────────────────
SCHEMA_CONTEXT = f"""You are a BigQuery SQL expert for the Australian National Electricity Market (NEM).
Target: `{PROJECT_ID}.{DATASET}`

Tables & Essential Columns:
1. dispatch_region:
   - SETTLEMENTDATE (DATETIME, Partition Column)
   - REGIONID (NSW1, QLD1, SA1, TAS1, VIC1)
   - RRP, TOTALDEMAND, AVAILABLEGENERATION, NETINTERCHANGE

2. dispatch_unit_scada:
   - SETTLEMENTDATE (DATETIME, Partition Column)
   - DUID (STRING), SCADAVALUE (FLOAT64)

3. duid_reference:
   - DUID, STATIONNAME, PARTICIPANTNAME, REGIONID, FUELSOURCEPRIMARY, TECHNOLOGYTYPE

STRICT GENERATION RULES:
- Output ONLY the raw SQL code. 
- NO markdown fences (```sql), NO explanations, NO leading/trailing text.
- MANDATORY: Every query must have a WHERE clause on SETTLEMENTDATE for partitioning.
- DEFAULT: Last 7 days if no range is specified.
- TERMINATION: Ensure the query ends correctly with a semicolon.
"""

# ── Simple pattern-based SQL fallback ───────────────────────────────────────
def try_local_sql(question: str) -> str | None:
    q = question.strip().lower()
    # Average price pattern
    avg_price_match = re.search(r"average .*price.*\b(nsw|qld|sa|tas|vic)\b", q)
    if avg_price_match:
        region = avg_price_match.group(1).upper() + "1"
        return f"SELECT DATE(SETTLEMENTDATE) as d, ROUND(AVG(RRP), 2) as avg_price FROM `{PROJECT_ID}.{DATASET}.dispatch_region` WHERE SETTLEMENTDATE >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 7 DAY) AND REGIONID = '{region}' GROUP BY 1 ORDER BY 1"
    return None

# ── Gemini helpers ────────────────────────────────────────────────────────────
def call_gemini(prompt: str, api_key: str, model: str | None = None, max_retries: int = 5) -> str:
    model = model or DEFAULT_GEMINI_MODEL
    url = f"[https://generativelanguage.googleapis.com/v1beta/models/](https://generativelanguage.googleapis.com/v1beta/models/){model}:generateContent?key={api_key}"

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.0,
            "maxOutputTokens": 2048, # Increased to prevent truncation
            "candidateCount": 1,
        },
    }

    for attempt in range(max_retries):
        try:
            resp = requests.post(url, json=payload, timeout=30)
            
            # Handling the 429 TooManyRequests
            if resp.status_code == 429:
                # Exponential backoff: 2s, 4s, 8s...
                wait = (2 ** attempt) + random.random()
                time.sleep(wait)
                continue

            resp.raise_for_status()
            data = resp.json()
            
            text = data['candidates'][0]['content']['parts'][0]['text'].strip()
            # Safety strip in case model ignores "No Markdown" instruction
            text = re.sub(r"```sql|```", "", text, flags=re.IGNORECASE).strip()
            return text

        except Exception as e:
            if attempt == max_retries - 1:
                raise e
            time.sleep(2)
    return ""

@st.cache_data(ttl=3600)
def cached_generate_sql(question: str, api_key: str, model: str) -> str:
    local_sql = try_local_sql(question)
    if local_sql:
        return local_sql

    full_prompt = f"{SCHEMA_CONTEXT}\n\nQuestion: {question}\n\nComplete BigQuery SQL:"
    return call_gemini(full_prompt, api_key, model)

# ── UI ────────────────────────────────────────────────────────────────────────
def show():
    st.title("🤖 NEM AI SQL Agent")
    
    # Session State for tracking requests
    if "last_ask_ts" not in st.session_state:
        st.session_state.last_ask_ts = 0.0

    with st.sidebar:
        user_key = st.text_input("Gemini API Key", type="password")
        model_name = st.selectbox("Model", ["gemini-1.5-flash", "gemini-1.5-pro", "gemini-2.0-flash-exp"])
        max_gb = st.slider("Max Scan (GB)", 0.1, 5.0, 1.0)

    api_key = user_key if user_key else os.environ.get("GEMINI_API_KEY")
    
    question = st.text_area("What would you like to know about the NEM?", 
                           placeholder="e.g., Show me total demand in QLD for the last 24 hours")

    if st.button("Generate & Run"):
        if not api_key:
            st.error("Please provide an API Key.")
            return

        # Simple Rate Limit Prevention
        now = time.time()
        if now - st.session_state.last_ask_ts < 5:
            st.warning("Cooling down... please wait a few seconds.")
            return
        st.session_state.last_ask_ts = now

        with st.spinner("Analyzing schema and generating query..."):
            try:
                sql = cached_generate_sql(question, api_key, model_name)
                
                # Validation check for truncation
                if not ("FROM" in sql.upper() or "SELECT" in sql.upper()):
                    st.error("The model generated an incomplete query. Please try rephrasing.")
                    return

                with st.expander("View SQL Statement", expanded=True):
                    st.code(sql, language="sql")

                # Execution
                df = query(sql, ttl=300)
                
                if df.empty:
                    st.info("No data found for this query.")
                else:
                    st.success(f"Found {len(df)} rows.")
                    st.dataframe(df, use_container_width=True)
                    
                    # Visualization logic
                    if len(df.columns) >= 2:
                        # Attempt to find a time column and a value column
                        date_col = next((c for c in df.columns if 'date' in c.lower() or 'time' in c.lower()), df.columns[0])
                        val_col = next((c for c in df.columns if df[c].dtype in ['float64', 'int64']), df.columns[-1])
                        
                        fig = px.line(df, x=date_col, y=val_col, title=f"{val_col} over {date_col}")
                        st.plotly_chart(fig, use_container_width=True)

            except Exception as e:
                st.error(f"Error: {str(e)}")

if __name__ == "__main__":
    show()
