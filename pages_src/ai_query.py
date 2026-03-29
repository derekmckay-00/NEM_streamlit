import streamlit as st
import pandas as pd
import plotly.express as px
import requests
import json
import re
import os
from pages_src.bq import get_client, query, table

PROJECT_ID = "nem-data-491304"
DATASET    = "nem_data_store"

# ── Schema context for Gemini ────────────────────────────────
SCHEMA_CONTEXT = """BigQuery SQL expert for Australian NEM (National Electricity Market).
Project: nem-data-491304, Dataset: nem_data_store

Tables:
1. dispatch_region - 5-min regional prices/demand. DATETIME SETTLEMENTDATE, STRING REGIONID (NSW1/QLD1/SA1/TAS1/VIC1), FLOAT64: RRP, RAISE6SECRRP, RAISE60SECRRP, RAISE5MINRRP, RAISEREGRRP, RAISE1SECRRP, LOWER6SECRRP, LOWER60SECRRP, LOWER5MINRRP, LOWERREGRRP, LOWER1SECRRP, TOTALDEMAND, AVAILABLEGENERATION, AVAILABLELOAD, DISPATCHABLEGENERATION, DISPATCHABLELOAD, NETINTERCHANGE. Partitioned by DATE(SETTLEMENTDATE), clustered by REGIONID.

2. dispatch_unit_scada - 5-min unit output. DATETIME SETTLEMENTDATE, STRING DUID, FLOAT64 SCADAVALUE (MW). Partitioned by DATE(SETTLEMENTDATE).

3. duid_reference - unit info. STRING: DUID, STATIONNAME, PARTICIPANTNAME, REGIONID, FUELSOURCEPRIMARY, TECHNOLOGYTYPE, DISPATCHTYPE. FLOAT64: REGISTEREDCAPACITY, MAXCAPACITY. No partition.

Rules:
- ALWAYS filter: WHERE SETTLEMENTDATE >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL N DAY)
- Max 90 days without aggregation. Use GROUP BY + AVG/MAX/MIN/SUM for long ranges.
- LIMIT 1000 on raw rows. Use ROUND(val,2) for prices.
- Return ONLY the SELECT statement. No explanation, no markdown, no backticks."""

def call_gemini(prompt, api_key):
    """Call Gemini API and return the response text."""
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={api_key}"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.1, "maxOutputTokens": 1024},
    }
    resp = requests.post(url, json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data["candidates"][0]["content"]["parts"][0]["text"].strip()

def extract_sql(text):
    """Extract SQL from Gemini response — strip markdown fences if present."""
    # Remove ```sql ... ``` or ``` ... ```
    text = re.sub(r"```(?:sql)?", "", text, flags=re.IGNORECASE).strip()
    text = text.replace("```", "").strip()
    # Must start with SELECT
    if not text.upper().startswith("SELECT"):
        match = re.search(r"(SELECT\s.+)", text, re.IGNORECASE | re.DOTALL)
        if match:
            text = match.group(1)
    return text.strip()

def dry_run_cost(sql):
    """Run a BigQuery dry run and return estimated bytes processed."""
    client = get_client()
    job_config = __import__("google.cloud.bigquery", fromlist=["QueryJobConfig"]).QueryJobConfig(
        dry_run=True, use_query_cache=False
    )
    job = client.query(sql, job_config=job_config)
    return job.total_bytes_processed

def show():
    st.title("🤖 AI Query")
    st.markdown(
        "Ask questions about the NEM data in plain English. "
        "Gemini converts your question to SQL and runs it against BigQuery."
    )

    # ── API key setup ────────────────────────────────────────
    with st.expander("⚙️ Gemini API Key", expanded=False):
        st.markdown(
            "The default key is pre-configured. "
            "Enter your own [Google AI Studio](https://aistudio.google.com) key below to use it instead."
        )
        user_key = st.text_input(
            "Your Gemini API key (optional)",
            type="password",
            placeholder="AIza...",
        )

    # Resolve which key to use — env var first, then user input
    default_key = os.environ.get("GEMINI_API_KEY", "")
    api_key = user_key.strip() if user_key.strip() else default_key

    if not api_key:
        st.warning("No Gemini API key configured. Add GEMINI_API_KEY to Cloud Run environment variables.")
        return

    # ── Cost threshold ───────────────────────────────────────
    max_gb = st.slider(
        "Max query size (GB) — queries above this will be blocked",
        min_value=0.1, max_value=10.0, value=1.0, step=0.1
    )

    # ── Query input ──────────────────────────────────────────
    st.markdown("---")
    question = st.text_area(
        "Your question",
        placeholder="e.g. What was the average electricity price in NSW last week?\n"
                    "e.g. Show me the top 10 generators by output right now\n"
                    "e.g. How much wind generation was there in SA today?",
        height=100,
    )

    col_ask, col_clear = st.columns([2, 6])
    with col_ask:
        ask = st.button("🔍 Ask", use_container_width=True)
    with col_clear:
        if st.button("Clear", use_container_width=False):
            st.rerun()

    if not ask or not question.strip():
        return

    # ── Step 1: Generate SQL ─────────────────────────────────
    with st.spinner("Generating SQL..."):
        full_prompt = f"{SCHEMA_CONTEXT}\n\nUser question: {question}\n\nReturn only the SQL SELECT statement:"
        try:
            raw_response = call_gemini(full_prompt, api_key)
            sql = extract_sql(raw_response)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                st.error("Gemini free tier rate limit hit. Try again in a moment or provide your own API key.")
            else:
                st.error(f"Gemini API error: {e}")
            return
        except Exception as e:
            st.error(f"Failed to generate SQL: {e}")
            return

    with st.expander("📋 Generated SQL", expanded=True):
        st.code(sql, language="sql")

    # ── Step 2: Dry run cost check ───────────────────────────
    with st.spinner("Checking query cost..."):
        try:
            bytes_processed = dry_run_cost(sql)
            gb_processed    = bytes_processed / 1_000_000_000
            cost_usd        = gb_processed * 0.0065  # BigQuery on-demand ~$6.50/TB

            col_bytes, col_cost = st.columns(2)
            with col_bytes:
                st.metric("Estimated data scanned", f"{gb_processed:.3f} GB")
            with col_cost:
                st.metric("Estimated cost", f"${cost_usd:.4f} USD")

            if gb_processed > max_gb:
                st.error(
                    f"Query would scan {gb_processed:.2f} GB which exceeds your {max_gb} GB limit. "
                    "Try narrowing the date range or adding more filters."
                )
                # ── Ask Gemini to fix it ──────────────────────
                fix_prompt = (
                    f"{SCHEMA_CONTEXT}\n\n"
                    f"The following SQL scans too much data ({gb_processed:.2f} GB). "
                    f"Rewrite it to scan less than {max_gb} GB by adding tighter date filters or aggregations.\n\n"
                    f"Original SQL:\n{sql}\n\n"
                    f"Return only the revised SQL SELECT statement:"
                )
                with st.spinner("Asking Gemini to optimise the query..."):
                    try:
                        fixed_raw = call_gemini(fix_prompt, api_key)
                        fixed_sql = extract_sql(fixed_raw)
                        with st.expander("📋 Optimised SQL", expanded=True):
                            st.code(fixed_sql, language="sql")
                        # Re-check
                        new_bytes = dry_run_cost(fixed_sql)
                        new_gb    = new_bytes / 1_000_000_000
                        if new_gb <= max_gb:
                            sql = fixed_sql
                            st.success(f"Optimised query scans {new_gb:.3f} GB — proceeding.")
                        else:
                            st.error(f"Optimised query still scans {new_gb:.2f} GB. Please refine your question.")
                            return
                    except Exception as e:
                        st.error(f"Could not optimise query: {e}")
                        return

        except Exception as e:
            st.warning(f"Could not estimate cost: {e}. Proceeding anyway.")

    # ── Step 3: Execute ──────────────────────────────────────
    with st.spinner("Running query..."):
        try:
            result_df = query(sql, ttl=120)
        except Exception as e:
            st.error(f"BigQuery error: {e}")
            return

    if result_df.empty:
        st.info("Query returned no results.")
        return

    st.success(f"✅ {len(result_df):,} rows returned")
    st.markdown("---")

    # ── Step 4: Display results ──────────────────────────────
    tab_table, tab_chart = st.tabs(["Table", "Chart"])

    with tab_table:
        st.dataframe(result_df, use_container_width=True, height=400)
        # Download
        csv = result_df.to_csv(index=False)
        st.download_button(
            "⬇️ Download CSV",
            data=csv,
            file_name="nem_query_result.csv",
            mime="text/csv",
        )

    with tab_chart:
        numeric_cols = result_df.select_dtypes(include="number").columns.tolist()
        str_cols     = result_df.select_dtypes(include="object").columns.tolist()
        datetime_cols = result_df.select_dtypes(include=["datetime", "datetimetz"]).columns.tolist()

        if not numeric_cols:
            st.info("No numeric columns to chart.")
        else:
            col_ct, col_x, col_y = st.columns(3)
            with col_ct:
                chart_type = st.selectbox("Chart type", ["Line", "Bar", "Scatter"])
            with col_x:
                x_options = datetime_cols + str_cols + numeric_cols
                x_col = st.selectbox("X axis", x_options)
            with col_y:
                y_col = st.selectbox("Y axis", numeric_cols)

            color_col = None
            if str_cols:
                color_col = st.selectbox("Colour by (optional)", ["None"] + str_cols)
                if color_col == "None":
                    color_col = None

            try:
                kwargs = dict(
                    x=x_col, y=y_col,
                    color=color_col,
                    color_discrete_sequence=["#58a6ff","#3fb950","#d29922","#f85149","#bc8cff"],
                )
                if chart_type == "Line":
                    fig = px.line(result_df, **kwargs)
                elif chart_type == "Bar":
                    fig = px.bar(result_df, **kwargs)
                else:
                    fig = px.scatter(result_df, **kwargs)

                fig.update_layout(
                    paper_bgcolor="#0d1117", plot_bgcolor="#0d1117",
                    font=dict(family="DM Mono", color="#8b949e"),
                    xaxis=dict(gridcolor="#21262d"),
                    yaxis=dict(gridcolor="#21262d"),
                    legend=dict(bgcolor="#161b22", bordercolor="#30363d"),
                )
                st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.error(f"Could not render chart: {e}")