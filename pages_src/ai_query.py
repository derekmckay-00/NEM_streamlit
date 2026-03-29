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

# Prefer setting this in Cloud Run env vars so you can switch models without code changes.
DEFAULT_GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")

# ── Schema context for Gemini ────────────────────────────────────────────────
SCHEMA_CONTEXT = f"""BigQuery SQL expert for Australian NEM (National Electricity Market).
Project: {PROJECT_ID}, Dataset: {DATASET}

Tables:
1. dispatch_region - 5-min regional prices/demand.
   Columns:
   - SETTLEMENTDATE DATETIME
   - REGIONID STRING (NSW1/QLD1/SA1/TAS1/VIC1)
   - RRP FLOAT64
   - RAISE6SECRRP FLOAT64
   - RAISE60SECRRP FLOAT64
   - RAISE5MINRRP FLOAT64
   - RAISEREGRRP FLOAT64
   - RAISE1SECRRP FLOAT64
   - LOWER6SECRRP FLOAT64
   - LOWER60SECRRP FLOAT64
   - LOWER5MINRRP FLOAT64
   - LOWERREGRRP FLOAT64
   - LOWER1SECRRP FLOAT64
   - TOTALDEMAND FLOAT64
   - AVAILABLEGENERATION FLOAT64
   - AVAILABLELOAD FLOAT64
   - DISPATCHABLEGENERATION FLOAT64
   - DISPATCHABLELOAD FLOAT64
   - NETINTERCHANGE FLOAT64
   Partitioned by DATE(SETTLEMENTDATE), clustered by REGIONID.

2. dispatch_unit_scada - 5-min unit output.
   Columns:
   - SETTLEMENTDATE DATETIME
   - DUID STRING
   - SCADAVALUE FLOAT64
   Partitioned by DATE(SETTLEMENTDATE).

3. duid_reference - unit info.
   Columns:
   - DUID STRING
   - STATIONNAME STRING
   - PARTICIPANTNAME STRING
   - REGIONID STRING
   - FUELSOURCEPRIMARY STRING
   - TECHNOLOGYTYPE STRING
   - DISPATCHTYPE STRING
   - REGISTEREDCAPACITY FLOAT64
   - MAXCAPACITY FLOAT64

Hard rules:
- Return ONLY a single BigQuery SELECT statement.
- No explanation, no markdown, no backticks.
- ALWAYS include a SETTLEMENTDATE filter on partitioned tables.
- Default to the last 7 days unless the user explicitly asks for another time range.
- If user asks for "today", use CURRENT_DATE() and CURRENT_DATETIME().
- If user asks for "now" or "current", use the latest available SETTLEMENTDATE.
- Never scan an unnecessarily large date range.
- If the request spans more than 30 days, aggregate by day.
- If the request spans more than 90 days, aggregate by week or month.
- Use LIMIT 500 for raw row-level outputs.
- Avoid SELECT *.
- Use ROUND(value, 2) for price outputs where sensible.
- Prefer dispatch_region unless unit-level detail is explicitly required.
- Join duid_reference only when needed.
- Use Standard BigQuery SQL syntax only.
"""

# ── Simple pattern-based SQL fallback for common questions ───────────────────
def try_local_sql(question: str) -> str | None:
    q = question.strip().lower()

    # Average electricity price in a region over the last week
    avg_price_match = re.search(
        r"average .*price.*\b(nsw|qld|sa|tas|vic)\b.*(last week|past week|7 days)",
        q,
    )
    if avg_price_match:
        region = avg_price_match.group(1).upper() + "1"
        return f"""
SELECT
  DATE(SETTLEMENTDATE) AS trading_date,
  REGIONID,
  ROUND(AVG(RRP), 2) AS avg_rrp
FROM `{PROJECT_ID}.{DATASET}.dispatch_region`
WHERE SETTLEMENTDATE >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 7 DAY)
  AND REGIONID = '{region}'
GROUP BY trading_date, REGIONID
ORDER BY trading_date
""".strip()

    # Top generators right now / latest available interval
    if ("top" in q and "generator" in q and ("right now" in q or "now" in q or "current" in q)):
        return f"""
SELECT
  s.SETTLEMENTDATE,
  s.DUID,
  r.STATIONNAME,
  r.PARTICIPANTNAME,
  r.REGIONID,
  r.FUELSOURCEPRIMARY,
  r.TECHNOLOGYTYPE,
  ROUND(s.SCADAVALUE, 2) AS scadavalue_mw
FROM `{PROJECT_ID}.{DATASET}.dispatch_unit_scada` s
LEFT JOIN `{PROJECT_ID}.{DATASET}.duid_reference` r
  ON s.DUID = r.DUID
WHERE s.SETTLEMENTDATE = (
  SELECT MAX(SETTLEMENTDATE)
  FROM `{PROJECT_ID}.{DATASET}.dispatch_unit_scada`
  WHERE SETTLEMENTDATE >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 2 DAY)
)
ORDER BY s.SCADAVALUE DESC
LIMIT 10
""".strip()

    # Wind generation in SA today
    if "wind" in q and "generation" in q and re.search(r"\bsa\b|\bsouth australia\b", q) and "today" in q:
        return f"""
SELECT
  DATE(s.SETTLEMENTDATE) AS trading_date,
  EXTRACT(HOUR FROM s.SETTLEMENTDATE) AS hour_of_day,
  ROUND(AVG(s.SCADAVALUE), 2) AS avg_wind_generation_mw
FROM `{PROJECT_ID}.{DATASET}.dispatch_unit_scada` s
JOIN `{PROJECT_ID}.{DATASET}.duid_reference` r
  ON s.DUID = r.DUID
WHERE s.SETTLEMENTDATE >= DATETIME(CURRENT_DATE())
  AND r.REGIONID = 'SA1'
  AND LOWER(r.FUELSOURCEPRIMARY) = 'wind'
GROUP BY trading_date, hour_of_day
ORDER BY trading_date, hour_of_day
""".strip()

    return None


# ── Gemini helpers ────────────────────────────────────────────────────────────
def call_gemini(prompt: str, api_key: str, model: str | None = None, max_retries: int = 4) -> str:
    """Call Gemini API with retry/backoff and return text."""
    model = model or DEFAULT_GEMINI_MODEL
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.0,
            "maxOutputTokens": 512,
            "candidateCount": 1,
        },
    }

    last_error = None

    for attempt in range(max_retries):
        try:
            resp = requests.post(url, json=payload, timeout=30)

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                sleep_seconds = float(retry_after) if retry_after else min((2 ** attempt) + random.random(), 12)
                time.sleep(sleep_seconds)
                continue

            resp.raise_for_status()
            data = resp.json()

            candidates = data.get("candidates", [])
            if not candidates:
                raise ValueError(f"No candidates returned from Gemini: {data}")

            parts = candidates[0].get("content", {}).get("parts", [])
            if not parts:
                raise ValueError(f"No content parts returned from Gemini: {data}")

            text = parts[0].get("text", "").strip()
            if not text:
                raise ValueError(f"Empty text returned from Gemini: {data}")

            return text

        except requests.exceptions.HTTPError as e:
            last_error = e
            status = e.response.status_code if e.response is not None else None

            if status in (429, 500, 503, 504) and attempt < max_retries - 1:
                time.sleep(min((2 ** attempt) + random.random(), 12))
                continue
            raise

        except requests.exceptions.RequestException as e:
            last_error = e
            if attempt < max_retries - 1:
                time.sleep(min((2 ** attempt) + random.random(), 12))
                continue
            raise

        except Exception as e:
            last_error = e
            raise

    raise last_error if last_error else RuntimeError("Gemini request failed for unknown reason.")


def extract_sql(text: str) -> str:
    """Extract SQL from model response and strip markdown fences if present."""
    if not text:
        return ""

    text = re.sub(r"```(?:sql)?", "", text, flags=re.IGNORECASE).strip()
    text = text.replace("```", "").strip()

    if not text.upper().startswith("SELECT"):
        match = re.search(r"(SELECT\s.+)", text, re.IGNORECASE | re.DOTALL)
        if match:
            text = match.group(1)

    return text.strip().rstrip(";")


@st.cache_data(ttl=3600, show_spinner=False)
def cached_generate_sql(question: str, api_key: str, model: str) -> str:
    """Cache SQL generation so repeated questions do not re-hit Gemini."""
    local_sql = try_local_sql(question)
    if local_sql:
        return local_sql

    full_prompt = (
        f"{SCHEMA_CONTEXT}\n\n"
        f"User question: {question}\n\n"
        f"Return only the SQL SELECT statement:"
    )
    raw_response = call_gemini(full_prompt, api_key=api_key, model=model)
    return extract_sql(raw_response)


def optimise_sql_once(original_sql: str, question: str, max_gb: float, api_key: str, model: str) -> str:
    """Ask Gemini once to reduce scan size."""
    prompt = f"""
{SCHEMA_CONTEXT}

The SQL below is too expensive.
Rewrite it to reduce bytes scanned to less than {max_gb:.2f} GB.

Optimisation requirements:
- Tighten the date range.
- Aggregate results if needed.
- Prefer dispatch_region over dispatch_unit_scada unless unit-level detail is necessary.
- Keep the query faithful to the user question.
- Return only one SELECT statement.

User question:
{question}

Original SQL:
{original_sql}
""".strip()

    raw_response = call_gemini(prompt, api_key=api_key, model=model)
    return extract_sql(raw_response)


# ── BigQuery helpers ──────────────────────────────────────────────────────────
def dry_run_cost(sql: str) -> int:
    """Run a BigQuery dry run and return estimated bytes processed."""
    client = get_client()
    bigquery = __import__("google.cloud.bigquery", fromlist=["QueryJobConfig"])
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    job = client.query(sql, job_config=job_config)
    return job.total_bytes_processed


# ── UI ────────────────────────────────────────────────────────────────────────
def show():
    st.title("🤖 AI Query")
    st.markdown(
        "Ask questions about the NEM data in plain English. "
        "The app converts your question to SQL and runs it against BigQuery."
    )

    if "last_ask_ts" not in st.session_state:
        st.session_state.last_ask_ts = 0.0

    if "is_running" not in st.session_state:
        st.session_state.is_running = False

    with st.expander("⚙️ Gemini API Settings", expanded=False):
        st.markdown(
            "A default Gemini API key can be supplied through environment variables. "
            "You can also provide your own key below."
        )
        user_key = st.text_input(
            "Your Gemini API key (optional)",
            type="password",
            placeholder="AIza...",
        )
        model_name = st.text_input(
            "Gemini model",
            value=DEFAULT_GEMINI_MODEL,
            help="Override via env var GEMINI_MODEL if preferred.",
        )

    default_key = os.environ.get("GEMINI_API_KEY", "")
    api_key = user_key.strip() if user_key.strip() else default_key

    if not api_key:
        st.warning("No Gemini API key configured. Add GEMINI_API_KEY to Cloud Run environment variables.")
        return

    max_gb = st.slider(
        "Max query size (GB) — queries above this will be blocked",
        min_value=0.1,
        max_value=10.0,
        value=1.0,
        step=0.1,
    )

    st.markdown("---")

    question = st.text_area(
        "Your question",
        placeholder=(
            "e.g. What was the average electricity price in NSW last week?\n"
            "e.g. Show me the top 10 generators by output right now\n"
            "e.g. How much wind generation was there in SA today?"
        ),
        height=100,
    )

    col_ask, col_clear = st.columns([2, 6])

    with col_ask:
        ask = st.button(
            "🔍 Ask",
            use_container_width=True,
            disabled=st.session_state.is_running,
        )

    with col_clear:
        clear = st.button(
            "Clear",
            use_container_width=False,
            disabled=st.session_state.is_running,
        )

    if clear:
        st.rerun()

    if not ask or not question.strip():
        return

    now = time.time()
    cooldown_seconds = 3
    elapsed = now - st.session_state.last_ask_ts
    if elapsed < cooldown_seconds:
        st.warning(f"Please wait {cooldown_seconds - elapsed:.1f} more seconds before sending another request.")
        return

    st.session_state.last_ask_ts = now
    st.session_state.is_running = True

    try:
        # ── Step 1: Generate SQL ─────────────────────────────────────────────
        with st.spinner("Generating SQL..."):
            try:
                sql = cached_generate_sql(question.strip(), api_key, model_name.strip() or DEFAULT_GEMINI_MODEL)
            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code if e.response is not None else None
                if status_code == 429:
                    st.error(
                        "Gemini rate limit was hit. Please try again shortly, use a different project/key, "
                        "or reduce repeated requests."
                    )
                else:
                    st.error(f"Gemini API error: {e}")
                return
            except Exception as e:
                st.error(f"Failed to generate SQL: {e}")
                return

        if not sql or not sql.upper().startswith("SELECT"):
            st.error("The model did not return a valid SELECT statement.")
            return

        with st.expander("📋 Generated SQL", expanded=True):
            st.code(sql, language="sql")

        # ── Step 2: Dry run cost check ───────────────────────────────────────
        with st.spinner("Checking query cost..."):
            try:
                bytes_processed = dry_run_cost(sql)
                gb_processed = bytes_processed / 1_000_000_000
                cost_usd = gb_processed * 0.0065  # Approx on-demand BigQuery rate

                col_bytes, col_cost = st.columns(2)
                with col_bytes:
                    st.metric("Estimated data scanned", f"{gb_processed:.3f} GB")
                with col_cost:
                    st.metric("Estimated cost", f"${cost_usd:.4f} USD")

                if gb_processed > max_gb:
                    st.warning(
                        f"Initial query would scan {gb_processed:.2f} GB, above your {max_gb:.2f} GB limit. "
                        "Trying one optimisation pass."
                    )

                    try:
                        with st.spinner("Optimising query..."):
                            fixed_sql = optimise_sql_once(
                                original_sql=sql,
                                question=question.strip(),
                                max_gb=max_gb,
                                api_key=api_key,
                                model=model_name.strip() or DEFAULT_GEMINI_MODEL,
                            )

                        with st.expander("📋 Optimised SQL", expanded=True):
                            st.code(fixed_sql, language="sql")

                        new_bytes = dry_run_cost(fixed_sql)
                        new_gb = new_bytes / 1_000_000_000

                        if new_gb <= max_gb:
                            sql = fixed_sql
                            st.success(f"Optimised query scans {new_gb:.3f} GB — proceeding.")
                        else:
                            st.error(
                                f"Optimised query still scans {new_gb:.2f} GB, above your {max_gb:.2f} GB limit. "
                                "Please narrow the question or specify a shorter date range."
                            )
                            return

                    except requests.exceptions.HTTPError as e:
                        status_code = e.response.status_code if e.response is not None else None
                        if status_code == 429:
                            st.error(
                                "Gemini rate limit was hit while optimising the query. "
                                "Please refine the question manually or try again shortly."
                            )
                        else:
                            st.error(f"Could not optimise query: {e}")
                        return
                    except Exception as e:
                        st.error(f"Could not optimise query: {e}")
                        return

            except Exception as e:
                st.warning(f"Could not estimate query cost: {e}. Proceeding anyway.")

        # ── Step 3: Execute ──────────────────────────────────────────────────
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

        # ── Step 4: Display results ──────────────────────────────────────────
        tab_table, tab_chart = st.tabs(["Table", "Chart"])

        with tab_table:
            st.dataframe(result_df, use_container_width=True, height=400)
            csv_data = result_df.to_csv(index=False)
            st.download_button(
                "⬇️ Download CSV",
                data=csv_data,
                file_name="nem_query_result.csv",
                mime="text/csv",
            )

        with tab_chart:
            # Normalise datetime columns for chart selection
            datetime_cols = result_df.select_dtypes(include=["datetime", "datetimetz"]).columns.tolist()
            object_cols = result_df.select_dtypes(include=["object"]).columns.tolist()
            numeric_cols = result_df.select_dtypes(include="number").columns.tolist()

            # Try converting object columns that look datetime-like
            for col in result_df.columns:
                if col not in datetime_cols and result_df[col].dtype == "object":
                    try:
                        converted = pd.to_datetime(result_df[col], errors="raise")
                        result_df[col] = converted
                        datetime_cols.append(col)
                        if col in object_cols:
                            object_cols.remove(col)
                    except Exception:
                        pass

            numeric_cols = result_df.select_dtypes(include="number").columns.tolist()
            object_cols = result_df.select_dtypes(include=["object"]).columns.tolist()
            datetime_cols = result_df.select_dtypes(include=["datetime", "datetimetz"]).columns.tolist()

            if not numeric_cols:
                st.info("No numeric columns available for charting.")
            else:
                col_ct, col_x, col_y = st.columns(3)

                with col_ct:
                    chart_type = st.selectbox("Chart type", ["Line", "Bar", "Scatter"])

                with col_x:
                    x_options = datetime_cols + object_cols + numeric_cols
                    x_col = st.selectbox("X axis", x_options)

                with col_y:
                    y_col = st.selectbox("Y axis", numeric_cols)

                color_col = None
                if object_cols:
                    selected_colour = st.selectbox("Colour by (optional)", ["None"] + object_cols)
                    color_col = None if selected_colour == "None" else selected_colour

                try:
                    kwargs = {
                        "data_frame": result_df,
                        "x": x_col,
                        "y": y_col,
                        "color": color_col,
                        "color_discrete_sequence": ["#58a6ff", "#3fb950", "#d29922", "#f85149", "#bc8cff"],
                    }

                    if chart_type == "Line":
                        fig = px.line(**kwargs)
                    elif chart_type == "Bar":
                        fig = px.bar(**kwargs)
                    else:
                        fig = px.scatter(**kwargs)

                    fig.update_layout(
                        paper_bgcolor="#0d1117",
                        plot_bgcolor="#0d1117",
                        font=dict(family="DM Mono", color="#8b949e"),
                        xaxis=dict(gridcolor="#21262d"),
                        yaxis=dict(gridcolor="#21262d"),
                        legend=dict(bgcolor="#161b22", bordercolor="#30363d"),
                    )
                    st.plotly_chart(fig, use_container_width=True)

                except Exception as e:
                    st.error(f"Could not render chart: {e}")

    finally:
        st.session_state.is_running = False