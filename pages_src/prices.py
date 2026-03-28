import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from pages_src.bq import query, table

REGIONS      = ["NSW1", "QLD1", "SA1", "TAS1", "VIC1"]
REGION_NAMES = {"NSW1": "New South Wales", "QLD1": "Queensland",
                "SA1": "South Australia",  "TAS1": "Tasmania", "VIC1": "Victoria"}

FCAS_COLS = [
    "RAISE6SECRRP", "RAISE60SECRRP", "RAISE5MINRRP", "RAISEREGRRP",
    "RAISE1SECRRP", "LOWER6SECRRP",  "LOWER60SECRRP","LOWER5MINRRP",
    "LOWERREGRRP",  "LOWER1SECRRP",
]

DEMAND_COLS = [
    "TOTALDEMAND", "AVAILABLEGENERATION", "AVAILABLELOAD",
    "DISPATCHABLEGENERATION", "DISPATCHABLELOAD", "NETINTERCHANGE",
]

def price_color(rrp):
    if rrp is None:   return "#8b949e"
    if rrp >= 300:    return "#f85149"
    if rrp >= 100:    return "#d29922"
    if rrp < 0:       return "#58a6ff"
    return "#3fb950"

def fmt(v, decimals=2):
    if v is None or pd.isna(v): return "—"
    return f"${v:,.{decimals}f}"

def fmt_mw(v):
    if v is None or pd.isna(v): return "—"
    return f"{v:,.0f} MW"

def show():
    st.title("⚡ Current Prices & Demand")

    col_refresh, col_ts = st.columns([1, 6])
    with col_refresh:
        if st.button("⟳ Refresh"):
            st.cache_data.clear()
            st.rerun()

    # ── Latest interval ──────────────────────────────────────
    latest_sql = f"""
        SELECT MAX(SETTLEMENTDATE) as latest
        FROM {table('dispatch_region')}
        WHERE SETTLEMENTDATE >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 2 DAY)
    """
    try:
        latest_df = query(latest_sql, ttl=60)
        latest_ts = latest_df["latest"].iloc[0]
        with col_ts:
            st.markdown(
                f'<div class="timestamp" style="padding-top:0.6rem">Latest interval: {latest_ts} NEM time</div>',
                unsafe_allow_html=True
            )
    except Exception as e:
        st.error(f"Could not connect to BigQuery: {e}")
        return

    # ── Current prices ───────────────────────────────────────
    current_sql = f"""
        SELECT *
        FROM {table('dispatch_region')}
        WHERE SETTLEMENTDATE = '{latest_ts}'
        ORDER BY REGIONID
    """
    curr_df = query(current_sql, ttl=60)

    st.markdown("### ENERGY PRICE  —  $/MWh")
    cols = st.columns(5)
    for i, region in enumerate(REGIONS):
        row = curr_df[curr_df["REGIONID"] == region]
        rrp = float(row["RRP"].iloc[0]) if not row.empty else None
        color = price_color(rrp)
        with cols[i]:
            st.metric(
                label=f"{region} · {REGION_NAMES[region]}",
                value=fmt(rrp) if rrp is not None else "—",
            )

    st.markdown("---")

    # ── Tabs: FCAS | Demand | History ───────────────────────
    tab_fcas, tab_demand, tab_history = st.tabs(["FCAS Prices", "Demand", "24hr History"])

    with tab_fcas:
        st.markdown("### FCAS PRICES  —  $/MWh")
        if not curr_df.empty:
            fcas_display = curr_df[["REGIONID"] + [c for c in FCAS_COLS if c in curr_df.columns]].copy()
            fcas_display = fcas_display.set_index("REGIONID")
            # Round for display
            fcas_display = fcas_display.round(4)
            st.dataframe(fcas_display, use_container_width=True)

    with tab_demand:
        st.markdown("### DEMAND & GENERATION  —  MW")
        if not curr_df.empty:
            demand_display = curr_df[["REGIONID"] + [c for c in DEMAND_COLS if c in curr_df.columns]].copy()
            demand_display = demand_display.set_index("REGIONID")
            demand_display = demand_display.round(0)
            st.dataframe(demand_display, use_container_width=True)

        st.markdown("&nbsp;")
        # Bar chart — total demand by region
        if not curr_df.empty and "TOTALDEMAND" in curr_df.columns:
            fig = px.bar(
                curr_df,
                x="REGIONID",
                y="TOTALDEMAND",
                color="REGIONID",
                color_discrete_sequence=["#58a6ff","#3fb950","#d29922","#f85149","#bc8cff"],
                labels={"REGIONID": "Region", "TOTALDEMAND": "Total Demand (MW)"},
                title="Total Demand by Region",
            )
            fig.update_layout(
                paper_bgcolor="#0d1117", plot_bgcolor="#0d1117",
                font=dict(family="DM Mono", color="#8b949e"),
                title_font=dict(family="DM Sans", color="#c9d1d9"),
                showlegend=False,
                xaxis=dict(gridcolor="#21262d"),
                yaxis=dict(gridcolor="#21262d"),
            )
            st.plotly_chart(fig, use_container_width=True)

    with tab_history:
        st.markdown("### 24-HOUR PRICE HISTORY")
        col_region, col_metric = st.columns([2, 2])
        with col_region:
            selected_regions = st.multiselect(
                "Regions", REGIONS, default=REGIONS
            )
        with col_metric:
            metric = st.selectbox(
                "Metric",
                ["RRP"] + FCAS_COLS,
                index=0
            )

        if selected_regions:
            region_list = "', '".join(selected_regions)
            history_sql = f"""
                SELECT SETTLEMENTDATE, REGIONID, {metric}
                FROM {table('dispatch_region')}
                WHERE SETTLEMENTDATE >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 24 HOUR)
                  AND REGIONID IN ('{region_list}')
                ORDER BY SETTLEMENTDATE
            """
            hist_df = query(history_sql, ttl=300)

            if not hist_df.empty:
                fig = px.line(
                    hist_df,
                    x="SETTLEMENTDATE",
                    y=metric,
                    color="REGIONID",
                    color_discrete_sequence=["#58a6ff","#3fb950","#d29922","#f85149","#bc8cff"],
                    labels={"SETTLEMENTDATE": "Time", metric: f"{metric} ($/MWh)", "REGIONID": "Region"},
                )
                fig.update_layout(
                    paper_bgcolor="#0d1117", plot_bgcolor="#0d1117",
                    font=dict(family="DM Mono", color="#8b949e"),
                    xaxis=dict(gridcolor="#21262d"),
                    yaxis=dict(gridcolor="#21262d"),
                    legend=dict(bgcolor="#161b22", bordercolor="#30363d"),
                )
                fig.update_traces(line=dict(width=1.5))
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No history data available.")
