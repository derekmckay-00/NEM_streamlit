import streamlit as st
import pandas as pd
import plotly.express as px
from pages_src.bq import query, table

REGIONS = ["NSW1", "QLD1", "SA1", "TAS1", "VIC1"]

def show():
    st.title("🔌 Generation SCADA")

    col_refresh, _ = st.columns([1, 6])
    with col_refresh:
        if st.button("⟳ Refresh"):
            st.cache_data.clear()
            st.rerun()

    # ── Latest SCADA interval ────────────────────────────────
    latest_sql = f"""
        SELECT MAX(SETTLEMENTDATE) as latest
        FROM {table('dispatch_unit_scada')}
        WHERE SETTLEMENTDATE >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 2 DAY)
    """
    try:
        latest_ts = query(latest_sql, ttl=60)["latest"].iloc[0]
        st.markdown(
            f'<div class="timestamp">Latest interval: {latest_ts} NEM time</div>',
            unsafe_allow_html=True
        )
    except Exception as e:
        st.error(f"Could not connect to BigQuery: {e}")
        return

    # ── Filters ──────────────────────────────────────────────
    st.markdown("---")
    col_f1, col_f2, col_f3, col_f4 = st.columns([2, 2, 2, 2])

    with col_f1:
        region_filter = st.multiselect("Region", REGIONS, default=REGIONS)
    with col_f2:
        duid_search = st.text_input("DUID search", placeholder="e.g. ERGT01")
    with col_f3:
        fuel_sql = f"""
            SELECT DISTINCT FUELSOURCEPRIMARY
            FROM {table('duid_reference')}
            WHERE FUELSOURCEPRIMARY IS NOT NULL
            ORDER BY 1
        """
        try:
            fuels = ["All"] + query(fuel_sql, ttl=3600)["FUELSOURCEPRIMARY"].tolist()
        except Exception:
            fuels = ["All"]
        fuel_filter = st.selectbox("Fuel type", fuels)
    with col_f4:
        sort_by = st.selectbox("Sort by", ["SCADAVALUE desc", "SCADAVALUE asc", "DUID"])

    # ── Build query ──────────────────────────────────────────
    region_list = "', '".join(region_filter) if region_filter else "''"

    sort_map = {
        "SCADAVALUE desc": "s.SCADAVALUE DESC",
        "SCADAVALUE asc":  "s.SCADAVALUE ASC",
        "DUID":            "s.DUID ASC",
    }
    order_clause = sort_map.get(sort_by, "s.SCADAVALUE DESC")

    fuel_clause = ""
    if fuel_filter != "All":
        fuel_clause = f"AND r.FUELSOURCEPRIMARY = '{fuel_filter}'"

    duid_clause = ""
    if duid_search:
        duid_clause = f"AND s.DUID LIKE '%{duid_search.upper()}%'"

    scada_sql = f"""
        SELECT
            s.DUID,
            s.SCADAVALUE,
            s.SETTLEMENTDATE,
            r.STATIONNAME,
            r.REGIONID,
            r.FUELSOURCEPRIMARY,
            r.TECHNOLOGYTYPE,
            r.REGISTEREDCAPACITY
        FROM {table('dispatch_unit_scada')} s
        LEFT JOIN {table('duid_reference')} r ON s.DUID = r.DUID
        WHERE s.SETTLEMENTDATE = '{latest_ts}'
          AND r.REGIONID IN ('{region_list}')
          {fuel_clause}
          {duid_clause}
        ORDER BY {order_clause}
        LIMIT 500
    """

    try:
        scada_df = query(scada_sql, ttl=60)
    except Exception as e:
        st.error(f"Query error: {e}")
        return

    if scada_df.empty:
        st.info("No data found for selected filters.")
        return

    # ── Summary metrics ──────────────────────────────────────
    st.markdown("---")
    col_m1, col_m2, col_m3, col_m4 = st.columns(4)
    with col_m1:
        st.metric("Units", f"{len(scada_df):,}")
    with col_m2:
        total_gen = scada_df[scada_df["SCADAVALUE"] > 0]["SCADAVALUE"].sum()
        st.metric("Total Generation", f"{total_gen:,.0f} MW")
    with col_m3:
        total_load = scada_df[scada_df["SCADAVALUE"] < 0]["SCADAVALUE"].sum()
        st.metric("Total Load", f"{total_load:,.0f} MW")
    with col_m4:
        max_unit = scada_df.loc[scada_df["SCADAVALUE"].idxmax()]
        st.metric("Largest Unit", f"{max_unit['DUID']} ({max_unit['SCADAVALUE']:,.0f} MW)")

    st.markdown("---")

    # ── Tabs: Table | Charts ─────────────────────────────────
    tab_table, tab_fuel, tab_region = st.tabs(["Data Table", "By Fuel Type", "By Region"])

    with tab_table:
        display_cols = ["DUID", "STATIONNAME", "REGIONID", "FUELSOURCEPRIMARY",
                        "TECHNOLOGYTYPE", "SCADAVALUE", "REGISTEREDCAPACITY"]
        display_df = scada_df[[c for c in display_cols if c in scada_df.columns]].copy()
        display_df = display_df.rename(columns={"SCADAVALUE": "OUTPUT_MW"})
        st.dataframe(display_df, use_container_width=True, height=500)

    with tab_fuel:
        if "FUELSOURCEPRIMARY" in scada_df.columns:
            fuel_agg = scada_df.groupby("FUELSOURCEPRIMARY")["SCADAVALUE"].sum().reset_index()
            fuel_agg = fuel_agg[fuel_agg["SCADAVALUE"] > 0].sort_values("SCADAVALUE", ascending=False)
            fig = px.bar(
                fuel_agg,
                x="FUELSOURCEPRIMARY",
                y="SCADAVALUE",
                color="FUELSOURCEPRIMARY",
                labels={"FUELSOURCEPRIMARY": "Fuel Type", "SCADAVALUE": "Generation (MW)"},
                title="Generation by Fuel Type",
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

    with tab_region:
        if "REGIONID" in scada_df.columns:
            region_agg = scada_df.groupby("REGIONID")["SCADAVALUE"].sum().reset_index()
            fig = px.bar(
                region_agg,
                x="REGIONID",
                y="SCADAVALUE",
                color="REGIONID",
                color_discrete_sequence=["#58a6ff","#3fb950","#d29922","#f85149","#bc8cff"],
                labels={"REGIONID": "Region", "SCADAVALUE": "Generation (MW)"},
                title="Generation by Region",
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
