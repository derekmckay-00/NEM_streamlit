import streamlit as st

st.set_page_config(
    page_title="NEM Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Global styles ────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@300;400;500&family=DM+Sans:wght@300;400;600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
    background-color: #0d1117;
    color: #e6edf3;
}
.block-container { padding: 1.5rem 2rem 2rem 2rem; }

/* Sidebar */
section[data-testid="stSidebar"] {
    background-color: #161b22;
    border-right: 1px solid #30363d;
}
section[data-testid="stSidebar"] .stRadio label {
    font-family: 'DM Mono', monospace;
    font-size: 0.85rem;
    color: #8b949e;
}

/* Headers */
h1 { font-family: 'DM Sans', sans-serif; font-weight: 700;
     color: #e6edf3; letter-spacing: -0.02em; }
h2 { font-family: 'DM Sans', sans-serif; font-weight: 600;
     color: #c9d1d9; font-size: 1.1rem; }
h3 { font-family: 'DM Mono', monospace; font-weight: 400;
     color: #8b949e; font-size: 0.85rem; letter-spacing: 0.05em;
     text-transform: uppercase; }

/* Metric cards */
div[data-testid="metric-container"] {
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 8px;
    padding: 1rem;
}
div[data-testid="metric-container"] label {
    font-family: 'DM Mono', monospace !important;
    font-size: 0.72rem !important;
    color: #8b949e !important;
    letter-spacing: 0.08em;
    text-transform: uppercase;
}
div[data-testid="metric-container"] div[data-testid="stMetricValue"] {
    font-family: 'DM Sans', sans-serif !important;
    font-weight: 700 !important;
    font-size: 1.6rem !important;
}

/* Dataframe */
div[data-testid="stDataFrame"] { border: 1px solid #30363d; border-radius: 8px; }

/* Buttons */
.stButton > button {
    background: #21262d;
    border: 1px solid #30363d;
    color: #c9d1d9;
    font-family: 'DM Mono', monospace;
    font-size: 0.8rem;
    border-radius: 6px;
    transition: all 0.15s;
}
.stButton > button:hover {
    background: #30363d;
    border-color: #58a6ff;
    color: #58a6ff;
}

/* Text input */
.stTextArea textarea, .stTextInput input {
    background: #161b22 !important;
    border: 1px solid #30363d !important;
    color: #e6edf3 !important;
    font-family: 'DM Mono', monospace !important;
    font-size: 0.85rem !important;
    border-radius: 6px !important;
}

/* Selectbox */
.stSelectbox div[data-baseweb="select"] {
    background: #161b22;
    border-color: #30363d;
}

/* Tab styling */
.stTabs [data-baseweb="tab-list"] {
    background: transparent;
    border-bottom: 1px solid #30363d;
    gap: 0;
}
.stTabs [data-baseweb="tab"] {
    font-family: 'DM Mono', monospace;
    font-size: 0.8rem;
    color: #8b949e;
    background: transparent;
    border: none;
    padding: 0.5rem 1rem;
}
.stTabs [aria-selected="true"] {
    color: #58a6ff !important;
    border-bottom: 2px solid #58a6ff !important;
    background: transparent !important;
}

.timestamp {
    font-family: 'DM Mono', monospace;
    font-size: 0.7rem;
    color: #484f58;
}
.price-positive { color: #3fb950; }
.price-negative { color: #f85149; }
.price-high     { color: #d29922; }
.price-spike    { color: #f85149; }
</style>
""", unsafe_allow_html=True)

# ── Navigation ───────────────────────────────────────────────
with st.sidebar:
    st.markdown("### ⚡ NEM Dashboard")
    st.markdown("---")
    page = st.radio(
        "Navigate",
        ["Current Prices & Demand", "Generation SCADA", "AI Query"],
        label_visibility="collapsed"
    )
    st.markdown("---")
    st.markdown(
        '<div class="timestamp">Data: AEMO NEM<br>Prices: AUD/MWh<br>Auto-refresh: 5 min</div>',
        unsafe_allow_html=True
    )

# ── Route to pages ───────────────────────────────────────────
if page == "Current Prices & Demand":
    from pages_src.prices import show
    show()
elif page == "Generation SCADA":
    from pages_src.scada import show
    show()
elif page == "AI Query":
    from pages_src.ai_query import show
    show()
