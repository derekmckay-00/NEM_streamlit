import streamlit as st
from google.cloud import bigquery

PROJECT_ID = "nem-data-491304"
DATASET    = "nem_data_store"

@st.cache_resource
def get_client():
    """Get BigQuery client using application default credentials (Cloud Run)."""
    return bigquery.Client(project=PROJECT_ID)

def query(sql, ttl=300):
    """Run a BigQuery query and return a dataframe. Cached for ttl seconds."""
    @st.cache_data(ttl=ttl)
    def _run(sql):
        return get_client().query(sql).to_dataframe()
    return _run(sql)

def table(name):
    return f"`{PROJECT_ID}.{DATASET}.{name}`"
