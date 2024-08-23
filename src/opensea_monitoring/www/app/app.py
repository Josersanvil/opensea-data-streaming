import polars as pl
import streamlit as st
from dotenv import load_dotenv

from opensea_monitoring.www.app.utils import components as comps

st.set_page_config(
    "OpenSea Monitoring", layout="wide", initial_sidebar_state="collapsed"
)


load_dotenv()

st.title("Global Metrics")
c1, _ = st.columns([0.2, 0.8])
grain = str(comps.grain_options(container=c1))
st.divider()
col1, col2 = st.columns([0.6, 0.4])
with col1:
    comps.linear_plot("total_transfers", grain)
    comps.multilinear_plot("top_collections_by_transactions", grain)
with col2:
    df = comps.get_metric("top_collections_by_transactions", grain, as_frame=True)
    _, c, _ = st.columns([0.05, 0.95, 0.05])
    c.write("#### Top 10 collections by volume")
    if not df.is_empty():  # type: ignore
        comps.render_as_table(
            "top_collections_by_transactions",
            "collection",
            grain,
            n=10,
            href_page="/collections?collection=",
            col_group_alias="Coleccion",
            value_alias="Nro de Transacciones",
        )
