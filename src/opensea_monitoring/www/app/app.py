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
    df = pl.DataFrame(
        comps.get_metric("top_collections_by_transactions", grain),
    )
    print(df)
    _, c, _ = st.columns([0.05, 0.95, 0.05])
    c.write("#### Top 10 collections by volume")
    if not df.is_empty():
        # top_10_collections = (
        #     df.group_by("collection")
        #     .agg(pl.sum("value").alias("volume"))
        #     .sort("volume", descending=True)
        #     .head(10)
        # )
        # md_table_str = "| Name  | Count | Details |\n| --- | --- | --- |\n"
        # for i in range(len(top_10_collections)):
        #     see_more = (
        #         "<a target='_Self' href='/collections?name="
        #         f"{top_10_collections.item(i, 'collection')}'>Ver detalles</a>"
        #     )
        #     md_table_str += f"{top_10_collections.item(i, 'collection')} |"
        #     md_table_str += f"{top_10_collections.item(i, 'volume'):,.0f} |"
        #     md_table_str += f"{see_more} |\n"
        # c.markdown(md_table_str, unsafe_allow_html=True)
        comps.render_as_table(
            "top_collections_by_transactions",
            grain,
            n=10,
            collection_href_col="collection",
        )
