import streamlit as st
from dotenv import load_dotenv

from opensea_monitoring.www.app.utils import components as comps

query_params = st.query_params
collection_name = query_params.get("collection")

if collection_name:
    page_params = comps.init_page(f"Métricas '{collection_name}'")
    grain = page_params["grain"]

    col1, col2 = st.columns([0.6, 0.4])
    with col1:
        comps.linear_plot("total_transfers", grain, collection=collection_name)
        comps.linear_plot("total_sales", grain, collection=collection_name)
        comps.multilinear_plot(
            "collection_top_assets_by_usd_volume",
            grain,
            collection=collection_name,
            n=20,
        )
    with col2:
        df = comps.get_metric(
            "collection_top_assets_by_usd_volume", grain, as_frame=True
        )
        _, c, _ = st.columns([0.05, 0.95, 0.05])
        c.write("#### Top assets de la coleccion")
        if not df.is_empty():  # type: ignore
            comps.render_as_table(
                "collection_top_assets_by_usd_volume",
                "asset",
                grain,
                n=20,
                href_page=f"https://opensea.io/collections/{collection_name}/assets/",
                col_group_alias="Asset",
                value_alias="Nro de transacciones",
            )
else:
    st.error("Ninguna coleccion ha sido seleccionada")
