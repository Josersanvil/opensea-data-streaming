import streamlit as st

from opensea_monitoring.www.app.utils import components as comps

query_params = st.query_params
collection_name = query_params.get("collection")

if collection_name:
    page_params = comps.init_page(f"Métricas '{collection_name}'")
    grain = page_params["grain"]
    c1, c2, c3, c4 = st.columns([0.25, 0.25, 0.25, 0.25])
    with c1:
        # st.text("Num. de Transacciones")
        # comps.render_as_table(
        #     "collection_total_transfers",
        #     "collection",
        #     grain,
        #     collection=collection_name,
        #     col_group_alias="Coleccion",
        # )
        comps.indicator(
            "collection_total_transfers",
            grain=grain,
            collection=collection_name,
        )
    with c2:
        comps.indicator(
            "collection_total_items_transferred",
            grain=grain,
            collection=collection_name,
        )
    with c3:
        comps.indicator(
            "collection_total_usd_volume",
            grain=grain,
            collection=collection_name,
        )
    with c4:
        comps.indicator(
            "floor_assets_usd_price",
            grain=grain,
            collection=collection_name,
        )
    st.divider()
    col1, _, col2 = st.columns([0.55, 0.05, 0.4])
    with col1:
        comps.linear_plot(
            "collection_total_items_transferred", grain, collection=collection_name
        )
        comps.linear_plot(
            "collection_total_transfers", grain, collection=collection_name
        )
    with col2:
        comps.multilinear_plot(
            "collection_top_assets_by_transfers",
            grain,
            collection=collection_name,
            n=20,
        )
        st.write("**Top assets de la coleccion**")
        comps.render_as_table(
            "collection_top_assets_by_transfers",
            "asset_name",
            grain,
            collection=collection_name,
            n=20,
            href_col="asset_url",
            col_group_alias="Asset",
            value_alias="Nro de transacciones",
        )
else:
    st.error("Ninguna coleccion ha sido seleccionada")
