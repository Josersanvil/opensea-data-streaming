import streamlit as st

from opensea_monitoring.www.app.utils import components as comps

query_params = st.query_params
collection_name = query_params.get("collection")
all_time_metrics = [
    "collection_total_transfers",
    "collection_total_items_transferred",
    "collection_total_sales_count",
    "collection_total_usd_volume",
    "collection_floor_assets_usd_price",
]
if collection_name:
    comps.init_page(
        f"Métricas '{collection_name}'",
    )
    st.write("## Métricas totales")
    comps.show_topbar_metrics(all_time_metrics, "all time", collection=collection_name)
    _, c1, _, c2 = st.columns([0.15, 0.35, 0.15, 0.35])
    st.write("**Top assets de la coleccion por volumen de transferencias**")
    comps.render_as_table(
        "collection_top_assets_by_transfers",
        "asset_name",
        "all_time",
        collection=collection_name,
        n=20,
        href_col="asset_url",
        col_group_alias="Asset",
        value_alias="Nro de transacciones",
    )
    st.write("**Top assets de la coleccion por volumen de ventas**")
    comps.render_as_table(
        "collection_top_assets_by_usd_volume",
        "asset_name",
        "all_time",
        collection=collection_name,
        n=20,
        href_col="asset_url",
        col_group_alias="Asset",
        value_alias="Volumen de ventas (USD)",
    )
    st.divider()
    comps.show_granular_metrics_config()
    grain = comps.get_option("grain")
    refresh_rate = comps.get_option("refresh_rate")
    comps.linear_plot(
        "collection_total_items_transferred", grain, collection=collection_name
    )
    comps.linear_plot("collection_total_transfers", grain, collection=collection_name)
    comps.multilinear_plot(
        "collection_top_assets_by_transfers", grain, collection=collection_name
    )

    comps.multilinear_plot(
        "collection_top_assets_by_usd_volume", grain, collection=collection_name
    )


else:
    st.error("Ninguna coleccion ha sido seleccionada")
