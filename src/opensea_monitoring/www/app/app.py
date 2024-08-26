import streamlit as st

from opensea_monitoring.www.app.utils import components as comps

page_params = comps.init_page("Métricas Globales")
grain = page_params["grain"]
col1, col2 = st.columns([0.6, 0.4])
with col1:
    comps.linear_plot("total_transfers", grain)
    comps.multilinear_plot("top_collections_by_transfers_volume_transfers_count", grain)
with col2:
    n = 20
    col2.write(f"**Top {n} colecciones por volumen**")
    comps.render_as_table(
        "top_collections_by_transfers_volume_transfers_count",
        "collection",
        grain,
        n=n,
        href_page="/collections?collection=",
        col_group_alias="Coleccion",
        value_alias="Nro de Transacciones",
        container=col2,
    )
