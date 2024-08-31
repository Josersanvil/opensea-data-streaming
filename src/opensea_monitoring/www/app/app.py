import streamlit as st

from opensea_monitoring.www.app.utils import components as comps

comps.init_page("Métricas Globales")
all_time_metrics = [
    "total_transfers",
    "total_items_transferred",
    "total_sales",
    "total_usd_volume",
]
st.write("## Métricas totales")
comps.show_topbar_metrics(all_time_metrics, "all time")

_, c1, _, c2 = st.columns([0.15, 0.35, 0.15, 0.35])

with c1:
    st.write("**Top colecciones por volumen de transacciones**")
    comps.render_as_table(
        "top_collections_by_transfers_volume_transfers_count",
        "collection",
        "all_time",
        n=20,
        href_page="/collections?collection=",
        col_group_alias="Coleccion",
        value_alias="Nro de Transacciones",
    )

with c2:
    st.write("**Top colecciones por volumen de ventas**")
    comps.render_as_table(
        "top_collections_by_sales_volume_usd_price",
        "collection",
        "all_time",
        n=20,
        href_page="/collections?collection=",
        col_group_alias="Coleccion",
        value_alias="Volumen de Ventas (USD)",
    )
st.divider()

comps.show_granular_metrics_config()

grain = comps.get_option("grain")
refresh_rate = comps.get_option("refresh_rate")


@st.fragment(run_every=int(comps.get_option("refresh_rate")))
def show_linear_metrics():
    comps.linear_plot("total_transfers", grain)
    comps.linear_plot("total_usd_volume", grain)
    comps.multilinear_plot("top_collections_by_transfers_volume_transfers_count", grain)


show_linear_metrics()
