from typing import TYPE_CHECKING, Any, Iterable, Optional, Type

import plotly.express as px
import polars as pl
import streamlit as st
from dotenv import load_dotenv

from opensea_monitoring.www.app.utils.configs import (
    AppConfig,
    CollectionMetricsConfig,
    GlobalMetricsConfig,
)
from opensea_monitoring.www.backend.client import OpenSeaDataMonitoringClient

if TYPE_CHECKING:
    from types import ModuleType

    from streamlit.delta_generator import DeltaGenerator


@st.cache_data
def get_metric(
    metric_name: str,
    grain: str,
    collection: Optional[str] = None,
    as_frame: bool = False,
) -> Iterable[dict[str, Any]] | pl.DataFrame:
    client = OpenSeaDataMonitoringClient(default_keyspace="opensea")
    grain = AppConfig.grain_options["options"][grain]["grain_value"]
    if collection:
        results = client.get_collection_metrics(
            collection=collection, metric=metric_name, grain=grain, order_ascending=True
        )
    else:
        results = client.get_global_metrics(metric=metric_name, grain=grain)
    results_iter = map(dict, results)
    if as_frame:
        return pl.DataFrame(results_iter)
    return results_iter


def get_config(
    metric_type: str,
) -> "Type[GlobalMetricsConfig] | Type[CollectionMetricsConfig]":
    if metric_type == "global":
        return GlobalMetricsConfig
    elif metric_type == "collection":
        return CollectionMetricsConfig
    else:
        raise ValueError("Invalid metric type")


def linear_plot(
    metric: str,
    grain: str,
    collection: Optional[str] = None,
):
    """
    Renders a linear plot for a given metric
    using the specified grain and collection
    with plotly.
    """
    config = get_config("collection" if collection else "global")
    data = get_metric(metric, grain, collection)
    df = pl.DataFrame(data, schema=config.df_schema)
    fig = px.line(df.to_pandas(), x="timestamp_at", y="value")
    fig.update_layout(title=f"{config.plots_config[metric]['title']} ({grain})")
    fig.update_xaxes(title_text="Timestamp")
    st.plotly_chart(fig)


def multilinear_plot(
    metric: str,
    grain: str,
    collection: Optional[str] = None,
    n: int = 10,
):
    """
    Renders a multilinear plot for a given metric
    using 'collection' as the color hue for the
    lines in the plot with plotly.
    """
    config = get_config("collection" if collection else "global")
    data = get_metric(metric, grain, collection)
    df = pl.DataFrame(data, schema=config.df_schema).limit(n)
    fig = px.line(df.to_pandas(), x="timestamp_at", y="value", color="collection")
    fig.update_layout(title=f"{config.plots_config[metric]['title']} ({grain})")
    fig.update_xaxes(title_text="Timestamp")
    st.plotly_chart(fig)


def render_as_table(
    metric: str,
    col_group: str,
    grain: str,
    collection: Optional[str] = None,
    n: int = 10,
    href_page: Optional[str] = None,
    col_group_alias: Optional[str] = None,
    value_alias: Optional[str] = None,
    container: "DeltaGenerator | ModuleType" = st,
) -> "DeltaGenerator":
    """
    Renders the metric as a table with the
    specified grain and collection.
    """
    data = get_metric(metric, grain, collection)
    table_df = (
        pl.DataFrame(data)
        .group_by(col_group)
        .sum()
        .limit(n)
        .sort("value", descending=True)
    )
    if href_page:
        table_df = table_df.with_columns(
            pl.concat_str(
                [
                    pl.lit(f"<a target='_Self' href='{href_page}"),
                    table_df[col_group],
                    pl.lit("'>"),
                    table_df[col_group],
                    pl.lit("</a>"),
                ]
            ).alias(
                col_group,
            )
        )
    col_group_alias = col_group_alias or col_group
    value_alias = value_alias or "Value"
    md_table_str = f"| {col_group_alias}  | {value_alias} |\n| --- | --- |\n"
    for i in range(len(table_df)):
        md_table_str += f"{table_df.item(i, col_group)} |"
        md_table_str += f"{table_df.item(i, 'value'):,.0f} |\n"
    return container.markdown(md_table_str, unsafe_allow_html=True)


def grain_options(
    title: str = "Grain",
    container: "DeltaGenerator | ModuleType" = st,
):
    """
    Renders a selectbox with the grain options
    for the plots.

    @param title: The title of the selectbox
    @param container: Optional container to render the selectbox in
    """
    opts = list(AppConfig.grain_options["options"])
    default_option = AppConfig.grain_options["default_value"]
    return container.selectbox(
        title,
        opts,
        index=opts.index(default_option),
    )


def refresh_rate_options(
    title: str = "Refresh rate",
    container: "DeltaGenerator | ModuleType" = st,
):
    """
    Renders a selectbox with the refresh rate options
    for the plots.

    @param container: Optional container to render the selectbox in
    """
    opts = list(AppConfig.refresh_rate_options["options"])
    default_option = AppConfig.refresh_rate_options["default_value"]
    return container.selectbox(
        title,
        opts,
        index=opts.index(default_option),
    )


def init_page(page_title: str) -> dict[str, Any]:
    st.set_page_config(page_title, layout="wide", initial_sidebar_state="collapsed")

    load_dotenv()

    st.title(page_title)
    c1, c2, _ = st.columns([0.2, 0.2, 0.6])
    grain = str(grain_options("Granularidad", container=c1))
    refresh_rate = str(refresh_rate_options("Tiempo de refresh", container=c2))
    refresh_rate_value_secs = AppConfig.refresh_rate_options["options"][refresh_rate][
        "value_secs"
    ]
    st.text(f"Refresh rate is set to {refresh_rate_value_secs} seconds")
    st.divider()
    return {"grain": grain, "refresh_rate_value_secs": refresh_rate_value_secs}
