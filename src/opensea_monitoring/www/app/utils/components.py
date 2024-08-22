from typing import TYPE_CHECKING, Any, Iterable, Optional, Type

import plotly.express as px
import polars as pl
import streamlit as st

from opensea_monitoring.www.app.utils.configs import (
    CollectionMetricsConfig,
    GlobalMetricsConfig,
)
from opensea_monitoring.www.backend.client import OpenSeaDataMonitoringClient

if TYPE_CHECKING:
    from types import ModuleType

    from streamlit.delta_generator import DeltaGenerator


@st.cache_data
def get_metric(
    metric_name: str, grain: str, collection: Optional[str] = None
) -> Iterable[dict[str, Any]]:
    client = OpenSeaDataMonitoringClient(default_keyspace="opensea")
    if collection:
        results = client.get_collection_metrics(
            collection=collection, metric=metric_name, grain=grain, order_ascending=True
        )
    else:
        results = client.get_global_metrics(
            metric=metric_name, grain=grain, order_ascending=True
        )
    return map(dict, results)


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
    grain: str = "1 day",
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
    grain: str = "1 day",
    collection: Optional[str] = None,
):
    """
    Renders a multilinear plot for a given metric
    using 'collection' as the color hue for the
    lines in the plot with plotly.
    """
    config = get_config("collection" if collection else "global")
    data = get_metric(metric, grain, collection)
    df = pl.DataFrame(data, schema=config.df_schema)
    fig = px.line(df.to_pandas(), x="timestamp_at", y="value", color="collection")
    fig.update_layout(title=f"{config.plots_config[metric]['title']} ({grain})")
    fig.update_xaxes(title_text="Timestamp")
    st.plotly_chart(fig)


def render_as_table(
    metric: str,
    grain: str = "1 day",
    collection: Optional[str] = None,
    n: int = 10,
    collection_href_col: Optional[str] = None,
    container: "DeltaGenerator | ModuleType" = st,
) -> "DeltaGenerator":
    """
    Renders the metric as a table with the
    specified grain and collection.
    """
    data = get_metric(metric, grain, collection)
    table_df = pl.DataFrame(data).limit(n)
    if collection_href_col:
        table_df = table_df.with_columns(
            pl.concat_str(
                [
                    pl.lit("<a target='_Self' href='/collections?name="),
                    table_df[collection_href_col],
                    pl.lit("'>Ver detalles</a>"),
                ]
            ).alias(
                collection_href_col,
            )
        )
    return container.table(table_df)


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
    grain_opts = GlobalMetricsConfig.plots_config["transfers_count"]["grain_options"]
    return container.selectbox(
        title,
        grain_opts,
        index=0,
    )
