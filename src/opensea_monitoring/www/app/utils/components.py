from typing import TYPE_CHECKING, Any, Iterable, Optional, Type, cast

import plotly.express as px
import plotly.graph_objects as go
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


def get_metric(
    metric_name: str,
    grain: str,
    collection: Optional[str] = None,
    as_frame: bool = False,
) -> Iterable[dict[str, Any]] | pl.DataFrame:
    client = OpenSeaDataMonitoringClient(default_keyspace="opensea")
    grain = (
        AppConfig.grain_options.get("options", {})
        .get(grain, {})
        .get("grain_value", grain)
    )
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


def indicator(
    metric: str,
    grain: str = "",
    collection: Optional[str] = None,
    container: "DeltaGenerator | ModuleType" = st,
    order_by: Optional[str] = None,
    order_descending: bool = True,
):
    """
    Renders an indicator for the specified metric
    using the specified collection.
    """
    config = get_config("collection" if collection else "global")
    data = get_metric(metric, grain, collection, as_frame=True)
    data = cast(pl.DataFrame, data)
    metric_config = config.plots_config[metric]
    if data.is_empty():
        value = None
        data_timestamp = None
    else:
        if order_by:
            data = data.sort(order_by, descending=order_descending)
        value = data["value"][0]
        data_timestamp = data["timestamp_at"][0]
    fig = go.Figure(
        layout=go.Layout(height=250),
    )
    indicator_kwargs = {
        "mode": "number",
        "value": value,
        "title": {"text": metric_config["title"]},
    }
    if metric_config.get("value_prefix"):
        indicator_kwargs["number"] = {"prefix": metric_config.get("value_prefix")}
    fig.add_trace(go.Indicator(**indicator_kwargs))
    if data_timestamp:
        # Add a last updated timestamp at the bottom center of the plot
        fig.add_annotation(
            xref="paper",
            yref="paper",
            x=0.5,
            y=-1,
            showarrow=False,
            text=(
                "Ultima actualización:<br>"
                f"{data_timestamp.strftime('%Y-%m-%d %H:%M:%S (UTC)')}"
            ),
        )
    container.plotly_chart(fig)


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
    metric_config = config.plots_config[metric]
    df = pl.DataFrame(data, schema=config.df_schema)
    fig = px.line(
        df.to_pandas(),
        x=metric_config["axes"]["x"],
        y=metric_config["axes"]["y"],
        markers=True,
    )
    fig.update_layout(title=metric_config["title"])
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
    metric_config = config.plots_config[metric]
    df = pl.DataFrame(data, schema=config.df_schema).limit(n)
    fig = px.line(
        df.to_pandas(),
        x=metric_config["axes"]["x"],
        y=metric_config["axes"]["y"],
        color=metric_config["axes"]["hue"],
    )
    fig.update_layout(
        title=metric_config["title"],
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01),
    )
    fig.update_xaxes(title_text="Timestamp")
    st.plotly_chart(fig)


def render_as_table(
    metric: str,
    col_group: str,
    grain: str,
    collection: Optional[str] = None,
    n: int = 10,
    href_page: Optional[str] = None,
    href_col: Optional[str] = None,
    col_group_alias: Optional[str] = None,
    value_alias: Optional[str] = None,
    container: "DeltaGenerator | ModuleType" = st,
):
    """
    Renders the metric as a table with the
    specified grain and collection.
    Retrieves the latest values for the
    specified metric and group.
    """
    data = get_metric(metric, grain, collection, as_frame=True)
    data = cast(pl.DataFrame, data)
    if data.is_empty():
        return container.write("No data available")
    group_latest = (
        data.group_by(col_group).agg(
            pl.max("timestamp_at").alias("timestamp_at"),
        )
    ).select(
        col_group,
        "timestamp_at",
    )
    table_df = (
        data.join(group_latest, on=[col_group, "timestamp_at"], how="inner")
        .sort("value", descending=True)
        .limit(n)
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
    elif href_col:
        table_df = table_df.with_columns(
            pl.concat_str(
                [
                    pl.lit("<a href='"),
                    table_df[href_col],
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
    return container.write(md_table_str, unsafe_allow_html=True)


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


@st.fragment(run_every=300)
def show_topbar_metrics(
    metrics: list[str], grain: str, collection: Optional[str] = None
):
    metrics_cols = st.columns(len(metrics))
    for metric, col in zip(metrics, metrics_cols):
        indicator(
            metric,
            grain=grain,
            collection=collection,
            container=col,
            order_by="timestamp_at",
        )


def init_page(
    page_title: str,
) -> dict[str, Any]:
    st.set_page_config(page_title, layout="wide", initial_sidebar_state="collapsed")
    load_dotenv()
    st.title(page_title)


def show_granular_metrics_config():
    st.write("## Métricas por granularidad")
    c1, c2, _ = st.columns([0.2, 0.2, 0.6])
    grain = str(grain_options("Granularidad", container=c1))
    refresh_rate = str(refresh_rate_options("Tiempo de refresh", container=c2))
    refresh_rate_value_secs = AppConfig.refresh_rate_options["options"][refresh_rate][
        "value_secs"
    ]
    set_option("refresh_rate", refresh_rate_value_secs)
    set_option("grain", grain)
    st.text(f"Refresh rate is set to {refresh_rate_value_secs} seconds")
    st.divider()


def get_option(
    name: str,
):
    option_value = st.session_state.get(name)
    config_options = getattr(AppConfig, f"{name}_options")
    if not option_value:
        option_value = config_options["options"][config_options["default_value"]][
            "grain_value"
        ]
        set_option(name, option_value)
    return option_value


def set_option(
    name: str,
    value: str,
):
    st.session_state[name] = value
    return value
