from typing import TYPE_CHECKING, Optional

import pyspark.sql.functions as F
from pyspark.sql.window import Window

from opensea_monitoring.processors.preprocessing import (
    get_sales_items,
    get_transferred_items,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def get_transactions_events(
    clean_events: "DataFrame",
    window_duration: str,
    slide_duration: Optional[str] = None,
    watermark_duration: Optional[str] = None,
) -> "DataFrame":
    """
    Extracts the transactions events from a cleaned events DataFrame,
    by grouping the transferred items by in the specified time frame.

    @param clean_events: The cleaned events DataFrame.
    @return: A DataFrame with the transactions events.
    """
    transferred_items = get_transferred_items(clean_events)
    time_frame_txt = "_".join(window_duration.split())
    time_window = F.window("sent_at", window_duration, slide_duration)
    if watermark_duration:
        transferred_items = transferred_items.withWatermark(
            "sent_at", watermark_duration
        )
    windowed_transactions = (
        transferred_items.groupBy(time_window)
        .agg(
            F.count("*").alias("total_transfers"),
            F.sum("quantity").alias("total_items_transferred"),
        )
        .orderBy("window")
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "transfers_count",
            "items_transferred_count",
        )
    )
    windowed_transactions_events = windowed_transactions.unpivot(
        ["window_start", "window_end"],
        ["transfers_count", "items_transferred_count"],
        "metric",
        "value",
    ).select(
        F.concat("metric", F.lit(f"__{time_frame_txt}")).alias("metric"),
        F.col("window_end").alias("timestamp"),
        "value",
        F.lit(None).alias("collection"),
    )
    return windowed_transactions_events


def get_sales_volume_events(
    clean_events: "DataFrame",
    window_duration: str,
    slide_duration: Optional[str] = None,
    watermark_duration: Optional[str] = None,
) -> "DataFrame":
    """
    Extracts the sales volume over time from a cleaned events DataFrame,
    by grouping the transferred items by in the specified time frame.

    @param clean_events: The cleaned events DataFrame.
    @return: A DataFrame with the sales volume over time.
    """
    sales_items = get_sales_items(clean_events)
    time_frame_txt = "_".join(window_duration.split())
    time_window = F.window("sent_at", window_duration, slide_duration)
    if watermark_duration:
        sales_items = sales_items.withWatermark("sent_at", watermark_duration)
    windowed_sales = (
        sales_items.groupBy(time_window)
        .agg(
            F.count("*").alias("total_sales"),
            F.sum("payload.payment_token.eth_price").alias("total_eth_volume"),
            F.sum("payload.payment_token.usd_price").alias("total_usd_volume"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "total_sales",
            "total_eth_volume",
            "total_usd_volume",
        )
    )
    windowed_sales_events = windowed_sales.unpivot(
        ["window_start", "window_end"],
        ["total_sales", "total_eth_volume", "total_usd_volume"],
        "metric",
        "value",
    ).select(
        F.concat("metric", F.lit(f"__{time_frame_txt}")).alias("metric"),
        F.col("window_end").alias("timestamp"),
        "value",
        F.lit(None).alias("collection"),
    )
    return windowed_sales_events


def get_top_collections_by_volume_events(
    clean_events: "DataFrame",
    window_duration: str,
    slide_duration: Optional[str] = None,
) -> "DataFrame":
    """
    Extracts the top collections by volume over time from a cleaned events DataFrame,
    by grouping the transferred items by in the specified time frame.

    @param clean_events: The cleaned events DataFrame.
    @return: A DataFrame with the top collections by volume over time.
    """
    sold_items = get_sales_items(clean_events)
    time_frame_txt = "_".join(window_duration.split())
    time_window = F.window("sent_at", window_duration, slide_duration)
    top_collections = (
        sold_items.groupby("collection_slug", time_window)
        .agg(F.sum("usd_price").alias("usd_volume"), F.count("*").alias("sales_count"))
        .orderBy(F.desc("usd_volume"))
        .select(
            "collection_slug",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "usd_volume",
            "sales_count",
        )
    ).withColumn(
        "window_rank",
        (
            F.row_number().over(
                Window.partitionBy("window_start", "window_end").orderBy(
                    F.desc("usd_volume")
                )
            )
        ),
    )
    top_collections_events = top_collections.select(
        F.lit(f"top_collections_by_volume__{time_frame_txt}").alias("metric"),
        F.col("window_end").alias("timestamp"),
        F.col("usd_volume").alias("value"),
        F.col("collection_slug").alias("collection"),
    ).filter(F.col("window_rank") <= 10)
    return top_collections_events
