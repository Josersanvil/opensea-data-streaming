from typing import TYPE_CHECKING, Optional

import pyspark.sql.functions as F
from pyspark.sql.window import Window

from opensea_monitoring.processors.preprocessing import (
    get_sales_items,
    get_transferred_items,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def get_all_time_metrics(
    clean_events: "DataFrame",
) -> "DataFrame":
    """
    Extracts the all time metrics from a cleaned events DataFrame.
    This function is only available for batch processing.
    The timestamp of the events is the current timestamp
    used when the data is processed.
    """
    transferred_items = get_transferred_items(clean_events)
    sales_items = get_sales_items(clean_events)
    all_time_metrics = transferred_items.agg(
        F.count("*").alias("total_transfers"),
        F.sum("quantity").alias("total_items_transferred"),
    ).crossJoin(
        sales_items.agg(
            F.count("*").alias("total_sales"),
            F.sum(F.coalesce("eth_price", F.lit(0))).alias("total_eth_volume"),
            F.sum(F.coalesce("usd_price", F.lit(0))).alias("total_usd_volume"),
        )
    )
    all_time_metrics_events = all_time_metrics.unpivot(
        [],
        [
            "total_transfers",
            "total_items_transferred",
            "total_sales",
            "total_eth_volume",
            "total_usd_volume",
        ],
        "metric",
        "value",
    ).select(
        F.concat(
            "metric",
            F.lit("__all_time"),
        ).alias("metric"),
        F.current_timestamp().alias("timestamp"),
        "value",
        F.lit("").alias("collection"),
    )
    return all_time_metrics_events


def get_all_time_top_collections(
    clean_events: "DataFrame",
    n: int = 10,
) -> "DataFrame":
    """
    Extracts the top collections by volume of transactions and
    volume of sales from a cleaned events DataFrame.
    This function is only available for batch processing.
    The timestamp of the events is the current timestamp
    used when the data is processed.
    """
    transferred_items = get_transferred_items(clean_events)
    sales_items = get_sales_items(clean_events)
    top_collections_by_usd_volume = (
        sales_items.groupby("collection_slug")
        .agg(
            F.sum("usd_price").alias("usd_volume"),
        )
        .sort(F.desc("usd_volume"))
        .limit(n)
    ).select(
        F.lit("top_collections_by_sales_volume_usd_price__all_time").alias("metric"),
        F.current_timestamp().alias("timestamp"),
        F.col("usd_volume").alias("value"),
        F.col("collection_slug").alias("collection"),
    )
    top_collections_by_transfers_count = (
        transferred_items.groupby("collection_slug")
        .agg(
            F.count("*").alias("transfers_count"),
        )
        .sort(F.desc("transfers_count"))
        .limit(n)
    ).select(
        F.lit("top_collections_by_transfers_volume_transfers_count__all_time").alias(
            "metric"
        ),
        F.current_timestamp().alias("timestamp"),
        F.col("transfers_count").alias("value"),
        F.col("collection_slug").alias("collection"),
    )
    top_collections_events = top_collections_by_usd_volume.union(
        top_collections_by_transfers_count
    )
    return top_collections_events


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
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "total_transfers",
            "total_items_transferred",
        )
    )
    windowed_transactions_events = windowed_transactions.unpivot(
        ["window_start", "window_end"],
        ["total_transfers", "total_items_transferred"],
        "metric",
        "value",
    ).select(
        F.concat("metric", F.lit(f"__{time_frame_txt}")).alias("metric"),
        F.col("window_end").alias("timestamp"),
        "value",
        F.lit("").alias("collection"),
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
            F.sum("eth_price").alias("total_eth_volume"),
            F.sum("usd_price").alias("total_usd_volume"),
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
        F.lit("").alias("collection"),
    )
    return windowed_sales_events


def get_top_collections_by_sales_volume(
    clean_events: "DataFrame",
    window_duration: str,
    slide_duration: Optional[str] = None,
) -> "DataFrame":
    """
    Extracts the collections by volume of sales over time from a cleaned events
    DataFrame, by grouping the sold items in the specified time frame.

    This function is only available for batch processing.

    @param clean_events: The cleaned events DataFrame.
    @return: A DataFrame with the top collections by sales volume over time.
    """
    sold_items = get_sales_items(clean_events)
    time_frame_txt = "_".join(window_duration.split())
    time_window = F.window("sent_at", window_duration, slide_duration)
    top_collections = (
        (
            sold_items.groupby("collection_slug", time_window)
            .agg(
                F.sum("usd_price").alias("usd_volume"),
                F.count("*").alias("sales_count"),
                F.min("usd_price").alias("floor_usd_price"),
                F.avg("usd_price").alias("avg_usd_price"),
            )
            .select(
                "collection_slug",
                F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                "usd_volume",
                "sales_count",
                "floor_usd_price",
                "avg_usd_price",
            )
        )
        .withColumn(
            "window_rank",
            (
                F.row_number().over(
                    Window.partitionBy("window_start", "window_end").orderBy(
                        F.desc("usd_volume")
                    )
                )
            ),
        )
        .filter(F.col("window_rank") <= 10)
    )
    top_collections_events = top_collections.unpivot(
        ["collection_slug", "window_start", "window_end"],
        ["usd_volume", "sales_count", "floor_usd_price", "avg_usd_price"],
        "metric",
        "value",
    ).select(
        F.concat(
            F.lit("top_collections_by_sales_volume_"),
            "metric",
            F.lit(f"__{time_frame_txt}"),
        ).alias("metric"),
        F.col("window_end").alias("timestamp"),
        "value",
        F.col("collection_slug").alias("collection"),
    )
    return top_collections_events


def get_top_collections_by_transactions_volume(
    clean_events: "DataFrame",
    window_duration: str,
    slide_duration: Optional[str] = None,
) -> "DataFrame":
    """
    Extracts the collections by volume of transactions over time from a
    cleaned events DataFrame by grouping the transferred items
    in the specified time frame.

    This function is only available for batch processing.

    @param clean_events: The cleaned events DataFrame.
    @return: A DataFrame with collections by transaction volume over time.
    """
    transferred_items = get_transferred_items(clean_events)
    time_frame_txt = "_".join(window_duration.split())
    time_window = F.window("sent_at", window_duration, slide_duration)
    top_collections = (
        transferred_items.groupby("collection_slug", time_window)
        .agg(
            F.count("*").alias("transfers_count"),
            F.sum("quantity").alias("items_transferred"),
        )
        .select(
            "collection_slug",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "transfers_count",
            "items_transferred",
        )
        .withColumn(
            "window_rank",
            (
                F.row_number().over(
                    Window.partitionBy("window_start", "window_end").orderBy(
                        F.desc("transfers_count")
                    )
                )
            ),
        )
        .filter(F.col("window_rank") <= 10)
    )

    top_collections_events = top_collections.unpivot(
        ["collection_slug", "window_start", "window_end"],
        ["transfers_count", "items_transferred"],
        "metric",
        "value",
    ).select(
        F.concat(
            F.lit("top_collections_by_transfers_volume_"),
            "metric",
            F.lit(f"__{time_frame_txt}"),
        ).alias("metric"),
        F.col("window_end").alias("timestamp"),
        "value",
        F.col("collection_slug").alias("collection"),
    )
    return top_collections_events
