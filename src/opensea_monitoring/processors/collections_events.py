from typing import TYPE_CHECKING, Optional

import pyspark.sql.functions as F
from pyspark.sql.window import Window

from opensea_monitoring.processors.preprocessing import (
    get_sales_items,
    get_transferred_items,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def get_top_collections_assets_events(
    clean_events: "DataFrame",
    collection_name: str | list[str],
    window_duration: str,
    slide_duration: Optional[str] = None,
) -> "DataFrame":
    """
    Extracts the collections assets over time from a cleaned events DataFrame,
    by grouping the transferred items by in the specified time frame.

    @param clean_events: The cleaned events DataFrame.
    @return: A DataFrame with the collections assets over time.
    """
    sold_items = get_sales_items(clean_events)
    time_frame_txt = "_".join(window_duration.split())
    time_window = F.window("sent_at", window_duration, slide_duration)
    if isinstance(collection_name, str):
        collection_name = [collection_name]
    collections_sold_items = sold_items.filter(
        F.col("collection_slug").isin(collection_name)
    )
    collections_assets_ranked = collections_sold_items.select(
        "collection_slug",
        "item_blockchain",
        "item_nft_id",
        "item_name",
        "image_url",
        "usd_price",
        "sent_at",
        time_window,
    ).withColumn(
        "rank_by_price",
        F.row_number().over(
            Window.partitionBy("collection_slug", "window.start", "window.end").orderBy(
                F.desc("usd_price")
            )
        ),
    )
    top_collections_assets_events = collections_assets_ranked.select(
        F.lit(f"collection_top_assets_by_usd_volume__{time_frame_txt}").alias("metric"),
        F.col("window.end").alias("timestamp"),
        F.col("collection_slug").alias("collection"),
        F.col("usd_price").alias("value"),
        F.col("item_name").alias("asset_name"),
        F.col("item_url").alias("asset_url"),
        "image_url",
    ).filter(F.col("rank_by_price") <= 20)
    return top_collections_assets_events


def get_collection_transactions_events(
    clean_events: "DataFrame",
    collection_name: str | list[str],
    window_duration: str,
    slide_duration: Optional[str] = None,
    watermark_duration: Optional[str] = None,
) -> "DataFrame":
    """
    Extracts the collection transactions over time from a cleaned events DataFrame,
    by grouping the transferred items by in the specified time frame.

    @param clean_events: The cleaned events DataFrame.
    @return: A DataFrame with the collection transactions over time.
    """
    transaction_events = get_transferred_items(clean_events)
    time_frame_txt = "_".join(window_duration.split())
    time_window = F.window("sent_at", window_duration, slide_duration)
    if isinstance(collection_name, str):
        collection_name = [collection_name]
    if watermark_duration:
        transaction_events = transaction_events.withWatermark(
            "sent_at", watermark_duration
        )
    selected_collections_transactions = transaction_events.filter(
        F.col("collection_slug").isin(collection_name)
    )
    collections_transactions = (
        selected_collections_transactions.groupBy("collection_slug", time_window).agg(
            F.count("*").alias("total_transfers"),
            F.sum("quantity").alias("total_items_transferred"),
        )
    ).select(
        "collection_slug",
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "total_transfers",
        "total_items_transferred",
    )
    collections_transactions_events = collections_transactions.unpivot(
        ["collection_slug", "window_start", "window_end"],
        [
            "total_transfers",
            "total_items_transferred",
        ],
        "metric",
        "value",
    ).select(
        F.concat(
            F.lit("collection_"), F.col("metric"), F.lit(f"__{time_frame_txt}")
        ).alias("metric"),
        F.col("window_end").alias("timestamp"),
        F.col("collection_slug").alias("collection"),
        "value",
        F.lit(None).alias("asset_name"),
        F.lit(None).alias("asset_url"),
        F.lit(None).alias("image_url"),
    )
    return collections_transactions_events


def get_collection_sales_events(
    clean_events: "DataFrame",
    collection_name: str | list[str],
    window_duration: str,
    slide_duration: Optional[str] = None,
    watermark_duration: Optional[str] = None,
) -> "DataFrame":
    """
    Extracts the collection stats over time from a cleaned events DataFrame,
    by grouping the transferred items by in the specified time frame.

    @param clean_events: The cleaned events DataFrame.
    @return: A DataFrame with the collection stats over time.
    """
    sold_items = get_sales_items(clean_events)
    time_frame_txt = "_".join(window_duration.split())
    time_window = F.window("sent_at", window_duration, slide_duration)
    if isinstance(collection_name, str):
        collection_name = [collection_name]
    if watermark_duration:
        sold_items = sold_items.withWatermark("sent_at", watermark_duration)
    selected_collections_sold_items = sold_items.filter(
        F.col("collection_slug").isin(collection_name)
    )
    collections_sales = (
        selected_collections_sold_items.groupBy("collection_slug", time_window)
        .agg(
            F.sum("usd_price").alias("total_usd_volume"),
            F.count("*").alias("total_sales_count"),
            F.sum("quantity").alias("total_assets_sold"),
            F.min("usd_price").alias("floor_assets_usd_price"),
            F.avg("usd_price").alias("avg_assets_usd_price"),
        )
        .select(
            "collection_slug",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "total_usd_volume",
            "total_sales_count",
            "total_assets_sold",
            "floor_assets_usd_price",
            "avg_assets_usd_price",
        )
    )
    collections_sales_events = collections_sales.unpivot(
        ["collection_slug", "window_start", "window_end"],
        [
            "total_usd_volume",
            "total_sales_count",
            "total_assets_sold",
            "floor_assets_usd_price",
            "avg_assets_usd_price",
        ],
        "metric",
        "value",
    ).select(
        F.concat(
            F.lit("collection_"), F.col("metric"), F.lit(f"__{time_frame_txt}")
        ).alias("metric"),
        F.col("window_end").alias("timestamp"),
        F.col("collection_slug").alias("collection"),
        "value",
        F.lit(None).alias("asset_name"),
        F.lit(None).alias("asset_url"),
        F.lit(None).alias("image_url"),
    )
    return collections_sales_events
