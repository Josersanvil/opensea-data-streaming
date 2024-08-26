from typing import TYPE_CHECKING, Optional

import pyspark.sql.functions as F
from pyspark.sql.window import Window

from opensea_monitoring.processors.preprocessing import (
    get_sales_items,
    get_transferred_items,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def get_top_collections_assets_by_sales(
    clean_events: "DataFrame",
    window_duration: str,
    slide_duration: Optional[str] = None,
) -> "DataFrame":
    """
    Extracts the top collections' assets over time from a
    cleaned events DataFrame by grouping the sold items
    by the specified time frame.

    @param clean_events: The cleaned events DataFrame.
    @return: A DataFrame with the collections assets over time.
    """
    sold_items = get_sales_items(clean_events)
    time_frame_txt = "_".join(window_duration.split())
    time_window = F.window("sent_at", window_duration, slide_duration)
    collections_assets_sales = (
        sold_items.groupBy(
            "collection_slug",
            "item_nft_id",
            "item_name",
            "item_url",
            "image_url",
            time_window,
        )
        .agg(F.sum("usd_price").alias("usd_volume"))
        .select(
            F.col("collection_slug").alias("collection"),
            F.col("window.end").alias("timestamp"),
            F.coalesce("item_name", "item_nft_id").alias("asset_name"),
            F.col("item_url").alias("asset_url"),
            "image_url",
            "usd_volume",
        )
    )
    collections_assets_ranked = collections_assets_sales.withColumn(
        "rank_by_volume",
        F.row_number().over(
            Window.partitionBy("collection", "timestamp").orderBy(F.desc("usd_volume"))
        ),
    )
    top_collections_assets_events = collections_assets_ranked.select(
        F.lit(f"collection_top_assets_by_sales_volume__{time_frame_txt}").alias(
            "metric"
        ),
        "timestamp",
        "collection",
        F.col("usd_volume").alias("value"),
        "asset_name",
        "asset_url",
        "image_url",
    ).filter(F.col("rank_by_volume") <= 20)
    return top_collections_assets_events


def get_top_collections_assets_by_transfers(
    clean_events: "DataFrame",
    window_duration: str,
    slide_duration: Optional[str] = None,
) -> "DataFrame":
    """
    Extracts the top collections' assets over time from a
    cleaned events DataFrame by grouping the transferred items
    by the specified time frame.

    @param clean_events: The cleaned events DataFrame.
    @return: A DataFrame with the collections assets over time.
    """
    transferred_items = get_transferred_items(clean_events)
    time_frame_txt = "_".join(window_duration.split())
    time_window = F.window("sent_at", window_duration, slide_duration)
    collections_assets_transfers = (
        transferred_items.groupBy(
            "collection_slug",
            "item_nft_id",
            "item_name",
            "item_url",
            "image_url",
            time_window,
        )
        .agg(F.count("*").alias("total_transfers"))
        .select(
            F.col("collection_slug").alias("collection"),
            F.col("window.end").alias("timestamp"),
            F.coalesce("item_name", "item_nft_id").alias("asset_name"),
            F.col("item_url").alias("asset_url"),
            "image_url",
            "total_transfers",
        )
    )
    collections_assets_ranked = collections_assets_transfers.withColumn(
        "rank_by_transfers",
        F.row_number().over(
            Window.partitionBy("collection", "timestamp").orderBy(
                F.desc("total_transfers")
            )
        ),
    )
    top_collections_assets_events = collections_assets_ranked.select(
        F.lit(f"collection_top_assets_by_transfers__{time_frame_txt}").alias("metric"),
        "timestamp",
        "collection",
        F.col("total_transfers").alias("value"),
        "asset_name",
        "asset_url",
        "image_url",
    ).filter(F.col("rank_by_transfers") <= 20)
    return top_collections_assets_events


def get_collection_transactions_events(
    clean_events: "DataFrame",
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
    if watermark_duration:
        transaction_events = transaction_events.withWatermark(
            "sent_at", watermark_duration
        )
    collections_transactions = (
        transaction_events.groupBy("collection_slug", time_window).agg(
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
    if watermark_duration:
        sold_items = sold_items.withWatermark("sent_at", watermark_duration)
    collections_sales = (
        sold_items.groupBy("collection_slug", time_window)
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
