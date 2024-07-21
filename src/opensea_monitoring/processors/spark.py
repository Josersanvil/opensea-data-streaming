from typing import TYPE_CHECKING, Optional

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def get_clean_events(raw_events: "DataFrame") -> "DataFrame":
    """
    Cleans a DataFrame containing raw events from the OpenSea Web Stream.
    The provided DataFrame should contain the following columns:
    - event: The event name.
    - payload: The event payload.
    - topic: The event topic, if any.

    @param raw_events: The raw events DataFrame.
    @return: A cleaned DataFrame with relevant columns extracted from the payload.
    """
    df_events = raw_events.select(
        "event",
        F.col("payload.event_type").alias("event_type"),
        F.col("payload.payload.collection.slug").alias("collection_slug"),
        F.to_timestamp("payload.sent_at").alias("sent_at"),
        F.col("payload.status").alias("status"),
        F.col("payload.payload.item.metadata.name").alias("item_name"),
        F.col("payload.payload.item.permalink").alias("item_url"),
        F.col("payload.payload.item.nft_id").alias("item_nft_id"),
        F.col("payload.payload.item.metadata.image_url").alias("image_url"),
        F.col("payload.payload.item.chain.name").alias("item_blockchain"),
        F.to_timestamp(F.col("payload.payload.listing_date")).alias("listing_date"),
        F.col("payload.payload.listing_type").alias("listing_type"),
        F.col("payload.payload.from_account.address").alias("from_account"),
        F.col("payload.payload.to_account.address").alias("to_account"),
        F.col("payload.payload.payment_token.symbol").alias("payment_symbol"),
        F.col("payload.payload.payment_token.eth_price")
        .cast(T.DoubleType())
        .alias("eth_price"),
        F.col("payload.payload.payment_token.usd_price")
        .cast(T.DoubleType())
        .alias("usd_price"),
        F.col("payload.payload.quantity").cast(T.IntegerType()).alias("quantity"),
    ).filter(F.col("event") != "phx_reply")
    return df_events


def get_transferred_items(clean_events: "DataFrame") -> "DataFrame":
    """
    Extracts the transferred items from a cleaned events DataFrame.

    @param clean_events: The cleaned events DataFrame.
    @return: A DataFrame with the transferred items.
    """
    transferred_items = clean_events.filter(F.col("event_type") == "item_transferred")
    # Ensure quantity is a positive integer
    transferred_items = transferred_items.withColumn(
        "quantity",
        F.when(
            F.col("quantity").cast("int") > 0, F.col("quantity").cast("int")
        ).otherwise(0),
    )
    return transferred_items


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


def get_sales_items(clean_events: "DataFrame") -> "DataFrame":
    """
    Extracts the sales items from a cleaned events DataFrame.

    @param clean_events: The cleaned events DataFrame.
    @return: A DataFrame with the sales items.
    """
    sales_items = clean_events.filter(
        F.col("event_type").isin(["item_sold", "item_listed"])
    )
    return sales_items


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
