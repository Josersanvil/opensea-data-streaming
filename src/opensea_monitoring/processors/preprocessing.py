from typing import TYPE_CHECKING

import pyspark.sql.functions as F
import pyspark.sql.types as T

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
        (
            F.col("payload.payload.payment_token.eth_price")
            .cast(T.DoubleType())
            .alias("eth_price")
        ),
        (
            F.col("payload.payload.payment_token.usd_price")
            .cast(T.DoubleType())
            .alias("usd_price")
        ),
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
