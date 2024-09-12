from typing import TYPE_CHECKING

import pyspark.sql.functions as F
import pyspark.sql.types as T

from opensea_monitoring.utils.schemas import get_opensea_payload_raw_schema

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame


def get_clean_events(
    raw_events: "DataFrame", is_json_payload: bool = False
) -> "DataFrame":
    """
    Cleans a DataFrame containing raw events from the OpenSea Web Stream.
    The provided DataFrame should contain the following columns:
    - event: The event name.
    - payload: The event payload.
    - topic: The event topic, if any.

    :param DataFrame raw_events:
        The raw events DataFrame.
    :param bool is_json_payload:
        Whether the payload is a JSON string.
        If True, the payload will be parsed as a JSON object
    :return:
        A cleaned DataFrame with relevant columns extracted from the payload.
    :rtype: DataFrame
    """
    if is_json_payload:
        raw_events = raw_events.withColumn(
            "payload",
            F.from_json(
                F.col("payload"),
                get_opensea_payload_raw_schema(),
            ),
        )
    df_events = raw_events.select(
        "event",
        _get_payload_value_as_col("event_type", "event_type"),
        _get_payload_value_as_col(
            "payload.collection.slug",
            "collection_slug",
        ),
        _get_payload_value_as_col("sent_at", "sent_at", T.TimestampType()),
        _get_payload_value_as_col("status", "status"),
        _get_payload_value_as_col("payload.item.metadata.name", "item_name"),
        _get_payload_value_as_col("payload.item.permalink", "item_url"),
        _get_payload_value_as_col("payload.item.nft_id", "item_nft_id"),
        _get_payload_value_as_col(
            "payload.item.metadata.image_url",
            "image_url",
        ),
        _get_payload_value_as_col(
            "payload.item.chain.name",
            "item_blockchain",
        ),
        _get_payload_value_as_col(
            "payload.listing_date",
            "listing_date",
            T.TimestampType(),
        ),
        _get_payload_value_as_col("payload.listing_type", "listing_type"),
        _get_payload_value_as_col(
            "payload.from_account.address",
            "from_account",
        ),
        _get_payload_value_as_col("payload.to_account.address", "to_account"),
        _get_payload_value_as_col(
            "payload.payment_token.symbol",
            "payment_symbol",
        ),
        _get_payload_value_as_col(
            "payload.payment_token.eth_price",
            "eth_price",
            T.DoubleType(),
        ),
        _get_payload_value_as_col(
            "payload.payment_token.usd_price",
            "usd_price",
            T.DoubleType(),
        ),
        _get_payload_value_as_col(
            "payload.quantity",
            "quantity",
            T.IntegerType(),
        ),
    ).filter(F.col("event") != "phx_reply")
    # Ensure that the quantity is always positive
    df_events = df_events.withColumn(
        "quantity",
        F.when(
            F.col("quantity").cast("int") > 0, F.col("quantity").cast("int")
        ).otherwise(1),
    )
    return df_events


def _get_payload_value_as_col(
    value: str,
    column_name: str,
    as_type: T.DataType | str = T.StringType(),
) -> "Column":
    """
    Extracts a value from the payload of a DataFrame containing raw events
    from the OpenSea Web Stream.

    :param str value: The value to extract from the payload.
        Nested fields can be accessed using dot notation.
    :param str column_name: The name of the column to create.
    :param DataType as_type: The data type to cast the extracted value to.
    :return: A Column object with the extracted value.
    :rtype: Column
    """
    col_obj = F.col(f"payload.{value}")
    return col_obj.cast(as_type).alias(column_name)


def get_transferred_items(clean_events: "DataFrame") -> "DataFrame":
    """
    Extracts the transferred items from a cleaned events DataFrame.

    :param DataFrame clean_events: The cleaned events DataFrame.
    :return: A DataFrame with the transferred items.
    :rtype: DataFrame
    """
    transferred_items = clean_events.filter(F.col("event_type") == "item_transferred")
    return transferred_items


def get_sales_items(clean_events: "DataFrame") -> "DataFrame":
    """
    Extracts the sales items from a cleaned events DataFrame.

    :param DataFrame clean_events: The cleaned events DataFrame.
    :return: A DataFrame with the sales items.
    :rtype: DataFrame
    """
    sales_items = clean_events.filter(
        F.col("event_type").isin(["item_sold", "item_listed"])
    )
    return sales_items
