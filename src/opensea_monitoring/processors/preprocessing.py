from typing import TYPE_CHECKING

import pyspark.sql.functions as F
import pyspark.sql.types as T

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

    @param raw_events: The raw events DataFrame.
    @param is_json_payload: Whether the payload is a JSON string.
    @return: A cleaned DataFrame with relevant columns extracted from the payload.
    """
    df_events = raw_events.select(
        "event",
        _get_payload_value_as_col(
            "event_type", "event_type", is_json_payload=is_json_payload
        ),
        _get_payload_value_as_col(
            "payload.collection.slug",
            "collection_slug",
            is_json_payload=is_json_payload,
        ),
        _get_payload_value_as_col(
            "sent_at", "sent_at", T.TimestampType(), is_json_payload=is_json_payload
        ),
        _get_payload_value_as_col("status", "status", is_json_payload=is_json_payload),
        _get_payload_value_as_col(
            "payload.item.metadata.name", "item_name", is_json_payload=is_json_payload
        ),
        _get_payload_value_as_col(
            "payload.item.permalink", "item_url", is_json_payload=is_json_payload
        ),
        _get_payload_value_as_col(
            "payload.item.nft_id", "item_nft_id", is_json_payload=is_json_payload
        ),
        _get_payload_value_as_col(
            "payload.item.metadata.image_url",
            "image_url",
            is_json_payload=is_json_payload,
        ),
        _get_payload_value_as_col(
            "payload.item.chain.name",
            "item_blockchain",
            is_json_payload=is_json_payload,
        ),
        _get_payload_value_as_col(
            "payload.listing_date",
            "listing_date",
            T.TimestampType(),
            is_json_payload=is_json_payload,
        ),
        _get_payload_value_as_col(
            "payload.listing_type", "listing_type", is_json_payload=is_json_payload
        ),
        _get_payload_value_as_col(
            "payload.from_account.address",
            "from_account",
            is_json_payload=is_json_payload,
        ),
        _get_payload_value_as_col(
            "payload.to_account.address", "to_account", is_json_payload=is_json_payload
        ),
        _get_payload_value_as_col(
            "payload.payment_token.symbol",
            "payment_symbol",
            is_json_payload=is_json_payload,
        ),
        _get_payload_value_as_col(
            "payload.payment_token.eth_price",
            "eth_price",
            T.DoubleType(),
            is_json_payload=is_json_payload,
        ),
        _get_payload_value_as_col(
            "payload.payment_token.usd_price",
            "usd_price",
            T.DoubleType(),
            is_json_payload=is_json_payload,
        ),
        _get_payload_value_as_col(
            "payload.quantity",
            "quantity",
            T.IntegerType(),
            is_json_payload=is_json_payload,
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
    is_json_payload: bool = False,
) -> "Column":
    """
    Extracts a value from the payload of a DataFrame containing raw events
    from the OpenSea Web Stream.

    @param value: The value to extract from the payload.
        Nested fields can be accessed using dot notation.
    @param column_name: The name of the column to create.
    @param as_type: The data type to cast the extracted value to.
    @param is_json_payload: Whether the payload is a JSON string.
    """
    if is_json_payload:
        col_obj = F.get_json_object("payload", f"$.{value}")
    else:
        col_obj = F.col(f"payload.{value}")
    return col_obj.cast(as_type).alias(column_name)


def get_transferred_items(clean_events: "DataFrame") -> "DataFrame":
    """
    Extracts the transferred items from a cleaned events DataFrame.

    @param clean_events: The cleaned events DataFrame.
    @return: A DataFrame with the transferred items.
    """
    transferred_items = clean_events.filter(F.col("event_type") == "item_transferred")
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
