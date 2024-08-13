import logging
from typing import TYPE_CHECKING, Optional

import pyspark.sql.functions as F

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.streaming import DataStreamWriter


def get_kafka_stream_writer(
    df: "DataFrame",
    topic: str,
    kafka_brokers: str,
    checkpoint_location: str,
    debug: bool = False,
    key_column: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
) -> "DataStreamWriter":
    """
    Returns a DataStreamWriter object to write a
    streaming DataFrame to a Kafka topic.

    @param df: The streaming DataFrame to write.
    @param topic: The Kafka topic to write to.
    @param kafka_brokers: The Kafka brokers to connect to.
    @param checkpoint_location: The location to store the checkpoint data.
    @param debug: If True, the results will be written to the console
        instead of to a Kafka Topic.
    @param key_column: The column to use as the key. If None, no key will be used.
        The value column is a column with the JSON representation
        of the rest of the columns.
    @param logger: The logger to use. If None, a new logger will be created.
    @return: The DataStreamWriter object.
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    stream_df_cols = []
    if key_column:
        key_col = F.col(key_column).alias("key")
        value_cols = [col for col in df.columns if col != key_column]
        stream_df_cols.append(key_col)
    else:
        value_cols = ["*"]
    value_col = F.to_json(
        F.struct(*value_cols), options={"ignoreNullFields": "false"}
    ).alias("value")
    stream_df_cols.append(value_col)
    stream_df = df.select(*stream_df_cols)
    if debug:
        logger.warning(
            "Debug mode enabled. The results of the streaming query will "
            "be written to the console instead of to a Kafka Topic"
        )
        stream_writer = (
            stream_df.writeStream.format("console")
            .outputMode("complete")
            .option("truncate", "false")
        )
    else:
        logger.info(f"Writing streaming data to Kafka topic {topic} at {kafka_brokers}")
        stream_writer = (
            stream_df.writeStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka_brokers)
            .option("topic", topic)
            .option("checkpointLocation", checkpoint_location)
        )
    return stream_writer


def write_df_to_kafka_topic(
    df: "DataFrame",
    topic: str,
    kafka_brokers: str,
    key_column: Optional[str] = None,
) -> None:
    """
    Writes a DataFrame to a Kafka topic.

    @param df: The DataFrame to write.
    @param topic: The Kafka topic to write to.
    @param kafka_brokers: The Kafka brokers to connect to.
    @param key_column: The column to use as the key. If None, no key will be used.
        The value column is a column with the JSON representation
        of the rest of the columns.
    """
    key_col = F.col(key_column) if key_column else F.lit(None)
    df_cols = [col for col in df.columns if col != key_column]
    value_col = F.to_json(
        F.struct(*df_cols), options={"ignoreNullFields": "false"}
    ).alias("value")
    df.select(key_col, value_col).write.format("kafka").option(
        "kafka.bootstrap.servers", kafka_brokers
    ).option("topic", topic).save()
