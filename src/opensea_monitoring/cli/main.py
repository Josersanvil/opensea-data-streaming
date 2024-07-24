import argparse
import logging
import time
from typing import TYPE_CHECKING, Optional

import pyspark.sql.functions as F

import opensea_monitoring.processors.collections_events as collections_processors
import opensea_monitoring.processors.global_events as global_processors
from opensea_monitoring.processors.preprocessing import get_clean_events
from opensea_monitoring.utils.configs import settings
from opensea_monitoring.utils.spark import get_spark_session, write_df_to_kafka_topic

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

global_events_cols = ["metric", "timestamp", "value", "collection"]
collections_events_cols = [
    "metric",
    "timestamp",
    "collection",
    "value",
    "asset_name",
    "asset_url",
    "image_url",
]
batch_options = {
    "14 days",
    "7 days",
    "1 day",
    "12 hours",
    "1 hour",
}
stream_options = {
    "5 minutes",
    "1 minute",
}


def get_logger(subname: Optional[str] = None) -> logging.Logger:
    logger_name = f"{__name__}.{subname}" if subname else __name__
    logger = logging.getLogger(logger_name)
    return logger


def get_argparser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="OpenSea Monitoring CLI",
    )
    parser.add_argument(
        "type",
        type=str,
        help="The type of events to process.",
        choices=["global", "collections"],
    )
    parser.add_argument(
        "time_window",
        type=str,
        help="The time window in which to group the events.",
        choices=batch_options.union(stream_options),
    )
    parser.add_argument(
        "--raw-events-s3-uri",
        type=str,
        help="The S3 URI where the raw events are stored.",
    )
    parser.add_argument(
        "--kafka-topic",
        type=str,
        help="The Kafka topic to write the processed events.",
    )
    parser.add_argument(
        "--raw-events-kafka-topic",
        type=str,
        help="The Kafka topic to read the raw events from.",
    )
    parser.add_argument(
        "--kafka-brokers",
        type=str,
        help="The Kafka brokers to connect to.",
    )
    parser.add_argument(
        "--slide-duration",
        type=str,
        help="The slide duration for the time window.",
    )
    parser.add_argument(
        "--watermark-duration",
        type=str,
        help="The watermark duration for the time window. Only required for streaming data.",
    )
    parser.add_argument(
        "--checkpoint-dir",
        type=str,
        help="The checkpoint directory for the streaming query.",
    )
    parser.add_argument(
        "--log-level",
        "-l",
        type=str,
        help="The log level for the application.",
        default=settings.log_level or "WARNING",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )
    return parser


def _get_raw_events_batch(
    spark: "SparkSession", args: argparse.Namespace
) -> "DataFrame":
    if not args.raw_events_s3_uri:
        raise ValueError(
            "The raw events S3 URI is required when processing data in batch"
        )
    s3_path = f"{args.raw_events_s3_uri}/year=*/month=*/day=*/hour=*/*.json.gz"
    logger = get_logger("get_raw_events_batch")
    logger.debug(f"Reading raw events from '{s3_path}'")
    raw_events = spark.read.json(s3_path)
    return raw_events


def _get_raw_events_stream(
    spark: "SparkSession", args: argparse.Namespace
) -> "DataFrame":
    if not (
        args.raw_events_kakfa_topic
        and args.kafka_brokers
        and args.kafka_topic
        and args.checkpoint_dir
    ):
        raise ValueError(
            "The raw events Kafka topic, Kafka brokers, Kafka topic, and checkpoint "
            "directory are required when processing data in stream"
        )
    logger = get_logger("get_raw_events_stream")
    logger.debug(f"Reading raw events from Kafka topic '{args.raw_events_kakfa_topic}'")
    raw_events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_brokers)
        .option("subscribe", args.raw_events_kakfa_topic)
        .load()
    ).select(
        F.get_json_object(F.col("value").cast("string"), "$.event").alias("event"),
        F.get_json_object(F.col("value").cast("string"), "$.payload").alias("payload"),
        F.get_json_object(F.col("value").cast("string"), "$.topic").alias("topic"),
    )
    return raw_events


def process_global_metrics_batch(args: argparse.Namespace) -> None:
    logger = get_logger("process_global_metrics_batch")
    spark = get_spark_session(logger.name)
    raw_events = _get_raw_events_batch(spark, args)
    clean_events = get_clean_events(raw_events)
    clean_events.cache()
    # Global metrics:
    transactions_events = global_processors.get_transactions_events(
        clean_events, args.time_window
    )
    marketplace_sales = global_processors.get_sales_volume_events(
        clean_events, args.time_window
    )
    top_collections_sales = global_processors.get_top_collections_by_volume_events(
        clean_events, args.time_window
    )
    # Concatenate the events:
    global_events = (
        transactions_events[global_events_cols]
        .union(marketplace_sales[global_events_cols])
        .union(top_collections_sales[global_events_cols])
    )
    events_cnt = global_events.count()
    logger.info(f"Processed {events_cnt} events.")
    if args.kafka_topic:
        logger.info(f"Exporting the metrics to Kafka topic {args.kafka_topic}")
        if not args.kafka_brokers:
            raise ValueError(
                "Kafka brokers are required when writing to a Kafka topic."
            )
            # Export the metrics to Kafka:
        write_df_to_kafka_topic(global_events, args.kafka_topic, args.kafka_brokers)


def process_global_metrics_stream(args: argparse.Namespace) -> None:
    logger = get_logger("process_global_metrics_stream")
    spark = get_spark_session(logger.name)
    raw_events = _get_raw_events_stream(spark, args)
    clean_events = get_clean_events(raw_events)
    # Global events
    transactions_events = global_processors.get_transactions_events(
        clean_events, args.time_window, args.slide_duration, args.watermark_duration
    )
    sold_items_events = global_processors.get_sales_volume_events(
        clean_events, args.time_window, args.slide_duration, args.watermark_duration
    )
    global_events = transactions_events[global_events_cols].union(
        sold_items_events[global_events_cols]
    )
    global_events_stream = (
        global_events.select(F.to_json(F.struct("*")).alias("value"))
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_brokers)
        .option("topic", args.kafka_topic)
        .option("checkpointLocation", args.checkpoint_dir)
    )
    # Start the structured streaming query:
    global_events_query = global_events_stream.start()
    logger.info(f"Started streaming query '{global_events_query.id}'")
    global_events_query.awaitTermination()


def process_collections_metrics_batch(args: argparse.Namespace) -> None:
    logger = get_logger("process_collections_metrics_batch")
    spark = get_spark_session(logger.name)
    raw_events = _get_raw_events_batch(spark, args)
    clean_events = get_clean_events(raw_events)
    collections_assets_events = (
        collections_processors.get_top_collections_assets_events(
            clean_events, args.collection_name, args.time_window
        )
    )
    collection_sales_events = collections_processors.get_collection_sales_events(
        clean_events, args.collection_name, args.time_window
    )
    collection_transactions_events = (
        collections_processors.get_collection_transactions_events(
            clean_events, args.collection_name, args.time_window
        )
    )
    # Concatenate the events:
    collections_events = (
        collections_assets_events[collections_events_cols]
        .union(collection_sales_events[collections_events_cols])
        .union(collection_transactions_events[collections_events_cols])
    )
    events_cnt = collections_events.count()
    logger.info(f"Processed {events_cnt} events.")
    # Export the metrics to Kafka:
    if args.kafka_topic:
        logger.info(f"Exporting the metrics to Kafka topic {args.kafka_topic}")
        if not args.kafka_brokers:
            raise ValueError(
                "Kafka brokers are required when writing to a Kafka topic."
            )
        # Export the metrics to Kafka:
        write_df_to_kafka_topic(
            collections_events, args.kafka_topic, args.kafka_brokers
        )


def process_collections_metrics_stream(args: argparse.Namespace) -> None:
    logger = get_logger("process_collections_metrics_stream")
    spark = get_spark_session(logger.name)
    raw_events = _get_raw_events_stream(spark, args)
    clean_events = get_clean_events(raw_events)
    collection_sales_events = collections_processors.get_collection_sales_events(
        clean_events,
        args.collection_name,
        args.time_window,
        args.slide_duration,
        args.watermark_duration,
    )
    collection_transactions_events = (
        collections_processors.get_collection_transactions_events(
            clean_events,
            args.collection_name,
            args.time_window,
            args.slide_duration,
            args.watermark_duration,
        )
    )
    # Concatenate the events:
    collections_events = collection_sales_events[collections_events_cols].union(
        collection_transactions_events[collections_events_cols]
    )
    collections_events_stream = (
        collections_events.select(F.to_json(F.struct("*")).alias("value"))
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_brokers)
        .option("topic", args.kafka_topic)
        .option("checkpointLocation", args.checkpoint_dir)
    )
    collections_events_query = collections_events_stream.start()
    # Start the structured streaming query:
    logger.info(f"Started streaming query '{collections_events_query.id}'")
    collections_events_query.awaitTermination()


def main() -> None:
    parser = get_argparser()
    args = parser.parse_args()
    logging.basicConfig()
    logger = get_logger()
    logger.setLevel(args.log_level)
    time_start = time.time()
    if args.type == "global":
        if args.time_window in batch_options:
            logger.info("Processing global metrics in batch mode.")
            process_global_metrics_batch(args)
        elif args.time_window in stream_options:
            logger.info("Processing global metrics in stream mode.")
            process_global_metrics_stream(args)
        else:
            raise ValueError("Invalid time window for global metrics.")
    elif args.type == "collections":
        if args.time_window in batch_options:
            logger.info("Processing collections metrics in batch mode.")
            process_collections_metrics_batch(args)
        elif args.time_window in stream_options:
            logger.info("Processing collections metrics in stream mode.")
            process_collections_metrics_stream(args)
        else:
            raise ValueError("Invalid time window for collections metrics.")
    else:
        raise ValueError("Invalid type of events to process.")
    time_end = time.time()
    logger.info("Done!")
    logger.info(f"Processing took {time_end - time_start:.2f} seconds.")
