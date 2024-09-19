import argparse
import logging
import time
from typing import TYPE_CHECKING, Optional

import pyspark.sql.functions as F
from py4j.protocol import Py4JError

import opensea_monitoring.processors.collections_events as collections_processors
import opensea_monitoring.processors.global_events as global_processors
from opensea_monitoring.processors.preprocessing import get_clean_events
from opensea_monitoring.utils.configs import settings
from opensea_monitoring.utils.kafka import (
    get_kafka_stream_writer,
    write_df_to_kafka_topic,
)
from opensea_monitoring.utils.schemas import get_opensea_raw_events_schema
from opensea_monitoring.utils.spark import get_spark_session
from opensea_monitoring.utils.timestamp import parse_timestamp

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery

GLOBAL_EVENTS_COLS = ["metric", "timestamp", "value", "collection"]
COLLECTIONS_EVENTS_COLS = [
    "metric",
    "timestamp",
    "collection",
    "value",
    "asset_name",
    "asset_url",
    "image_url",
]
BATCH_OPTIONS = {
    "all time",
    "14 days",
    "7 days",
    "1 day",
    "12 hours",
    "1 hour",
}
STREAM_OPTIONS = {
    "30 minutes",
    "5 minutes",
    "1 minute",
    "30 seconds",
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
        "events_type",
        type=str,
        help="The type of events to process.",
        choices=["global", "collections", "all"],
    )
    parser.add_argument(
        "time_window",
        type=lambda s: str(s.lower()),
        help="The time window in which to group the events.",
        choices=BATCH_OPTIONS.union(STREAM_OPTIONS),
    )
    parser.add_argument(
        "--slide-duration",
        type=str,
        help="The slide duration for the time window.",
    )
    parser.add_argument(
        "--kafka-topic",
        type=str,
        help="The Kafka topic to write the processed events to.",
    )
    parser.add_argument(
        "--global-kafka-topic",
        type=str,
        help="The Kafka topic for global metrics when processing all events.",
    )
    parser.add_argument(
        "--collections-kafka-topic",
        type=str,
        help="The Kafka topic for collections metrics when processing all events.",
    )
    parser.add_argument(
        "--kafka-brokers",
        type=str,
        help="The Kafka brokers to connect to.",
    )
    parser.add_argument(
        "--debug",
        dest="debug_mode",
        action="store_true",
        help=(
            "Enable debug mode. The behavior depends on if the application "
            "is running in batch or stream mode."
        ),
    )
    parser.add_argument(
        "--log-level",
        "-l",
        type=str,
        help="The log level for the application.",
        default=settings.log_level or "WARNING",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )
    # Batch arguments
    parser.add_argument(
        "--raw-events-s3-uri",
        type=str,
        help="The S3 URI where the raw events are stored.",
    )
    parser.add_argument(
        "--timestamp-start",
        type=parse_timestamp,
        help=(
            "The start timestamp for batch processing. "
            "Events sent before this timestamp will be ignored."
        ),
    )
    parser.add_argument(
        "--timestamp-end",
        type=parse_timestamp,
        help=(
            "The end timestamp for batch processing. "
            "Events sent after this timestamp will be ignored."
        ),
    )
    # Stream arguments
    parser.add_argument(
        "--raw-events-kafka-topic",
        type=str,
        help="The Kafka topic to read the raw events from.",
    )
    parser.add_argument(
        "--watermark-duration",
        type=str,
        help="The watermark duration for the time window. Only required for streaming.",
    )
    parser.add_argument(
        "--checkpoint-dir",
        type=str,
        help="The checkpoint directory for the streaming query.",
    )
    parser.add_argument(
        "--start-from-beginning",
        action="store_true",
        help="Start processing from the beginning of the Kafka topic (only for stream)",
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
    raw_events = spark.read.json(s3_path, schema=get_opensea_raw_events_schema())
    return raw_events


def _get_raw_events_stream(
    spark: "SparkSession", args: argparse.Namespace
) -> "DataFrame":

    if not (args.raw_events_kafka_topic and args.kafka_brokers and args.checkpoint_dir):
        raise ValueError(
            "The raw events Kafka topic, Kafka brokers, and checkpoint "
            "directory are required when processing data in stream"
        )
    logger = get_logger("get_raw_events_stream")
    logger.debug(f"Reading raw events from Kafka topic '{args.raw_events_kafka_topic}'")
    stream_reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_brokers)
        .option("subscribe", args.raw_events_kafka_topic)
    )
    if args.start_from_beginning:
        stream_reader = stream_reader.option("startingOffsets", "earliest")
    raw_events = stream_reader.load().select(
        F.get_json_object(F.col("value").cast("string"), "$.event").alias("event"),
        F.get_json_object(F.col("value").cast("string"), "$.payload").alias("payload"),
        F.get_json_object(F.col("value").cast("string"), "$.topic").alias("topic"),
    )
    return raw_events


def get_global_metrics(
    clean_events: "DataFrame", args: argparse.Namespace, logger: logging.Logger = None
) -> "DataFrame":
    logger = logger or get_logger()
    # Get global metrics:
    if args.time_window == "all time":
        all_time_metrics = global_processors.get_all_time_metrics(clean_events)
        all_time_top_collections = global_processors.get_all_time_top_collections(
            clean_events
        )
        global_events = all_time_metrics[GLOBAL_EVENTS_COLS].union(
            all_time_top_collections[GLOBAL_EVENTS_COLS]
        )
    else:
        global_metrics = global_processors.get_global_metrics(
            clean_events, args.time_window
        )
        top_collections_sales = global_processors.get_top_collections_by_sales_volume(
            clean_events, args.time_window
        )
        top_collections_transactions = (
            global_processors.get_top_collections_by_transactions_volume(
                clean_events, args.time_window
            )
        )
        # Concatenate the events:
        global_events = (
            global_metrics[GLOBAL_EVENTS_COLS]
            .union(top_collections_sales[GLOBAL_EVENTS_COLS])
            .union(top_collections_transactions[GLOBAL_EVENTS_COLS])
        )
    return global_events


def get_collections_metrics(
    clean_events: "DataFrame", args: argparse.Namespace, logger: logging.Logger = None
) -> "DataFrame":
    logger = logger or get_logger()
    if args.time_window == "all time":
        logger.info("Getting all time metrics.")
        collections_all_time_metrics = collections_processors.get_all_time_metrics(
            clean_events
        )
        collections_all_time_top_assets = (
            collections_processors.get_all_time_top_collections_assets(
                clean_events, n_per_collection=50
            )
        )
        collections_events = collections_all_time_metrics[
            COLLECTIONS_EVENTS_COLS
        ].union(collections_all_time_top_assets[COLLECTIONS_EVENTS_COLS])
    else:
        collections_assets_events = (
            collections_processors.get_top_collections_assets_by_sales(
                clean_events, args.time_window
            )
        )
        collections_assets_by_transfers_events = (
            collections_processors.get_collections_assets_transfers_stats(
                clean_events, args.time_window
            )
        )
        collection_sales_events = collections_processors.get_collection_sales_events(
            clean_events, args.time_window
        )
        collection_transactions_events = (
            collections_processors.get_collection_transactions_events(
                clean_events, args.time_window
            )
        )
        # Concatenate the events:
        collections_events = (
            collections_assets_events[COLLECTIONS_EVENTS_COLS]
            .union(collections_assets_by_transfers_events[COLLECTIONS_EVENTS_COLS])
            .union(collection_sales_events[COLLECTIONS_EVENTS_COLS])
            .union(collection_transactions_events[COLLECTIONS_EVENTS_COLS])
        )
    return collections_events


def process_global_metrics_batch(args: argparse.Namespace) -> None:
    logger = get_logger("process_global_metrics_batch")
    spark = get_spark_session(logger.name)
    raw_events = _get_raw_events_batch(spark, args)
    clean_events = get_clean_events(raw_events)
    if args.timestamp_start:
        clean_events = clean_events.filter(
            F.col("sent_at") >= F.lit(args.timestamp_start)
        )
    if args.timestamp_end:
        clean_events = clean_events.filter(
            F.col("sent_at") <= F.lit(args.timestamp_end)
        )
    global_events = get_global_metrics(clean_events, args)
    events_cnt = global_events.count()
    logger.info(f"Processed {events_cnt} events.")
    kafka_topic = args.kafka_topic or args.global_kafka_topic
    if kafka_topic:
        logger.info(f"Exporting the metrics to Kafka topic {kafka_topic}")
        if not args.kafka_brokers:
            raise ValueError(
                "Kafka brokers are required when writing to a Kafka topic."
            )
            # Export the metrics to Kafka:
        write_df_to_kafka_topic(global_events, kafka_topic, args.kafka_brokers)


def process_global_metrics_stream(
    args: argparse.Namespace, wait: bool = False
) -> "None | StreamingQuery":
    logger = get_logger("process_global_metrics_stream")
    spark = get_spark_session(logger.name)
    raw_events = _get_raw_events_stream(spark, args)
    clean_events = get_clean_events(raw_events, is_json_payload=True)
    global_events = global_processors.get_global_metrics(
        clean_events, args.time_window, args.slide_duration, args.watermark_duration
    )
    kafka_topic = args.kafka_topic or args.global_kafka_topic
    global_events_stream = get_kafka_stream_writer(
        global_events,
        kafka_topic,
        args.kafka_brokers,
        args.checkpoint_dir,
        debug=args.debug_mode,
        logger=logger,
        add_run_suffix_to_checkpoint=True,
    )
    # Start the structured streaming query:
    global_events_query = global_events_stream.start()
    logger.info(f"Started streaming query '{global_events_query.id}'")
    if wait:
        return global_events_query.awaitTermination()
    return global_events_query


def process_collections_metrics_batch(args: argparse.Namespace) -> None:
    logger = get_logger("process_collections_metrics_batch")
    spark = get_spark_session(logger.name)
    raw_events = _get_raw_events_batch(spark, args)
    clean_events = get_clean_events(raw_events)
    if args.timestamp_start:
        clean_events = clean_events.filter(
            F.col("sent_at") >= F.lit(args.timestamp_start)
        )
    if args.timestamp_end:
        clean_events = clean_events.filter(
            F.col("sent_at") <= F.lit(args.timestamp_end)
        )
    collections_events = get_collections_metrics(clean_events, args)
    events_cnt = collections_events.count()
    logger.info(f"Processed {events_cnt} events.")
    # Export the metrics to Kafka:
    kafka_topic = args.kafka_topic or args.collections_kafka_topic
    if kafka_topic:
        logger.info(f"Exporting the metrics to Kafka topic {kafka_topic}")
        if not args.kafka_brokers:
            raise ValueError(
                "Kafka brokers are required when writing to a Kafka topic."
            )
            # Export the metrics to Kafka:
        write_df_to_kafka_topic(collections_events, kafka_topic, args.kafka_brokers)


def process_all_metrics_batch(args: argparse.Namespace) -> None:
    """
    Process all metrics in batch mode.
    """
    logger = get_logger("process_all_metrics_batch")
    spark = get_spark_session(logger.name)
    raw_events = _get_raw_events_batch(spark, args)
    clean_events = get_clean_events(raw_events)
    if args.timestamp_start:
        clean_events = clean_events.filter(
            F.col("sent_at") >= F.lit(args.timestamp_start)
        )
    if args.timestamp_end:
        clean_events = clean_events.filter(
            F.col("sent_at") <= F.lit(args.timestamp_end)
        )
    clean_events.cache()
    global_events = get_global_metrics(clean_events, args)
    global_events_cnt = global_events.count()
    logger.info(f"Processed {global_events_cnt} global events.")
    collections_events = get_collections_metrics(clean_events, args)
    collections_events_cnt = collections_events.count()
    logger.info(f"Processed {collections_events_cnt} collections events.")
    # Export the metrics to Kafka:
    if args.global_kafka_topic and args.collections_kafka_topic:
        if not args.kafka_brokers:
            raise ValueError(
                "Kafka brokers are required when writing to a Kafka topic."
            )
        # Export the metrics to Kafka:
        write_df_to_kafka_topic(
            global_events, args.global_kafka_topic, args.kafka_brokers
        )
        write_df_to_kafka_topic(
            collections_events, args.collections_kafka_topic, args.kafka_brokers
        )


def process_collections_metrics_stream(
    args: argparse.Namespace,
    wait: bool = False,
) -> "None | StreamingQuery":

    logger = get_logger("process_collections_metrics_stream")
    spark = get_spark_session(logger.name)
    raw_events = _get_raw_events_stream(spark, args)
    clean_events = get_clean_events(raw_events, is_json_payload=True)
    collection_sales_events = collections_processors.get_collection_sales_events(
        clean_events,
        args.time_window,
        args.slide_duration,
        args.watermark_duration,
    )
    collection_transactions_events = (
        collections_processors.get_collection_transactions_events(
            clean_events,
            args.time_window,
            args.slide_duration,
            args.watermark_duration,
        )
    )
    collections_assets_events = (
        collections_processors.get_collections_assets_transfers_stats(
            clean_events,
            args.time_window,
            args.slide_duration,
            args.watermark_duration,
        )
    )
    # Concatenate the events:
    collections_events = (
        collection_sales_events[COLLECTIONS_EVENTS_COLS]
        .union(collection_transactions_events[COLLECTIONS_EVENTS_COLS])
        .union(collections_assets_events[COLLECTIONS_EVENTS_COLS])
    )
    kafka_topic = args.kafka_topic or args.collections_kafka_topic
    collections_events_stream = get_kafka_stream_writer(
        collections_events,
        kafka_topic,
        args.kafka_brokers,
        args.checkpoint_dir,
        debug=args.debug_mode,
        logger=logger,
        add_run_suffix_to_checkpoint=True,
    )
    collections_events_query = collections_events_stream.start()
    # Start the structured streaming query:
    logger.info(f"Started streaming query '{collections_events_query.id}'")
    if wait:
        return collections_events_query.awaitTermination()
    return collections_events_query


def main() -> None:
    parser = get_argparser()
    args = parser.parse_args()
    logging.basicConfig()
    logger = get_logger()
    logger.setLevel(args.log_level)
    time_start = time.time()
    if args.events_type == "global":
        if args.time_window in BATCH_OPTIONS:
            logger.info("Processing global metrics in batch mode.")
            process_global_metrics_batch(args)
        elif args.time_window in STREAM_OPTIONS:
            logger.info("Processing global metrics in stream mode.")
            try:
                process_global_metrics_stream(args, wait=True)
            except Py4JError:
                logger.exception(
                    "The stream was terminated, check the logs for more info."
                )
        else:
            raise ValueError("Invalid time window for global metrics.")
    elif args.events_type == "collections":
        if args.time_window in BATCH_OPTIONS:
            logger.info("Processing collections metrics in batch mode.")
            process_collections_metrics_batch(args)
        elif args.time_window in STREAM_OPTIONS:
            logger.info("Processing collections metrics in stream mode.")
            try:
                process_collections_metrics_stream(args, wait=True)
            except Py4JError:
                logger.exception(
                    "The stream was terminated, check the logs for more info."
                )
        else:
            raise ValueError("Invalid time window for collections metrics.")
    elif args.events_type == "all":
        if args.time_window in BATCH_OPTIONS:
            logger.info("Processing global and collections metrics in batch mode.")
            process_all_metrics_batch(args)
        elif args.time_window in STREAM_OPTIONS:
            logger.info("Processing global and collections metrics in stream mode.")
            global_query = process_global_metrics_stream(args)
            collections_query = process_collections_metrics_stream(args)
            global_query.awaitTermination()
            collections_query.awaitTermination()
        else:
            raise ValueError("Invalid time window for events.")
    else:
        raise ValueError("Invalid type of events to process.")
    time_end = time.time()
    logger.info("Done!")
    logger.info(f"Processing took {time_end - time_start:.2f} seconds.")
