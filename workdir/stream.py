import argparse

from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def get_argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--host",
    )
    parser.add_argument(
        "--port",
        "-p",
        type=int,
        default=9999,
    )
    parser.add_argument(
        "--log-level",
        "-l",
        choices=["ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN"],
        default="WARN",
    )
    return parser


if __name__ == "__main__":
    # Parse command line arguments
    parser = get_argparser()
    args = parser.parse_args()
    # Create SparkSession
    spark = SparkSession.builder.appName("Streaming").getOrCreate()
    spark.sparkContext.setLogLevel(args.log_level)
    # Create DataFrame representing the stream of input lines from the socket
    socket_df = (
        spark.readStream.format("socket")
        .option("host", args.host)
        .option("port", args.port)
        .load()
    )
    # Parse JSON data from the stream
    parsed_df = socket_df.selectExpr(
        "CAST(value AS STRING) as event_json",
        "get_json_object(value, '$.payload.payload.collection.slug') as collection_slug",
        "to_timestamp(get_json_object(value, '$.payload.sent_at')) as sent_at",
    )
    # Aggregate the data
    agg_df = parsed_df.groupBy(
        F.window(
            "sent_at", "30 seconds", "10 seconds"
        ),  # Every 10 seconds, aggregate the data from the last 30 seconds
        "collection_slug",
    ).count()
    # Write the query to the console
    query = (
        agg_df.writeStream.outputMode("complete")
        .format("console")
        .option("truncate", False)
    )
    # Start the query
    query.start().awaitTermination()
