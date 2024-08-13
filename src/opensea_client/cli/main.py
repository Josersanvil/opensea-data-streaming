import argparse
import asyncio
import logging
import os
from datetime import datetime
from datetime import timezone as tz

from opensea_client.client import OpenSeaClient
from opensea_client.dispatchers.kafka import KafkaTopicDispatcher


def get_argparser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Uses the OpenSeaClient to connect to the OpenSea WebSocket stream "
            "and write messages to a file, stdout, or a Kafka topic."
        )
    )
    parser.add_argument(
        "--log-level",
        "-l",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
    )
    parser.add_argument(
        "--output-file",
        "-o",
        type=argparse.FileType("a"),
        default=None,
        help=(
            "The file to write the messages to. Use '-' to write to stdout. "
            "If not specified, one will be created in the outdir (if specified).",
        ),
    )
    parser.add_argument(
        "--kafka-topic",
        help="The Kafka topic to write the messages to.",
        metavar="topic_name",
    )
    parser.add_argument(
        "--kafka-brokers",
        help=(
            "The Kafka brokers to connect to. "
            "Multiple brokers should be separated by commas."
        ),
        metavar="host1:port1,host2:port2,...",
    )

    parser.add_argument(
        "--kafka-client-id",
        help="The Kafka client ID to use.",
        default="opensea-client",
    )
    parser.add_argument(
        "--outdir",
        "-d",
        help="The directory to write the messages to.",
    )
    return parser


def main():
    parser = get_argparser()
    args = parser.parse_args()
    collection = "*"
    payload = {
        "topic": f"collection:{collection}",
        "event": "phx_join",
        "payload": {
            "event_type": "item_transferred",
        },
        "ref": 0,
    }
    fb = None
    if args.output_file:
        fb = args.output_file
    elif args.outdir:
        if not os.path.exists(args.outdir):
            os.makedirs(args.outdir)
        ts = round(datetime.now(tz.utc).timestamp())
        filename = f"{ts}_messages.jsonl"
        fb = open(os.path.join(args.outdir, filename), "a")
    client = OpenSeaClient(payload, data_file=fb)
    logging.getLogger("opensea_client").setLevel(args.log_level)
    if args.kafka_topic:
        client.add_message_dispatcher(
            KafkaTopicDispatcher(
                topic_name=args.kafka_topic,
                client_id=args.kafka_client_id,
                bootstrap_servers=args.kafka_brokers,
            )
        )
    asyncio.run(client.run())


if __name__ == "__main__":
    main()
