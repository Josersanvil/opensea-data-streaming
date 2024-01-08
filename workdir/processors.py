from typing import Any


def opensea_kafka_processor(message: dict[str, Any]):
    """
    Processor for the OpenSea Client to send messages to a Kafka
    topic.
    """
    