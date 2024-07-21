import json
import logging
import os
from typing import Any, Optional

from confluent_kafka import Producer

from opensea_client.dispatchers.base import MessageDispatcher


class KafkaTopicDispatcher(MessageDispatcher):
    """
    Message dispatcher that sends messages to a Kafka topic.

    @param topic_name:
        The Kafka topic to send messages to.
    @param client_id:
        The Kafka client ID to use.
    @param bootstrap_servers:
        The Kafka brokers to connect to. Can be a comma-separated list of brokers.

    The following environment variables can be used to configure the processor:
    - OPENSEA_CLIENT_KAFKA_TOPIC
    - OPENSEA_CLIENT_KAFKA_CLIENT_ID
    - OPENSEA_CLIENT_KAFKA_BOOTSTRAP_SERVERS
    """

    def __init__(
        self,
        topic_name: Optional[str] = None,
        client_id: Optional[str] = None,
        bootstrap_servers: Optional[str] = None,
    ):
        self._topic_name = topic_name
        self._client_id = client_id
        self._bootstrap_servers = bootstrap_servers
        self._producer = None

    @property
    def logger(self) -> logging.Logger:
        return logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    def topic_name(self) -> str:
        value = os.environ.get("OPENSEA_CLIENT_KAFKA_TOPIC", self._topic_name)
        if not value:
            raise ValueError(
                "topic_name is not set. Please provide a value for the topic_name parameter "
                "or set the OPENSEA_CLIENT_KAFKA_TOPIC environment variable."
            )
        return value

    @property
    def client_id(self) -> str:
        client_id = os.getenv("OPENSEA_CLIENT_KAFKA_CLIENT_ID", self._client_id)
        if not client_id:
            return "opensea-client"
        return client_id

    @property
    def bootstrap_servers(self) -> str:
        bootstrap_servers = os.getenv(
            "OPENSEA_CLIENT_KAFKA_BOOTSTRAP_SERVERS", self._bootstrap_servers
        )
        if not bootstrap_servers:
            return "localhost:9092"
        return bootstrap_servers

    @property
    def producer(self) -> Producer:
        if self._producer is None:
            self._producer = self._make_producer()
        return self._producer

    def _make_producer(self) -> Producer:
        conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
        }
        self.logger.debug(f"Creating Kafka producer with config: '{conf}'")
        producer = Producer(conf, logger=self.logger)
        return producer

    def process_message(self, message: dict[str, Any]):
        """
        Produce the given message to the Kafka topic.
        """
        msg_value = json.dumps(message).encode("utf-8")
        self.producer.produce(self.topic_name, value=msg_value)
