import asyncio
import json
import logging
import os
import signal
import threading
from typing import Any, Optional, TextIO, Union

import dotenv
from websockets.client import WebSocketClientProtocol, connect

from opensea_client.processors.base import MessageProcessor
from opensea_client.processors.data_files import DataFilesProcessor


class OpenSeaClient:
    """
    A simple WebSocket client implementation to retrieve events from the OpenSea Web Stream.

    @param payload: The payload to send to the socket.
        See https://docs.opensea.io/reference/websockets for more information.
    @param data_file: The file to write the messages to. If None, messages will not be written to a file.
    """

    opensea_main_socket = "wss://stream.openseabeta.com/socket/websocket"
    opensea_test_socket = "wss://testnets-stream.openseabeta.com/socket/websocket"

    def __init__(
        self, payload: dict[str, Any], data_file: Optional[Union[TextIO, str]] = None
    ):
        self._logger = self.init_logger()
        self._message_processors = []
        self.payload = payload
        self.data_file: TextIO | None = data_file  # type: ignore
        if isinstance(data_file, str):
            self.data_file = open(data_file, "a")
        if self.data_file:
            self.add_message_processor(DataFilesProcessor(self.data_file))

    @property
    def logger(self) -> logging.Logger:
        return self._logger

    @classmethod
    def init_logger(cls):
        logger = cls.get_logger()
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        )
        logger.addHandler(handler)
        return logger

    @classmethod
    def get_logger(cls):
        return logging.getLogger(f"{__name__}.{cls.__name__}")

    @property
    def message_processors(self) -> list[MessageProcessor]:
        """
        A list of functions that will be called with the message as the only argument.
        The functions will be called in different threads.
        """
        return self._message_processors

    @message_processors.setter
    def message_processors(self, message_processors: list[MessageProcessor]):
        self._message_processors = message_processors

    def add_message_processor(self, processor: MessageProcessor):
        self._message_processors.append(processor)  # type: ignore

    async def heartbeat(self, socket: WebSocketClientProtocol):
        while True:
            self.logger.info("Sending heartbeat ...")
            payload = {
                "topic": "phoenix",
                "event": "heartbeat",
                "payload": {},
                "ref": 0,
            }
            await socket.send(json.dumps(payload))
            await asyncio.sleep(30)

    async def handle_terminate(self, sig, socket: WebSocketClientProtocol):
        self.logger.warning(
            f"Received {signal.Signals(sig).name}. Please wait while the connection is closed..."
        )
        await socket.close()
        if self.data_file:
            self.data_file.close()

    async def process_messages(self, socket: WebSocketClientProtocol):
        async for message in socket:
            response = json.loads(message)
            self.logger.info(response)
            # Process messages in different threads
            for processor in self.message_processors:
                threading.Thread(
                    target=processor.process_message, args=(response,)
                ).start()

    def write_message_to_data_file(self, message: dict[str, Any]):
        if self.data_file:
            self.data_file.write(json.dumps(message) + "\n")

    def get_endpoint(self):
        dotenv.load_dotenv()
        api_key = os.environ["OPENSEA_API_KEY"]
        environment = os.environ.get("OPENSEA_DATA_STREAM_ENV", "development")
        if environment.lower() == "production":
            return f"{self.opensea_main_socket}?token={api_key}"
        return f"{self.opensea_test_socket}?token={api_key}"

    async def run(self):
        """
        Opens a connection to the OpenSea Web Socket and retrieves messages from it.
        The connection is kept alive by sending a heartbeat every 30 seconds,
        it is closed when a SIGINT or SIGTERM signal is received.
        """
        socket_endpoint = self.get_endpoint()
        async with connect(socket_endpoint) as socket:
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(
                    sig,
                    lambda sig=sig: asyncio.create_task(
                        self.handle_terminate(sig, socket)
                    ),
                )

            await socket.send(json.dumps(self.payload))
            tasks = [self.heartbeat(socket), self.process_messages(socket)]
            await asyncio.gather(*tasks)
