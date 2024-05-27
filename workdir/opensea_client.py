import argparse
import asyncio
import json
import logging
import os
import signal
import threading
from datetime import datetime
from datetime import timezone as tz
from typing import Any, Callable, Optional, TextIO, Union

import dotenv
from websockets.client import WebSocketClientProtocol, connect

opensea_main_socket = "wss://stream.openseabeta.com/socket/websocket"
opensea_test_socket = "wss://testnets-stream.openseabeta.com/socket/websocket"


class OpenSeaClient:
    """
    A simple WebSocket client implementation to retrieve events from the OpenSea Web Stream.

    @param payload: The payload to send to the socket.
        See https://docs.opensea.io/reference/websockets for more information.
    @param data_file: The file to write the messages to. If None, messages will not be written to a file.
    """

    def __init__(
        self, payload: dict[str, Any], data_file: Optional[Union[TextIO, str]] = None
    ):
        self._logger = self.init_logger()
        self.payload = payload
        self.data_file: TextIO | None = data_file  # type: ignore
        if isinstance(data_file, str):
            self.data_file = open(data_file, "a")
        self._message_processors = [
            self.write_message_to_data_file,
        ]

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
        logger.setLevel(logging.INFO)  # Default log level
        return logger

    @classmethod
    def get_logger(cls):
        return logging.getLogger(f"{__name__}.{cls.__name__}")

    @property
    def message_processors(self) -> list[Callable[[dict[str, Any]], None]]:
        """
        A list of functions that will be called with the message as the only argument.
        The functions will be called in different threads.
        """
        return self._message_processors

    @message_processors.setter
    def message_processors(
        self, message_processors: list[Callable[[dict[str, Any]], None]]
    ):
        self._message_processors = message_processors

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
                threading.Thread(target=processor, args=(response,)).start()

    def write_message_to_data_file(self, message: dict[str, Any]):
        if self.data_file:
            self.data_file.write(json.dumps(message) + "\n")

    def get_endpoint(self):
        dotenv.load_dotenv()
        api_key = os.environ["OPENSEA_API_KEY"]
        environment = os.environ.get("OPENSEA_DATA_STREAM_ENV", "development")
        if environment.lower() == "production":
            return f"{opensea_main_socket}?token={api_key}"
        return f"{opensea_test_socket}?token={api_key}"

    async def run(self):
        """
        Opens a connection to the OpenSea Web Socket and retrieves messages from it
        to write them to the data file.
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


def get_argparser():
    parser = argparse.ArgumentParser()
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
        help="The file to write the messages to. Use '-' to write to stdout. If not specified, one will be created in the outdir (if specified).",
    )
    parser.add_argument(
        "--outdir",
        "-d",
        help="The directory to write the messages to.",
    )
    parser.add_argument(
        "--silent",
        "-s",
        action="store_true",
        help="If specified, no messages will be printed to stdout.",
    )
    return parser


if __name__ == "__main__":
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
    client = OpenSeaClient(payload, fb)
    client.logger.setLevel(args.log_level)
    if args.silent:
        client.logger.setLevel(logging.CRITICAL)
    asyncio.run(client.run())
