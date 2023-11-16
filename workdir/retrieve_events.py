import asyncio
import argparse
import json
import logging
import os
import signal
from datetime import datetime
from typing import TextIO, Any

import dotenv
from websockets.client import connect, WebSocketClientProtocol

opensea_main_socket = "wss://stream.openseabeta.com/socket/websocket"
opensea_test_socket = "wss://testnets-stream.openseabeta.com/socket/websocket"


class WebSocketClient:
    """
    A simple WebSocket client implementation using websockets
    to retrieve events from OpenSea.

    @param data_file: The file to write the messages to. If None, messages will not be written to a file.
    """

    def __init__(self, payload: dict[str, Any], data_file: TextIO | str | None = None):
        self._logger = self.init_logger()
        self.payload = payload
        self.data_file: TextIO | None = data_file  # type: ignore
        if isinstance(data_file, str):
            self.data_file = open(data_file, "a")

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
            f"Received {signal.Signals(sig).name}. Closing connection..."
        )
        if self.data_file:
            self.data_file.close()
        await socket.close()

    async def retrieve_messages(self, socket: WebSocketClientProtocol):
        async for message in socket:
            response = json.loads(message)
            self.logger.info(response)
            if self.data_file:
                self.data_file.write(json.dumps(response) + "\n")

    def get_endpoint(self):
        dotenv.load_dotenv()
        api_key = os.environ["OPENSEA_API_KEY"]
        environment = os.environ.get("OPENSEA_DATA_STREAM_ENV", "development")
        if environment.lower() == "production":
            return f"{opensea_main_socket}?token={api_key}"
        return f"{opensea_test_socket}?token={api_key}"

    async def run(self):
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
            tasks = [self.heartbeat(socket), self.retrieve_messages(socket)]
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
        ts = round(datetime.utcnow().timestamp())
        filename = f"{ts}_messages.jsonl"
        fb = open(os.path.join(args.outdir, filename), "a")
    client = WebSocketClient(payload, fb)
    client.logger.setLevel(args.log_level)
    if args.silent:
        client.logger.setLevel(logging.CRITICAL)
    asyncio.run(client.run())
