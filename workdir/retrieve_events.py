import asyncio
import argparse
import json
import logging
import os
import signal
from datetime import datetime

import dotenv
from websockets.client import connect, WebSocketClientProtocol

opensea_main_socket = "wss://stream.openseabeta.com/socket/websocket"
opensea_test_socket = "wss://testnets-stream.openseabeta.com/socket/websocket"

collection = "*"
# Open socket connection to OpenSea
payload = {
    "topic": f"collection:{collection}",
    "event": "phx_join",
    "payload": {},
    "ref": 0,
}

ts = round(datetime.utcnow().timestamp())
fb = open(f"messages/{ts}_messages.jsonl", "a")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


async def heartbeat(socket: WebSocketClientProtocol):
    """
    Send heartbeat to the socket every 30s
    to keep the connection alive
    """
    while True:
        logging.info("Sending heartbeat ...")
        payload = {"topic": "phoenix", "event": "heartbeat", "payload": {}, "ref": 0}
        await socket.send(json.dumps(payload))
        await asyncio.sleep(30)


async def handle_terminate(sig, socket):
    logging.warning(f"Received {signal.Signals(sig).name}. Closing connection...")
    # Close the file gracefully
    fb.close()
    # Close the socket gracefully
    await socket.close()


async def retrieve_messages(socket: WebSocketClientProtocol):
    """
    Retrieve and process messages from the WebSocket
    """
    async for message in socket:
        response = json.loads(message)
        logging.info(response)
        fb.write(json.dumps(response) + "\n")


def get_endpoint() -> str:
    """
    Get the endpoint based on the environment
    """
    dotenv.load_dotenv()

    api_key = os.environ["OPENSEA_API_KEY"]
    environment = os.environ.get("ENVIRONMENT", "development")
    if environment.lower() == "production":
        return f"{opensea_main_socket}?token={api_key}"
    return f"{opensea_test_socket}?token={api_key}"


async def main():
    socket_endpoint = get_endpoint()
    async with connect(socket_endpoint) as socket:
        # Close the connection when receiving SIGTERM or SIGINT
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                sig, lambda sig=sig: asyncio.create_task(handle_terminate(sig, socket))
            )
        # Subscribe to the collection
        await socket.send(json.dumps(payload))
        # Start the tasks
        tasks = [heartbeat(socket), retrieve_messages(socket)]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--log-level",
        "-l",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
    )
    args = parser.parse_args()
    logging.getLogger().setLevel(args.log_level)
    asyncio.run(main())
