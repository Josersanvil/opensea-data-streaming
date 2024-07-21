import json
import os
from typing import Any, Optional, TextIO

from opensea_client.dispatchers.base import MessageDispatcher


class DataFilesDispatcher(MessageDispatcher):
    """
    Distpatches the messages received to a file.
    """

    def __init__(self, data_file: Optional[str | TextIO] = None):
        self._data_file = data_file

    def process_message(self, message: dict[str, Any]):
        data_file = self._data_file or os.getenv("OPENSEA_CLIENT_DATA_FILE")
        if not data_file:
            raise ValueError(
                "OPENSEA_CLIENT_DATA_FILE environment variable is not set "
                "and no data file was provided."
            )
        message_value = json.dumps(message) + "\n"
        if isinstance(data_file, str):
            with open(data_file, "a") as f:
                f.write(message_value)
        else:
            data_file.write(message_value)
