from abc import ABC, abstractmethod
from typing import Any


class MessageProcessor(ABC):

    @abstractmethod
    def process_message(self, message: dict[str, Any]):
        """
        Process the message.
        """
        pass
