from abc import ABC, abstractmethod
from typing import Any


class KernelWebsocketConnectionABC(ABC):
    """项目内部接口说明。"""

    websocket_handler: Any

    @abstractmethod
    async def connect(self):
        """项目内部接口说明。"""

    @abstractmethod
    async def disconnect(self):
        """项目内部接口说明。"""

    @abstractmethod
    def handle_incoming_message(self, incoming_msg: str) -> None:
        """项目内部接口说明。"""

    @abstractmethod
    def handle_outgoing_message(self, stream: str, outgoing_msg: list[Any]) -> None:
        """项目内部接口说明。"""
