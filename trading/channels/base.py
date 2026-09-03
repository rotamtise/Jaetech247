"""
trading/channels/base.py
Abstract base class for all trading channel implementations
"""
import asyncio
from abc import ABC, abstractmethod
from typing import Optional
import httpx


class BaseChannel(ABC):
    def __init__(self, channel_id: int, state):
        self.channel_id = channel_id
        self.state = state
        self.creds: dict = {}
        self._client: Optional[httpx.AsyncClient] = None

    def set_credentials(self, creds: dict):
        self.creds = creds

    def log(self, msg: str, level: str = "INFO"):
        self.state.add_log(msg, level)

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=10.0)
        return self._client

    async def on_start(self):
        """Called once before tick loop begins."""
        self._client = httpx.AsyncClient(timeout=10.0)
        self.log(f"[CH{self.channel_id:02d}] 초기화 완료")

    async def on_stop(self):
        """Called once after tick loop ends."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    @abstractmethod
    async def tick(self):
        """Called every 6 seconds (time-staggered). Core trading logic here."""
        ...
