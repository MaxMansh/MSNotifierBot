from abc import ABC, abstractmethod
from typing import List
from core.domain.entities import Product
import aiohttp
import ssl

class BaseChecker(ABC):
    def __init__(self, notifier, cache_manager):
        self.notifier = notifier
        self.cache_manager = cache_manager
        self.ssl_context = ssl.create_default_context()
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE

    @abstractmethod
    async def process(self, products: List[Product]) -> None:
        pass

    async def create_session(self) -> aiohttp.ClientSession:
        return aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=self.ssl_context))