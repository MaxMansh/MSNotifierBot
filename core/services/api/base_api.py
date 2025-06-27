import ssl
import asyncio
from abc import ABC, abstractmethod
import aiohttp
from typing import Dict, List, Any
from utils.logger import AppLogger

logger = AppLogger().get_logger(__name__)

class BaseAPI(ABC):
    def __init__(self, token: str, base_url: str):
        self.token = token
        self.base_url = base_url
        self._ssl_context = self._create_ssl_context()

    def _create_ssl_context(self):
        """Создает кастомный SSL контекст"""
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        return context

    async def _make_request(
        self,
        session: aiohttp.ClientSession,
        endpoint: str,
        params: Dict[str, Any] = None,
        method: str = "GET",
        json: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept-Encoding": "gzip"
        }

        try:
            async with session.request(
                method,
                url,
                headers=headers,
                params=params,
                json=json,
                ssl=self._ssl_context,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                response.raise_for_status()
                return await response.json()
        except Exception as e:
            logger.error(f"Ошибка {method} запроса к {url}: {str(e)}")
            raise

    @abstractmethod
    async def fetch_all_products(self, session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
        pass