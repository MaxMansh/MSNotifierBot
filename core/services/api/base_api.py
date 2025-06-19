import logging
import ssl
import asyncio
from abc import ABC, abstractmethod
import aiohttp
from typing import Dict, List, Any

logger = logging.getLogger(__name__)

class BaseAPI(ABC):
    def __init__(self, token: str, base_url: str):
        self.token = token
        self.base_url = base_url

    @abstractmethod
    async def fetch_all_products(self, session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
        pass

    async def _make_request(
            self,
            session: aiohttp.ClientSession,
            endpoint: str,
            params: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        headers = {"Authorization": f"Bearer {self.token}"}

        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        logger.debug(f"Запрос к {url} с параметрами: {params}")
        try:
            async with session.get(
                    url,
                    headers=headers,
                    params=params,
                    ssl=ssl_context,
                    timeout=aiohttp.ClientTimeout(total=30)  # 30 секунд таймаут
            ) as response:
                response.raise_for_status()
                return await response.json()
        except asyncio.TimeoutError:
            logger.error(f"Таймаут запроса к {url}")
            raise
        except Exception as e:
            logger.error(f"Ошибка API запроса: {str(e)}")
            raise