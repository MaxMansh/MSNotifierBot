from typing import Dict, List
import aiohttp
import asyncio
from core.services.api.base_api import BaseAPI
from utils.logger import AppLogger

logger = AppLogger().get_logger(__name__)

class MoyskladAPI(BaseAPI):
    BASE_URL = "https://api.moysklad.ru/api/remap/1.2"

    def __init__(self, token: str):
        super().__init__(token, self.BASE_URL)
        self.retry_count = 3
        self.retry_delay = 5
        logger.debug("Инициализирован клиент API МойСклад")

    async def fetch_all_product_folders(self, session: aiohttp.ClientSession) -> Dict[str, Dict]:
        all_folders = {}
        offset = 0
        logger.info("Начало загрузки групп товаров...")

        while True:
            params = {"limit": 500, "offset": offset}
            data = await self._make_request(session, "entity/productfolder", params)

            for folder in data.get('rows', []):
                folder_id = folder['id']
                parent_id = self._extract_parent_id(folder)
                all_folders[folder_id] = {
                    "name": folder.get('name', 'Без названия'),
                    "parent_id": parent_id
                }

            if len(data.get('rows', [])) < 500:
                break

            offset += len(data['rows'])
            await asyncio.sleep(1)

        logger.info(f"Загружено групп товаров: {len(all_folders)}")
        return all_folders

    async def fetch_all_products(self, session: aiohttp.ClientSession) -> List[Dict]:
        logger.info("Начало загрузки товаров...")

        for attempt in range(self.retry_count):
            try:
                folders = await self.fetch_all_product_folders(session)
                products = []
                offset = 0

                while True:
                    params = {"limit": 500, "offset": offset, "filter": "type=product"}
                    data = await self._make_request(session, "entity/assortment", params)

                    for product in data.get('rows', []):
                        product['group_path'] = self._get_product_group_path(product, folders)
                        products.append(product)

                    if len(data.get('rows', [])) < 500:
                        break

                    offset += len(data['rows'])
                    await asyncio.sleep(1)

                logger.info(f"Загружено товаров: {len(products)}")
                return products

            except Exception as e:
                if attempt == self.retry_count - 1:
                    logger.error(f"Не удалось загрузить товары после {self.retry_count} попыток")
                    raise
                logger.warning(f"Ошибка загрузки (попытка {attempt + 1}): {str(e)}")
                await asyncio.sleep(self.retry_delay * (attempt + 1))

    def _extract_parent_id(self, folder_data: Dict) -> str:
        parent_href = folder_data.get('productFolder', {}).get('meta', {}).get('href', '')
        return parent_href.split('/')[-1] if parent_href else None

    def _get_product_group_path(self, product: Dict, folders: Dict) -> str:
        folder_href = product.get('productFolder', {}).get('meta', {}).get('href', '')
        if not folder_href:
            return "Без группы"

        folder_id = folder_href.split('/')[-1]
        path = []
        current_id = folder_id

        while current_id in folders:
            folder = folders[current_id]
            path.append(folder['name'])
            current_id = folder['parent_id']

        return " > ".join(reversed(path)) if path else "Без группы"