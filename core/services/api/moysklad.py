from typing import Dict, List, Any
import aiohttp
import asyncio
from core.services.api.base_api import BaseAPI
from utils import CacheManager
from utils.logger import AppLogger

logger = AppLogger().get_logger(__name__)

class MoyskladAPI(BaseAPI):
    BASE_URL = "https://api.moysklad.ru/api/remap/1.2"
    

    def __init__(self, token: str):
        super().__init__(token, self.BASE_URL)
        self.retry_count = 3
        self.retry_delay = 5
        logger.debug("Инициализирован клиент API МойСклад")

    async def initialize_counterparties_cache(self, cache_manager: CacheManager) -> bool:
        """Загружает контрагентов и сохраняет в кэш"""
        try:
            async with aiohttp.ClientSession() as session:
                counterparties = await self.load_counterparties(session)
                for phone, name in counterparties.items():
                    cache_manager.add_counterparty(phone, name)
                logger.info(f"Инициализирован кэш контрагентов: {len(counterparties)} записей")
                return True
        except Exception as e:
            logger.error(f"Ошибка инициализации кэша: {str(e)}")
            return False

    async def load_counterparties(self, session: aiohttp.ClientSession) -> dict:
        """Загружает всех контрагентов и возвращает словарь {name: companyType}"""
        counterparties = {}
        offset = 0

        logger.info("Начало загрузки списка контрагентов через API.")

        while True:
            params = {"limit": 500, "offset": offset}
            try:
                data = await self._make_request(session, "entity/counterparty", params)
                loaded_count = len(data.get('rows', []))
                logger.debug(f"Загружено контрагентов в текущей порции: {loaded_count}")

                for item in data.get('rows', []):
                    name = item.get('name', '')
                    if name:  # Используем name как ключ
                        counterparties[name] = {
                            'companyType': item.get('companyType', 'legal')
                        }

                if len(data.get('rows', [])) < 500:
                    logger.info(f"Загрузка контрагентов завершена. Всего загружено: {len(counterparties)}")
                    break
                offset += len(data['rows'])
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Ошибка загрузки контрагентов: {str(e)}")
                break

        return counterparties

    async def create_counterparty(self, session: aiohttp.ClientSession, phone: str) -> bool:
        """Создает контрагента как физическое лицо с тегом"""
        payload = {
            "name": phone,
            "phone": phone,
            "companyType": "individual",
            "tags": ["наличный рассчет"],
            "legalAddress": "Не указан"
        }

        try:
            await self._make_request(
                session,
                "entity/counterparty",
                method="POST",
                json=payload
            )
            logger.info(f"Создан контрагент: {phone}")
            return True
        except Exception as e:
            logger.error(f"Ошибка создания контрагента: {str(e)}")
            return False

    async def fetch_all_products(self, session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
        """Основной метод для получения товаров (оставлен без изменений)"""
        logger.info("Начало загрузки товаров...")
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
            logger.error(f"Ошибка загрузки товаров: {str(e)}")
            return []

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