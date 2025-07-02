import aiohttp
import asyncio
from datetime import datetime
from core.api.base_api import BaseAPI
from core.infrastructure import AppLogger
from typing import Dict, List, Any, AsyncGenerator

logger = AppLogger().get_logger(__name__)

class MoyskladAPI(BaseAPI):
    BASE_URL = "https://api.moysklad.ru/api/remap/1.2"
    PARALLEL_REQUESTS = 5

    def __init__(self, token: str):
        super().__init__(token, self.BASE_URL)
        self.retry_count = 3
        self.retry_delay = 5
        logger.debug("Инициализирован клиент API МойСклад")

    async def check_connection(self, session: aiohttp.ClientSession) -> bool:
        """Проверяет соединение с API МойСклад"""
        try:
            # Используем небольшой запрос для проверки соединения
            await self._make_request(session, "entity/counterparty", {"limit": 1})
            return True
        except Exception as e:
            logger.error(f"Ошибка проверки соединения: {str(e)}")
            return False

    async def initialize_counterparties_cache(self, cache_manager) -> bool:
        """Оптимизированная загрузка кэша"""
        try:
            async with aiohttp.ClientSession() as session:
                total = await self._get_total_count(session)
                logger.info(f"Всего контрагентов для загрузки: {total}")

                # Параллельная загрузка и обработка
                batch_size = 5000
                batch = {}
                processed = 0

                async for name, data in self._stream_counterparties(session, total):
                    batch[name] = data['companyType']
                    processed += 1

                    if processed % 1000 == 0:
                        logger.info(f"Обработано {processed}/{total} контрагентов")

                    if len(batch) >= batch_size:
                        cache_manager.add_counterparties_batch(batch)
                        batch = {}

                if batch:
                    try:
                        cache_manager.add_counterparties_batch(batch)
                    except Exception as e:
                        logger.error(f"Ошибка добавления последнего пакета: {str(e)}")
                        return False

            logger.info(f"Загрузка кэша завершена. Всего обработано: {processed} контрагентов")
            return True
        except Exception as e:
            logger.error(f"Ошибка загрузки кэша: {str(e)}", exc_info=True)
            return False

    async def _stream_counterparties(self, session: aiohttp.ClientSession, total: int) -> AsyncGenerator:
        """Потоковая загрузка контрагентов"""
        limit = 1000
        offsets = range(0, total, limit)

        semaphore = asyncio.Semaphore(self.PARALLEL_REQUESTS)

        async def fetch_page(offset):
            async with semaphore:
                if offset % 10000 == 0:
                    logger.info(f"Загружено {offset}/{total} контрагентов")

                params = {"limit": limit, "offset": offset, "filter": "archived=false"}
                data = await self._make_request(session, "entity/counterparty", params)
                return {
                    item['name']: {'companyType': item.get('companyType', 'legal')}
                    for item in data.get('rows', []) if item.get('name')
                }

        tasks = [fetch_page(offset) for offset in offsets]

        for future in asyncio.as_completed(tasks):
            page_data = await future
            for name, data in page_data.items():
                yield name, data

    async def _get_total_count(self, session: aiohttp.ClientSession) -> int:
        """Получаем общее количество контрагентов"""
        data = await self._make_request(session, "entity/counterparty", {"limit": 1})
        return data['meta']['size']

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
            "legalAddress": "Не указан",
            "description": "Автоматически создано ботом\n"
                           f"Дата: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
                           "Тип: Физическое лицо"
        }

        try:
            response = await self._make_request(
                session,
                "entity/counterparty",
                method="POST",
                json=payload
            )

            # Проверяем, что ответ содержит ID созданного контрагента
            if response.get('id'):
                logger.info(f"Создан контрагент: {phone}")
                return True

            logger.warning(f"Неожиданный ответ API при создании контрагента: {response}")
            return False

        except aiohttp.ClientResponseError as e:
            if e.status == 409:  # Конфликт - контрагент уже существует
                logger.info(f"Контрагент {phone} уже существует (получено из API)")
                return True
            logger.error(f"Ошибка HTTP {e.status} при создании контрагента: {e.message}")
            return False
        except Exception as e:
            logger.error(f"Ошибка создания контрагента: {str(e)}")
            raise  # Пробрасываем исключение для обработки повторных попыток

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