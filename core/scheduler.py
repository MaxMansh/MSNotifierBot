import asyncio
import aiohttp
from datetime import datetime, timedelta
from typing import List, Optional
from core.entities.product import Product
from core.services.api.base_api import BaseAPI
import ssl
from utils.logger import AppLogger

logger = AppLogger().get_logger(__name__)




class CheckerScheduler:
    def __init__(self, api: BaseAPI, checkers: List, check_interval: int, logger):
        self.api = api
        self.checkers = checkers
        self.interval = check_interval
        self.logger = logger
        self._running = False

    async def run(self) -> None:
        self._running = True
        logger.info(f"Запуск планировщика с интервалом {self.interval} минут")

        while self._running:
            start_time = datetime.now()
            logger.info("=== ИНИЦИАЛИЗАЦИЯ ===")

            try:
                # Добавляем таймаут для всего цикла проверок
                try:
                    async with asyncio.timeout(300):  # 5 минут на весь цикл
                        async with aiohttp.ClientSession() as session:
                            logger.debug("Загрузка данных о товарах из API...")
                            products = await self._fetch_products(session)

                            if products:
                                logger.debug(f"Получено товаров: {len(products)}")

                                # Последовательно выполняем проверки
                                logger.info("=== НАЧАЛО ПРОВЕРКИ ОСТАТКОВ ===")
                                await self.checkers[0].process(products)  # StockChecker
                                logger.info("=== ПРОВЕРКА ОСТАТКОВ ЗАВЕРШЕНА ===")

                                logger.info("=== НАЧАЛО ПРОВЕРКИ СРОКОВ ГОДНОСТИ ===")
                                await self.checkers[1].process(products)  # ExpirationChecker
                                logger.info("=== ПРОВЕРКА СРОКОВ ГОДНОСТИ ЗАВЕРШЕНА ===")

                            else:
                                logger.warning("Не получено ни одного товара для проверки")
                except TimeoutError:
                    logger.error("Таймаут цикла проверок (превышено 5 минут)")
                    continue

            except asyncio.CancelledError:
                logger.info("Получен сигнал остановки")
                break
            except Exception as e:
                logger.error(f"Ошибка в цикле проверок: {str(e)}", exc_info=True)

            duration = (datetime.now() - start_time).total_seconds()
            minutes, seconds = divmod(duration, 60)
            logger.info(f"Цикл проверок завершен за {int(minutes)} мин. {int(seconds)} сек.")

            if self._running:
                sleep_time = max(0, self.interval * 60 - duration)
                sleep_min, sleep_sec = divmod(sleep_time, 60)
                logger.info(f"Ожидание следующего цикла: {int(sleep_min)} мин. {int(sleep_sec)} сек...")
                await asyncio.sleep(sleep_time)

    async def stop(self):
        self._running = False
        logger.info("Планировщик остановлен")

    async def _fetch_products(self, session) -> List[Product]:
        try:
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            connector = aiohttp.TCPConnector(ssl=ssl_context)
            async with aiohttp.ClientSession(connector=connector) as session:
                logger.debug("Запрос данных о товарах...")
                raw_products = await self.api.fetch_all_products(session)

                products = []
                for p in raw_products:
                    try:
                        products.append(Product(
                            id=p['id'],
                            name=p.get('name', 'Без названия'),
                            stock=p.get('stock', 0),
                            min_balance=p.get('minimumBalance'),
                            group_path=p.get('group_path', 'Без группы'),
                            expiration_date=self._parse_expiration(p),
                            raw_data=p
                        ))
                    except Exception as e:
                        logger.error(f"Ошибка создания продукта: {str(e)}")

                logger.debug(f"Успешно создано объектов Product: {len(products)}")
                return products

        except Exception as e:
            logger.error(f"Ошибка загрузки товаров: {str(e)}")
            return []

    def _parse_expiration(self, product_data: dict) -> Optional[datetime]:
        for attr in product_data.get('attributes', []):
            if attr.get('name') == 'Срок годности' and attr.get('value'):
                from utils.date_parser import DateParser
                return DateParser.parse(attr['value'])
        return None