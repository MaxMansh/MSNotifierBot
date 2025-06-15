import os
import json
import asyncio
import logging
import ssl
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional

import aiohttp
from aiogram import Bot, html
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()

class Config:
    """Конфигурация приложения"""
    def __init__(self):
        self.BOT_TOKEN = os.getenv("BOT_TOKEN")
        self.MS_TOKEN = os.getenv("MS_TOKEN")
        self.CHAT_ID = os.getenv("CHAT_ID")
        self.CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL_MINUTES", 120))
        self.LIMIT = 500
        self.DELAY = 5
        self.LOG_RESET_INTERVAL = timedelta(days=30)  # Интервал для очистки логов
        self.CACHE_RESET_INTERVAL = timedelta(days=30)  # Интервал для очистки кэша
        self.TG_MESSAGE_LIMIT = 4096
        self.LOGS_DIR = Path("logs")
        self.CACHE_DIR = Path("cache")
        self.STOCKS_CACHE_FILE = self.CACHE_DIR / "MSCache.json"
        self.EXPIRATION_CACHE_FILE = self.CACHE_DIR / "Expiration_cache.json"
config = Config()


class LoggerManager:
    """Настройка логгера"""

    @staticmethod
    def setup():
        """Инициализация логгера при старте приложения"""
        LoggerManager._setup_new_logger()
        return logging.getLogger()

    @staticmethod
    def _setup_new_logger():
        """Создание нового логгера с новым файлом"""
        Path(config.LOGS_DIR).mkdir(exist_ok=True)
        LoggerManager._current_log_file = config.LOGS_DIR / f"MainLog - {datetime.now().strftime('%d.%m.%Y')}.log"

        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        # Удаляем все старые файловые обработчики
        for handler in logger.handlers[:]:
            if isinstance(handler, logging.FileHandler):
                logger.removeHandler(handler)
                handler.close()

        # Добавляем новый файловый обработчик
        file_handler = logging.FileHandler(LoggerManager._current_log_file, encoding="utf-8")
        file_handler.setFormatter(logging.Formatter(
            "[{asctime}] #{levelname:8} {filename}:{lineno} - {message}\n"
            "────────────────────────────────────────────",
            style="{"
        ))
        logger.addHandler(file_handler)

        logger.info("=" * 60)
        logger.info("ИНИЦИАЛИЗАЦИЯ СИСТЕМЫ")
        logger.info("=" * 60)

    @staticmethod
    def cleanup_old_logs():
        """Очистка старых лог-файлов"""
        current_log_file = datetime.now().strftime('%d.%m.%Y')
        logger.info("Начало очистки старых лог-файлов...")

        files_to_delete = []  # Сначала собираем файлы для удаления

        # Сначала собираем все файлы, которые нужно удалить
        for log_file in Path(config.LOGS_DIR).glob("MainLog - *.log"):
            try:
                log_date_str = log_file.stem.split(" - ")[1]
                log_date = datetime.strptime(log_date_str, "%d.%m.%Y")
                logger.info(f"Проверка лог-файла: {log_file}, дата: {log_date_str}")

                if datetime.now() - log_date > config.LOG_RESET_INTERVAL and log_date_str != current_log_file:
                    files_to_delete.append(log_file)
            except Exception as e:
                logger.error(f"Ошибка при обработке лог-файла {log_file}: {e}")

        # Затем удаляем все файлы из списка
        for log_file in files_to_delete:
            try:
                log_file.unlink()
                logger.info(f"Удален старый лог-файл: {log_file}")
            except Exception as e:
                logger.error(f"Ошибка при удалении лог-файла {log_file}: {e}")

        logger.info("Очистка старых лог-файлов завершена.")

    @staticmethod
    def check_and_switch_logger():
        """Проверка и переключение на новый лог-файл"""
        current_date = datetime.now().strftime('%d.%m.%Y')
        if not hasattr(LoggerManager, '_current_log_file') or current_date not in str(LoggerManager._current_log_file):
            LoggerManager._setup_new_logger()
            LoggerManager.cleanup_old_logs()



logger = LoggerManager.setup()

class CacheManager:
    """Управление кэшем"""

    @staticmethod
    def init():
        config.CACHE_DIR.mkdir(exist_ok=True)

    @staticmethod
    def reset_if_needed():
        """Очистка кэша, если файлы старше заданного интервала"""
        logger.info("Проверка необходимости сброса кэша...")
        for cache_file in [config.STOCKS_CACHE_FILE, config.EXPIRATION_CACHE_FILE]:
            if cache_file.exists():
                cache_date = datetime.fromtimestamp(cache_file.stat().st_mtime)
                if datetime.now() - cache_date > config.CACHE_RESET_INTERVAL:
                    logger.info(f"Удаляем кэш-файл: {cache_file}")
                    cache_file.unlink()
        logger.info("Проверка кэша завершена.")

    @staticmethod
    def load(cache_file: Path) -> Dict:
        try:
            return json.loads(cache_file.read_text()) if cache_file.exists() else {}
        except Exception as e:
            logger.error(f"Ошибка загрузки кэша: {e}")
            return {}

    @staticmethod
    def save(cache_file: Path, data: Dict):
        try:
            cache_file.write_text(json.dumps(data, indent=2))
        except Exception as e:
            logger.error(f"Ошибка сохранения кэша: {e}")


class MoyskladAPI:
    """Работа с API МойСклад"""

    @staticmethod
    async def fetch_all_product_folders(session: aiohttp.ClientSession) -> Dict[str, Dict]:
        """Загрузка всех групп товаров с учетом вложенности.
        Возвращает словарь, где ключ — ID группы, а значение — словарь с данными о группе.
        """
        url = "https://api.moysklad.ru/api/remap/1.2/entity/productfolder"
        headers = {"Authorization": f"Bearer {config.MS_TOKEN}"}
        all_folders = {}  # {folder_id: {"name": "...", "parent_id": "..."}}
        offset = 0

        while True:
            params = {
                "limit": config.LIMIT,
                "offset": offset,
            }

            try:
                logger.debug(f"Загрузка групп товаров (offset: {offset}, limit: {config.LIMIT})")
                async with session.get(url, headers=headers, params=params) as response:
                    response.raise_for_status()
                    data = await response.json()
                    folders = data.get('rows', [])

                    for folder in folders:
                        folder_id = folder.get('id')
                        folder_name = folder.get('name', 'Без названия')
                        parent_href = folder.get('productFolder', {}).get('meta', {}).get('href', '')
                        parent_id = parent_href.split('/')[-1] if parent_href else None

                        all_folders[folder_id] = {
                            "name": folder_name,
                            "parent_id": parent_id
                        }

                    logger.debug(f"Загружено групп: {len(folders)} (всего: {len(all_folders)})")

                    # Если загружено меньше, чем лимит, значит, это последняя страница
                    if len(folders) < config.LIMIT:
                        break

                    offset += len(folders)
                    await asyncio.sleep(config.DELAY)

            except Exception as e:
                logger.error(f"Ошибка при загрузке групп товаров: {str(e)[:200]}")
                await asyncio.sleep(config.DELAY * 2)
                continue

        logger.info(f"Всего загружено групп товаров: {len(all_folders)}")
        logger.debug(f"Пример данных о группах: {list(all_folders.items())[:5]}")
        return all_folders

    @staticmethod
    def get_full_group_path(group_id: str, all_folders: Dict[str, Dict]) -> str:
        """Возвращает полный путь группы (например, 'Группа 1 > Подгруппа 1 > Подгруппа 2')."""
        path = []
        current_id = group_id

        while current_id:
            folder = all_folders.get(current_id)
            if not folder:
                break
            path.append(folder["name"])
            current_id = folder["parent_id"]

        return " > ".join(reversed(path)) if path else "Без группы"

    @staticmethod
    async def fetch_all_products(session: aiohttp.ClientSession, all_folders: Dict[str, Dict]) -> List[Dict]:
        """Загрузка всех товаров с добавлением полного пути группы."""
        url = "https://api.moysklad.ru/api/remap/1.2/entity/assortment"
        headers = {"Authorization": f"Bearer {config.MS_TOKEN}"}
        all_products = []
        offset = 0
        total_products = None

        while True:
            params = {
                "limit": config.LIMIT,
                "offset": offset,
                "filter": "type=product"
            }

            try:
                logger.debug(f"Загрузка товаров (offset: {offset}, limit: {config.LIMIT})")
                async with session.get(url, headers=headers, params=params) as response:
                    response.raise_for_status()
                    data = await response.json()
                    products = data.get('rows', [])

                    for product in products:
                        # Используем productFolder для определения группы
                        folder_href = product.get('productFolder', {}).get('meta', {}).get('href', '')
                        if folder_href:
                            folder_id = folder_href.split('/')[-1]
                            full_path = MoyskladAPI.get_full_group_path(folder_id, all_folders)
                            product['group_path'] = full_path
                        else:
                            product['group_path'] = "Без группы"
                            logger.warning(f"Товар {product.get('name')} не имеет группы")

                        # Логируем структуру товара для диагностики
                        logger.debug(f"Товар: {product.get('name')}, группа: {product['group_path']}")

                        all_products.append(product)

                    # Получаем общее количество товаров при первом запросе
                    if total_products is None:
                        total_products = data.get('meta', {}).get('size', 0)
                        logger.info(f"Всего товаров для загрузки: {total_products}")

                    logger.debug(f"Загружено {len(products)} товаров (всего загружено: {len(all_products)})")

                    # Если загружено меньше, чем лимит, значит, это последняя страница
                    if len(products) < config.LIMIT:
                        break

                    offset += len(products)
                    await asyncio.sleep(config.DELAY)

            except Exception as e:
                logger.error(f"Ошибка при загрузке товаров: {str(e)[:200]}")
                await asyncio.sleep(config.DELAY * 2)
                continue

        return all_products

    @staticmethod
    async def fetch_products(session: aiohttp.ClientSession, offset: int = 0) -> Dict:
        """Получение одной страницы товаров (для обратной совместимости)"""
        url = "https://api.moysklad.ru/api/remap/1.2/entity/assortment"
        headers = {"Authorization": f"Bearer {config.MS_TOKEN}"}
        params = {
            "limit": config.LIMIT,
            "offset": offset,
            "filter": "type=product"
        }

        try:
            async with session.get(url, headers=headers, params=params) as response:
                response.raise_for_status()
                return await response.json()
        except Exception as e:
            logger.error(f"Ошибка API: {str(e)[:200]}")
            raise


class TelegramNotifier:
    """Отправка уведомлений"""

    def __init__(self, bot: Bot):
        self.bot = bot

    async def send(self, header: str, alerts: List[str]):
        if not alerts:
            logger.debug("Нет уведомлений для отправки")
            return

        # Разбиваем уведомления на пакеты по 10 сообщений
        BATCH_SIZE = 10
        for batch_idx in range(0, len(alerts), BATCH_SIZE):
            batch = alerts[batch_idx:batch_idx + BATCH_SIZE]

            # Собираем полное сообщение для текущего пакета
            full_message = f"{header}\n\n" + "\n\n".join(batch)

            # Если сообщение в пределах лимита - отправляем как есть
            if len(full_message) <= config.TG_MESSAGE_LIMIT:
                await self._send_safe(full_message)
            else:
                # Если превышен лимит - разбиваем на части
                logger.warning(f"Сообщение слишком длинное ({len(full_message)} символов), разбиваю на части")
                message_parts = self._split_message(full_message)

                # Отправляем каждую часть с небольшой задержкой
                for i, part in enumerate(message_parts, 1):
                    logger.debug(f"Отправка части {i}/{len(message_parts)} (пакет {batch_idx // BATCH_SIZE + 1})")
                    await self._send_safe(part)
                    if i < len(message_parts):  # Не ждем после последней части
                        await asyncio.sleep(1)  # Небольшая задержка между частями

            # Задержка между пакетами сообщений (кроме последнего пакета)
            if batch_idx + BATCH_SIZE < len(alerts):
                await asyncio.sleep(2)  # Увеличиваем задержку между пакетами

    async def _send_safe(self, message: str):
        """Безопасная отправка сообщения с обработкой ошибок"""
        try:
            await self.bot.send_message(
                chat_id=config.CHAT_ID,
                text=message,
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.error(f"Ошибка отправки сообщения: {str(e)[:200]}")

    def _split_message(self, message: str) -> List[str]:
        """Разбивает сообщение на части по границам уведомлений"""
        parts = []
        current_part = ""

        # Разделяем по двойным переносам строк (между уведомлениями)
        notifications = message.split('\n\n')

        for notification in notifications:
            # Если текущая часть плюс новое уведомление превышают лимит
            if len(current_part) + len(notification) + 2 > config.TG_MESSAGE_LIMIT:
                if current_part:  # Если текущая часть не пустая
                    parts.append(current_part.strip())
                    current_part = ""

                # Если отдельное уведомление слишком длинное
                if len(notification) > config.TG_MESSAGE_LIMIT:
                    logger.warning("Обнаружено очень длинное уведомление, разбиваю по строкам")
                    # Разбиваем по строкам
                    lines = notification.split('\n')
                    temp_part = ""
                    for line in lines:
                        if len(temp_part) + len(line) + 1 > config.TG_MESSAGE_LIMIT:
                            if temp_part:
                                parts.append(temp_part.strip())
                                temp_part = ""
                        temp_part += line + '\n'
                    if temp_part:
                        parts.append(temp_part.strip())
                else:
                    current_part = notification + '\n\n'
            else:
                current_part += notification + '\n\n'

        if current_part.strip():
            parts.append(current_part.strip())

        logger.info(f"Сообщение разбито на {len(parts)} частей")
        return parts


class ProductChecker:
    """Базовый класс проверок"""

    def __init__(self, notifier: TelegramNotifier):
        self.notifier = notifier
        self.ssl_context = ssl.create_default_context()
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE

    async def create_session(self) -> aiohttp.ClientSession:
        return aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=self.ssl_context))


class StockChecker(ProductChecker):
    async def process(self, products: List[Dict]):
        logger.info("=== НАЧАЛО ПРОВЕРКИ ОСТАТКОВ ТОВАРОВ ===")
        logger.info(f"Получено товаров для проверки: {len(products)}")
        cache = CacheManager.load(config.STOCKS_CACHE_FILE)
        alerts_by_group = {}  # Группируем уведомления по группам
        processed_count = 0
        alerted_count = 0

        for product in products:
            if not self._is_valid_product(product):
                logger.debug(f"Товар {product.get('name')} невалиден, пропускаем")
                continue

            product_id = product['id']
            name = product.get('name', 'Без названия')
            stock = product.get('stock', 0)
            min_balance = product.get('minimumBalance')
            group_path = product.get('group_path', 'Без группы')
            processed_count += 1

            if min_balance is None:
                if product_id in cache:
                    logger.debug(f"У товара {name} нет минимального баланса, удаляем из кэша")
                    del cache[product_id]
                continue

            if isinstance(min_balance, (int, float)) and min_balance > 0:
                cached = cache.get(product_id, {})
                was_below = cached.get('was_below_min', False)
                now_below = stock <= min_balance

                # Определяем нужно ли уведомление
                should_alert = False

                # Если впервые упал ниже минимума
                if not was_below and now_below:
                    should_alert = True
                # Или если остался ниже минимума, но количество изменилось
                elif was_below and now_below and stock != cached.get('last_stock'):
                    should_alert = True

                if should_alert:
                    logger.info(f"Обнаружено изменение остатка: {name} ({stock}/{min_balance})")
                    if group_path not in alerts_by_group:
                        alerts_by_group[group_path] = []
                    alerts_by_group[group_path].append(self._create_alert(name, stock, min_balance))
                    alerted_count += 1

                # Обновляем кэш в любом случае
                cache[product_id] = {
                    'last_stock': stock,
                    'was_below_min': now_below,
                    'last_updated': datetime.now().isoformat()
                }

        logger.info(f"Проверено товаров: {processed_count}, уведомлений: {alerted_count}")
        if alerts_by_group:
            logger.info("Отправка уведомлений о остатках...")
            for group_path, alerts in alerts_by_group.items():
                await self.notifier.send(
                    f"📊 {html.bold('УВЕДОМЛЕНИЯ ПО ОСТАТКАМ')} ({group_path})",
                    alerts
                )
        else:
            logger.info("Нет товаров с изменением остатков для уведомления")
        logger.info("Сохранение кэша остатков...")
        CacheManager.save(config.STOCKS_CACHE_FILE, cache)
        logger.info("=== ПРОВЕРКА ОСТАТКОВ ЗАВЕРШЕНА ===")
        logger.info(
            f"Итоги проверки:\n"
            f"  Обработано товаров: {processed_count}\n"
            f"  Всего уведомлений: {len(alerts_by_group)}"
        )


    def _is_valid_product(self, product: Dict) -> bool:
        return (product.get('meta', {}).get('type') == 'product' and
                isinstance(product.get('name'), str) and
                isinstance(product.get('stock'), (int, float)))

    def _should_alert(self, stock: float, min_balance: float, cached: Dict) -> bool:
        last_stock = cached.get('last_stock')
        was_below = cached.get('was_below_min', False)
        now_below = stock <= min_balance

        # Уведомляем только если:
        # 1. Впервые упал ниже минимума
        # 2. Остался ниже минимума, но количество изменилось
        return (not was_below and now_below) or (was_below and now_below and stock != last_stock)

    def _create_alert(self, name: str, stock: float, min_balance: float) -> str:
        return (
            f"⚠️ {html.bold(f'Товар: {name} достиг минимума!')}\n"
            f"▸ Остаток: {int(stock)} (минимум: {int(min_balance)})\n"
            f"▸ {datetime.now().strftime('%d.%m.%Y %H:%M')}"
        )


class ExpirationChecker(ProductChecker):
    """Проверка сроков годности"""

    async def process(self, products: List[Dict]):
        logger.info("=== НАЧАЛО ПРОВЕРКИ СРОКОВ ГОДНОСТИ ===")
        logger.info(f"Получено товаров для проверки: {len(products)}")
        cache = CacheManager.load(config.EXPIRATION_CACHE_FILE)
        alerts_by_group = {}
        expired_count = 0
        near_expired_count = 0
        processed_count = 0

        for product in products:
            if not self._is_valid_product(product):
                logger.debug(f"Товар:{product.get('name')} не валидный, пропускаем")
                continue

            product_id = product['id']
            name = product.get('name', 'Без названия')
            group_path = product.get('group_path', 'Без группы')
            logger.debug(f"Обработка товара: {name}")
            expiration_data = self._get_expiration_data(product)

            if not expiration_data:
                if product_id in cache:
                    logger.debug(f"У товара {name} больше нет срока годности, удаляем из кэша")
                    del cache[product_id]
                continue

            days_left = expiration_data['days_left']
            logger.debug(f"Товар: {name}, дней осталось: {days_left}")

            if self._should_alert(expiration_data, cache.get(product_id, {})):
                alert_type = "ПРОСРОЧЕННЫЙ" if days_left < 0 else "ИСТЕКАЮЩИЙ"
                logger.info(f"{alert_type} товар: {name}, дней осталось: {days_left}")

                alert = self._create_alert(name, expiration_data)
                if group_path not in alerts_by_group:
                    alerts_by_group[group_path] = []
                alerts_by_group[group_path].append(alert)

                if days_left < 0:
                    expired_count += 1
                else:
                    near_expired_count += 1

            cache[product_id] = {
                'expiration_date': expiration_data['value'],
                'was_expired': expiration_data['days_left'] < 0,
                'was_near_expired': 0 <= expiration_data['days_left'] <= 7,
                'last_check': datetime.now().isoformat()
            }

        if alerts_by_group:
            logger.info("Отправка уведомлений о сроках годности...")
            for group_name, alerts in alerts_by_group.items():
                await self.notifier.send(
                    f"⏳ {html.bold('УВЕДОМЛЕНИЯ ПО СРОКАМ ГОДНОСТИ')} ({group_name})",
                    alerts
            )
        else:
            logger.info("Нет товаров с истекающим сроком для уведомления")

        logger.info("Сохранение кэша сроков годности...")
        CacheManager.save(config.EXPIRATION_CACHE_FILE, cache)
        logger.info("=== ПРОВЕРКА СРОКОВ ГОДНОСТИ ЗАВЕРШЕНА ===")
        logger.info(
            f"Итоги проверки:\n"
            f"  Обработано товаров: {processed_count}\n"
            f"  Просроченных: {expired_count}\n"
            f"  С истекающим сроком: {near_expired_count}\n"
            f"  Всего уведомлений: {len(alerts_by_group)}"
        )

    def _is_valid_product(self, product: Dict) -> bool:
        return product.get('meta', {}).get('type') == 'product'

    def _get_expiration_data(self, product: Dict) -> Optional[Dict]:
        attr = next((a for a in product.get('attributes', [])
                     if a.get('name') == 'Срок годности'), None)
        if not attr or not attr.get('value'):
            return None

        date = self._parse_date(attr['value'])
        if not date:
            return None

        return {
            'value': attr['value'],
            'date': date,
            'days_left': (date - datetime.now()).days
        }

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        for fmt in ["%d.%m.%Y %H:%M", "%Y-%m-%d %H:%M:%S.%f"]:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        return None

    def _should_alert(self, expiration: Dict, cached: Dict) -> bool:
        if expiration['days_left'] < 0:
            return not cached.get('was_expired', False)
        elif 0 <= expiration['days_left'] <= 7:
            return (not cached.get('was_near_expired', False) and
                    expiration['days_left'] <= 7)
        return False

    def _create_alert(self, name: str, expiration: Dict) -> str:
        if expiration['days_left'] < 0:
            return (
                f"🚨 {html.bold('ПРОСРОЧЕННЫЙ ТОВАР')}\n"
                f"▸ Товар: {name}\n"
                f"▸ Срок истёк: {expiration['date'].strftime('%d.%m.%Y')}"
            )
        else:
            emoji = "🔴" if expiration['days_left'] <= 3 else "🟡"
            return (
                f"{emoji} {html.bold('ТОВАР С ИСТЕКАЮЩИМ СРОКОМ')}\n"
                f"▸ Товар: {name}\n"
                f"▸ Срок: {expiration['date'].strftime('%d.%m.%Y')}\n"
                f"▸ Осталось дней: {expiration['days_left']}"
            )


class Scheduler:
    """Планировщик проверок"""

    def __init__(self, bot: Bot):
        self.notifier = TelegramNotifier(bot)
        self.stock_checker = StockChecker(self.notifier)
        self.expiration_checker = ExpirationChecker(self.notifier)
        self.last_log_cleanup = datetime.now()

    async def run(self):  # Добавляем async перед def
        """Основной цикл проверок"""
        logger.info("Запуск планировщика с интервалом %d минут", config.CHECK_INTERVAL)
        while True:
            try:
                LoggerManager.check_and_switch_logger()

                CacheManager.reset_if_needed()

                await self._run_checks()


            except Exception as e:
                logger.error(f"КРИТИЧЕСКАЯ ОШИБКА в планировщике: {str(e)}", exc_info=True)
            finally:
                logger.info(f"Ожидание следующего цикла ({config.CHECK_INTERVAL} минут)...")
                await asyncio.sleep(config.CHECK_INTERVAL * 60)

    async def _run_checks(self):
        logger.info("=" * 60)
        logger.info("НАЧАЛО ПОЛНОГО ЦИКЛА ПРОВЕРОК")
        logger.info(f"Время запуска: {datetime.now()}")
        start_time = datetime.now()

        CacheManager.init()

        try:
            async with await self.stock_checker.create_session() as session:
                logger.info("Получение групп товаров...")
                product_folders = await MoyskladAPI.fetch_all_product_folders(session)

                # Получаем ВСЕ товары один раз
                logger.info("Получение товаров...")
                products = await MoyskladAPI.fetch_all_products(session, product_folders)
                logger.info(f"Загружено товаров: {len(products)}")

                if products:
                    # Проверяем остатки для всех товаров
                    await self.stock_checker.process(products)

                    # Проверяем сроки годности для всех товаров
                    await self.expiration_checker.process(products)

                duration_td = datetime.now() - start_time
                total_seconds = duration_td.total_seconds()
                minutes = int(total_seconds // 60)
                seconds = int(total_seconds % 60)
                logger.info(f"ПОЛНЫЙ ЦИКЛ ПРОВЕРОК ЗАВЕРШЕН ЗА {minutes} Мин. {seconds} Сек.")
                logger.info("=" * 60)

        except Exception as e:
            logger.error(f"КРИТИЧЕСКАЯ ОШИБКА В ЦИКЛЕ ПРОВЕРОК: {str(e)}", exc_info=True)
        finally:
            if 'session' in locals():
                await session.close()

# Инициализация бота
bot = Bot(token=config.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))


async def main():
    scheduler = Scheduler(bot)
    await asyncio.gather(scheduler.run())


if __name__ == "__main__":
    asyncio.run(main())