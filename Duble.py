import asyncio
from collections import defaultdict
import logging
from typing import Dict, List
import aiohttp
from config import Settings, PathManager
from core.services.api.moysklad import MoyskladAPI

# Настройка логгирования
log_manager = PathManager()
log_file = log_manager.logs_dir / "counterparty_duplicates.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
    ]
)
logger = logging.getLogger(__name__)


class CounterpartyDuplicateFinder(MoyskladAPI):
    def __init__(self, token: str):
        super().__init__(token)
        logger.info("Инициализирован поиск дубликатов контрагентов")

    async def find_and_log_duplicates(self):
        """Основной метод для поиска и логирования дубликатов"""
        try:
            async with aiohttp.ClientSession() as session:
                counterparties = await self._get_all_counterparties(session)
                duplicates = self._find_duplicates(counterparties)
                self._log_duplicates(duplicates)
        except Exception as e:
            logger.error(f"Критическая ошибка: {str(e)}", exc_info=True)

    async def _get_all_counterparties(self, session: aiohttp.ClientSession) -> List[Dict]:
        """Получает всех контрагентов"""
        counterparties = []
        offset = 0
        total = await self._get_total_count(session)
        logger.info(f"Всего контрагентов для проверки: {total}")

        while True:
            params = {
                "limit": 500,
                "offset": offset,
                "filter": "archived=false"
            }
            data = await self._make_request(session, "entity/counterparty", params)
            counterparties.extend(data.get('rows', []))

            if len(data.get('rows', [])) < 500:
                break

            offset += len(data['rows'])
            logger.info(f"Загружено {len(counterparties)}/{total} контрагентов")
            await asyncio.sleep(0.1)

        logger.info(f"Загрузка завершена. Всего загружено: {len(counterparties)}")
        return counterparties

    async def _get_total_count(self, session: aiohttp.ClientSession) -> int:
        """Получает общее количество контрагентов"""
        data = await self._make_request(session, "entity/counterparty", {"limit": 1})
        return data['meta']['size']

    def _find_duplicates(self, counterparties: List[Dict]) -> Dict[str, List[Dict]]:
        """Находит дубликаты по полю name"""
        name_map = defaultdict(list)

        for counterparty in counterparties:
            name = counterparty.get('name', '').strip().lower()
            if name:
                name_map[name].append(counterparty)

        return {name: items for name, items in name_map.items() if len(items) > 1}

    def _log_duplicates(self, duplicates: Dict[str, List[Dict]]):
        """Логирует найденные дубликаты"""
        if not duplicates:
            logger.info("Дубликаты контрагентов не найдены")
            return

        logger.info(f"\nНайдено {len(duplicates)} групп дубликатов:\n")

        for name, items in duplicates.items():
            logger.info(f"Дубликаты для имени: {name}")
            for item in items:
                logger.info(f"  ID: {item['id']}")
                logger.info(f"  Дата создания: {item.get('created')}")
                logger.info(f"  Тип: {item.get('companyType', 'не указан')}")
                logger.info(f"  Ссылка: {item['meta']['href']}")
                logger.info("-" * 60)
            logger.info("\n")


async def main():
    try:
        settings = Settings()
        finder = CounterpartyDuplicateFinder(settings.MS_TOKEN)
        task = asyncio.create_task(finder.find_and_log_duplicates())
        await task
    except KeyboardInterrupt:
        logger.info("Программа была прервана пользователем")
        task.cancel()  # Отменяем задачу
    except Exception as e:
        logger.error(f"Произошла ошибка: {e}", exc_info=True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Программа была прервана пользователем")

