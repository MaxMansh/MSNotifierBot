import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any
from utils.logger import AppLogger

logger = AppLogger().get_logger(__name__)



class CacheManager:
    def __init__(self, cache_file: Path, max_age_days: int):
        self.cache_file = cache_file
        self.max_age = timedelta(days=max_age_days)
        self.cache_data = {'phones': set(), 'counterparties': {}}  # Измененная структура
        self._load_cache()
        logger.info(f"Инициализирован CacheManager для файла: {self.cache_file}, срок хранения: {max_age_days} дней")

    def _load_cache(self):
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.cache_data['phones'] = set(data.get('phones', []))
                    self.cache_data['counterparties'] = data.get('counterparties', {})
            except Exception as e:
                logger.error(f"Ошибка загрузки кэша: {str(e)}")

    def add_counterparty(self, phone: str, name: str) -> bool:
        """Добавляет контрагента в кэш. Возвращает True если был добавлен новый."""
        if phone not in self.cache_data['counterparties']:
            self.cache_data['counterparties'][phone] = name
            self._save_cache()
            return True
        return False

    def has_counterparty(self, phone: str) -> bool:
        """Проверяет наличие контрагента по номеру телефона."""
        return phone in self.cache_data['counterparties']

    def load_counterparties(self) -> dict:
        """Возвращает словарь всех контрагентов {phone: name}."""
        return self.cache_data.get('counterparties', {})

    def _save_cache(self):
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'phones': list(self.cache_data['phones']),
                    'counterparties': self.cache_data['counterparties'],
                    'last_update': datetime.now().isoformat()
                }, f, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения кэша: {str(e)}")

    def load(self) -> Dict[str, Any]:
        try:
            if not self.cache_file.exists():
                logger.info(f"Файл кэша {self.cache_file} не существует, возвращаем пустой словарь")
                return {}

            if self._is_expired():
                logger.info(f"Файл кэша {self.cache_file} устарел (старше {self.max_age.days} дней), удаляем")
                self.cache_file.unlink()
                return {}

            logger.info(f"Загрузка кэша из файла {self.cache_file}")
            with open(self.cache_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                logger.debug(f"Успешно загружено {len(data)} записей из кэша")
                return data

        except json.JSONDecodeError as e:
            logger.error(f"Ошибка декодирования JSON в файле {self.cache_file}: {str(e)}")
            return {}
        except Exception as e:
            logger.error(f"Неожиданная ошибка при загрузке кэша: {str(e)}", exc_info=True)
            return {}

    def save(self, data: Dict[str, Any]) -> None:
        try:
            logger.info(f"Сохранение {len(data)} записей в кэш файл {self.cache_file}")
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.info(f"Кэш успешно сохранен в {self.cache_file}")

        except Exception as e:
            logger.error(f"Ошибка сохранения кэша: {str(e)}", exc_info=True)
            raise

    def _is_expired(self) -> bool:
        try:
            file_mtime = datetime.fromtimestamp(self.cache_file.stat().st_mtime)
            is_expired = (datetime.now() - file_mtime) > self.max_age
            if is_expired:
                logger.info(f"Проверка срока годности кэша: файл изменен {file_mtime}, срок истёк")
            else:
                logger.info(f"Проверка срока годности кэша: файл изменен {file_mtime}, срок действителен")
            return is_expired
        except Exception as e:
            logger.error(f"Ошибка проверки срока годности кэша: {str(e)}")
            return True