import logging
from pathlib import Path
from datetime import datetime, timedelta
import sys
import logging.handlers


class AppLogger:
    def __init__(self, log_dir: Path, max_log_age: timedelta, name: str = "bot"):
        self._log_dir = log_dir
        self._max_age = max_log_age
        self._setup_logging(name)
        self._cleanup_old_logs()

    def _setup_logging(self, name: str):
        # Создаем директорию для логов если не существует
        self._log_dir.mkdir(parents=True, exist_ok=True)

        # Основной логгер приложения
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)  # Ловим все сообщения от DEBUG и выше

        # Форматтер для логов
        formatter = logging.Formatter(
            "[{asctime}] #{levelname:8} {filename}:{lineno} - {message}\n"
            "────────────────────────────────────────────",
            style="{"
        )

        # Файловый обработчик с ротацией по дням
        log_file = self._log_dir / f"MainLog - {datetime.now().strftime('%d.%m.%Y')}.log"
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.INFO)

        # Консольный обработчик (только INFO и выше)
        # console_handler = logging.StreamHandler(sys.stdout)
        # console_handler.setFormatter(formatter)
        # console_handler.setLevel(logging.INFO)

        # Настраиваем корневой логгер для перехвата всех логов
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)
        root_logger.addHandler(file_handler)
        # root_logger.addHandler(console_handler)

        # Перехватываем логи asyncio и aiohttp
        logging.getLogger('asyncio').setLevel(logging.DEBUG)
        logging.getLogger('aiohttp').setLevel(logging.DEBUG)
        logging.getLogger('aiogram').setLevel(logging.DEBUG)

        self._logger = logger

    def _cleanup_old_logs(self):
        now = datetime.now()
        for log_file in self._log_dir.glob("*.log"):
            try:
                file_date_str = log_file.stem.split('_')[-1]
                file_date = datetime.strptime(file_date_str, "%Y%m%d")
                if (now - file_date) > self._max_age:
                    log_file.unlink()
            except (ValueError, IndexError):
                continue

    def __getattr__(self, name):
        return getattr(self._logger, name)