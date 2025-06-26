import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from config.config import Settings

class AppLogger:
    _instance = None

    def __new__(cls, logs_dir: Path = None, days_to_keep: int = None):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.__initialized = False
        return cls._instance

    def __init__(self, logs_dir: Path = None, days_to_keep: int = None):
        if self.__initialized:
            return

        self.__initialized = True
        self.logs_dir = logs_dir or Path("data/logs")
        self.days_to_keep = days_to_keep if days_to_keep is not None else Settings().DAYS_TO_KEEP
        self._ensure_logs_dir_exists()
        self.logger = self._setup_logger()


    def _ensure_logs_dir_exists(self):
        """Создаёт директорию для логов, если её нет"""
        try:
            self.logs_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"Failed to create logs directory: {e}")
            raise

    def _setup_logger(self) -> logging.Logger:
        """Настройка основного логгера"""
        logger = logging.getLogger("app")
        logger.setLevel(logging.INFO)

        formatter = logging.Formatter(
            "[{asctime}] #{levelname:8} {module}:{lineno} - {message}",
            style="{"
        )

        log_file = self.logs_dir / f"Main.log"

        handler = TimedRotatingFileHandler(
            log_file,
            when="midnight",
            interval=1,
            backupCount=self.days_to_keep,
            encoding="utf-8"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        return logger

    def get_logger(self, name: str = None) -> logging.Logger:
        """Возвращает логгер для конкретного модуля"""
        if name:
            return logging.getLogger(f"app.{name}")
        return self.logger


# Fallback логгер на случай проблем с инициализацией
_fallback_logger = logging.getLogger("app.fallback")
_fallback_logger.addHandler(logging.StreamHandler())
logger = _fallback_logger