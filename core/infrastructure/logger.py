import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from aiogram.types import User
from core.config import Settings

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
            print(f"Ошибка при создании директории для логов: {e}")
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

    def log_user_activity_debug(self, user: User, action: str, details: str = "") -> None:
        """Логирует действия пользователя с уровнем DEBUG"""
        self._log_user_activity(user, action, details, "debug")

    def log_user_activity_info(self, user: User, action: str, details: str = "") -> None:
        """Логирует действия пользователя с уровнем INFO"""
        self._log_user_activity(user, action, details, "info")

    def log_user_activity_warning(self, user: User, action: str, details: str = "") -> None:
        """Логирует действия пользователя с уровнем WARNING"""
        self._log_user_activity(user, action, details, "warning")

    def log_user_activity_error(self, user: User, action: str, details: str = "") -> None:
        """Логирует действия пользователя с уровнем ERROR"""
        self._log_user_activity(user, action, details, "error")

    def _log_user_activity(self, user: User, action: str, details: str, level: str) -> None:
        """Внутренний метод для логирования действий пользователя"""
        user_info = (
            f"Пользователь: ID={user.id}, "
            f"Имя={user.full_name}, "
            f"Никнейм=@{user.username}" if user.username else "Без никнейма"
        )
        log_message = f"{user_info} | Действие: {action}"
        if details:
            log_message += f" | Детали: {details}"

        logger = self.get_logger("user_activity")

        if level == "debug":
            logger.debug(log_message)
        elif level == "info":
            logger.info(log_message)
        elif level == "warning":
            logger.warning(log_message)
        elif level == "error":
            logger.error(log_message)
        else:
            logger.info(log_message)


# Fallback логгер на случай проблем с инициализацией
_fallback_logger = logging.getLogger("app.fallback")
_fallback_logger.addHandler(logging.StreamHandler())
logger = _fallback_logger