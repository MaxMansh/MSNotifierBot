from pydantic_settings import BaseSettings
from pathlib import Path

class Settings(BaseSettings):
    BOT_TOKEN: str
    MS_TOKEN: str
    CHAT_ID: str
    CHECK_INTERVAL_MINUTES: int = 720
    TG_MESSAGE_LIMIT: int = 4096
    DAYS_TO_KEEP: int = 30
    CACHE_RESET_DAYS: int = 30
    API_REQUEST_LIMIT: int = 500
    API_REQUEST_DELAY: int = 5

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

class PathManager:
    def __init__(self, base_dir: str = "data"):
        self.base_dir = Path(base_dir)
        self._create_dirs()

    def _create_dirs(self):
        (self.base_dir / "cache").mkdir(parents=True, exist_ok=True)
        (self.base_dir / "logs").mkdir(parents=True, exist_ok=True)

    @property
    def cache_dir(self) -> Path:
        return self.base_dir / "cache"

    @property
    def logs_dir(self) -> Path:
        return self.base_dir / "logs"

    @property
    def stocks_cache(self) -> Path:
        return self.cache_dir / "stocks_cache.json"

    @property
    def expiration_cache(self) -> Path:
        return self.cache_dir / "expiration_cache.json"

    @property
    def phone_cache(self) -> Path:  # Добавить для кэша номеров
        return self.cache_dir / "phones.json"

    def glob(self, param):
        pass