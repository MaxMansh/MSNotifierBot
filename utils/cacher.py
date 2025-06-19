import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any

class CacheManager:
    def __init__(self, cache_file: Path, max_age_days: int):
        self.cache_file = cache_file
        self.max_age = timedelta(days=max_age_days)

    def load(self) -> Dict[str, Any]:
        if not self.cache_file.exists():
            return {}

        if self._is_expired():
            self.cache_file.unlink()
            return {}

        try:
            with open(self.cache_file, 'r') as f:
                return json.load(f)
        except Exception:
            return {}

    def save(self, data: Dict[str, Any]) -> None:
        with open(self.cache_file, 'w') as f:
            json.dump(data, f, indent=2)

    def _is_expired(self) -> bool:
        file_mtime = datetime.fromtimestamp(self.cache_file.stat().st_mtime)
        return (datetime.now() - file_mtime) > self.max_age