import logging
from pathlib import Path
from datetime import datetime, timedelta
import sys
from collections import defaultdict

class AppLogger:
    def __init__(self, log_dir: Path, max_log_age: timedelta, name: str = "bot"):
        self._log_dir = log_dir
        self._max_age = max_log_age
        self._logger = self._setup_logger(name)
        self._cleanup_old_logs()

    def _setup_logger(self, name: str) -> logging.Logger:
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)

        formatter = logging.Formatter(
            "[{asctime}] #{levelname:8} {filename}:{lineno} - {message}\n"
            "────────────────────────────────────────────",
            style="{"
        )

        log_file = self._log_dir / f"MainLog - {datetime.now().strftime('%d.%m.%Y')}.log"
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        #logger.addHandler(console_handler)

        return logger

    def _cleanup_old_logs(self):
        now = datetime.now()
        for log_file in self._log_dir.glob("MainLog - *.log"):
            try:
                file_date_str = log_file.stem.split(" - ")[1]
                file_date = datetime.strptime(file_date_str, "%d.%m.%Y")
                if (now - file_date) > self._max_age:
                    log_file.unlink()
            except (ValueError, IndexError):
                continue

    def __getattr__(self, name):
        return getattr(self._logger, name)