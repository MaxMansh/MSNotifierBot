from datetime import datetime
from typing import Optional

class DateParser:
    FORMATS = [
        "%d.%m.%Y %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d"
    ]

    @classmethod
    def parse(cls, date_str: str) -> Optional[datetime]:
        for fmt in cls.FORMATS:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        return None