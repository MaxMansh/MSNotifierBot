from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any

@dataclass
class Product:
    id: str
    name: str
    stock: float
    min_balance: Optional[float]
    group_path: str
    expiration_date: Optional[datetime]
    raw_data: Dict[str, Any]

    @property
    def is_valid(self) -> bool:
        return isinstance(self.name, str) and isinstance(self.stock, (int, float))

    @property
    def needs_stock_check(self) -> bool:
        return self.min_balance is not None and self.min_balance > 0

    @property
    def needs_expiration_check(self) -> bool:
        return self.expiration_date is not None