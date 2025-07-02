import pandas as pd
from pathlib import Path
from typing import List

class ExcelProcessor:
    @staticmethod
    def parse_phones(file_path: Path) -> List[str]:
        """Извлекает номера из столбца 'Наименование' в Excel."""
        try:
            df = pd.read_excel(file_path)
            return df["Наименование"].astype(str).tolist()
        except Exception as e:
            raise ValueError(f"Ошибка чтения Excel: {str(e)}")