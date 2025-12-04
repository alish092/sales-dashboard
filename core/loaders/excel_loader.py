import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)

class ExcelLoader:
    def __init__(self, excel_path):
        self.excel_path = excel_path

    def load(self, target_sheet_name=None):
        if not os.path.exists(self.excel_path):
            raise FileNotFoundError(f"Файл не найден: {self.excel_path}")

        xl = pd.ExcelFile(self.excel_path)
        if target_sheet_name and target_sheet_name in xl.sheet_names:
            logger.info(f"Загружаем выбранный лист: {target_sheet_name}")
            return xl.parse(target_sheet_name)
        else:
            sheet_names = [s for s in xl.sheet_names if s.startswith("Отчет")]
            if not sheet_names:
                raise ValueError("Не найдены листы, начинающиеся с 'Отчет'")
            logger.info(f"Загружаем лист по умолчанию: {sheet_names[0]}")
            return xl.parse(sheet_names[0])
