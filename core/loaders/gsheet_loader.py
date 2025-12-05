import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import logging
import os

logger = logging.getLogger(__name__)


class GoogleSheetsLoader:
    def __init__(self, service_account_file, scopes):
        # ✅ ДОБАВЛЕНА ПРОВЕРКА ФАЙЛА
        if not os.path.exists(service_account_file):
            raise FileNotFoundError(
                f"Service account файл не найден: {service_account_file}\n"
                f"Создайте файл service_account.json в папке resources/"
            )

        self.service_account_file = service_account_file
        self.scopes = scopes
        self._cache = {}

    def list_sheets(self, sheet_id: str):
        """
        Вернуть список названий листов в указанной таблице.
        """
        try:
            creds = Credentials.from_service_account_file(
                self.service_account_file,
                scopes=self.scopes,
            )
            client = gspread.authorize(creds)
            sh = client.open_by_key(sheet_id)
            worksheets = sh.worksheets()
            return [ws.title for ws in worksheets]
        except Exception as e:
            logger.error(f"Ошибка получения списка листов: {e}")
            raise

    def load(self, sheet_id, sheet_name, force_reload=False):
        cache_key = (sheet_id, sheet_name)

        # Если кэш есть и не просим перезагрузить — возвращаем кэш
        if not force_reload and cache_key in self._cache:
            logger.info(f"[GSHEET LOADER] Возвращаю данные из кэша: {cache_key}")
            return self._cache[cache_key]

        # Иначе — грузим заново и обновляем кэш
        try:
            creds = Credentials.from_service_account_file(
                self.service_account_file, scopes=self.scopes
            )
            client = gspread.authorize(creds)
            ws = client.open_by_key(sheet_id).worksheet(sheet_name)
            values = ws.get_all_values()

            if not values:
                raise ValueError("Лист пуст")

            first_row = [cell.strip() for cell in values[0]]
            if "Модель" in first_row or "Показатель" in first_row:
                header_row_index = 0
            else:
                header_row_index = 1

            header = [h.strip() for h in values[header_row_index]]
            data_rows = values[header_row_index + 1:]
            df = pd.DataFrame(data_rows, columns=header)
            df.columns = df.columns.str.strip()

            logger.info(
                f"[GSHEET LOADER] Загружено: {df.shape}, "
                f"header_row_index={header_row_index}"
            )

            # Обновляем кэш
            self._cache[cache_key] = df
            return df

        except gspread.exceptions.APIError as e:
            logger.error(f"Google API ошибка: {e}")
            raise
        except Exception as e:
            logger.error(f"Ошибка загрузки данных: {e}")
            raise

    def load_sheet(self, sheet_id, sheet_name, force_reload=False):
        """
        Обёртка для совместимости: вызывает основной метод load().
        """
        return self.load(sheet_id, sheet_name, force_reload=force_reload)

    def clear_cache(self):
        """Очистить кэш загруженных данных"""
        self._cache.clear()
        logger.info("[GSHEET LOADER] Кэш очищен")