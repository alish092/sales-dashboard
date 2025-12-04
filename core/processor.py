import logging
import pandas as pd
from core.loaders.excel_loader import ExcelLoader

logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self, excel_path=None):
        self.excel_path = excel_path
        self.df = None
        self.data_columns = []
        self.max_day = 0
        self.target_sheet_name = None
        self.data_source = 'excel'
        self._cached_agg_data = None

        # Загружаем Excel по умолчанию
        if excel_path:
            self._load_from_excel()

    def _load_from_excel(self):
        try:
            loader = ExcelLoader(self.excel_path)
            self.df = loader.load(self.target_sheet_name)

            self.data_columns = self.df.columns[2:].tolist()
            self.max_day = len(self.data_columns)
            self.data_source = 'excel'

            logger.info(f"[EXCEL] Загружено: {self.df.shape[0]} строк, {self.max_day} дней")
        except Exception as e:
            logger.error(f"[EXCEL] Ошибка загрузки: {e}")
            self.df = pd.DataFrame(columns=['Показатель'] + [f'День {i}' for i in range(1, 6)])
            self.data_columns = self.df.columns[1:].tolist()
            self.max_day = len(self.data_columns)

    def load_from_gsheet(self, df, sheet_name):
        """Загрузка данных из Google Sheets"""
        try:
            self.df = df
            self.target_sheet_name = sheet_name
            self.data_source = 'gsheet'

            self.data_columns = self.df.columns[2:].tolist()
            self.max_day = len(self.data_columns)

            logger.info(f"[GSHEET] Загружено: {self.df.shape[0]} строк, {self.max_day} дней — Лист: {sheet_name}")
        except Exception as e:
            logger.error(f"[GSHEET] Ошибка: {e}")
            raise

    def process_data(self, day_range):
        try:
            start_day, end_day = day_range
            day_cols = self.data_columns[start_day - 1:end_day]

            normalized_day_cols = [self.normalize_date(col) for col in day_cols]
            col_mapping = {self.normalize_date(col): col for col in self.df.columns[2:]}
            selected_cols = [col_mapping[col] for col in normalized_day_cols if col in col_mapping]

            if not selected_cols:
                return pd.DataFrame(), pd.DataFrame()

            data_period = self.df[['Показатель'] + selected_cols].copy()
            num_data = data_period[selected_cols].apply(pd.to_numeric, errors='coerce').fillna(0)

            data_period_aggregated = data_period[['Показатель']].copy()
            data_period_aggregated['Сумма за период'] = num_data.sum(axis=1)

            self._cached_agg_data = data_period_aggregated
            return data_period, data_period_aggregated
        except Exception as e:
            logger.error(f"Ошибка обработки данных: {e}")
            return pd.DataFrame(), pd.DataFrame()

    def find_metric_value(self, df_hash, metric_name_part):
        if self.df.empty or 'Показатель' not in self.df.columns:
            return 0
        try:
            agg_data = self._cached_agg_data
            if agg_data is None:
                return 0

            match = agg_data[agg_data['Показатель'].str.contains(
                metric_name_part, na=False, case=False)]

            if not match.empty:
                return pd.to_numeric(match['Сумма за период'].iloc[0], errors='coerce') or 0
        except Exception as e:
            logger.warning(f"Ошибка поиска метрики '{metric_name_part}': {e}")
        return 0

    @staticmethod
    def normalize_date(col):
        try:
            if isinstance(col, pd.Timestamp):
                return col.strftime('%Y-%m-%d')
            if isinstance(col, str):
                dt = pd.to_datetime(col, errors='coerce')
                if pd.notna(dt):
                    return dt.strftime('%Y-%m-%d')
            return str(col)
        except Exception:
            return str(col)
