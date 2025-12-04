import dash
from dash import dcc, html, Output, Input, State, callback, dash_table, ctx
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import logging
import numpy as np
from functools import lru_cache
from datetime import datetime
from dash import dash_table
import warnings
import gspread
from dash.exceptions import PreventUpdate
from google.oauth2.service_account import Credentials
from dash import State
from dash import Dash, html, dcc, Input, Output, State
from config import Config
from processor import DataProcessor

import sys, os
import webbrowser
from core.processor import DataProcessor
from core.loaders.excel_loader import ExcelLoader
from core.loaders.gsheet_loader import GoogleSheetsLoaderдава

loader = GoogleSheetsLoader("service_account.json", ["https://www.googleapis.com/auth/spreadsheets"])

# при запуске из exe _MEIPASS указывает на временную папку с ресурсами
if getattr(sys, 'frozen', False):
    BASE_DIR = sys._MEIPASS
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# теперь используем относительные пути
SERVICE_ACCOUNT_FILE = os.path.join(BASE_DIR, "service_account.json")
EXCEL_PATH = os.path.join(BASE_DIR, "диаграмма.xlsx")
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)
# === DASH APP ===
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)
df_stock_data = None
# Список корней моделей
root_models = ["718", "911", "Cayenne", "Panamera", "Macan", "Taycan"]


def resource_path(relative_path):
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.abspath(relative_path)

# Для доступа к Google Sheets
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]
loader = GoogleSheetsLoader(SERVICE_ACCOUNT_FILE, SCOPES)

SERVICE_ACCOUNT_FILE = resource_path(os.getenv("GSERVICE_JSON", "service_account.json"))
# Настройка логирования
print("JSON PATH:", SERVICE_ACCOUNT_FILE)
print("EXISTS:", os.path.exists(SERVICE_ACCOUNT_FILE))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
warnings.filterwarnings('ignore')


def get_delta(val1, val2, percent=False):
    if val2 == 0:
        return "—"
    delta = (val1 - val2) / val2 * 100
    color = "green" if delta > 0 else "red"
    sign = "+" if delta > 0 else ""
    return html.Span(f"{sign}{delta:.1f}%", style={"color": color, "marginLeft": 8})


def create_comparison_metrics_cards(metrics_current, metrics_prev):
    rows = []
    for key, label, percent in [
        ("total_incoming", "Входящие звонки", False),
        ("total_accepted", "Принято звонков", False),
        ("acceptance_rate", "Процент принятых", True),
        ("target_ads_spend", "Траты на таргет", False)
    ]:
        rows.append(
            dbc.Col(
                dbc.Card(
                    dbc.CardBody([
                        html.H6(label, className="card-title text-muted"),
                        html.H4(f"{metrics_current[key]:,.0f}", className="card-text text-primary"),
                        html.Div([
                            html.Span("Сравнение: "),
                            html.Span(f"{metrics_prev[key]:,.0f}", style={"color": "#888"}),
                            get_delta(metrics_current[key], metrics_prev[key], percent)
                        ], className="mt-2 small")
                    ]), className="mb-3 shadow-sm"
                ), width=12, md=6, lg=3
            )
        )
    return dbc.Row(rows)


class MetricsCalculator:
    def __init__(self, data_processor):
        self.data_processor = data_processor

    def debug_available_metrics(self, data_period_aggregated):
        """Функция для отладки - показывает все доступные показатели"""
        if not data_period_aggregated.empty and 'Показатель' in data_period_aggregated.columns:
            logger.info("=== ДОСТУПНЫЕ ПОКАЗАТЕЛИ ===")
            rows = data_period_aggregated[['Показатель', 'Сумма за период']].dropna()
            for metric_name, metric_value in rows.values:
                logger.info(f"'{metric_name}' = {metric_value}")
            logger.info("=== КОНЕЦ СПИСКА ===")
        else:
            logger.warning("Нет данных для отладки показателей")

    def calculate_key_metrics(self, data_period_aggregated, data_period=None, selected_cols=None):
        """Расчет ключевых показателей"""
        try:
            # Основные метрики
            total_incoming = self._safe_find_metric(data_period_aggregated, "Входящие звонки - ВЗ")
            total_accepted = self._safe_find_metric(data_period_aggregated, "Принятые ВЗ")
            total_missed = self._safe_find_metric(data_period_aggregated, "Непринятые ВЗ")

            # Расчет процента принятых
            acceptance_rate = round((total_accepted / total_incoming * 100), 1) if total_incoming > 0 else 0

            # Альтернативный поиск трат на таргет
            if data_period is not None and selected_cols is not None:
                target_ads_spend = self.find_target_spending_alternative(data_period, data_period_aggregated,
                                                                         selected_cols)
            else:
                target_ads_spend = self._safe_find_metric(data_period_aggregated, "Траты на таргет")

            leads = (
                    self._safe_find_metric(data_period_aggregated, "Лиды \\(ИЗ\\)") or
                    self._safe_find_metric(data_period_aggregated, "Лиды (ИЗ)") or
                    self._safe_find_metric(data_period_aggregated, "Лиды ИЗ") or
                    self._safe_find_metric(data_period_aggregated, "ИНТЕРНЕТ ЗАЯВКИ - ИЗ") or
                    self._safe_find_metric(data_period_aggregated, "Интернет заявки")
            )

            visits = self._safe_find_metric(data_period_aggregated, "Визиты")
            deliveries = self._safe_find_metric(data_period_aggregated, "Выдачи")

            # Логирование для отладки
            logger.info(f"Target ads spend: {target_ads_spend}")
            logger.info(f"Leads: {leads}")
            logger.info(f"Visits: {visits}")
            logger.info(f"Deliveries: {deliveries}")

            # Расчет стоимостей с проверкой деления на ноль
            cost_per_lead = round(target_ads_spend / leads, 0) if leads > 0 and target_ads_spend > 0 else 0
            cost_per_visit = round(target_ads_spend / visits, 0) if visits > 0 and target_ads_spend > 0 else 0
            cost_per_delivery = round(target_ads_spend / deliveries,
                                      0) if deliveries > 0 and target_ads_spend > 0 else 0

            return {
                'total_incoming': total_incoming,
                'total_accepted': total_accepted,
                'total_missed': total_missed,
                'acceptance_rate': acceptance_rate,
                'target_ads_spend': target_ads_spend,
                'cost_per_lead': cost_per_lead,
                'cost_per_visit': cost_per_visit,
                'cost_per_delivery': cost_per_delivery
            }

        except Exception as e:
            logger.error(f"Ошибка расчета метрик: {e}")
            return self._get_default_metrics()

    def _safe_find_metric(self, df_agg, metric_name_part):
        """Безопасный поиск метрики с улучшенным алгоритмом"""
        if df_agg.empty or 'Показатель' not in df_agg.columns or 'Сумма за период' not in df_agg.columns:
            return 0

        try:
            # Сначала попробуем точное совпадение
            exact_match = df_agg[df_agg['Показатель'].str.strip() == metric_name_part.strip()]
            if not exact_match.empty:
                value = exact_match['Сумма за период'].iloc[0]
                result = pd.to_numeric(value, errors='coerce') or 0
                logger.debug(f"Exact match found for '{metric_name_part}': {result}")
                return result

            # Если точного совпадения нет, попробуем частичное совпадение
            partial_match = df_agg[df_agg['Показатель'].str.contains(metric_name_part, na=False, case=False)]
            if not partial_match.empty:
                value = partial_match['Сумма за период'].iloc[0]
                result = pd.to_numeric(value, errors='coerce') or 0
                logger.debug(f"Partial match found for '{metric_name_part}': {result}")
                return result

            # Если и частичного совпадения нет, попробуем поиск по ключевым словам
            if "траты" in metric_name_part.lower() or "таргет" in metric_name_part.lower():
                # Ищем строки содержащие "траты" И "таргет"
                target_match = df_agg[
                    (df_agg['Показатель'].str.contains("траты", na=False, case=False)) &
                    (df_agg['Показатель'].str.contains("таргет", na=False, case=False))
                    ]
                if not target_match.empty:
                    value = target_match['Сумма за период'].iloc[0]
                    result = pd.to_numeric(value, errors='coerce') or 0
                    logger.debug(f"Keyword match found for '{metric_name_part}': {result}")
                    return result

            logger.debug(f"No match found for '{metric_name_part}'")
            return 0

        except Exception as e:
            logger.warning(f"Ошибка поиска метрики '{metric_name_part}': {e}")
            return 0

    def debug_target_spending(self, data_period_aggregated):
        """Специальная отладка для поиска трат на таргет"""
        logger.info("=== ОТЛАДКА ТРАТ НА ТАРГЕТ ===")

        if data_period_aggregated.empty or 'Показатель' not in data_period_aggregated.columns:
            logger.warning("Нет данных для поиска")
            return

        # Ищем все строки, содержащие слова связанные с тратами/рекламой
        keywords = ["траты", "таргет", "реклама", "расход", "бюджет", "стоимость"]

        for keyword in keywords:
            matches = data_period_aggregated[
                data_period_aggregated['Показатель'].str.contains(keyword, na=False, case=False)
            ]
            if not matches.empty:
                logger.info(f"Найдено по ключевому слову '{keyword}':")
                for idx, row in matches.iterrows():
                    logger.info(f"  '{row['Показатель']}' = {row.get('Сумма за период', 0)}")

        # Специально ищем точное совпадение "Траты на таргет"
        exact_match = data_period_aggregated[
            data_period_aggregated['Показатель'].str.strip() == "Траты на таргет"
            ]
        if not exact_match.empty:
            logger.info(f"Точное совпадение 'Траты на таргет': {exact_match['Сумма за период'].iloc[0]}")
        else:
            logger.warning("Точного совпадения 'Траты на таргет' не найдено")

        logger.info("=== КОНЕЦ ОТЛАДКИ ТРАТ ===")

    # Также добавьте эту функцию в класс MetricsCalculator и вызовите её в callback:
    # metrics_calculator.debug_target_spending(data_period_aggregated)

    def _get_default_metrics(self):
        """Дефолтные метрики при ошибке"""
        return {
            'total_incoming': 0,
            'total_accepted': 0,
            'total_missed': 0,
            'acceptance_rate': 0,
            'target_ads_spend': 0,
            'cost_per_lead': 0,
            'cost_per_visit': 0,
            'cost_per_delivery': 0
        }


class ChartGenerator:
    def __init__(self, config):
        self.config = config

    def create_metrics_cards(self, metrics):
        """Создание карточек с метриками"""
        cards_data = [
            ("Всего входящих", f"{int(metrics['total_incoming']):,}".replace(",", " ")),
            ("Принято звонков", f"{int(metrics['total_accepted']):,}".replace(",", " ")),
            ("Процент принятых", f"{metrics['acceptance_rate']}%"),
            ("Пропущено", f"{int(metrics['total_missed']):,}".replace(",", " ")),
            ("Траты на таргет", f"{metrics['target_ads_spend']:,.0f}₸"),
            ("Стоимость лида (ИЗ)", f"{metrics['cost_per_lead']:,.0f}₸"),
            ("Стоимость визита", f"{metrics['cost_per_visit']:,.0f}₸"),
            ("Стоимость выдачи", f"{metrics['cost_per_delivery']:,.0f}₸"),
        ]

        cards = []
        for title, value in cards_data:
            card = dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H6(title, className="card-title text-muted"),
                        html.H4(value, className="card-text text-primary")
                    ])
                ], className="mb-3 shadow-sm")
            ], width=12, md=6, lg=3)
            cards.append(card)

        return dbc.Row(cards)



        cards = []
        for key, label in [("total_incoming", "Входящие звонки"),
                           ("total_accepted", "Принято звонков"),
                           ("acceptance_rate", "Процент принятых"),
                           ("target_ads_spend", "Траты на таргет")]:
            cards.append(
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody([
                            html.H6(label, className="card-title text-muted"),
                            html.H4(f"{metrics_current[key]:,.0f}", className="card-text text-primary"),
                            html.Div([
                                html.Span("Было: "),
                                html.Span(f"{metrics_prev[key]:,.0f}", style={"color": "#888"}),
                                get_delta(metrics_current[key], metrics_prev[key])
                            ], className="mt-2")
                        ]), className="mb-3 shadow-sm"
                    ), width=12, md=6, lg=3
                )
            )


    def create_staff_charts(self, data_period_aggregated):
        """Создание диаграмм по сотрудникам"""
        try:
            bar_df = pd.DataFrame({
                "Сотрудник": self.config.STAFF_NAMES,
                "Переадресовано успешно ВЗ": [
                    self._safe_find_metric(data_period_aggregated, name)
                    for name in self.config.STAFF_NAMES
                ]
            })

            # Столбчатая диаграмма
            fig_staff = px.bar(
                bar_df,
                x="Сотрудник",
                y="Переадресовано успешно ВЗ",
                title="Переадресовано успешно ВЗ по сотрудникам",
                text_auto=True,
                color_discrete_sequence=[self.config.COLORS['primary']]
            )
            fig_staff.update_layout(showlegend=False)

            # Круговая диаграмма
            fig_pie = px.pie(
                bar_df,
                names="Сотрудник",
                values="Переадресовано успешно ВЗ",
                title="Распределение переадресаций",
                color="Сотрудник"
            )
            fig_pie.update_traces(textinfo='percent+value')

            return fig_staff, fig_pie

        except Exception as e:
            logger.error(f"Ошибка создания диаграмм сотрудников: {e}")
            return self._empty_figure(), self._empty_figure()

    def create_internet_pie(self, data_period_aggregated):
        """Создание диаграммы распределения интернет заявок"""
        try:
            iz_labels = ["Дозвонились ИЗ", "Не обработаны ИЗ", "Не дозвонились ИЗ"]
            iz_values = [self._safe_find_metric(data_period_aggregated, l) for l in iz_labels]

            iz_df = pd.DataFrame({
                "Статус": iz_labels,
                "Количество": iz_values
            })

            fig_iz = px.pie(
                iz_df,
                names="Статус",
                values="Количество",
                title="Распределение интернет-заявок (ИЗ)",
                color="Статус",
                color_discrete_map={
                    "Дозвонились ИЗ": "green",
                    "Не обработаны ИЗ": "red",
                    "Не дозвонились ИЗ": "yellow"
                }
            )
            fig_iz.update_traces(textinfo='percent+value')
            return fig_iz

        except Exception as e:
            logger.error(f"Ошибка создания диаграммы ИЗ: {e}")
            return self._empty_figure()

    import logging
    logger = logging.getLogger(__name__)

    def create_scripts_bar(self, data_period, data_period_aggregated, selected_cols):
        logger.debug(">>> Selected columns for scripts: %r", selected_cols)
        logger.debug(">>> Data period shape: %s", data_period.shape)

        try:
            def safe_script_average(name):
                metric = f"Выполнение скрипта {name}"
                # Ищем строку с метрикой в исходных данных за период (не в агрегированных)
                mask = data_period['Показатель'].str.contains(metric, case=False, na=False)
                logger.debug(">>> Looking for %r: matched %d rows", metric, mask.sum())

                if mask.sum() == 0:
                    logger.debug(">>> No rows found for %r", metric)
                    return 0

                # Получаем строку с данными
                matched_row = data_period.loc[mask]
                logger.debug(">>> Matched rows for %r:\n%s", metric, matched_row)

                if matched_row.empty:
                    return 0

                # Берем только колонки с данными за выбранный период
                row_data = matched_row[selected_cols].iloc[0]
                logger.debug(">>> Raw data for %r: %r", metric, row_data.values)

                # Очищаем данные от символов % и преобразуем в числа
                cleaned_data = []
                for val in row_data:
                    if pd.isna(val):
                        continue
                    # Убираем % если есть
                    val_str = str(val).replace('%', '').strip()
                    try:
                        num_val = float(val_str)
                        cleaned_data.append(num_val)
                    except (ValueError, TypeError):
                        continue

                logger.debug(">>> Cleaned data for %r: %r", metric, cleaned_data)

                if not cleaned_data:
                    return 0

                # Вычисляем среднее значение
                average = sum(cleaned_data) / len(cleaned_data)
                logger.debug(">>> Average for %r: %f", metric, average)

                return round(average, 1)

            # Получаем средние значения для каждого сотрудника
            script_percents = [safe_script_average(name) for name in self.config.STAFF_NAMES]
            logger.debug(">>> Final script percentages: %r", script_percents)

            # Определяем цвета в зависимости от процента
            script_colors = []
            for v in script_percents:
                if v < 50:
                    script_colors.append("red")
                elif v < 80:
                    script_colors.append("orange")
                else:
                    script_colors.append("green")

            # Создаем DataFrame для диаграммы
            script_df = pd.DataFrame({
                "Менеджер": self.config.STAFF_NAMES,
                "Выполнение скрипта (%)": script_percents,
                "Цвет": script_colors
            })

            # Создаем столбчатую диаграмму
            fig_script = px.bar(
                script_df,
                x="Менеджер",
                y="Выполнение скрипта (%)",
                color="Цвет",
                color_discrete_map={
                    "red": "red",
                    "orange": "orange",
                    "green": "green"
                },
                text="Выполнение скрипта (%)",
                title="Выполнение скрипта по менеджерам (среднее значение)"
            )
            fig_script.update_traces(textposition='outside')
            fig_script.update_layout(showlegend=False)

            return fig_script

        except Exception as e:
            logger.error(f"Ошибка создания диаграммы скриптов: {e}")
            return self._empty_figure()

    def create_sales_funnel(self, data_period_aggregated):
        #raise Exception("🛑 create_sales_funnel точно вызван?")
        print("🧪 create_sales_funnel вызван!")

        """Создание воронки продаж"""
        try:
            # Новый "Общий трафик"
            forwarded = sum([
                self._safe_find_metric(data_period_aggregated, f"Переадресованные успешно ВЗ {name}")
                for name in ["Мади", "Алишер"]
            ])
            iz_1 = self._safe_find_metric(data_period_aggregated, "ИНТЕРНЕТ ЗАЯВКИ - ИЗ")
            iz_2 = self._safe_find_metric(data_period_aggregated, "ИНТЕРНЕТ ЗАЯВКИ Импортер - ИЗ")
            print(f"IZ-1: {iz_1}, IZ-2: {iz_2}")

            total_traffic = forwarded + iz_1 + iz_2
            print(f"TOTAL_TRAFFIC: {total_traffic}")

            visits = self._safe_find_metric(data_period_aggregated, "Визиты")
            test_drives = self._safe_find_metric(data_period_aggregated, "Тест-драйвы")
            commercial_offers = self._safe_find_metric(data_period_aggregated, "КОММЕРЧЕСКОЕ ПРЕДЛОЖЕНИЕ")

            contracts = data_period_aggregated[
                data_period_aggregated['Показатель'].str.contains('Контракт', na=False, case=False)
            ]['Сумма за период'].sum()

            deliveries = data_period_aggregated[
                data_period_aggregated['Показатель'].str.contains('Выдач', na=False, case=False)
            ]['Сумма за период'].sum()

            funnel_labels = ["Общий трафик", "Визиты", "Тест-драйвы", "Коммерческие предложения", "Контракты", "Выдачи"]
            funnel_values = [total_traffic, visits, test_drives, commercial_offers, contracts, deliveries]

            fig_funnel = go.Figure(go.Funnel(
                y=funnel_labels,
                x=funnel_values,
                textinfo="value+percent previous",
                connector={"line": {"color": "gray", "width": 2}}
            ))
            fig_funnel.update_layout(title="Воронка продаж", width=600, height=400)
            print("create_sales_funnel обновлена!")
            return fig_funnel

        except Exception as e:
            logger.error(f"Ошибка создания воронки продаж: {e}")
            return self._empty_figure()

    def create_reasons_bar(self, data_period_aggregated, df):
        """Диаграмма отказов — от 'ОТКАЗЫ' до 'Траты на таргет'"""
        try:
            df = df.copy()
            df.iloc[:, 0] = df.iloc[:, 0].astype(str).str.strip()

            # Найти начало "ОТКАЗЫ"
            start_index = df[df.iloc[:, 0].str.upper() == "ОТКАЗЫ"].index
            if start_index.empty:
                start_row = self.config.REASONS_START_ROW
            else:
                start_row = start_index[0] + 1

            # Найти конец: "Траты на таргет" (или ближайший с таким словом)
            end_index = df[df.iloc[:, 0].str.lower().str.contains("траты на таргет")].index
            if end_index.empty:
                end_row = df.shape[0]
            else:
                end_row = end_index[0]

            # Отрезаем только нужный блок
            df_filtered = df.iloc[start_row:end_row].copy()

            reasons = df_filtered.iloc[:, 0].dropna().astype(str).str.strip()

            values = [self._safe_find_metric(data_period_aggregated, name) for name in reasons]
            final_df = pd.DataFrame({"Причина отказа": reasons.values, "Количество": values})
            final_df = final_df[final_df["Количество"] > 0]

            if len(final_df) > self.config.MAX_REASONS_DISPLAY:
                final_df = final_df.nlargest(self.config.MAX_REASONS_DISPLAY, "Количество")

            final_df = final_df.sort_values("Количество", ascending=True)

            if final_df.empty:
                return self._empty_figure()

            fig_reasons = go.Figure(go.Bar(
                y=final_df["Причина отказа"],
                x=final_df["Количество"],
                orientation='h',
                text=final_df["Количество"],
                textposition='auto',
                marker_color=self.config.COLORS['primary']
            ))
            fig_reasons.update_layout(
                title="Причины отказа",
                height=max(400, len(final_df) * 40),
                margin=dict(l=250, r=40, t=60, b=40)
            )
            return fig_reasons

        except Exception as e:
            logger.error(f"Ошибка создания диаграммы причин отказов: {e}")
            return self._empty_figure()

    def _find_metric_average_daily(self, df_containing_daily_data, metric_name_part, columns_to_average):
        """Поиск среднего дневного значения метрики"""
        if not df_containing_daily_data.empty and columns_to_average:
            try:
                match = df_containing_daily_data[
                    df_containing_daily_data['Показатель'].str.contains(metric_name_part, na=False, case=False)]
                if not match.empty:
                    daily_values = match[columns_to_average].iloc[0]
                    daily_values_cleaned = pd.Series(daily_values).astype(str).str.replace('%', '', regex=False)
                    numeric_daily_values = pd.to_numeric(daily_values_cleaned, errors='coerce').dropna()
                    if numeric_daily_values.empty:
                        return 0
                    if numeric_daily_values.mean() > 1 and numeric_daily_values.max() > 100:
                        return numeric_daily_values.mean() / 100.0
                    else:
                        return numeric_daily_values.mean()
            except Exception:
                pass
        return 0

    def _safe_find_metric(self, df_agg, metric_name_part):
        """Безопасный поиск метрики для диаграмм"""
        if df_agg.empty or 'Показатель' not in df_agg.columns:
            return 0
        try:
            match = df_agg[df_agg['Показатель'].str.contains(metric_name_part, na=False, case=False)]
            if not match.empty:
                value = match['Сумма за период'].iloc[0]
                return pd.to_numeric(value, errors='coerce') or 0
        except Exception:
            pass
        return 0

    def _empty_figure(self):
        """Пустая фигура для случаев ошибок"""
        fig = go.Figure()
        fig.update_layout(
            title="Нет данных для отображения",
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            annotations=[
                dict(
                    text="Данные недоступны",
                    x=0.5, y=0.5,
                    xref="paper", yref="paper",
                    showarrow=False,
                    font=dict(size=16, color="gray")
                )
            ]
        )
        return fig

    def create_calls_trend(self, data_period, selected_cols, day_range, show_trend=True):
        """График динамики звонков по дням"""
        start_day, end_day = day_range
        days = list(range(start_day, end_day + 1))

        # helper to извлечь ряд по метрике
        def get_series(metric_label):
            dfm = data_period[data_period['Показатель'].str.contains(metric_label, na=False, case=False)]
            if dfm.empty:
                return [0] * len(selected_cols)
            return pd.to_numeric(dfm.iloc[0, 1:], errors='coerce').fillna(0).tolist()

        inc = get_series("Входящие звонки - ВЗ")
        acc = get_series("Принятые ВЗ")
        miss = get_series("Непринятые ВЗ")
        fwd = get_series("Переадресованные успешно ВЗ")

        df_chart = pd.DataFrame({
            "День": days,
            "Входящие": inc,
            "Принятые":  acc,
            "Непринятые": miss,
            "Переадресованные": fwd
        })
        df_long = df_chart.melt(id_vars="День", var_name="Категория", value_name="Количество")

        fig = px.line(
            df_long, x="День", y="Количество", color="Категория",
            title=f"Динамика звонков (Дни {start_day}–{end_day})"
        )
        fig.update_layout(xaxis=dict(dtick=1))
        # --- ДОБАВЛЯЕМ ТРЕНД ---
        if show_trend:
            # Скользящее среднее
            window = min(5, len(inc))  # длина окна можно вынести в config
            ma = pd.Series(inc).rolling(window, min_periods=1).mean()
            fig.add_scatter(x=days, y=ma, mode="lines", name="Скользящее среднее", line=dict(dash='dot', color="black"))

            # Линейная регрессия
            if len(days) > 1:
                coeffs = np.polyfit(days, inc, deg=1)
                trend = np.polyval(coeffs, days)
                fig.add_scatter(x=days, y=trend, mode="lines", name="Линейный тренд", line=dict(color="gray"))

        return fig
    def create_calls_funnel(self, data_period_aggregated):
        """Воронка обработки звонков"""
        labels = [
            "Всего входящих",
            "Принятые ВЗ",
            "Переадресованные успешно ВЗ",
            "Непринятые ВЗ"
        ]
        vals = [
            self._safe_find_metric(data_period_aggregated, "Входящие звонки - ВЗ"),
            self._safe_find_metric(data_period_aggregated, "Принятые ВЗ"),
            self._safe_find_metric(data_period_aggregated, "Переадресованные успешно ВЗ"),
            self._safe_find_metric(data_period_aggregated, "Непринятые ВЗ"),
        ]
        fig = go.Figure(go.Funnel(
            y=labels, x=vals,
            textinfo="value+percent previous",
            marker=dict(color=[self.config.COLORS['primary'],
                               self.config.COLORS['success'],
                               self.config.COLORS['warning'],
                               self.config.COLORS['danger']])
        ))
        fig.update_layout(title="Воронка обработки звонков")
        return fig

# === ИНИЦИАЛИЗАЦИЯ ===
try:
    data_processor = DataProcessor(Config.EXCEL_PATH)
    metrics_calculator = MetricsCalculator(data_processor)
    chart_generator = ChartGenerator(Config)

    # Проверка наличия данных
    if data_processor.df.empty:
        logger.warning("Данные не загружены, используются тестовые данные")

except Exception as e:
    logger.error(f"Ошибка инициализации: {e}")
    # Создаем заглушки для предотвращения краха
    data_processor = None

def get_calls_dashboard():
    logging.info("Рендерится layout звонков!")
    return dbc.Container([
        # Контролы
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.Label("Выберите диапазон дней:", className="form-label"),
                        # ——— Google Sheets loader ———
                        dbc.Row([
                            dbc.Col(
                                dbc.Input(id="gsheet-id", placeholder="Google Sheet ID", type="text"),
                                width=6
                            ),
                            dbc.Col(
                                dcc.Dropdown(
                                    id="gsheet-name",
                                    placeholder="Выберите лист",
                                    options=[],
                                    searchable=True,
                                    clearable=False,
                                ),
                                width=6
                            ),
                            dbc.Col(
                                dcc.Dropdown(
                                    id="compare-gsheet-name",
                                    placeholder="Сравнить с...",
                                    options=[],
                                    searchable=True,
                                    clearable=False,
                                ),
                                width=6
                            )
                        ], className="mb-2"),
                        dbc.Row([
                            dbc.Col(
                                dbc.Button("Загрузить Google Sheets", id="load-gsheet", color="primary"),
                                width=12
                            )
                        ], className="mb-4"),
                        # — end Google Sheets loader —

                        dcc.RangeSlider(
                            id="day-range",
                            min=1,
                            max=data_processor.max_day if data_processor else 5,
                            value=[1, min(5, data_processor.max_day if data_processor else 5)],
                            marks={i: str(i) for i in range(1, (data_processor.max_day if data_processor else 5) + 1)},
                            step=1,
                            allowCross=False,
                            tooltip={"placement": "bottom", "always_visible": True}
                        ),
                        html.Div(id="day-range-output", className="mt-2 text-muted"),
                        html.Label("Период сравнения:", className="form-label mt-4"),
                        dcc.RangeSlider(
                            id="compare-range",
                            min=1,
                            max=data_processor.max_day if data_processor else 5,
                            value=[1, min(5, data_processor.max_day if data_processor else 5)],
                            marks={i: str(i) for i in range(1, (data_processor.max_day if data_processor else 5) + 1)},
                            step=1,
                            allowCross=False,
                            tooltip={"placement": "bottom", "always_visible": True}
                        ),
                        html.Div(id="compare-range-output", className="mt-2 text-muted")
                    ])
                ], className="mb-4")
            ], width=12)
        ]),
        dbc.Row([
            dbc.Col(
                dbc.Checkbox(id="show-trend", className="me-2", value=True),
                width="auto"
            ),
            dbc.Col(
                dbc.Label("Показывать тренд", html_for="show-trend"),
                width="auto"
            ),
        ], className="mb-2", align="center"),
        # Ключевые показатели
        html.Div(id="metrics-cards", className="mb-4"),
        # Динамика звонков и воронка обработки
        dbc.Row([
            dbc.Col([
                dcc.Loading(
                    id="loading-calls-trend",
                    children=dcc.Graph(id="calls-trend"),
                    type="default"
                )
            ], width=12, lg=8),
            dbc.Col([
                dcc.Loading(
                    id="loading-calls-funnel",
                    children=dcc.Graph(id="calls-funnel"),
                    type="default"
                )
            ], width=12, lg=4),
        ], className="mb-4"),
        # Диаграммы сотрудников
        dbc.Row([
            dbc.Col([
                dcc.Loading(
                    id="loading-staff-bar",
                    children=dcc.Graph(id="staff-bar"),
                    type="default"
                )
            ], width=12, lg=7),
            dbc.Col([
                dcc.Loading(
                    id="loading-staff-pie",
                    children=dcc.Graph(id="staff-pie"),
                    type="default"
                )
            ], width=12, lg=5),
        ], className="mb-4"),

        # Остальные диаграммы
        dbc.Row([
            dbc.Col([
                dcc.Loading(
                    id="loading-internet-pie",
                    children=dcc.Graph(id="internet-pie"),
                    type="default"
                )
            ], width=12, lg=6),
            dbc.Col([
                dcc.Loading(
                    id="loading-scripts-bar",
                    children=dcc.Graph(id="scripts-bar"),
                    type="default"
                )
            ], width=12, lg=6),
        ], className="mb-4"),

        dbc.Row([
            dbc.Col([
                dcc.Loading(
                    id="loading-sales-funnel",
                    children=dcc.Graph(id="sales-funnel"),
                    type="default"
                )
            ], width=12, lg=6),
            dbc.Col([
                dcc.Loading(
                    id="loading-reasons-bar",
                    children=dcc.Graph(id="reasons-bar"),
                    type="default"
                )
            ], width=12, lg=6),
        ], className="mb-4"),

        # Футер
        dbc.Row([
            dbc.Col([
                html.Hr(),
                html.P(
                    f"Последнее обновление: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                    className="text-muted text-center"
                )
            ], width=12)
        ]),
        # Тост для ошибок
        dbc.Toast(
            id="error-toast",
            header="Ошибка",
            is_open=False,
            duration=5000,
            dismissable=True,
            icon="danger",
            style={"position": "fixed", "top": 10, "right": 10, "width": 350, "zIndex": 9999},
        )

    ], fluid=True, className="px-4", style={"paddingTop": "60px"})
def get_stock_layout():
    print("✅ layout ok")
    logging.info("Рендерится layout склада!")

    return html.Div([
        html.H3("🚗 Аналитика склада"),
        # Блок выбора Google Sheet
        dbc.Row([
            dbc.Col(dbc.Input(id="stock-gsheet-id", placeholder="Google Sheet ID", type="text"), width=5),
            dbc.Col(dcc.Dropdown(id="stock-gsheet-name", placeholder="Выберите лист", options=[]), width=5),
            dbc.Col(dbc.Button("Загрузить", id="load-stock-gsheet", color="primary"), width=2),
        ], className="mb-4"),

        # Фильтры
        dbc.Row([
            dbc.Col([
                html.Label("Фильтр по модели"),
                dcc.Dropdown(id="model-filter", multi=True, value=None),
            ], width=4),
            dbc.Col([
                html.Label("Фильтр по модификации"),
                dcc.Dropdown(id="mod-filter", multi=True, value=None),
            ], width=4),
            dbc.Col([
                html.Label("Фильтр по статусу"),
                dcc.Dropdown(id="status-filter", multi=True, value=None),
            ], width=4)
        ], className="mb-4"),
        dbc.Row([
            dbc.Col([
                dcc.Interval(
                    id="stock-debounce-timer",
                    interval=800,
                    n_intervals=0,
                    disabled=True
                )
            ])
        ]),
        # Диаграмма
        dbc.Row([
            dbc.Col(dcc.Graph(id="stock-bar-chart"))
        ]),
        # КОНТРОЛ для выбора количества строк
        dbc.Row([
            dbc.Col([
                html.Label("Показывать строк на странице:"),
                dcc.Input(id="table-page-size", type="number", value=20, min=5, max=100, step=1, style={"width": "100px"})
            ], width=3),
        ], className="mb-2"),
        # Таблица
        dbc.Row([
            dbc.Col(
                dcc.Loading(
                    dash.dash_table.DataTable(
                        id="stock-table",
                        columns=[
                            {"name": "№ кузова", "id": "№ кузова"},
                            {"name": "№ заказа", "id": "№ заказа"},
                            {"name": "Год выпуска", "id": "Год выпуска"},
                            {"name": "Модель_корень_корень", "id": "Модель_корень_корень"},
                            {"name": "Модификация", "id": "Модификация"},
                            {"name": "Статус", "id": "Статус"},
                        ],
                        style_table={"overflowX": "auto"},
                        page_size=20
                    )
                )
            )
        ])
    ])

@app.callback(
    Output("tab-content", "children"),
    Input("main-tabs", "active_tab"),
)
def render_tab_content(tab):
    import logging
    logging.info(f"Рендер вкладки: {tab}")
    if tab == "tab-calls":
        return get_calls_dashboard()
    elif tab == "tab-stock":
        return get_stock_layout()
    return html.Div("Ошибка: вкладка не найдена")
app.layout = dbc.Container([
    dcc.Store(id="stock-data"),
    dcc.Store(id="stock-filters"),
    dcc.Store(id="main-df"),
    dcc.Store(id="compare-df"),
    dbc.Tabs(
        id="main-tabs",
        active_tab="tab-calls",
        children=[
            dbc.Tab(label="📞 Дашборд звонков", tab_id="tab-calls"),
            dbc.Tab(label="🚗 Аналитика склада", tab_id="tab-stock"),
        ]
    ),
    html.Div(id="tab-content")
], fluid=True)


@app.callback(
    Output("stock-data", "data"),
    Input("load-stock-gsheet", "n_clicks"),
    State("stock-gsheet-id", "value"),
    State("stock-gsheet-name", "value"),
    prevent_initial_call=True
)
def load_stock_to_store(n, sid, sname):
    if not n or not sid or not sname:
        logging.warning("Не все параметры указаны для загрузки склада!")

        raise PreventUpdate

    # Загружаем данные — у тебя обычно это loader.load
    df = loader.load(sid, sname, force_reload=True)
    df.columns = df.columns.str.strip()  # чистим имена столбцов от пробелов
    logging.info(f"Столбцы после загрузки: {df.columns.tolist()}")

    # --- Новый код для моделей, модификаций, статуса ---
    root_models = ["718", "911", "Cayenne", "Panamera", "Macan", "Taycan"]

    def extract_model_and_mod(row):
        value = str(row["Модель"]).strip()
        for root in root_models:
            if value.startswith(root):
                model = root
                mod = value[len(root):].strip()
                return pd.Series([model, mod if mod else "Base"])
        return pd.Series([None, None])

    df[["Модель_корень_корень", "Модификация"]] = df.apply(extract_model_and_mod, axis=1)

    # Определяем статус через "шапки"
    status_col = []
    current_status = None

    for idx, row in df.iterrows():
        if pd.isnull(row["Модель_корень_корень"]):  # если не Модель_корень_корень — это шапка
            current_status = " ".join([
                str(row[c]).strip() for c in ["№ кузова", "Модель_корень_корень", "Объем двиг"]
                if pd.notnull(row[c]) and str(row[c]).strip()
            ])
            status_col.append(None)
        else:
            status_col.append(current_status)

    df["Статус"] = status_col

    # Всё, отдаём Store как всегда:
    return df.to_dict("records")

@app.callback(
    Output("stock-filters", "data"),
    [
        Input("model-filter", "value"),
        Input("mod-filter", "value"),
        Input("status-filter", "value")
    ],
    prevent_initial_call=True
)
def save_stock_filters(model_val, mod_val, status_val):
    return {
        "model": model_val,
        "mod": mod_val,
        "status": status_val
    }

def refresh_gsheet_data(n_clicks):
    # Загружаем с флагом обновления
    df = loader.load("SHEET_ID", "SHEET_NAME", force_reload=True)
    return df.to_dict('records')

@app.callback(
    Output("stock-gsheet-name", "options"),
    Input("stock-gsheet-id", "value"),
    prevent_initial_call=True
)
def update_stock_worksheet_dropdown(sheet_id):
    if not sheet_id:
        raise PreventUpdate

    try:
        creds = Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES
        )
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(sheet_id)
        sheets = spreadsheet.worksheets()
        return [{"label": ws.title, "value": ws.title} for ws in sheets]
    except Exception as e:
        logger.error(f"Ошибка при получении листов из Google Sheets: {e}")
        return []
@app.callback(
    Output("gsheet-name", "options"),
    Input("gsheet-id", "value"),
    prevent_initial_call=True
)
def update_gsheet_name(sheet_id):
    if not sheet_id:
        raise PreventUpdate
    try:
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(sheet_id)
        sheets = spreadsheet.worksheets()
        return [{"label": ws.title, "value": ws.title} for ws in sheets]
    except Exception as e:
        logger.error(f"Ошибка при получении листов из Google Sheets: {e}")
        return []

@app.callback(
    Output("compare-gsheet-name", "options"),
    Input("gsheet-id", "value"),
    prevent_initial_call=True
)
def update_compare_worksheet_dropdown(sheet_id):
    # Просто повтори код из update_worksheet_dropdown!
    # Или, если листы одинаковые — можно вернуть то же самое.
    if not sheet_id:
        raise PreventUpdate
    try:
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(sheet_id)
        sheets = spreadsheet.worksheets()
        return [{"label": ws.title, "value": ws.title} for ws in sheets]
    except Exception as e:
        logger.error(f"Ошибка при получении листов из Google Sheets: {e}")
        return []


@app.callback(
    [Output("day-range", "max"),
     Output("day-range", "marks"),
     Output("day-range", "value"),
     Output("error-toast", "is_open"),
     Output("error-toast", "children"),
     Output("main-df", "data"),  # <-- добавь
     Output("compare-df", "data")],
    [Input("load-gsheet", "n_clicks")],
    [State("gsheet-id", "value"),
     State("gsheet-name", "value"),
     State("compare-gsheet-name", "value")]
)


def load_gsheet(n_clicks, sheet_id, sheet_name, compare_sheet_name):
    print("💾 Загружаем GSheet:", sheet_id, sheet_name)
    global data_processor

    if not n_clicks or not sheet_id or not sheet_name:
        raise PreventUpdate

    # Создаём loader один раз
    try:
        df_main = loader.load(sheet_id, sheet_name, force_reload=True)
        df_compare = loader.load(sheet_id, compare_sheet_name,
                                 force_reload=True) if compare_sheet_name else pd.DataFrame()
        logger.info(f"Загружаем Google Sheet: {sheet_id}, лист: {sheet_name}")

        data_processor.load_from_gsheet(df_main, sheet_name)

        max_day = data_processor.max_day
        marks = {i: str(i) for i in range(1, max_day + 1)}
        default_end = min(5, max_day)

        return (
            max_day,
            marks,
            [1, default_end],
            False,
            "",
            df_main.to_dict("records") if df_main is not None else [],
            df_compare.to_dict("records") if df_compare is not None else [],
        )

    except Exception as e:
        logger.error(f"Ошибка загрузки Google Sheets: {e}")
        return dash.no_update, dash.no_update, dash.no_update, True, f"Ошибка загрузки Google Sheets: {e}"


@app.callback(
    [
        Output("day-range-output", "children"),
        Output("metrics-cards", "children"),
        Output("calls-trend", "figure"),
        Output("calls-funnel", "figure"),
        Output("staff-bar", "figure"),
        Output("staff-pie", "figure"),
        Output("internet-pie", "figure"),
        Output("scripts-bar", "figure"),
        Output("sales-funnel", "figure"),
        Output("reasons-bar", "figure"),
    ],
    [
        Input("main-df", "data"),
        Input("compare-df", "data"),
        Input("day-range", "value"),
        Input("compare-range", "value"),
        Input("show-trend", "value")],
       [ State("gsheet-name", "value"),
        State("compare-gsheet-name", "value"),
    ]
)
def update_dashboard(main_df, compare_df, day_range, compare_range, show_trend, main_sheet_name, compare_sheet_name):
    if metrics_calculator is None:
        logger.error("metrics_calculator не инициализирован!")
        return (
            "Ошибка инициализации",
            html.Div("Нет метрик — попробуйте перезапустить"),
            *[chart_generator._empty_figure() for _ in range(8)]
        )
    try:
        df_main = pd.DataFrame(main_df)
        df_compare = pd.DataFrame(compare_df) if compare_df else pd.DataFrame()

        # Создаём два независимых DataProcessor
        main_processor = DataProcessor(excel_path="fake.xlsx")
        main_processor.load_from_gsheet(df_main, main_sheet_name)
        compare_processor = DataProcessor(excel_path="fake.xlsx")
        compare_processor.load_from_gsheet(df_compare, compare_sheet_name)

        # Обработка данных для выбранных диапазонов
        data_period, data_period_aggregated = main_processor.process_data(day_range)
        metrics_current = metrics_calculator.calculate_key_metrics(data_period_aggregated)

        compare_period, compare_period_aggregated = compare_processor.process_data(compare_range)
        metrics_prev = metrics_calculator.calculate_key_metrics(compare_period_aggregated)

        metrics_cards = create_comparison_metrics_cards(metrics_current, metrics_prev)

        if data_period_aggregated.empty:
            return ("Нет данных для выбранного периода",
                    html.Div("Выберите другой диапазон"),
                    *[chart_generator._empty_figure() for _ in range(8)])

        start_day, end_day = day_range
        # Получаем selected_cols для скриптов
        day_cols = main_processor.data_columns[start_day - 1:end_day]
        normalized_day_cols = [main_processor.normalize_date(col) for col in day_cols]
        col_mapping = {main_processor.normalize_date(col): col for col in main_processor.df.columns[2:]}
        selected_cols = [col_mapping[col] for col in normalized_day_cols if col in col_mapping]

        # Все графики и метрики — через main_processor/data_period/data_period_aggregated!
        fig_staff, fig_pie = chart_generator.create_staff_charts(data_period_aggregated)
        fig_iz = chart_generator.create_internet_pie(data_period_aggregated)
        fig_script = chart_generator.create_scripts_bar(data_period, data_period_aggregated, selected_cols)
        fig_funnel2 = chart_generator.create_sales_funnel(data_period_aggregated)
        fig_reasons = chart_generator.create_reasons_bar(data_period_aggregated, main_processor.df)
        fig_calls_trend = chart_generator.create_calls_trend(data_period, selected_cols, day_range, show_trend=show_trend)
        fig_calls_funnel = chart_generator.create_calls_funnel(data_period_aggregated)

        return (
            f"📅 Выбран период: {start_day} — {end_day} день{'ей' if end_day > 4 else 'я'}",
            metrics_cards,
            fig_calls_trend,
            fig_calls_funnel,
            fig_staff,
            fig_pie,
            fig_iz,
            fig_script,
            fig_funnel2,
            fig_reasons
        )

    except Exception as e:
        logger.error(f"Ошибка в callback: {e}")
        return ("Ошибка обработки данных", html.Div("Произошла ошибка"),
                *[chart_generator._empty_figure() for _ in range(8)])


def make_bar_chart(df):
    import plotly.express as px
    if df.empty:
        return px.histogram()
    return px.histogram(
        df, x="Статус", color="Модификация",
        title="📦 Автомобили по статусам", barmode="group"
    )


@app.callback(
    Output("stock-table", "page_size"),
    Input("table-page-size", "value"),
)
def update_page_size(val):
    return val or 20



@app.callback(
    [
        Output("model-filter", "options"),
        Output("model-filter", "value"),
        Output("mod-filter", "options"),
        Output("mod-filter", "value"),
        Output("status-filter", "options"),
        Output("status-filter", "value"),
    ],
    [
        Input("stock-data", "data"),
        Input("stock-filters", "data"),
        Input("model-filter", "value"),
    ],
    prevent_initial_call=True
)
def update_and_sync_filters(stock_data, stock_filter_data, model_value):
    import pandas as pd
    import dash
    ctx = dash.callback_context

    # Безопасная инициализация DataFrame
    df = pd.DataFrame(stock_data) if stock_data else pd.DataFrame()
    df = df[df["№ кузова"].str.match(r"WP.*")]

    # Фильтрация моделей/статусов/модификаций без None/nan/пустых строк
    def clean(values):
        return [v for v in values if isinstance(v, str) and v.strip() and v != "nan"]

    if not df.empty:
        model_opts = [{"label": m, "value": m} for m in sorted(clean(df.get("Модель_корень_корень", pd.Series([])).dropna().unique()))]
        status_opts = [{"label": s, "value": s} for s in sorted(clean(df.get("Статус", pd.Series([])).dropna().unique()))]
    else:
        model_opts = []
        status_opts = []

    try:
        # Если triggered — загрузка из Store (например, возврат на вкладку)
        if ctx.triggered and ctx.triggered[0]['prop_id'].startswith("stock-filters"):
            mval = stock_filter_data.get("model") if stock_filter_data else []
            modval = stock_filter_data.get("mod") if stock_filter_data else []
            sval = stock_filter_data.get("status") if stock_filter_data else []

            if mval and not df.empty and "Модификация" in df.columns and "Модель_корень_корень" in df.columns:
                filtered = df[df["Модель_корень_корень"].isin(mval)]
                mod_opts = [{"label": m, "value": m} for m in sorted(clean(filtered["Модификация"].dropna().unique()))]
            elif not df.empty and "Модификация" in df.columns:
                mod_opts = [{"label": m, "value": m} for m in sorted(clean(df["Модификация"].dropna().unique()))]
            else:
                mod_opts = []
            return model_opts, mval, mod_opts, modval, status_opts, sval

        # Если изменили модель
        if ctx.triggered and ctx.triggered[0]['prop_id'].startswith("model-filter"):
            if not model_value or df.empty or "Модификация" not in df.columns or "Модель_корень_корень" not in df.columns:
                mod_opts = []
                return model_opts, model_value, mod_opts, [], status_opts, dash.no_update
            filtered = df[df["Модель_корень_корень"].isin(model_value)]
            mod_opts = [{"label": m, "value": m} for m in sorted(clean(filtered["Модификация"].dropna().unique()))]
            return model_opts, model_value, mod_opts, [], status_opts, dash.no_update

        # По умолчанию (например, просто загрузили данные)
        if not df.empty and "Модификация" in df.columns:
            mods = clean(df["Модификация"].dropna().unique())
            mod_opts = [{"label": m, "value": m} for m in sorted(mods)]
        else:
            mod_opts = []

        return model_opts, dash.no_update, mod_opts, dash.no_update, status_opts, dash.no_update

    except Exception as e:
        import logging
        logging.error(f"Ошибка в update_and_sync_filters: {e}")
        # Возвращаем корректное количество dash.no_update
        return [dash.no_update] * 6


@app.callback(
    [Output("stock-bar-chart", "figure"),
     Output("stock-table", "data"),
     Output("stock-debounce-timer", "disabled")],
    [
        Input("stock-debounce-timer", "n_intervals"),
        Input("model-filter", "value"),
        Input("mod-filter", "value"),
        Input("status-filter", "value")
    ],
    [
        State("stock-data", "data"),
        State("model-filter", "value"),
        State("mod-filter", "value"),
        State("status-filter", "value")
    ],
    prevent_initial_call=True
)
def filter_stock_debounced(
    n_intervals, model_val, mod_val, status_val,
    stock_data, s_model, s_mod, s_status
):
    import pandas as pd
    ctx = dash.callback_context
    triggered_id = ctx.triggered[0]["prop_id"].split(".")[0] if ctx.triggered else None

    if triggered_id in ["model-filter", "mod-filter", "status-filter"]:
        return dash.no_update, dash.no_update, False

    if triggered_id == "stock-debounce-timer":
        if not stock_data:
            raise dash.exceptions.PreventUpdate
        df = pd.DataFrame(stock_data)
        df.columns = df.columns.str.strip()
        if s_model:
            df = df[df["Модель_корень_корень"].isin(s_model)]
        if s_mod:
            df = df[df["Модификация"].isin(s_mod)]
        if s_status:
            df = df[df["Статус"].isin(s_status)]
        fig = make_bar_chart(df)
        return fig, df.to_dict("records"), True
    raise dash.exceptions.PreventUpdate


def extract_mod(model):
    if "GTS" in model:
        return "GTS"
    elif "Turbo" in model:
        return "Turbo"
    elif "S" in model:
        return "S"
    return "Base"


if __name__ == "__main__":
    webbrowser.open("http://127.0.0.1:8050")
    app.run(debug=False, use_reloader=False, host='0.0.0.0', port=8050)
