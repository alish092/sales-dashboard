# viz/callbacks.py

from dash import Input, Output, State
from dash.exceptions import PreventUpdate

import pandas as pd
import plotly.graph_objects as go
import re

from config import Config
from core.loaders.gsheet_loader import GoogleSheetsLoader


# Один общий лоадер для всего приложения
loader = GoogleSheetsLoader(
    service_account_file=Config.SERVICE_ACCOUNT_FILE,
    scopes=Config.SCOPES,
)


def register_callbacks(app):
    """
    Коллбеки:
    1) загрузка списка листов в два дропдауна
    2) загрузка выбранного листа в main-df
    3) построение графика calls-trend по строкам ВЗ
    """

    # 1. Загружаем список листов
    @app.callback(
        [
            Output("gsheet-name", "options"),
            Output("compare-gsheet-name", "options"),
        ],
        Input("load-gsheet", "n_clicks"),
        State("gsheet-id", "value"),
        prevent_initial_call=True,
    )
    def load_gsheet_worksheets(n_clicks, sheet_id):
        if not sheet_id:
            raise PreventUpdate

        try:
            sheet_names = loader.list_sheets(sheet_id)
            options = [{"label": name, "value": name} for name in sheet_names]
            return options, options
        except Exception as e:
            print(f"[GSHEET ERROR] {e}")
            return [], []

    # 2. Загружаем выбранный лист и кладём его в main-df
    @app.callback(
        Output("main-df", "data"),
        Input("gsheet-name", "value"),
        State("gsheet-id", "value"),
        prevent_initial_call=True,
    )
    def load_selected_sheet(worksheet_name, sheet_id):
        if not worksheet_name or not sheet_id:
            raise PreventUpdate

        try:
            df = loader.load_sheet(sheet_id, worksheet_name)
            if df is None:
                return []

            # убираем дубли колонок на всякий случай
            if df.columns.duplicated().any():
                df = df.loc[:, ~df.columns.duplicated()].copy()

            return df.to_dict("records")
        except Exception as e:
            print(f"[GSHEET LOAD ERROR] {e}")
            return []

    # 3. Строим график по "ВХОДЯЩИЕ ЗВОНКИ - ВЗ", "Принятые ВЗ", "Пропущенные ВЗ"
    @app.callback(
        Output("calls-trend", "figure"),
        Input("main-df", "data"),
        prevent_initial_call=True,
    )
    def update_calls_trend(data):
        # Базовая фигура, чтобы всегда что-то вернуть
        fig = go.Figure()

        if not data:
            fig.update_layout(title="Нет данных для отображения")
            raise PreventUpdate

        try:
            df = pd.DataFrame(data)

            # Проверяем, что нужные колонки есть
            if "Показатель" not in df.columns:
                fig.update_layout(title="Колонка 'Показатель' не найдена")
                return fig

            # Находим колонки-дни: 01.12, 02.12, 03.12, ...
            day_cols = [
                c for c in df.columns
                if re.match(r"\d{2}\.\d{2}", str(c))
            ]
            if not day_cols:
                fig.update_layout(title="Не найдено колонок с датами (формат 01.12, 02.12, ...)")
                return fig

            # Функция, которая достаёт ряд по названию показателя
            def get_series(metric_name: str):
                row = df[df["Показатель"] == metric_name]
                if row.empty:
                    return None
                s = row.iloc[0][day_cols]
                # приводим к числам
                return pd.to_numeric(s, errors="coerce")

            series_config = [
                ("ВХОДЯЩИЕ ЗВОНКИ - ВЗ", "Входящие ВЗ"),
                ("Принятые ВЗ", "Принятые ВЗ"),
                ("Пропущенные ВЗ", "Пропущенные ВЗ"),
            ]

            for raw_name, label in series_config:
                y = get_series(raw_name)
                if y is not None and not y.isna().all():
                    fig.add_trace(
                        go.Scatter(
                            x=day_cols,
                            y=y,
                            mode="lines+markers",
                            name=label,
                        )
                    )

            if not fig.data:
                fig.update_layout(title="Не удалось найти строки с ВЗ для графика")
            else:
                fig.update_layout(
                    title="Динамика звонков по дням",
                    xaxis_title="Дата",
                    yaxis_title="Количество",
                    margin=dict(l=40, r=20, t=60, b=120),
                    xaxis_tickangle=-45,
                )

            return fig

        except Exception as e:
            # Логируем, но не роняем приложение
            print(f"[CALLS_TREND ERROR] {e}")
            fig.update_layout(title=f"Ошибка построения графика: {e}")
            return fig
