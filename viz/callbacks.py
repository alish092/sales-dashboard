# viz/callbacks.py

from dash import Input, Output, State
from dash.exceptions import PreventUpdate

from config import Config
from core.loaders.gsheet_loader import GoogleSheetsLoader


# Один общий лоадер для всего приложения
loader = GoogleSheetsLoader(
    service_account_file=Config.SERVICE_ACCOUNT_FILE,
    scopes=Config.SCOPES,
)


def register_callbacks(app):
    """
    Регистрируем ВСЕ коллбеки здесь.
    Пока делаем только:
    1) загрузка списка листов в два дропдауна
    2) загрузка выбранного листа в main-df
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
            # если не введён ID — ничего не делаем
            raise PreventUpdate

        try:
            sheet_names = loader.list_sheets(sheet_id)
            options = [{"label": name, "value": name} for name in sheet_names]
            # ДВА списка, как требует Dash
            return options, options

        except Exception as e:
            print(f"[GSHEET ERROR] {e}")
            # При ошибке возвращаем пустые списки, но не валим приложение
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

            # На всякий случай убираем дубли колонок
            if df.columns.duplicated().any():
                df = df.loc[:, ~df.columns.duplicated()].copy()

            # dcc.Store ждёт JSON-сериализуемый объект → records
            return df.to_dict("records")

        except Exception as e:
            print(f"[GSHEET LOAD ERROR] {e}")
            # При ошибке просто кладём пустой список, а не валим сервер
            return []
