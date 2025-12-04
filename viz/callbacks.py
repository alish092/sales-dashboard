from dash import Input, Output, State
from dash.exceptions import PreventUpdate

from config import Config
from core.loaders.gsheet_loader import GoogleSheetsLoader


loader = GoogleSheetsLoader(
    service_account_file=Config.SERVICE_ACCOUNT_FILE,
    scopes=Config.SCOPES,
)


def register_callbacks(app):

    # 1) Подгружаем список листов в оба дропдауна
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
            # тут ВАЖНО: возвращаем ДВА списка, а не dict
            return options, options

        except Exception as e:
            print(f"[GSHEET ERROR] {e}")
            # при ошибке тоже нужно вернуть ДВА списка
            return [], []

    # 2) По выбранному листу грузим данные в main-df
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
            # Кладём в Store список dict'ов (как в старой версии)
            return df.to_dict("records")

        except Exception as e:
            print(f"[GSHEET LOAD ERROR] {e}")
            return []
