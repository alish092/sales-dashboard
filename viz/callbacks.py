from dash import Input, Output, State
from dash.exceptions import PreventUpdate
from config import Config
from core.loaders.gsheet_loader import GoogleSheetsLoader


loader = GoogleSheetsLoader(
    service_account_file=Config.SERVICE_ACCOUNT_FILE,
    scopes=Config.SCOPES
)


def register_callbacks(app):

    @app.callback(
        [
            Output("gsheet-name", "options"),
            Output("compare-gsheet-name", "options"),
        ],
        Input("load-gsheet", "n_clicks"),
        State("gsheet-id", "value"),
        prevent_initial_call=True,
    )
    # @app.callback(
    #     Output("gsheet-data-store", "data"),
    #     Input("gsheet-name", "value"),
    #     State("gsheet-id", "value"),
    #     prevent_initial_call=True,
    # )
    def load_selected_sheet(worksheet_name, sheet_id):
        if not worksheet_name or not sheet_id:
            raise PreventUpdate

        try:
            data = loader.load_sheet(sheet_id, worksheet_name)
            return {"data": data}

        except Exception as e:
            print(f"[GSHEET LOAD ERROR] {e}")
            return {"data": []}

    def load_gsheet_worksheets(n_clicks, sheet_id):
        if not sheet_id:
            raise PreventUpdate

        try:
            sheet_names = loader.list_sheets(sheet_id)
            opts = [{"label": s, "value": s} for s in sheet_names]
            return opts, opts

        except Exception as e:
            print(f"[GSHEET ERROR] {e}")
            return [], []

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
            data = loader.load_sheet(sheet_id, worksheet_name)
            # data сейчас список словарей/строк, кладём как есть
            return {"rows": data}

        except Exception as e:
            print(f"[GSHEET LOAD ERROR] {e}")
            return {"rows": []}