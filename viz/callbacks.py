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
