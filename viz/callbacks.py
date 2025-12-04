# viz/callbacks.py

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from dash import Input, Output, State
from dash.exceptions import PreventUpdate
import dash
import dash_bootstrap_components as dbc

from data.loader import DataLoader
from core.processor import DataProcessor
from viz.charts import make_calls_funnel, make_staff_charts, make_internet_pie

_loader = DataLoader()

def get_client():
    creds = Credentials.from_service_account_file(_loader.sa_path, scopes=_loader.scopes)
    return gspread.authorize(creds)

def register_callbacks(app):

    @app.callback(
        [
            Output("gsheet-name", "options"),
            Output("compare-gsheet-name", "options"),
        ],
        Input("gsheet-id", "value"),
        prevent_initial_call=True,
    )
    def update_sheet_list(sheet_id):
        if not sheet_id:
            raise PreventUpdate
        try:
            sheet_names = _loader.list_sheets(sheet_id)
            options = [{"label": name, "value": name} for name in sheet_names]
            return options, options
        except Exception:
            # Если что-то пошло не так — просто не трогаем текущие options
            raise PreventUpdate

    @app.callback(
        [
            Output("day-range", "max"),
            Output("day-range", "marks"),
            Output("day-range", "value"),
            Output("main-df", "data"),
            Output("compare-df", "data"),
        ],
        Input("load-gsheet", "n_clicks"),
        State("gsheet-id", "value"),
        State("gsheet-name", "value"),
        State("compare-gsheet-name", "value"),
        prevent_initial_call=True,
    )
    def load_data(n, sheet_id, sheet_name, compare_name):
        if not n or not sheet_id or not sheet_name:
            raise PreventUpdate

        df_main = _loader.load_gsheet(sheet_id, sheet_name)
        df_compare = (
            _loader.load_gsheet(sheet_id, compare_name) if compare_name else pd.DataFrame()
        )

        p_main = DataProcessor()
        p_main.load_from_gsheet(df_main, sheet_name)

        p_comp = DataProcessor()
        p_comp.load_from_gsheet(df_compare, compare_name)

        max_day = p_main.max_day
        marks = {i: str(i) for i in range(1, max_day + 1)}

        return (
            max_day,
            marks,
            [1, min(5, max_day)],
            df_main.to_dict("records"),
            df_compare.to_dict("records"),
        )

    # остальные callbacks оставляешь как есть
