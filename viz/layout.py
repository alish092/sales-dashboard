# viz/layout.py
from dash import dcc, html
import dash_bootstrap_components as dbc

def build_layout():
    return dbc.Container([
        dcc.Store(id="main-df"),
        dcc.Store(id="compare-df"),

        dbc.Row([
            dbc.Col(
                dcc.Input(
                    id="gsheet-id",
                    type="text",
                    placeholder="Google Sheet ID",
                    value=""  # ключевое — сразу даём value
                ),
                width=5,
            ),
            dbc.Col(dcc.Dropdown(id="gsheet-name", placeholder="Лист"), width=4),
            dbc.Col(dcc.Dropdown(id="compare-gsheet-name", placeholder="Сравнить с..."), width=3),
        ], className="mb-3"),

        dbc.Button("Загрузить Google Sheets", id="load-gsheet", color="primary"),
        dcc.RangeSlider(id="day-range", min=1, max=5, value=[1,5]),
        dcc.RangeSlider(id="compare-range", min=1, max=5, value=[1,5]),
        html.Div(id="day-range-output"),

        html.Div(id="metrics-cards"),
        dcc.Graph(id="calls-trend"),
        dcc.Graph(id="calls-funnel"),
        dcc.Graph(id="staff-bar"),
        dcc.Graph(id="staff-pie"),
        dcc.Graph(id="internet-pie"),
    ], fluid=True)
