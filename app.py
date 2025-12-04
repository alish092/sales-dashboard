import dash
from dash import Dash
import dash_bootstrap_components as dbc
from core.loaders.gsheet_loader import GoogleSheetsLoaderдава

from viz.layout import build_layout
from viz.callbacks import register_callbacks


# Создаём Dash-приложение
app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True,
    title="Sales Dashboard"
)

# Подключаем layout
app.layout = build_layout()

# Регистрируем callbacks
register_callbacks(app)

# WSGI-сервер (для gunicorn/uwsgi и т.п.)
server = app.server


if __name__ == "__main__":
    app.run_server(debug=True, host="0.0.0.0", port=8060)

