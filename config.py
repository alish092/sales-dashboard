import os


class Config:
    # База проекта: папка, где лежит config.py
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    # Папка с ресурсами (Excel, service_account.json и т.д.)
    RESOURCES_DIR = os.path.join(BASE_DIR, "resources")

    # Пути к файлам
    EXCEL_PATH = os.path.join(RESOURCES_DIR, "диаграмма.xlsx")
    SERVICE_ACCOUNT_FILE = os.path.join(RESOURCES_DIR, "service_account.json")  # локально, в git НЕ коммитим

    # Остальные настройки
    STAFF_NAMES = ["Мади", "Ильяс"]
    REASONS_START_ROW = 37
    MAX_REASONS_DISPLAY = 10

    COLORS = {
        "primary": "#1976D2",
        "success": "green",
        "warning": "orange",
        "danger": "red",
        "light": "#f8f9fa",
    }

    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
