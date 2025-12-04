class Config:
    EXCEL_PATH = r"C:\Users\a.shmanov\Documents\ЭЦП\Шманов Алишер\sales_dashboard_clean\resources\диаграмма.xlsx"
    SERVICE_ACCOUNT_FILE = r"C:\Users\a.shmanov\Documents\ЭЦП\Шманов Алишер\sales_dashboard_clean\resources\service_account.json"  # вынеси из проекта
    STAFF_NAMES = ["Мади", "Ильяс"]
    REASONS_START_ROW = 37
    MAX_REASONS_DISPLAY = 10
    COLORS = {'primary': '#1976D2','success':'green','warning':'orange','danger':'red','light':'#f8f9fa'}
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly","https://www.googleapis.com/auth/drive.readonly"]
