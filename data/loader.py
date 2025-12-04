# data/loader.py
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from typing import Optional
from config import Config

class DataLoader:
    def __init__(self, excel_path: Optional[str] = None, sa_path: Optional[str] = None):
        self.excel_path = excel_path or Config.EXCEL_PATH
        self.sa_path = sa_path or Config.SERVICE_ACCOUNT_FILE
        self.scopes = getattr(Config, "SCOPES", ["https://www.googleapis.com/auth/spreadsheets.readonly"])

    def load_excel(self, sheet_name: Optional[str] = None) -> pd.DataFrame:
        return pd.read_excel(self.excel_path, sheet_name=sheet_name) if sheet_name else pd.read_excel(self.excel_path)

    def load_gsheet(self, sheet_id: str, worksheet: str) -> pd.DataFrame:
        creds = Credentials.from_service_account_file(self.sa_path, scopes=self.scopes)
        client = gspread.authorize(creds)
        ws = client.open_by_key(sheet_id).worksheet(worksheet)
        values = ws.get_all_values()
        return pd.DataFrame(values[1:], columns=values[0])
