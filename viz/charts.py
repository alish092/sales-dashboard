# viz/charts.py
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from config import Config

def _safe_value(df_agg: pd.DataFrame, name: str) -> float:
    if df_agg.empty or "Показатель" not in df_agg.columns:
        return 0
    m = df_agg[df_agg["Показатель"].str.contains(name, na=False, case=False)]
    if m.empty:
        return 0
    return float(pd.to_numeric(m["Сумма за период"].iloc[0], errors="coerce") or 0)

def make_staff_charts(df_agg: pd.DataFrame):
    bar_df = pd.DataFrame({
        "Сотрудник": Config.STAFF_NAMES,
        "Переадресовано успешно ВЗ": [_safe_value(df_agg, name) for name in Config.STAFF_NAMES]
    })
    bar_df = bar_df[bar_df["Переадресовано успешно ВЗ"] > 0]

    fig_bar = px.bar(bar_df, x="Сотрудник", y="Переадресовано успешно ВЗ",
                     title="Переадресовано успешно ВЗ по сотрудникам", text_auto=True,
                     color_discrete_sequence=[Config.COLORS['primary']])
    fig_bar.update_layout(showlegend=False)

    fig_pie = px.pie(bar_df, names="Сотрудник", values="Переадресовано успешно ВЗ",
                     title="Распределение переадресаций")
    fig_pie.update_traces(textinfo="percent+value")
    return fig_bar, fig_pie

def make_internet_pie(df_agg: pd.DataFrame):
    labels = ["Дозвонились ИЗ", "Не обработаны ИЗ", "Не дозвонились ИЗ"]
    values = [_safe_value(df_agg, l) for l in labels]
    df = pd.DataFrame({"Статус": labels, "Количество": values})
    df = df[df["Количество"] > 0]
    if df.empty:
        return go.Figure()
    fig = px.pie(df, names="Статус", values="Количество", title="Распределение интернет-заявок (ИЗ)")
    fig.update_traces(textinfo="percent+value")
    return fig

def make_calls_funnel(df_agg: pd.DataFrame):
    labels = ["Всего входящих", "Принятые ВЗ", "Переадресованные успешно ВЗ", "Непринятые ВЗ"]
    vals = [
        _safe_value(df_agg, "Входящие звонки - ВЗ"),
        _safe_value(df_agg, "Принятые ВЗ"),
        _safe_value(df_agg, "Переадресованные успешно ВЗ"),
        _safe_value(df_agg, "Непринятые ВЗ"),
    ]
    return go.Figure(go.Funnel(y=labels, x=vals, textinfo="value+percent previous"))
