import akshare as ak
import pandas as pd


def get_stock_history(symbol: str, start_date: str, end_date: str, adjust: str = "qfq") -> pd.DataFrame:
    """
    获取指定股票的历史行情数据，并进行基础清洗：
    - 重命名日期/收盘列为英文
    - 将收盘价转为数值并去除缺失
    """
    df = ak.stock_zh_a_hist(symbol=symbol, period="daily",
                            start_date=start_date, end_date=end_date, adjust=adjust)
    df.rename(columns={"日期": "date", "收盘": "close"}, inplace=True)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["close"]).reset_index(drop=True)
    return df
