import os
import sys
from pathlib import Path
from typing import List

import pandas as pd

# Ensure project root is on sys.path for module imports
CURRENT_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, os.pardir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from db_utils import DbUtils, create_db_engine


def guess_ts_code_from_code(code: str) -> str:
    """
    将6位数字代码猜测转换为 tushare ts_code（可能不完全准确，但覆盖主板/创业板/科创板常见前缀）
    """
    if code.startswith(("600", "601", "603", "605", "688", "689")):
        return f"{code}.SH"
    return f"{code}.SZ"


def normalize_daily_df(df: pd.DataFrame, fallback_ts_code: str) -> pd.DataFrame:
    """
    规范化从缓存CSV读取的日线数据，匹配表 stock_data 所需字段与类型。
    - trade_date: 转换为 YYYY-MM-DD
    - 数值列: 转换为数值类型，缺失填0
    - ts_code: 若缺失则用文件名推断
    """
    needed_cols = [
        "ts_code",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "change",
        "pct_chg",
        "vol",
        "amount",
    ]

    # 添加缺失列
    for c in needed_cols:
        if c not in df.columns:
            df[c] = None

    if df["ts_code"].isna().all():
        df["ts_code"] = fallback_ts_code

    # 日期格式
    df["trade_date"] = pd.to_datetime(df["trade_date"].astype(str), errors="coerce").dt.strftime("%Y-%m-%d")

    # 数值列
    num_cols = ["open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol", "amount"]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    # 仅保留必要列并去重
    df = df[needed_cols].dropna(subset=["ts_code", "trade_date"]).drop_duplicates(subset=["ts_code", "trade_date"])
    return df.reset_index(drop=True)


def load_all_history_csvs(history_dir: str) -> List[Path]:
    p = Path(history_dir)
    if not p.exists():
        return []
    return sorted(p.glob("*.csv"))


def main():
    # DB 连接
    engine = create_db_engine("mysql+pymysql://root:20001030@localhost:3306/finance?charset=utf8mb4")
    db = DbUtils(engine=engine)

    history_dir = os.path.join("cache", "history")
    files = load_all_history_csvs(history_dir)
    if not files:
        print("未找到任何历史数据CSV文件，路径: ", history_dir)
        return

    total_rows = 0
    for f in files:
        try:
            code = f.stem  # 文件名不含扩展名，如 000001
            ts_code = code
            df = pd.read_csv(f, dtype={"trade_date": str})
            if df is None or df.empty:
                continue

            df_norm = normalize_daily_df(df, ts_code)
            if df_norm.empty:
                continue

            # 批量 upsert 到 stock_data（主键: ts_code, trade_date）
            db.upsert_df(
                df=df_norm,
                table_name="stock_data",
                key_columns=["ts_code", "trade_date"],
            )
            total_rows += len(df_norm)
            print(f"✅ 导入 {f.name}: {len(df_norm)} 行")
        except Exception as e:
            print(f"❌ 导入 {f.name} 失败: {e}")

    print(f"完成导入，总计 {total_rows} 行")


if __name__ == "__main__":
    main()
