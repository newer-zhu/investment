import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import time
from pathlib import Path
import pandas as pd
from tqdm import tqdm
import tushare as ts
from utils.util import tushare_to_qlib 

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SOURCE_PATH = PROJECT_ROOT / "data" / "cache" / "stock_basic.csv"

# ======================
# 1. TuShare 初始化
# ======================
token = "2f80f707c09dc4ce6c59eb215739349f7a203485b19b4401a683bb6549ec"

pro = ts.pro_api(token)

pro._DataApi__token = token # 保证有这个代码，不然不可以获取
pro._DataApi__http_url = 'http://lianghua.nanyangqiankun.top'  # 保证有这个代码，不然不可以获取

# ======================
# 2. 路径配置
# ======================
OUT_DIR = PROJECT_ROOT / "data" / "fundamental" / "fina_indicator"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ======================
# 3. 核心字段（raw layer）
# ======================
CORE_COLS = [
    "ann_date", "end_date",
    "roe", "roe_dt", "roa", "roic",
    "grossprofit_margin", "netprofit_margin", "profit_to_gr",
    "or_yoy", "tr_yoy",
    "netprofit_yoy", "dt_netprofit_yoy", "roe_yoy",
    "debt_to_assets", "current_ratio", "quick_ratio",
    "interestdebt", "netdebt",
    "ocf_yoy", "fcff", "fcfe",
]

# ======================
# 3.5. 期望的输出列顺序
# ======================
DESIRED_COLUMNS = [
    "tradedate", "symbol", "end_date", "roe", "roe_dt", "roa", "roic",
    "grossprofit_margin", "netprofit_margin", "profit_to_gr", "or_yoy", 
    "tr_yoy", "netprofit_yoy", "dt_netprofit_yoy", "roe_yoy", 
    "debt_to_assets", "current_ratio", "quick_ratio", "interestdebt", 
    "netdebt", "ocf_yoy", "fcff", "fcfe"
]

# ======================
# 4. 下载单只股票（全量）
# ======================
def download_one(ts_code: str) -> pd.DataFrame | None:
    df = pro.fina_indicator(ts_code=ts_code)
    if df is None or df.empty:
        return None

    keep_cols = [c for c in CORE_COLS if c in df.columns]
    df = df[keep_cols].copy()

    # ===== 时间处理（关键）=====
    df["ann_date"] = pd.to_datetime(df["ann_date"], errors="coerce")
    df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce")
    df = df.dropna(subset=["ann_date", "end_date"])

    # 统一成 yyyy-mm-dd
    df["ann_date"] = df["ann_date"].dt.strftime("%Y-%m-%d")
    df["end_date"] = df["end_date"].dt.strftime("%Y-%m-%d")

    # 重命名 ann_date 为 tradedate
    df = df.rename(columns={"ann_date": "tradedate"})

    return df



# ======================
# 5. 主循环（增量 patch）
# ======================
df_stock = pd.read_csv(SOURCE_PATH, dtype=str)
ts_codes = df_stock["ts_code"].dropna().unique().tolist()

print(f"Total stocks: {len(ts_codes)}")

for ts_code in tqdm(ts_codes):
    try:
        qlib_code = tushare_to_qlib(ts_code)
        out_file = OUT_DIR / f"{qlib_code}-fi.csv"

        df_new = download_one(ts_code)
        if df_new is None or df_new.empty:
            continue

        df_new["symbol"] = qlib_code
        
        # Ensure columns are in the correct order
        df_new = df_new[DESIRED_COLUMNS] if all(col in df_new.columns for col in DESIRED_COLUMNS) else df_new
        if out_file.exists():
            df_old = pd.read_csv(out_file)
            df_old["tradedate"] = pd.to_datetime(df_old["tradedate"])
            df_old["end_date"] = pd.to_datetime(df_old["end_date"])

            df_all = pd.concat([df_old, df_new], ignore_index=True)

            # 🔑 核心：基于财报事实去重
            df_all = df_all.drop_duplicates(
                subset=["symbol", "tradedate", "end_date"],
                keep="last",
            )
        else:
            df_all = df_new

        df_all = df_all.sort_values(
            by=["tradedate", "end_date"],
            ascending=[True, True])
        
        # Ensure final output has correct column order
        df_all = df_all[DESIRED_COLUMNS] if all(col in df_all.columns for col in DESIRED_COLUMNS) else df_all
        
        df_all.to_csv(out_file, index=False)

        time.sleep(0.12)  # 稍微保守点

    except Exception as e:
        print(f"[ERROR] {ts_code}: {e}")
        time.sleep(1)
