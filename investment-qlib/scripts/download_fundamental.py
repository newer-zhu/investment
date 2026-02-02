import time
from pathlib import Path
import pandas as pd
from tqdm import tqdm
import tushare as ts
# ======================
# 1. TuShare 初始化
# ======================
token = "2f80f707c09dc4ce6c59eb215739349f7a203485b19b4401a683bb6549ec"

pro = ts.pro_api(token)

pro._DataApi__token = token # 保证有这个代码，不然不可以获取
pro._DataApi__http_url = 'http://lianghua.9vvn.com'  # 保证有这个代码，不然不可以获取



# ========== 输出目录 ==========
OUT_DIR = Path("/mnt/f/Code/investment/investment-qlib/data/fundamental/fina_indicator")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ========== 核心字段 ==========
CORE_COLS = [
    "ts_code",
    "ann_date",
    "end_date",
    "net_profit",          # 净利润
    "roe",                 # 净资产收益率
    "gross_margin",        # 销售毛利率
    "net_profit_yoy",      # 净利润同比
    "or_yoy",              # 营业收入同比
    "debt_to_assets",      # 资产负债率
    "current_ratio",       # 流动比率
]

def download_one(ts_code: str) -> pd.DataFrame | None:
    """
    下载单只股票【历史全量】财务指标
    """
    df = pro.fina_indicator(ts_code=ts_code)

    if df is None or df.empty:
        return None

    # 排序（非常重要，后面 dump / 回测都依赖这个）
    df = df.sort_values(["end_date", "ann_date"])

    # 只保留存在的核心列
    keep_cols = [c for c in CORE_COLS if c in df.columns]
    df = df[keep_cols]

    return df


# ========== 股票列表 ==========
stock_basic = pro.stock_basic(
    exchange="",
    list_status="L",
    fields="ts_code"
)
ts_codes = stock_basic["ts_code"].tolist()

print(f"Total stocks: {len(ts_codes)}")

# ========== 主循环 ==========
for ts_code in tqdm(ts_codes):
    try:
        df = download_one(ts_code)
        if df is None:
            continue

        out_file = OUT_DIR / f"{ts_code}.csv"
        df.to_csv(out_file, index=False)

        time.sleep(0.1)  # 控频

    except Exception as e:
        print(f"[ERROR] {ts_code}: {e}")
        time.sleep(1)
