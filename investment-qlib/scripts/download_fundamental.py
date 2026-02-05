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
pro._DataApi__token = token
pro._DataApi__http_url = 'http://lianghua.9vvn.com'

# ======================
# 2. 路径配置
# ======================
POOL_FILE = Path("/mnt/f/Code/investment/investment-qlib/data/source/instruments/my_filtered_pool.txt")

OUT_DIR = Path(
    "/mnt/f/Code/investment/investment-qlib/data/fundamental/fina_indicator"
)
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ======================
# 3. 核心字段
# ======================
CORE_COLS = [
    "ts_code",
    "ann_date",
    "end_date",
    "roe",
    "grossprofit_margin",
    "or_yoy",
    "debt_to_assets",
    "current_ratio",
    "netprofit_margin"
]

# ======================
# 4. 工具函数
# ======================
def qlib_to_tushare(code: str) -> str:
    """
    SH600000 -> 600000.SH
    SZ000001 -> 000001.SZ
    """
    code = code.strip().upper()
    if code.startswith("SH"):
        return code[2:] + ".SH"
    if code.startswith("SZ"):
        return code[2:] + ".SZ"
    raise ValueError(f"Unknown code format: {code}")


def download_one(ts_code: str) -> pd.DataFrame | None:
    """
    下载单只股票【历史全量】财务指标
    """
    df = pro.fina_indicator(ts_code=ts_code)

    if df is None or df.empty:
        return None

    df = df.sort_values(["ann_date"])

    keep_cols = [c for c in CORE_COLS if c in df.columns]
    return df[keep_cols]


# ======================
# 5. 读取自定义股票池
# ======================
with open(POOL_FILE, "r") as f:
    qlib_codes = [line.strip() for line in f if line.strip()]

ts_codes = [qlib_to_tushare(c) for c in qlib_codes]

print(f"Total stocks from pool: {len(ts_codes)}")

# ======================
# 6. 主循环（带文件存在判断）
# ======================
for ts_code in tqdm(ts_codes):
    try:
        out_file = OUT_DIR / f"{ts_code}.csv"

        # ---- 已存在则跳过 ----
        if out_file.exists():
            continue

        df = download_one(ts_code)
        if df is None:
            continue

        df.to_csv(out_file, index=False)
        time.sleep(0.1)  # 控频

    except Exception as e:
        print(f"[ERROR] {ts_code}: {e}")
        time.sleep(1)
