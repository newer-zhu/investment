import time
from pathlib import Path
import sys

# Ensure package imports work when running this script directly.
# Add the qlib_project folder to sys.path so `from utils.util` and `from constants`
# can be resolved.
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import pandas as pd
from tqdm import tqdm
import tushare as ts
from utils.util import tushare_to_qlib
from constants import SOURCE_PATH

# ======================
# 1. TuShare 初始化
# ======================
token = "2f80f707c09dc4ce6c59eb215739349f7a203485b19b4401a683bb6549ec"

pro = ts.pro_api(token)

pro._DataApi__token = token # 保证有这个代码，不然不可以获取
pro._DataApi__http_url = 'http://lianghua.nanyangqiankun.top'  # 保证有这个代码，不然不可以获取


SLEEP_SECONDS = 0.35      # 正常请求间隔（建议 0.3~0.5）
ERROR_SLEEP = 5           # 出错后退避时间
STOCKS_PATH = SOURCE_PATH / "cache" / "stock_basic.csv"
OUT_DIR = SOURCE_PATH / "cache" / "income"
OUT_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_START_DATE = ""
END_DATE = ""

INCOME_FIELDS = [
    "ts_code",
    "ann_date",
    "f_ann_date",
    "end_date",
    "report_type",
    "comp_type",

    "basic_eps",
    "diluted_eps",
    "n_income_attr_p",
    "continued_net_profit",

    "revenue",
    "total_revenue",

    "operate_profit",
    "total_profit",
    "ebit",
    "ebitda",

    "sell_exp",
    "admin_exp",
    "fin_exp",
    "rd_exp",

    "invest_income",
    "fv_value_chg_gain",
    "asset_disp_income",

    "income_tax",
    "update_flag",
]

DATE_COLS = ["ann_date", "f_ann_date", "end_date"]
# ======================
# 日期格式转换
# ======================
def format_dates(df: pd.DataFrame) -> pd.DataFrame:
    for col in DATE_COLS:
        if col in df.columns:
            df[col] = (
                pd.to_datetime(df[col], format="%Y%m%d", errors="coerce")
                .dt.strftime("%Y-%m-%d")
            )
    return df

def tushare_to_qlib(code: str) -> str:
    """
    600000.SH -> SH600000
    000001.SZ -> SZ000001
    """
    if "." not in str(code):
        return code
    num, exch = code.split(".")
    return f"{exch.upper()}{num}"

# ======================
# 读取已有文件，确定增量起点
# ======================
def get_incremental_start_date(csv_path: Path) -> str:
    """
    返回 yyyymmdd，用于 tushare start_date
    """
    if not csv_path.exists():
        return DEFAULT_START_DATE

    try:
        df = pd.read_csv(csv_path, usecols=["end_date"])
        if df.empty:
            return DEFAULT_START_DATE

        last_date = (
            pd.to_datetime(df["end_date"], errors="coerce")
            .max()
        )
        if pd.isna(last_date):
            return DEFAULT_START_DATE

        # tushare 用 yyyymmdd
        return last_date.strftime("%Y%m%d")

    except Exception:
        return DEFAULT_START_DATE

# ======================
# 股票列表
# ======================
df_stock = pd.read_csv(STOCKS_PATH, dtype=str)
ts_codes = df_stock["ts_code"].dropna().unique().tolist()

print(f"Total stocks: {len(ts_codes)}")

# ======================
# 主循环
# ======================
for ts_code in tqdm(ts_codes):
    qlib_code = tushare_to_qlib(ts_code)
    out_file = OUT_DIR / f"{qlib_code}.csv"

    start_date = get_incremental_start_date(out_file)

    try:
        df_new = pro.income(
            ts_code=ts_code,
            start_date=start_date,
            end_date=END_DATE,
            fields=",".join(INCOME_FIELDS),
        )

        # 无新数据
        if df_new is None or df_new.empty:
            time.sleep(SLEEP_SECONDS)
            continue

        df_new = df_new.sort_values("end_date")
        df_new = format_dates(df_new)

        # 合并旧数据
        if out_file.exists():
            df_old = pd.read_csv(out_file)
            df_all = (
                pd.concat([df_old, df_new], ignore_index=True)
                .drop_duplicates(subset=["end_date", "report_type"], keep="last")
                .sort_values("end_date")
            )
        else:
            df_all = df_new

        df_all.to_csv(out_file, index=False)

        time.sleep(SLEEP_SECONDS)

    except Exception as e:
        print(f"[ERROR] {ts_code}: {e}")
        time.sleep(ERROR_SLEEP)