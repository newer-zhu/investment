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
from constants import SOURCE_PATH, TUSHARE_TOKEN

# ======================
# 1. TuShare 初始化
# ======================

pro = ts.pro_api(TUSHARE_TOKEN)
pro._DataApi__http_url = 'http://lianghua.nanyangqiankun.top'  # 保证有这个代码，不然不可以获取


SLEEP_SECONDS = 0.35      # 正常请求间隔（建议 0.3~0.5）
ERROR_SLEEP = 5           # 出错后退避时间
STOCKS_PATH = SOURCE_PATH / "cache" / "stock_basic.csv"
OUT_DIR = SOURCE_PATH / "cache" / "balance"
OUT_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_START_DATE = ""
END_DATE = ""

BALANCE_FIELDS = [
    # 基本信息
    "ts_code",
    "ann_date",
    "f_ann_date",
    "end_date",
    "report_type",
    "comp_type",
    "end_type",

    # 权益 & 股本
    "total_share",
    "capital_reserve",
    "undistr_porfit",
    "surplus_rese",

    # 流动资产
    "money_cap",
    "trad_asset",
    "notes_receiv",
    "accounts_receiv",
    "prepayment",
    "inventories",
    "oth_cur_assets",
    "total_cur_assets",

    # 非流动资产
    "lt_eqt_invest",
    "invest_real_estate",
    "fix_assets",
    "cip",
    "intan_assets",
    "goodwill",
    "defer_tax_assets",
    "oth_nca",
    "total_nca",

    # 资产合计
    "total_assets",

    # 流动负债
    "st_borr",
    "notes_payable",
    "acct_payable",
    "adv_receipts",
    "taxes_payable",
    "oth_payable",
    "non_cur_liab_due_1y",
    "total_cur_liab",

    # 非流动负债
    "lt_borr",
    "bond_payable",
    "lt_payable",
    "defer_tax_liab",
    "oth_ncl",
    "total_ncl",

    # 负债合计
    "total_liab",

    # 所有者权益
    "treasury_share",
    "minority_int",
    "total_hldr_eqy_exc_min_int",
    "total_hldr_eqy_inc_min_int",

    # 负债 + 权益
    "total_liab_hldr_eqy",

    # 其他
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

# ======================
# 增量起点
# ======================
def get_incremental_start_date(csv_path: Path) -> str:
    if not csv_path.exists():
        return DEFAULT_START_DATE

    try:
        df = pd.read_csv(csv_path, usecols=["end_date"])
        if df.empty:
            return DEFAULT_START_DATE

        last_date = pd.to_datetime(df["end_date"], errors="coerce").max()
        if pd.isna(last_date):
            return DEFAULT_START_DATE

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
        df_new = pro.balancesheet(
            ts_code=ts_code,
            start_date=start_date,
            end_date=END_DATE,
            fields=",".join(BALANCE_FIELDS),
        )

        if df_new is None or df_new.empty:
            time.sleep(SLEEP_SECONDS)
            continue

        df_new = df_new.sort_values("end_date")
        df_new = format_dates(df_new)

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
