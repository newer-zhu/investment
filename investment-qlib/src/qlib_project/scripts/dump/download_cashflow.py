import time
from pathlib import Path
import sys

# Ensure package imports work when running this script directly.
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
pro._DataApi__http_url = 'http://lianghua.nanyangqiankun.top'

SLEEP_SECONDS = 0.35
ERROR_SLEEP = 5

STOCKS_PATH = SOURCE_PATH / "cache" / "stock_basic.csv"
OUT_DIR = SOURCE_PATH / "cache" / "cashflow"
OUT_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_START_DATE = ""
END_DATE = ""

# ======================
# Cashflow 字段
# ======================
CASHFLOW_FIELDS = [
    "ts_code",
    "ann_date",
    "f_ann_date",
    "end_date",
    "comp_type",
    "report_type",
    "end_type",

    "net_profit",
    "finan_exp",
    "c_fr_sale_sg",
    "recp_tax_rends",
    "n_depos_incr_fi",
    "n_incr_loans_cb",
    "n_inc_borr_oth_fi",
    "prem_fr_orig_contr",
    "n_incr_insured_dep",
    "n_reinsur_prem",
    "n_incr_disp_tfa",
    "ifc_cash_incr",
    "n_incr_disp_faas",
    "n_incr_loans_oth_bank",
    "n_cap_incr_repur",
    "c_fr_oth_operate_a",
    "c_inf_fr_operate_a",
    "c_paid_goods_s",
    "c_paid_to_for_empl",
    "c_paid_for_taxes",
    "n_incr_clt_loan_adv",
    "n_incr_dep_cbob",
    "c_pay_claims_orig_inco",
    "pay_handling_chrg",
    "pay_comm_insur_plcy",
    "oth_cash_pay_oper_act",
    "st_cash_out_act",
    "n_cashflow_act",

    "oth_recp_ral_inv_act",
    "c_disp_withdrwl_invest",
    "c_recp_return_invest",
    "n_recp_disp_fiolta",
    "n_recp_disp_sobu",
    "stot_inflows_inv_act",
    "c_pay_acq_const_fiolta",
    "c_paid_invest",
    "n_disp_subs_oth_biz",
    "oth_pay_ral_inv_act",
    "n_incr_pledge_loan",
    "stot_out_inv_act",
    "n_cashflow_inv_act",

    "c_recp_borrow",
    "proc_issue_bonds",
    "oth_cash_recp_ral_fnc_act",
    "stot_cash_in_fnc_act",
    "free_cashflow",
    "c_prepay_amt_borr",
    "c_pay_dist_dpcp_int_exp",
    "incl_dvd_profit_paid_sc_ms",
    "oth_cashpay_ral_fnc_act",
    "stot_cashout_fnc_act",
    "n_cash_flows_fnc_act",

    "eff_fx_flu_cash",
    "n_incr_cash_cash_equ",
    "c_cash_equ_beg_period",
    "c_cash_equ_end_period",
    "c_recp_cap_contrib",
    "incl_cash_rec_saims",
    "uncon_invest_loss",
    "prov_depr_assets",
    "depr_fa_coga_dpba",
    "amort_intang_assets",
    "lt_amort_deferred_exp",
    "decr_deferred_exp",
    "incr_acc_exp",
    "loss_disp_fiolta",
    "loss_scr_fa",
    "loss_fv_chg",
    "invest_loss",
    "decr_def_inc_tax_assets",
    "incr_def_inc_tax_liab",
    "decr_inventories",
    "decr_oper_payable",
    "incr_oper_payable",
    "others",
    "im_net_cashflow_oper_act",
    "conv_debt_into_cap",
    "conv_copbonds_due_within_1y",
    "fa_fnc_leases",
    "im_n_incr_cash_equ",
    "net_dism_capital_add",
    "net_cash_rece_sec",
    "credit_impa_loss",
    "use_right_asset_dep",
    "oth_loss_asset",
    "end_bal_cash",
    "beg_bal_cash",
    "end_bal_cash_equ",
    "beg_bal_cash_equ",
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
# 增量起点（基于 end_date）
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
        df_new = pro.cashflow(
            ts_code=ts_code,
            start_date=start_date,
            end_date=END_DATE,
            fields=",".join(CASHFLOW_FIELDS),
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
