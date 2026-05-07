import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import time
from pathlib import Path

import pandas as pd
from tqdm import tqdm
import tushare as ts

from utils.util import tushare_to_qlib

# ======================
# 1. 项目路径
# ======================
PROJECT_ROOT = Path(__file__).resolve().parents[4]

SOURCE_PATH = PROJECT_ROOT / "data" / "cache" / "stock_basic.csv"

OUT_DIR = PROJECT_ROOT / "data" / "fundamental" / "fina_indicator"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ======================
# 2. TuShare 初始化
# ======================
token = "2f80f707c09dc4ce6c59eb215739349f7a203485b19b4401a683bb6549ec"

pro = ts.pro_api(token)

# 如果你使用代理接口
pro._DataApi__token = token
pro._DataApi__http_url = "http://lianghua.nanyangqiankun.top"

# ======================
# 3. 财务字段
# ======================
CORE_COLS = [
    "ann_date",
    "end_date",
    "roe",
    "roe_dt",
    "roa",
    "roic",
    "grossprofit_margin",
    "netprofit_margin",
    "profit_to_gr",
    "or_yoy",
    "tr_yoy",
    "netprofit_yoy",
    "dt_netprofit_yoy",
    "roe_yoy",
    "debt_to_assets",
    "current_ratio",
    "quick_ratio",
    "interestdebt",
    "netdebt",
    "ocf_yoy",
    "fcff",
    "fcfe",
]

# ======================
# 4. 最终输出列顺序
# ======================
DESIRED_COLUMNS = [
    "tradedate",
    "symbol",
    "end_date",
    "roe",
    "roe_dt",
    "roa",
    "roic",
    "grossprofit_margin",
    "netprofit_margin",
    "profit_to_gr",
    "or_yoy",
    "tr_yoy",
    "netprofit_yoy",
    "dt_netprofit_yoy",
    "roe_yoy",
    "debt_to_assets",
    "current_ratio",
    "quick_ratio",
    "interestdebt",
    "netdebt",
    "ocf_yoy",
    "fcff",
    "fcfe",
]

# ======================
# 5. 数值字段
# ======================
NUMERIC_COLS = [
    c
    for c in DESIRED_COLUMNS
    if c not in ["tradedate", "symbol", "end_date"]
]


# ======================
# 6. 下载单只股票
# ======================
def download_one(ts_code: str) -> pd.DataFrame | None:
    """
    下载单只股票的财务指标
    """

    df = pro.fina_indicator(
        ts_code=ts_code,
        fields=",".join(CORE_COLS),
    )

    if df is None or df.empty:
        return None

    # 保留存在字段
    keep_cols = [c for c in CORE_COLS if c in df.columns]
    df = df[keep_cols].copy()

    # ======================
    # 时间处理
    # ======================
    df["ann_date"] = pd.to_datetime(
        df["ann_date"],
        errors="coerce",
    )

    df["end_date"] = pd.to_datetime(
        df["end_date"],
        errors="coerce",
    )

    df = df.dropna(subset=["ann_date", "end_date"])

    # ann_date -> tradedate
    df = df.rename(columns={"ann_date": "tradedate"})

    # ======================
    # 数值字段转 float
    # ======================
    exist_numeric_cols = [
        c for c in NUMERIC_COLS if c in df.columns
    ]

    df[exist_numeric_cols] = df[exist_numeric_cols].apply(
        pd.to_numeric,
        errors="coerce",
    )

    return df


# ======================
# 7. 主程序
# ======================
def main():

    df_stock = pd.read_csv(
        SOURCE_PATH,
        dtype=str,
    )

    ts_codes = (
        df_stock["ts_code"]
        .dropna()
        .unique()
        .tolist()
    )

    print(f"Total stocks: {len(ts_codes)}")

    for ts_code in tqdm(ts_codes):

        try:

            # ======================
            # 股票代码转换
            # ======================
            qlib_code = tushare_to_qlib(ts_code)

            out_file = OUT_DIR / f"{qlib_code}-fi.csv"

            # ======================
            # 下载新数据
            # ======================
            df_new = download_one(ts_code)

            if df_new is None or df_new.empty:
                continue

            df_new["symbol"] = qlib_code

            # ======================
            # 补齐缺失列
            # ======================
            for col in DESIRED_COLUMNS:
                if col not in df_new.columns:
                    df_new[col] = pd.NA

            # 列顺序统一
            df_new = df_new[DESIRED_COLUMNS]

            # ======================
            # 增量合并
            # ======================
            if out_file.exists():

                df_old = pd.read_csv(out_file)

                # 时间字段统一
                df_old["tradedate"] = pd.to_datetime(
                    df_old["tradedate"],
                    errors="coerce",
                )

                df_old["end_date"] = pd.to_datetime(
                    df_old["end_date"],
                    errors="coerce",
                )

                # 数值字段统一
                exist_old_numeric = [
                    c for c in NUMERIC_COLS if c in df_old.columns
                ]

                df_old[exist_old_numeric] = df_old[
                    exist_old_numeric
                ].apply(
                    pd.to_numeric,
                    errors="coerce",
                )

                # 合并
                df_all = pd.concat(
                    [df_old, df_new],
                    ignore_index=True,
                )

            else:
                df_all = df_new

            # ======================
            # 去重
            # 一个季度只保留最后版本
            # ======================
            df_all = df_all.drop_duplicates(
                subset=["symbol", "end_date"],
                keep="last",
            )

            # ======================
            # 排序
            # ======================
            df_all = df_all.sort_values(
                by=["symbol", "end_date", "tradedate"],
                ascending=[True, True, True],
            )

            # ======================
            # 保存前格式化日期
            # ======================
            df_all["tradedate"] = pd.to_datetime(
                df_all["tradedate"]
            ).dt.strftime("%Y-%m-%d")

            df_all["end_date"] = pd.to_datetime(
                df_all["end_date"]
            ).dt.strftime("%Y-%m-%d")

            # ======================
            # 最终列顺序
            # ======================
            df_all = df_all[DESIRED_COLUMNS]

            # ======================
            # 保存
            # ======================
            df_all.to_csv(
                out_file,
                index=False,
            )

            # ======================
            # 限流
            # ======================
            time.sleep(0.35)

        except Exception as e:

            print(f"[ERROR] {ts_code}: {e}")

            time.sleep(1)


# ======================
# 8. 启动
# ======================
if __name__ == "__main__":
    main()