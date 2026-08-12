"""
趋势跟踪股票池筛选
思路：选出已经形成上升趋势的股票（动量延续），而非超跌反弹
"""
import sys
import re
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import List

import qlib
from qlib.data import D
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
SRC_ROOT = PROJECT_ROOT.parent
for path in (str(BASE_DIR), str(PROJECT_ROOT), str(SRC_ROOT)):
    if path not in sys.path:
        sys.path.insert(0, path)

try:
    from constants import DATA_PATH, TREND_POOL
except ImportError:
    from qlib_project.constants import DATA_PATH, TREND_POOL

_MAINBOARD_RE = re.compile(
    r'^SH[69](?!88|89)\d{5}$'
    r'|^SZ(?!000|399|300|301)[02]\d{5}$'
)


def filter_by_trend(codes: List[str], min_price=3.0, max_price=80,
                    target_pool_size: int = 250):
    """
    趋势跟踪池筛选：选已经涨起来的，赌继续涨。
    
    条件：
    1. 基础: 价格/流动性过滤
    2. 偏5d > 2%（短期强势）
    3. 偏20d > 0%（中期多头排列）
    4. 量比 > 0.8（非冷门缩量）
    """
    if not codes:
        return []
    try:
        cal = D.calendar()
        last_day = cal[-1]

        fields = [
            "$close / $factor",                                       # real_price
            "$amount",                                                 # amount
            "($close - Mean($close, 5)) / Mean($close, 5)",          # bias_5d
            "($close - Mean($close, 20)) / Mean($close, 20)",        # bias_20d
            "Mean($amount, 5) / Mean($amount, 20)",                   # amount_ratio
            "$close / Ref($close, 20)",                               # 20日涨幅
            "Mean($close, 10) / Mean($close, 20)",                    # 短均/长均
        ]

        df = D.features(codes, fields, start_time=last_day, end_time=last_day)
        if df is None or df.empty:
            return codes
        df.columns = ["real_price", "amount", "bias_5d", "bias_20d",
                      "amount_ratio", "ret_20d", "ma_ratio"]

        # 基础过滤
        amt_thresh = df["amount"].quantile(0.20)
        mask_base = (
            (df["real_price"] > min_price) &
            (df["real_price"] < max_price) &
            (df["amount"] > amt_thresh)
        )

        # 趋势条件
        cond_strong = (df["bias_5d"] > 0.03) & (df["bias_20d"] > 0.02) & (df["ret_20d"] > 1.05)
        cond_steady = (df["bias_5d"] > 0.01) & (df["bias_20d"] > 0.01) & (df["ma_ratio"] > 1.03)
        cond_breakout = (df["bias_5d"] > 0.04) & (df["amount_ratio"] > 1.3) & (df["bias_20d"] > -0.02)

        final_mask = mask_base & (cond_strong | cond_steady | cond_breakout)
        filtered = df[final_mask].index.get_level_values("instrument").unique().tolist()

        # 扩容
        if len(filtered) < target_pool_size:
            print(f"📡 趋势池太小 ({len(filtered)}), 扩容...")
            loose = mask_base & (df["bias_5d"] > 0.01) & (df["bias_20d"] > -0.02)
            filtered = df[loose].index.get_level_values("instrument").unique().tolist()

        if len(filtered) < target_pool_size:
            loose2 = mask_base & (df["bias_5d"] > -0.01)
            filtered = df[loose2].index.get_level_values("instrument").unique().tolist()

        return filtered
    except Exception as e:
        print(f"❌ 趋势筛选异常: {e}")
        return codes


def main():
    qlib.init(provider_uri=str(DATA_PATH))

    today = datetime.today()
    start_dt = (today - relativedelta(months=1)).strftime('%Y-%m-%d')
    end_dt = today.strftime('%Y-%m-%d')

    print(f"正在提取 {start_dt} 至今活跃股票...")

    codes = D.list_instruments(
        instruments=D.instruments(market='all'),
        start_time=start_dt, end_time=end_dt, as_list=True
    )
    codes = [c.strip().upper() for c in codes if _MAINBOARD_RE.match(c.strip().upper())]
    print(f"主板股票: {len(codes)} 只")

    codes = filter_by_trend(codes)
    print(f"趋势筛选后: {len(codes)} 只")

    if codes:
        TREND_POOL.parent.mkdir(parents=True, exist_ok=True)
        with open(TREND_POOL, "w", encoding="utf-8") as f:
            for code in sorted(codes):
                f.write(f"{code}\n")
        print(f"✅ 趋势池已保存: {TREND_POOL}")


if __name__ == "__main__":
    main()
