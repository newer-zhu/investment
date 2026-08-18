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
import numpy as np

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


def _limit_up_threshold(code: str) -> float:
    """按板块返回涨停涨幅阈值(留少量舍入余量)"""
    c = code.upper()
    # 创业板 / 科创板: 20%
    if c.startswith(("SZ300", "SZ301", "SH688", "SH689")):
        return 0.198
    # 北交所: 30%
    if c.startswith(("BJ4", "BJ8", "BJ9")):
        return 0.298
    # 主板(60/00/001/002/003): 10%
    return 0.098


def _buyable_mask(df: pd.DataFrame) -> pd.Series:
    """标记当日可买入的股票: 剔除收盘涨停(含一字板/连板)与一字板"""
    lim = df.index.get_level_values("instrument").map(_limit_up_threshold)
    lim = pd.Series(lim.values, index=df.index)

    # 收盘涨停(含一字板/连板): 买单排队也买不进, 次日大概率继续一字/大幅高开
    is_limit_up = df["ret_real"] >= (lim - 0.002)
    # 一字板: 开盘=收盘=最高=最低 单一定价封板
    # (因涨停价四舍五入, 个别封板股涨幅会略低于阈值, 这里兜底拦截)
    is_yiziban = (
        (df["oc_ratio"].sub(1).abs() < 0.005) &
        (df["range_ratio"].sub(1).abs() < 0.005) &
        (df["ret_real"] > 0.05)
    )
    return ~(is_limit_up | is_yiziban)


def filter_by_trend(codes: List[str], min_price=3.0, max_price=80,
                    target_pool_size: int = 250, as_of_date=None):
    """
    趋势跟踪池筛选：选已经涨起来的，赌继续涨。

    条件：
    1. 基础: 价格/流动性过滤
    2. 偏5d > 2%（短期强势）
    3. 偏20d > 0%（中期多头排列）
    4. 量比 > 0.8（非冷门缩量）

    as_of_date: 筛选基准日(滚动回测/实盘用, 避免前视偏差)。
        None = 最新交易日; 传入 str/Timestamp 时取 <= 该日的最近交易日作基准。
    """
    if not codes:
        return []
    try:
        cal = D.calendar()
        if as_of_date is None:
            ref_idx = len(cal) - 1
        else:
            ref_idx = int(np.searchsorted(cal, pd.Timestamp(as_of_date), side='right')) - 1
            if ref_idx < 0:
                ref_idx = 0
        ref_day = cal[ref_idx]
        last_day = ref_day
        # 数据源可能滞后: 基准日未必已入库(如日历到08-12但特征只到08-11)。
        # 若直接按基准日查询, 结果为空会静默回退返回全部 codes, 涨停/一字板混入池中。
        # 这里回退探测, 取实际有特征数据的最近交易日作为过滤基准日。
        for _i in range(ref_idx, max(ref_idx - 5, -1), -1):
            _day = cal[_i]
            _probe = D.features(codes[:20], ["$close"], start_time=_day, end_time=_day)
            if _probe is not None and not _probe.empty:
                if _day != ref_day:
                    print(f"⚠️ 基准日 {ref_day.date()} 无特征数据, 实际使用 {_day.date()}")
                last_day = _day
                break

        fields = [
            "$close / $factor",                                       # real_price
            "$amount",                                                 # amount
            "($close - Mean($close, 5)) / Mean($close, 5)",          # bias_5d
            "($close - Mean($close, 20)) / Mean($close, 20)",        # bias_20d
            "Mean($amount, 5) / Mean($amount, 20)",                   # amount_ratio
            "$close / Ref($close, 20)",                               # 20日涨幅
            "Mean($close, 10) / Mean($close, 20)",                    # 短均/长均
            # --- 可买入性检查 ---
            # 真实日涨幅(用 factor 还原实际价格, 剔除除权除息造成的假涨幅)
            "($close / Ref($close, 1)) * (Ref($factor, 1) / $factor) - 1",
            "$open / $close",          # 开收比(≈1 表示一字)
            "$high / $low",            # 日内振幅比(≈1 表示单一定价)
        ]

        df = D.features(codes, fields, start_time=last_day, end_time=last_day)
        if df is None or df.empty:
            return codes
        df.columns = ["real_price", "amount", "bias_5d", "bias_20d",
                      "amount_ratio", "ret_20d", "ma_ratio",
                      "ret_real", "oc_ratio", "range_ratio"]

        # 基础过滤
        amt_thresh = df["amount"].quantile(0.20)
        mask_base = (
            (df["real_price"] > min_price) &
            (df["real_price"] < max_price) &
            (df["amount"] > amt_thresh)
        )

        # 可买入性过滤: 剔除收盘涨停/一字板(开盘顶一字根本买不进)
        mask_buyable = _buyable_mask(df)

        # 趋势条件
        cond_strong = (df["bias_5d"] > 0.03) & (df["bias_20d"] > 0.02) & (df["ret_20d"] > 1.05)
        cond_steady = (df["bias_5d"] > 0.01) & (df["bias_20d"] > 0.01) & (df["ma_ratio"] > 1.03)
        cond_breakout = (df["bias_5d"] > 0.04) & (df["amount_ratio"] > 1.3) & (df["bias_20d"] > -0.02)

        final_mask = mask_base & mask_buyable & (cond_strong | cond_steady | cond_breakout)
        filtered = df[final_mask].index.get_level_values("instrument").unique().tolist()

        # 扩容(同样保持可买入过滤, 避免重新引入涨停/一字板)
        if len(filtered) < target_pool_size:
            print(f"📡 趋势池太小 ({len(filtered)}), 扩容...")
            loose = mask_base & mask_buyable & (df["bias_5d"] > 0.01) & (df["bias_20d"] > -0.02)
            filtered = df[loose].index.get_level_values("instrument").unique().tolist()

        if len(filtered) < target_pool_size:
            loose2 = mask_base & mask_buyable & (df["bias_5d"] > -0.01)
            filtered = df[loose2].index.get_level_values("instrument").unique().tolist()

        return filtered
    except Exception as e:
        print(f"❌ 趋势筛选异常: {e}")
        return codes


def get_mainboard_universe(start_time=None, end_time=None) -> List[str]:
    """提取全主板候选(select_trend.main 同款口径), 供滚动训练每周重新选股"""
    codes = D.list_instruments(
        instruments=D.instruments(market='all'),
        start_time=start_time, end_time=end_time, as_list=True,
    )
    codes = [c.strip().upper() for c in codes if _MAINBOARD_RE.match(c.strip().upper())]
    return codes


def main():
    qlib.init(provider_uri=str(DATA_PATH))

    today = datetime.today()
    start_dt = (today - relativedelta(months=1)).strftime('%Y-%m-%d')
    end_dt = today.strftime('%Y-%m-%d')

    print(f"正在提取 {start_dt} 至今活跃股票...")

    codes = get_mainboard_universe(start_dt, end_dt)
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
