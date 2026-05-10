import sys
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import List

import qlib
from qlib.data import D
import pandas as pd

# ================= 路径配置 =================
BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
SRC_ROOT = PROJECT_ROOT.parent

for path in (str(BASE_DIR), str(PROJECT_ROOT), str(SRC_ROOT)):
    if path not in sys.path:
        sys.path.insert(0, path)

try:
    from constants import DATA_PATH
except ImportError:
    from qlib_project.constants import DATA_PATH

OUTPUT_PATH = DATA_PATH / "instruments" / "my_trash_pool.txt"


# ============================================================
# 1. 主板过滤
# ============================================================
def filter_mainboard_stocks(codes: List[str]):
    filtered = []

    for code in codes:
        c = code.strip().upper()

        if not (c.startswith("SH") or c.startswith("SZ")):
            continue

        digits = "".join(filter(str.isdigit, c))
        if len(digits) != 6:
            continue

        # 排除指数
        if digits.startswith(("000", "399")):
            continue

        # 排除创业板 / 科创板 / 北交所
        if digits.startswith(("300", "301", "688", "689")):
            continue

        filtered.append(c)

    return filtered


# ============================================================
# 2. 行为因子过滤（宽松情绪池）
# ============================================================
def filter_by_price_retail(codes: List[str]):
    """
    构建宽松情绪池：
    保留散户活跃票，给LGB后续筛选
    """

    if not codes:
        return []

    try:
        cal = D.calendar()
        last_day = cal[-1]

        fields = [
            "$close / $factor",                                # real_price
            "Mean(($high - $low) / $close, 20)",              # amp
            "$amount",                                         # amount
            "Mean(Abs($close - $vwap) / $vwap, 20)",          # vwap_dev
            "($close - Mean($close, 20)) / Mean($close, 20)", # bias
            "Mean($amount, 5) / Mean($amount, 20)",           # amount_ratio
        ]

        df = D.features(
            codes,
            fields,
            start_time=last_day,
            end_time=last_day
        )

        if df is None or df.empty:
            return codes

        df.columns = [
            "real_price",
            "amp",
            "amount",
            "vwap_dev",
            "bias",
            "amount_ratio",
        ]

        # =========================
        # 基础过滤
        # =========================

        # 价格区间
        mask_price = (
            (df["real_price"] > 1.5) &
            (df["real_price"] < 40)
        )

        # 流动性（只剔除最冷门 5%）
        amount_threshold = df["amount"].quantile(0.05)
        mask_liquidity = df["amount"] > amount_threshold

        # =========================
        # 情绪条件（满足其一）
        # =========================

        # 活跃票
        cond_active = (
            (df["amp"] > 0.015) &
            (df["vwap_dev"] > 0.004)
        )

        # 超跌票
        cond_oversold = (
            df["bias"] < -0.06
        )

        # 放量票
        cond_volume_attack = (
            df["amount_ratio"] > 1.2
        )

        final_mask = (
            mask_price &
            mask_liquidity &
            (
                cond_active |
                cond_oversold |
                cond_volume_attack
            )
        )

        res = (
            df[final_mask]
            .index
            .get_level_values("instrument")
            .unique()
            .tolist()
        )

        print(
            f"🔥 情绪过滤完成: "
            f"{len(codes)} -> {len(res)}"
        )

        return res

    except Exception as e:
        print(f"❌ 情绪过滤异常: {e}")
        return codes


# ============================================================
# 3. 财务过滤（只做 survival filter）
# ============================================================
def filter_financial(codes: List[str]):
    """
    不做价值判断
    只过滤可能暴雷公司
    """

    if not codes:
        return []

    try:
        cal = D.calendar()
        last_day = cal[-1]

        fields = [
            "debt_to_assets",
        ]

        df = D.features(
            [c + "-fi" for c in codes],
            fields,
            start_time=last_day,
            end_time=last_day
        )

        if df is None or df.empty:
            return codes

        df.columns = ["debt"]

        mask = (
            df["debt"] < 95
        )

        filtered = (
            df[mask]
            .index
            .get_level_values("instrument")
            .unique()
            .tolist()
        )

        filtered = [
            c.replace("-fi", "")
            for c in filtered
        ]

        print(
            f"🧾 财务过滤: "
            f"{len(codes)} -> {len(filtered)}"
        )

        return filtered

    except Exception as e:
        print(f"❌ 财务过滤异常: {e}")
        return codes


# ============================================================
# main
# ============================================================
def main():
    qlib.init(provider_uri=str(DATA_PATH))

    today = datetime.today()

    start_dt = (
        today - relativedelta(months=1)
    ).strftime("%Y-%m-%d")

    end_dt = today.strftime("%Y-%m-%d")

    print(
        f"获取股票池: "
        f"{start_dt} -> {end_dt}"
    )

    inst_obj = D.instruments(
        market="all"
    )

    codes = D.list_instruments(
        instruments=inst_obj,
        start_time=start_dt,
        end_time=end_dt,
        as_list=True,
    )

    print(f"初始数量: {len(codes)}")

    # 1 主板
    codes = filter_mainboard_stocks(codes)
    print(f"主板后: {len(codes)}")

    # 2 情绪池
    codes = filter_by_price_retail(codes)

    # 3 财务过滤
    codes = filter_financial(codes)

    # 保存
    if codes:
        OUTPUT_PATH.parent.mkdir(
            parents=True,
            exist_ok=True
        )

        with open(
            OUTPUT_PATH,
            "w",
            encoding="utf-8"
        ) as f:
            for c in codes:
                f.write(c + "\n")

        print(
            f"✅ 完成: "
            f"{len(codes)} 只股票 "
            f"-> {OUTPUT_PATH}"
        )
    else:
        print("❌ 股票池为空")


if __name__ == "__main__":
    main()