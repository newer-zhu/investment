import sys
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta

import qlib
from qlib.data import D
import pandas as pd
from typing import List

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


# ================= 股票过滤：主板 =================
def filter_mainboard_stocks(codes):
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

        # 排除创业板/科创/北交所
        if digits.startswith(("300", "301", "688", "689")) or c.startswith("BJ"):
            continue

        filtered.append(c)

    return filtered


# ================= 行为因子过滤（情绪核心）=================
def filter_by_price_retail(codes: List[str]):
    """
    针对还原真实价格后的行为因子过滤：
    结合绝对价格门槛与相对波动百分位，确保股票池数量。
    """
    if not codes:
        return []

    try:
        cal = D.calendar()
        if len(cal) == 0:
            return codes
        
        last_day = cal[-1]

        # 特征定义
        fields = [
            "$close / $factor",                               # 0. 真实价格 (还原后)
            "Mean(($high - $low) / $close, 20)",              # 1. 20日平均振幅 (波动率)
            "$amount",                                        # 2. 归一化成交额
            "Mean(Abs($close - $vwap) / $vwap, 20)",          # 3. 情绪偏离度
            "($close - Mean($close, 20)) / Mean($close, 20)", # 4. 20日乖离率 (超跌)
        ]

        df = D.features(
            codes,
            fields,
            start_time=last_day,
            end_time=last_day
        )

        if df is None or df.empty:
            return codes

        df.columns = ["real_price", "amp", "amount", "vwap_dev", "bias"]

        # ================= 筛选逻辑 =================
        
        # 1. 价格门槛：2元到25元（实盘垃圾票黄金区间）
        mask_price = (df["real_price"] > 2) & (df["real_price"] < 25)
        
        # 2. 流动性过滤：剔除成交额最后 15% 的极冷门股
        # 不使用绝对数值，确保在任何归一化尺度下都有效
        amount_threshold = df["amount"].quantile(0.15)
        mask_liquidity = df["amount"] > amount_threshold
        
        # 3. 核心筛选条件 (满足其一即可，增加覆盖面)
        # 条件A：波动活跃（振幅 > 2.5% 且 有情绪偏离）
        cond_active = (df["amp"] > 0.025) & (df["vwap_dev"] > 0.008)
        # 条件B：极度超跌（乖离率 < -12%）
        cond_over_dropped = (df["bias"] < -0.12)
        
        final_mask = mask_price & mask_liquidity & (cond_active | cond_over_dropped)

        res = df[final_mask].index.get_level_values("instrument").unique().tolist()

        # ================= 动态扩容机制 =================
        # 如果选出的股票不足 150 只，自动放宽“活跃度”限制进行二次增补
        if len(res) < 150:
            print(f"📡 初始筛选仅 {len(res)} 只，正在自动扩容...")
            # 降低振幅门槛到 1.8%，降低超跌门槛到 -8%
            loose_mask = mask_price & mask_liquidity & (
                (df["amp"] > 0.018) | (df["bias"] < -0.08)
            )
            res = df[loose_mask].index.get_level_values("instrument").unique().tolist()

        print(f"🔥 情绪过滤完成: {len(codes)} -> {len(res)} (真实价格区间: 2-25元)")
        return res

    except Exception as e:
        print(f"❌ 情绪过滤异常: {e}")
        return codes
    
# ================= 财务过滤（核心升级）=================
def filter_financial(codes: List[str]):
    """
    只做“生存过滤”，不做价值判断
    """

    if not codes:
        return []

    try:
        cal = D.calendar()
        last_day = cal[-1]

        # ⭐ 注意：fi后缀
        fields = [
            "debt_to_assets",
            "roe",
            "fcff",
        ]

        df = D.features(
            [c + "-fi" for c in codes],   # ⭐你要求的 fi 后缀
            fields,
            start_time=last_day,
            end_time=last_day
        )

        if df is None or df.empty:
            return codes

        df.columns = ["debt", "roe", "fcff"]

        mask = (
            (df["debt"] < 85) &     # 不要高负债爆雷
            (df["roe"] > -20)      # 不要极端亏损
        )

        filtered = df[mask].index.get_level_values("instrument").unique().tolist()

        # 去掉 fi 后缀
        filtered = [c.replace("-fi", "") for c in filtered]

        print(f"🧾 财务过滤: {len(codes)} -> {len(filtered)}")
        return filtered

    except Exception as e:
        print(f"❌ 财务过滤异常: {e}")
        return codes


# ================= 主程序 =================
def main():
    qlib.init(provider_uri=str(DATA_PATH))

    today = datetime.today()
    start_dt = (today - relativedelta(months=1)).strftime('%Y-%m-%d')
    end_dt = today.strftime('%Y-%m-%d')

    print(f"获取股票池 {start_dt} -> {end_dt}")

    inst_obj = D.instruments(market='all')
    codes = D.list_instruments(
        instruments=inst_obj,
        start_time=start_dt,
        end_time=end_dt,
        as_list=True
    )

    print(f"初始数量: {len(codes)}")

    # ================= 1. 主板过滤 =================
    codes = filter_mainboard_stocks(codes)
    print(f"主板后: {len(codes)}")

    # ================= 2. 情绪过滤 =================
    codes = filter_by_price_retail(codes)

    # ================= 3. 财务过滤 =================
    codes = filter_financial(codes)

    # ================= 保存 =================
    if codes:
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
            for c in codes:
                f.write(c + "\n")

        print(f"✅ 完成: {len(codes)} 只股票 -> {OUTPUT_PATH}")
    else:
        print("❌ 结果为空")


if __name__ == "__main__":
    main()