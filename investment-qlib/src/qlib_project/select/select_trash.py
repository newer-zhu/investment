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
    from utils.api import get_industry_by_code
    from utils.util import is_industry, qlib_to_tushare
except ImportError:
    from qlib_project.constants import DATA_PATH
    from qlib_project.utils.api import get_industry_by_code
    from qlib_project.utils.util import is_industry, qlib_to_tushare

OUTPUT_PATH = DATA_PATH / "instruments" / "my_trash_pool.txt"


# ================= 主板过滤 =================
def filter_mainboard_stocks(codes):
    """
    过滤：
    - 排除指数
    - 排除北交所
    - 排除创业板 / 科创板
    - 保留主板
    """

    filtered = []

    for code in codes:

        c = code.strip().upper()

        # ========= 仅 SH / SZ =========
        if not (
            c.startswith("SH")
            or c.startswith("SZ")
        ):
            continue

        digits = "".join(
            filter(str.isdigit, c)
        )

        if len(digits) != 6:
            continue

        # ========= 排除指数 =========
        if digits.startswith(
            ("000", "399")
        ):
            continue

        # ========= 排除创业板 / 科创 =========
        if digits.startswith(
            ("300", "301", "688", "689", "8")
        ):
            continue

        # ========= 排除北交所 =========
        if c.startswith("BJ"):
            continue

        filtered.append(c)

    return filtered

# ================= 行业过滤 =================
def filter_industry_emotion(codes: List[str]):
    """
    过滤低波动 / 非情绪行业

    目标：
    提高情绪池纯度
    """

    if not codes:
        return []

    # ==========================================================
    # 这些行业天然不适合超跌情绪反弹
    # ==========================================================
    exclude_keywords = (

        # ========= 金融 =========
        "银行",
        "保险",
        "证券",
        "多元金融",

        # ========= 公用事业 =========
        "电力",
        "燃气",
        "水务",
        "高速公路",
        "港口",
        "机场",
        "铁路",

        # ========= 周期防御 =========
        "煤炭",
        "石油",
        "油气",

        # ========= 通信运营 =========
        "运营商",
        "通信服务",

        # ========= 超级大票 =========
        "中字头",

        # ========= 防御 =========
        "白酒",

        # ========= 地产 =========
        "房地产",
    )

    filtered = []

    remove_count = 0

    for code in codes:

        try:

            # ==================================================
            # 获取行业
            # ==================================================
            industry = get_industry_by_code(
                qlib_to_tushare(code)
            )

            if not industry:
                filtered.append(code)
                continue

            # ==================================================
            # 行业过滤
            # ==================================================
            if is_industry(
                industry,
                exclude_keywords
            ):

                remove_count += 1
                continue

            filtered.append(code)

        except Exception:
            filtered.append(code)

    print(
        f"🏭 行业过滤完成: "
        f"{len(codes)} -> {len(filtered)} "
        f"(过滤 {remove_count} 只)"
    )

    return filtered

# ================= 情绪股票池过滤 =================
def filter_by_price_retail(codes: List[str], min_price=2.0, max_price=50):
    if not codes: return []
    try:
        cal = D.calendar()
        last_day = cal[-1]
        
        # 增加 $cap 字段，用于过滤市值
        fields = [
            "$close / $factor",                  # real_price
            "Mean(($high - $low) / $close, 20)", # amp
            "$amount",                           # amount
            "($close - Mean($close, 20)) / Mean($close, 20)", # bias_20
            "Mean($amount, 5) / Mean($amount, 20)",           # amount_ratio
            "Sum(If($close < Ref($close,1), 1, 0), 10)",      # down_days_10d
            "($close - Min($close,20)) / Min($close,20)",      # dist_from_low20
        ]

        df = D.features(codes, fields, start_time=last_day, end_time=last_day)
        if df is None or df.empty: return codes
        df.columns = ["real_price", "amp", "amount", "bias", "amount_ratio", "down_days", "dist_low"]

        # 1. 基础硬过滤：收紧价格和流动性
        # 排除全市场成交额后 15% 的冷门股
        amt_thresh = df["amount"].quantile(0.15)
        
        mask_base = (
            (df["real_price"] > min_price) & 
            (df["real_price"] < max_price) &
            (df["amount"] > amt_thresh)
        )

        # 2. 三大情绪子路径（严苛化）
        
        # 路径 A：真正的深跌（20日跌幅超过12%，且离底部很近）
        cond_oversold = (df["bias"] < -0.12) & (df["dist_low"] < 0.05)
        
        # 路径 B：持续阴跌后的波动极度收缩（变盘前夜）
        cond_compressed = (df["down_days"] >= 8) & (df["amp"] < 0.02)
        
        # 路径 C：活跃股放量杀跌（黄金坑机会）
        cond_active_drop = (df["amp"] > 0.035) & (df["amount_ratio"] > 1.2) & (df["bias"] < -0.05)

        final_mask = mask_base & (cond_oversold | cond_compressed | cond_active_drop)

        filtered = df[final_mask].index.get_level_values("instrument").unique().tolist()

        # 3. 极其克制的自动扩容
        if len(filtered) < 300:
            print(f"📡 池子太小 ({len(filtered)})，执行轻度扩容...")
            # 仅放宽 bias 到 -8%
            loose_mask = mask_base & ((df["bias"] < -0.08) | (df["down_days"] >= 7))
            filtered = df[loose_mask].index.get_level_values("instrument").unique().tolist()

        return filtered
    except Exception as e:
        print(f"❌ 异常: {e}")
        return codes

# ================= 主程序 =================
def main():

    # ==========================================================
    # 初始化 Qlib
    # ==========================================================
    qlib.init(
        provider_uri=str(DATA_PATH)
    )

    # ==========================================================
    # 获取股票池
    # ==========================================================
    today = datetime.today()

    start_dt = (
        today - relativedelta(months=1)
    ).strftime('%Y-%m-%d')

    end_dt = today.strftime('%Y-%m-%d')

    print(
        f"正在从 Qlib 提取 "
        f"{start_dt} 至今活跃股票..."
    )

    inst_obj = D.instruments(
        market='all'
    )

    codes = D.list_instruments(
        instruments=inst_obj,
        start_time=start_dt,
        end_time=end_dt,
        as_list=True
    )

    print(
        f"初始代码数量: {len(codes)}"
    )

    # ==========================================================
    # 主板过滤
    # ==========================================================
    codes = filter_mainboard_stocks(codes)

    print(
        f"主板过滤后数量: {len(codes)}"
    )

    # ==========================================================
    # 行业情绪过滤
    # ==========================================================
    codes = filter_industry_emotion(codes)

    # ==========================================================
    # 情绪池过滤
    # ==========================================================
    codes = filter_by_price_retail(codes)

    # ==========================================================
    # 保存
    # ==========================================================
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

            for code in codes:
                f.write(f"{code}\n")

        print(
            f"✅ 筛选完成: "
            f"{len(codes)} 只股票 "
            f"已保存至:\n{OUTPUT_PATH}"
        )

    else:

        print(
            "❌ 最终结果为空"
        )


if __name__ == "__main__":
    main()