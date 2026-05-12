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
def filter_by_price_retail(
    codes: List[str],
    min_price=1.5,
    max_price=60,
):
    """
    宽松情绪池：
    目标：
    1. 尽量不漏情绪股
    2. 给后续 LGB 学习
    3. 不提前做 alpha

    核心：
    active OR compressed OR oversold
    """

    if not codes:
        return []

    try:

        cal = D.calendar()

        if len(cal) == 0:
            return codes

        last_day = cal[-1]

        # ==========================================================
        # 特征
        # ==========================================================
        fields = [

            # ========= 真实价格 =========
            "$close / $factor",

            # ========= 20日平均振幅 =========
            "Mean(($high - $low) / $close, 20)",

            # ========= 成交额 =========
            "$amount",

            # ========= VWAP偏离 =========
            "Mean(Abs($close - $vwap) / $vwap, 20)",

            # ========= 20日乖离 =========
            "($close - Mean($close, 20)) / Mean($close, 20)",

            # ========= 成交额变化 =========
            "Mean($amount, 5) / Mean($amount, 20)",

            # ========= 波动聚集 =========
            "Std(($high - $low) / $close, 10)",

            # ========= 连续阴跌 =========
            "Sum(If($close < Ref($close,1), 1, 0), 10)",

            # ========= 距离20日低点 =========
            "($close - Min($close,20)) / Min($close,20)"
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
            "amp_std_10d",
            "down_days_10d",
            "dist_from_low20",
        ]

        # ==========================================================
        # 基础过滤
        # ==========================================================

        # ========= 价格 =========
        mask_price = (
            (df["real_price"] > min_price)
            &
            (df["real_price"] < max_price)
        )

        # ========= 流动性 =========
        # 仅过滤最冷门的 1%
        amount_threshold = (
            df["amount"]
            .quantile(0.01)
        )

        mask_liquidity = (
            df["amount"] > amount_threshold
        )

        # ==========================================================
        # 情绪行为条件
        # ==========================================================

        # ========= 当前活跃 =========
        cond_active = (
            (df["amp"] > 0.015)
            &
            (df["vwap_dev"] > 0.004)
        )

        # ========= 超跌 =========
        cond_oversold = (
            df["bias"] < -0.06
        )

        # ========= 放量 =========
        cond_volume_attack = (
            df["amount_ratio"] > 1.1
        )

        # ========= 波动压缩 =========
        cond_compressed = (
            df["amp_std_10d"] < 0.012
        )

        # ========= 连续阴跌 =========
        cond_downtrend = (
            df["down_days_10d"] >= 6
        )

        # ========= 接近20日低点 =========
        cond_near_bottom = (
            df["dist_from_low20"] < 0.08
        )

        # ==========================================================
        # 最终过滤
        # ==========================================================

        final_mask = (
            (
                cond_oversold
                &
                cond_near_bottom
            )

            |

            (
                cond_downtrend
                &
                cond_compressed
            )

            |

            (
                cond_active
                &
                cond_volume_attack
            )
        )

        filtered = (
            df[final_mask]
            .index
            .get_level_values("instrument")
            .unique()
            .tolist()
        )

        # ==========================================================
        # 自动扩容
        # ==========================================================

        if len(filtered) < 800:

            print(
                f"📡 当前股票池仅 "
                f"{len(filtered)} 只，自动扩容..."
            )

            loose_mask = (

                mask_price

                &

                (
                    (df["amp"] > 0.01)
                    |
                    (df["bias"] < -0.04)
                    |
                    (df["amount_ratio"] > 1.0)
                    |
                    (df["amp_std_10d"] < 0.015)
                )
            )

            filtered = (
                df[loose_mask]
                .index
                .get_level_values("instrument")
                .unique()
                .tolist()
            )

        # ==========================================================
        # 输出统计
        # ==========================================================

        print(
            f"🔥 情绪池过滤完成: "
            f"{len(codes)} -> {len(filtered)}"
        )

        print(
            f"📊 活跃股数量: "
            f"{cond_active.sum()}"
        )

        print(
            f"📊 超跌股数量: "
            f"{cond_oversold.sum()}"
        )

        print(
            f"📊 放量股数量: "
            f"{cond_volume_attack.sum()}"
        )

        print(
            f"📊 波动压缩股数量: "
            f"{cond_compressed.sum()}"
        )

        print(
            f"📊 阴跌股数量: "
            f"{cond_downtrend.sum()}"
        )

        print(
            f"📊 接近低点股数量: "
            f"{cond_near_bottom.sum()}"
        )

        return filtered

    except Exception as e:

        print(
            f"❌ filter_by_price_retail 异常: {e}"
        )

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