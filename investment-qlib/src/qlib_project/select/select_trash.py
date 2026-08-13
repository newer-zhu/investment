import sys
import re
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import List

import qlib
from qlib.data import D
from qlib.data.filter import NameDFilter
import pandas as pd

# ================= 路径配置 =================
BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
SRC_ROOT = PROJECT_ROOT.parent

for path in (str(BASE_DIR), str(PROJECT_ROOT), str(SRC_ROOT)):
    if path not in sys.path:
        sys.path.insert(0, path)

try:
    from constants import DATA_PATH, TRASH_POOL
    from utils.api import get_industry_by_code
    from utils.util import is_industry, qlib_to_tushare
except ImportError:
    from qlib_project.constants import DATA_PATH, TRASH_POOL
    from qlib_project.utils.api import get_industry_by_code
    from qlib_project.utils.util import is_industry, qlib_to_tushare



# ================= 主板过滤 =================
# 使用正则表达式匹配 A 股主板（排除创业板/科创板/北交所/指数）
# 匹配规则: SH6xxxxx, SH9xxxxx, SZ0xxxxx, SZ2xxxxx（且排除 000/399 指数, 300/301 创业, 688/689 科创）
_MAINBOARD_RE = re.compile(
    r'^SH[69](?!88|89)\d{5}$'              # 上海主板 (6xxxxx, 9xxxxx 排除 688/689)
    r'|^SZ(?!000|399|300|301)[02]\d{5}$'   # 深圳主板 (0xxxxx, 2xxxxx 排除指数和创业板)
)


def build_mainboard_filter_pipe() -> list:
    """
    构建 Qlib 原生的 NameDFilter 过滤管道，用于 D.instruments() 的 filter_pipe 参数。
    注意: filter_pipe 中多个 NameDFilter 是 AND 串联关系，
    因此必须合并为一条正则，同时匹配 SH 和 SZ 主板。
    
    Returns:
        list[NameDFilter]: 可直接传入 D.instruments(filter_pipe=...) 的过滤器列表
    """
    return [
        NameDFilter(name_rule_re=r'^(SH[69](?!88|89)\d{5}|SZ(?!000|399|300|301)[02]\d{5})$'),
    ]


def filter_mainboard_stocks(codes: list) -> list:
    """
    从代码列表中筛选 A 股主板股票。
    保留: SH6xxxxx, SH9xxxxx, SZ0xxxxx, SZ2xxxxx
    排除: 创业板(300/301), 科创板(688/689), 北交所(8), 指数(000/399)
    
    同时作为 D.list_instruments() 的后过滤
    和构建 Qlib filter_pipe 的正则来源。
    """
    return [c.strip().upper() for c in codes if _MAINBOARD_RE.match(c.strip().upper())]

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
def filter_by_price_retail(codes: List[str], min_price=2.0, max_price=50,
                           target_pool_size: int = 300):
    """
    三级递进式情绪池筛选:
    1. 基础硬过滤: 价格区间 + 流动性阈值
    2. 三大情绪路径 (严苛): 深跌/阴跌收缩/放量杀跌
    3. 自动扩容 (三级递进): 量比→振幅→乖离率，逐级放宽
    """
    if not codes: return []
    try:
        cal = D.calendar()
        last_day = cal[-1]

        fields = [
            "$close / $factor",                                       # real_price
            "Mean(($high - $low) / $close, 20)",                      # amp
            "$amount",                                                 # amount
            "($close - Mean($close, 20)) / Mean($close, 20)",         # bias_20
            "Mean($amount, 5) / Mean($amount, 20)",                   # amount_ratio
            "Sum(If($close < Ref($close,1), 1, 0), 10)",              # down_days_10d
            "($close - Min($close,20)) / Min($close,20)",             # dist_from_low20
        ]

        df = D.features(codes, fields, start_time=last_day, end_time=last_day)
        if df is None or df.empty: return codes
        df.columns = ["real_price", "amp", "amount", "bias", "amount_ratio",
                      "down_days", "dist_low"]

        # ---- 1. 基础硬过滤 ----
        amt_thresh = df["amount"].quantile(0.15)

        mask_base = (
            (df["real_price"] > min_price) &
            (df["real_price"] < max_price) &
            (df["amount"] > amt_thresh)
        )

        # ---- 2. 三大情绪子路径 (严苛模式) ----
        cond_oversold   = (df["bias"] < -0.12) & (df["dist_low"] < 0.05)
        cond_compressed = (df["down_days"] >= 8) & (df["amp"] < 0.02)
        cond_active     = (df["amp"] > 0.035) & (df["amount_ratio"] > 1.2) & (df["bias"] < -0.05)

        final_mask = mask_base & (cond_oversold | cond_compressed | cond_active)
        filtered = df[final_mask].index.get_level_values("instrument").unique().tolist()

        # ---- 3. 三级递进式自动扩容 ----
        if len(filtered) < target_pool_size:
            print(f"📡 池子太小 ({len(filtered)}), 启动三级递进扩容...")

            # Tier 1: 放宽量比约束（允许温和放量），振幅/乖离率不变
            cond_t1_active = (df["amp"] > 0.03) & (df["amount_ratio"] > 1.1) & (df["bias"] < -0.05)
            tier1_mask = mask_base & (cond_oversold | cond_compressed | cond_t1_active)
            tier1 = df[tier1_mask].index.get_level_values("instrument").unique().tolist()

            if len(tier1) >= target_pool_size:
                print(f"   ✅ Tier 1 (放宽量比): {len(filtered)} → {len(tier1)}")
                return tier1

            # Tier 2: 再放宽振幅约束（接受温和波动），乖离率仍守住
            cond_t2_compressed = (df["down_days"] >= 7) & (df["amp"] < 0.025)
            cond_t2_active = (df["amp"] > 0.025) & (df["amount_ratio"] > 1.05) & (df["bias"] < -0.05)
            tier2_mask = mask_base & (cond_oversold | cond_t2_compressed | cond_t2_active)
            tier2 = df[tier2_mask].index.get_level_values("instrument").unique().tolist()

            if len(tier2) >= target_pool_size:
                print(f"   ✅ Tier 2 (放宽振幅): {len(filtered)} → {len(tier2)}")
                return tier2

            # Tier 3: 最后放宽乖离率（兜底，只放宽到 -8% 而非 -12%）
            cond_t3 = (df["bias"] < -0.08) | (df["down_days"] >= 7)
            tier3_mask = mask_base & cond_t3
            tier3 = df[tier3_mask].index.get_level_values("instrument").unique().tolist()
            print(f"   ⚠️ Tier 3 (放宽乖离率): {len(filtered)} → {len(tier3)}")
            return tier3

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
    # 获取股票池（使用 Qlib 原生 NameDFilter 做主板过滤）
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

    # Qlib 原生方式: filter_pipe 中的 NameDFilter 会在下游 D.features() 等操作时自动生效
    inst_obj = D.instruments(
        market='all',
        filter_pipe=build_mainboard_filter_pipe()
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
    # 主板过滤（D.list_instruments 不应用 filter_pipe，需手动再滤一次）
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

        TRASH_POOL.parent.mkdir(
            parents=True,
            exist_ok=True
        )

        with open(
            TRASH_POOL,
            "w",
            encoding="utf-8"
        ) as f:

            for code in codes:
                f.write(f"{code}\n")

        print(
            f"✅ 筛选完成: "
            f"{len(codes)} 只股票 "
            f"已保存至:\n{TRASH_POOL}"
        )

    else:

        print(
            "❌ 最终结果为空"
        )


if __name__ == "__main__":
    main()