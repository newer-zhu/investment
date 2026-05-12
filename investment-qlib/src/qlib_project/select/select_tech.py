import sys
from pathlib import Path
from datetime import datetime
import qlib
from qlib.data import D
import pandas as pd
from typing import List, Dict
from dateutil.relativedelta import relativedelta
BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
SRC_ROOT = PROJECT_ROOT.parent
for path in (str(BASE_DIR), str(PROJECT_ROOT), str(SRC_ROOT)):
    if path not in sys.path:
        sys.path.insert(0, path)
try:
    from constants import DATA_PATH, FINANCE_PATH, TECH_KEYWORDS
except ImportError:
    from qlib_project.constants import DATA_PATH, FINANCE_PATH, TECH_KEYWORDS
# 导入你的行业工具
from utils.filter_stocks import get_industry_from_cache
from typing import Iterable, List
from utils.logger import logger
from utils.util import is_industry, qlib_to_tushare, get_latest_financial_row
from utils.api import get_industry_by_code, get_name_by_code
# ================= 路径配置 =================

OUTPUT_PATH = DATA_PATH / "instruments" / "my_filtered_pool.txt"

def filter_mainboard_stocks(codes):
    """
    过滤逻辑：
    - 排除指数
    - 排除京交所
    - 排除创业板 / 科创板 / 新三板
    - 只保留主板个股
    """
    filtered = []

    for code in codes:
        c = code.strip().upper()

        # ---- 1. 只接受 SH / SZ 开头 ----
        if not (c.startswith("SH") or c.startswith("SZ")):
            continue

        # ---- 2. 提取 6 位数字 ----
        digits = "".join(filter(str.isdigit, c))
        if len(digits) != 6:
            continue

        # ---- 3. 排除指数（关键）----
        # 上证指数：000xxx
        # 深证指数：399xxx
        if digits.startswith(("000", "399")):
            continue

        # ---- 4. 排除板块 ----
        if c.startswith("BJ"):
            continue
        if digits.startswith(("300", "301", "688", "689", "8")):
            continue

        filtered.append(c)

    return filtered

def filter_by_industry(
    codes: list[str],
    banned_industries: Iterable[str],
) -> list[str]:
    """
    行业过滤：
    - 命中 banned_industries → 剔除
    - 行业缺失 / 未识别 → 放行（宽松）
    """

    if not codes:
        return []

    banned_keywords = list(banned_industries)

    passed = []
    filtered = 0
    no_industry = 0

    for code in codes:
        try:
            # QLib → Tushare
            ts_code = qlib_to_tushare(code)

            if "ST" in get_name_by_code(ts_code).upper():
                continue
            
            # 获取行业字符串
            industry = get_industry_by_code(ts_code)

            if not industry:
                # 行业缺失，放行
                no_industry += 1
                passed.append(code)
                continue

            # 命中禁用行业关键词 → 剔除
            if is_industry(industry, banned_keywords):
                filtered += 1
                continue

            passed.append(code)

        except Exception as e:
            # 出异常也放行，防止误杀
            passed.append(code)

    print(
        f"🏭 行业过滤 | 原始 {len(codes)} | "
        f"剔除 {filtered} | "
        f"行业缺失放行 {no_industry} | "
        f"剩余 {len(passed)}"
    )

    return passed

def filter_by_price(
    codes,
    min_price=5.0,
    min_amount_avg=5000.0,  # 提高到 5000万，彻底告别织布机
    max_amp=0.15,
    max_vwap_gap=0.05,
    max_amount_cv=1.5,
):
    """
    针对你的数据列适配：自动处理复权和流动性
    """
    if not codes:
        return []

    try:
        cal = qlib.data.D.calendar()
        if len(cal) == 0:
            return codes
        last_day = cal[-1]

        # 核心：根据你提供的列名进行 Qlib 取数
        fields = [
            "$close / $factor",           # 真实收盘价（复权处理）
            "$high / $factor",            # 真实最高价
            "$low / $factor",             # 真实最低价
            "$vwap / $factor",            # 真实成交均价
            "Mean($amount, 20) ",  # 近20日均成交额（转为万元）
            "Std($amount, 20) ",   # 成交额波动（转为万元）
            # 由于你的列里没有 market_cap，我们通过 价格 * 因子 的某种逻辑，
            # 或者建议你在 dump 时把市值也带上。如果暂时没有，就先靠成交额硬扛。
        ]

        df = qlib.data.D.features(
            codes, fields,
            start_time=last_day,
            end_time=last_day
        )

        if df is None or df.empty:
            return codes

        df.columns = [
            "real_price", "real_high", "real_low", "real_vwap",
            "amount_mean", "amount_std"
        ]

        # --- 过滤逻辑 ---
        # 1. 计算真实振幅
        amp = (df["real_high"] - df["real_low"]) / df["real_price"]
        # 2. 价格与均价偏离
        vwap_gap = abs(df["real_price"] - df["real_vwap"]) / df["real_vwap"]
        # 3. 流动性稳定性
        amount_cv = df["amount_std"] / (df["amount_mean"] + 1e-6)

        mask = (
            (df["real_price"] > min_price) &           # 价格门槛
            (df["amount_mean"] > min_amount_avg) &    # 流动性门槛 (关键！)
            (amp < max_amp) &
            (vwap_gap < max_vwap_gap) &
            (amount_cv < max_amount_cv)
        )

        filtered = df[mask].index.get_level_values("instrument").unique().tolist()
        
        print(f"✅ 量价精筛完成: {len(codes)} -> {len(filtered)} (过滤掉成交低迷的僵尸股)")
        return filtered

    except Exception as e:
        print(f"❌ filter_by_price 异常: {e}")
        return codes
    

def filter_short_term_stocks(
    codes: List[str],
    start_time: str,
    end_time: str,
    # 基础阈值设置
    or_yoy_min: float = 15.0,  # 营收年增长至少15%
    roe_min: float = 3.0,  # 最低ROE 3%
    gpm_min: float = 12.0,  # 毛利率门槛 12%
    min_fcff_abs: float = 2_000_000.0,  # 最小现金流 200万
    debt_to_assets_max: float = 85.0  # 最大负债率 85%
) -> List[str]:
    """
    适合短线的股票筛选：没有行业限制，专注于财务指标过滤
    """
    logger.info(f"开始短线筛选，初始股票数: {len(codes)}")

    qlib.init(provider_uri=str(FINANCE_PATH))

    # 1. 需要的财务字段
    fields = [
        "$or_yoy", "$roe", "$gpm", "$fcff", "$debt_to_assets", "$interestdebt"
    ]

    df = D.features(
        instruments=codes,
        fields=fields,
        start_time=start_time,
        end_time=end_time,
    )

    if df is None or df.empty:
        return codes

    # 映射列名
    df.columns = [
        "or_yoy", "roe", "gpm", "fcff", "debt_to_assets", "interestdebt"
    ]

    passed_codes = []

    for code, df_code in df.groupby(level="instrument"):
        df_code = df_code.droplevel("instrument")
        df_valid = df_code.dropna(how="all")

        if df_valid.empty:
            # 无数据的跳过
            continue

        latest = df_valid.iloc[-1]

        failed = False

        # 核心过滤逻辑 1：营收年增长 > 15%
        if latest["or_yoy"] < or_yoy_min:
            failed = True
        
        # 核心过滤逻辑 2：ROE >= 3%
        if latest["roe"] < roe_min:
            failed = True

        # 核心过滤逻辑 3：毛利率 >= 12%
        if latest["gpm"] < gpm_min:
            failed = True

        # 核心过滤逻辑 4：现金流大于最低门槛
        if not pd.isna(latest["fcff"]) and abs(latest["fcff"]) < min_fcff_abs:
            failed = True

        # 核心过滤逻辑 5：负债率低于85%
        if not pd.isna(latest["debt_to_assets"]) and latest["debt_to_assets"] > debt_to_assets_max:
            failed = True

        # 核心过滤逻辑 6：长期债务少，避免陷入债务危机
        if not pd.isna(latest["interestdebt"]) and latest["interestdebt"] < 5_000_000:
            failed = True

        if not failed:
            passed_codes.append(code)

    logger.info(f"短线筛选完成 | 留存: {len(passed_codes)} | 过滤比例降低")
    return passed_codes

def filter_stocks_by_finance_tech_short(
    codes: List[str],
    start_time: str,
    end_time: str
) -> List[str]:
    """
    科技短线专用财务过滤 (V4.0 纯 Qlib 极速排雷版)
    原则：全量使用 Qlib 已有特征 (彻底移除外部慢速 CSV 查询)，
          引入速动比率、扣非净利、净债务和自由现金流进行多维交叉验证。
    """
    logger.info(f"科技短线财务过滤开始 | 初始股票数: {len(codes)}")

    # 1. 为股票代码加上 -fi 后缀
    codes_fi = [f"{code}-fi" for code in codes]

    safe_start_time = (pd.to_datetime(end_time) - pd.Timedelta(days=180)).strftime('%Y-%m-%d')
    query_start = min(pd.to_datetime(start_time), pd.to_datetime(safe_start_time)).strftime('%Y-%m-%d')

    # 3. 充分利用你 CSV 里的金矿字段
    fields = [
        "$roe", "$grossprofit_margin", "$or_yoy", 
        "$debt_to_assets", "$quick_ratio", "$current_ratio", 
        "$dt_netprofit_yoy",  # 扣非净利同比 (排雷神器)
        "$netprofit_yoy",     # 归母净利同比
        "$netdebt",           # 净债务 (单位: 元)
        "$fcff"               # 企业自由现金流 (单位: 元)
    ]

    df = D.features(
        instruments=codes_fi, 
        fields=fields, 
        start_time=query_start, 
        end_time=end_time
    )
    
    if df is None or df.empty: 
        logger.warning("未获取到任何科技财务数据，直接返回原始列表")
        return codes

    # 重命名列以方便调用
    df.columns = [
        "roe", "gpm", "or_yoy", "debt_to_assets", "quick_ratio", "current_ratio",
        "dt_netprofit_yoy", "netprofit_yoy", "netdebt", "fcff"
    ]
    passed = []

    for code_fi, df_code in df.groupby(level="instrument"):
        original_code = code_fi.replace("-fi", "")
        df_code = df_code.droplevel("instrument")
        
        # 前向填充，解决发布日外全 NaN 问题
        df_valid = df_code.ffill().dropna(how="all")

        # 没数据的（新股/退市等）直接放行
        if df_valid.empty:
            passed.append(original_code)
            continue

        latest = df_valid.iloc[-1]

        # 行业判断 (保留你的原逻辑)
        industry = get_industry_by_code(qlib_to_tushare(original_code))
        if not is_industry(industry, tuple(TECH_KEYWORDS)):
            continue

        failed = False

        # ==========================================
        # 🟢 核心排雷 1：债务与流动性枯竭 (替代原 money_cap 逻辑)
        # ==========================================
        # 你的 CSV 中 debt_to_assets 是百分比 (如 77.68)
        if not pd.isna(latest["debt_to_assets"]) and latest["debt_to_assets"] > 90:
            failed = True
            
        # 交叉验证流动性：如果速动比率小于 0.5 (短期还债能力极弱)，且净债务大于 0
        if not pd.isna(latest["quick_ratio"]) and latest["quick_ratio"] < 0.5:
            if not pd.isna(latest["netdebt"]) and latest["netdebt"] > 0:
                failed = True

        # ==========================================
        # 🟢 核心排雷 2：主业极度恶化 (替代原商誉/欺诈逻辑)
        # ==========================================
        # 如果营收暴跌超过 30%，且“扣非净利润”暴跌超过 50%，说明主业已经崩盘
        if (not pd.isna(latest["or_yoy"]) and latest["or_yoy"] < -30 and 
            not pd.isna(latest["dt_netprofit_yoy"]) and latest["dt_netprofit_yoy"] < -50):
            failed = True

        # ==========================================
        # 🟢 核心排雷 3：失血严重 (利用自由现金流绝对值)
        # ==========================================
        # fcff 是绝对值（元）。如果单季/年自由现金流流失超过 2亿 (-200_000_000) 且高负债
        if not pd.isna(latest["fcff"]) and latest["fcff"] < -200_000_000:
            if not pd.isna(latest["debt_to_assets"]) and latest["debt_to_assets"] > 70:
                failed = True

        # ==========================================
        # 🟢 宽松补偿：暴力成长股的豁免金牌
        # ==========================================
        # 科技股看重增速：只要营收增速 > 50%，或者扣非净利增速 > 100%，豁免流动性瑕疵
        if not pd.isna(latest["or_yoy"]) and latest["or_yoy"] > 50:
            failed = False 
        if not pd.isna(latest["dt_netprofit_yoy"]) and latest["dt_netprofit_yoy"] > 100:
            failed = False

        if not failed:
            passed.append(original_code)

    filter_ratio = (1 - len(passed) / len(codes)) * 100 if len(codes) > 0 else 0.0
    logger.info(f"科技短线排雷完成 | 入选: {len(passed)} | 过滤比例: {filter_ratio:.1f}%")
    
    return passed

def main():
    # 1. 初始化 Qlib
    qlib.init(provider_uri=str(DATA_PATH))
    
    # 2. 获取初始候选池 (使用 D 工具方法)
    today = datetime.today()
    start_dt = (today - relativedelta(months=1)).strftime('%Y-%m-%d')
    end_dt = today.strftime('%Y-%m-%d')

    print(f"正在从 Qlib 提取 {start_dt} 至今活跃的股票...")
    inst_obj = D.instruments(market='all')
    codes = D.list_instruments(instruments=inst_obj, start_time=start_dt, end_time=end_dt, as_list=True)
    print(f"初始代码数量: {len(codes)}")
    
    # 3. 执行多重过滤
    # (1) 板块过滤
    codes = filter_mainboard_stocks(codes)
    print(f"主板过滤后数量: {len(codes)}")
    
    # (2) 行业过滤
    codes = filter_by_industry(codes, banned_industries=("军工", "国防","银行"))
    print(f"行业过滤后数量: {len(codes)}")
    
    # (3) 价格过滤
    codes = filter_by_price(codes, min_price=5.0)
    print(f"价格过滤后数量: {len(codes)}")
    
    # (4) 财务指标过滤
    codes = filter_stocks_by_finance_tech_short(
        codes,
        start_time=(today - relativedelta(months=6)).strftime('%Y-%m-%d'),
        end_time=end_dt,)

    # 4. 保存为单列 TXT
    if codes:
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
            for code in codes:
                f.write(f"{code}\n")
        print(f"筛选完成，{len(codes)} 只股票已保存至: {OUTPUT_PATH}")
    else:
        print("最终结果为空，未生成文件。")

if __name__ == "__main__":
    main()