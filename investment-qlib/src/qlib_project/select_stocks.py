import sys
from pathlib import Path
from datetime import datetime
import qlib
from qlib.data import D
import pandas as pd
from dateutil.relativedelta import relativedelta
from constants import DATA_PATH, FINANCE_PATH
# 导入你的行业工具
from utils.filter_stocks import get_industry_from_cache
from typing import Iterable, List
from utils.logger import logger
from utils.util import is_industry, qlib_to_tushare
from utils.api import get_industry_by_code
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
    

def filter_stocks_by_finance(
    codes: List[str],
    start_time: str,
    end_time: str,
    # 基础阈值设置
    or_yoy_min: float = 0.0,
    debt_to_assets_max: float = 75.0,
    current_ratio_min: float = 1.0,
    # 新增：最小现金流规模（用于替代市值筛选，单位：元）
    # 假设 1000万 是一个极小公司的门槛
    min_fcff_abs: float = 10_000_000.0 
) -> List[str]:
    """
    独立财务精筛：利用多维财务指标过滤“虚假繁荣”的小盘僵尸股
    """
    logger.info(f"开始深度财务过滤，初始股票数: {len(codes)}")

    qlib.init(provider_uri=str(FINANCE_PATH))

    # 1. 充分利用你 CSV 中的所有列
    fields = [
        "$roe", "$roe_dt", "$roa", "$roic", 
        "$grossprofit_margin", "$netprofit_margin", 
        "$or_yoy", "$netprofit_yoy", "$roe_yoy",
        "$debt_to_assets", "$current_ratio", "$quick_ratio",
        "$interestdebt", "$ocf_yoy", "$fcff", "$fcfe"
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
        "roe", "roe_dt", "roa", "roic", 
        "gpm", "npm", 
        "or_yoy", "netprofit_yoy", "roe_yoy",
        "debt_to_assets", "current_ratio", "quick_ratio",
        "interestdebt", "ocf_yoy", "fcff", "fcfe"
    ]

    passed_codes = []
    
    for code, df_code in df.groupby(level="instrument"):
        df_code = df_code.droplevel("instrument")
        df_valid = df_code.dropna(how="all")

        if df_valid.empty:
            # 这里的策略：如果没有财务数据，说明可能是新股或数据缺失，通常选择不放行以规避织布机
            continue

        latest = df_valid.iloc[-1]
        industry = get_industry_by_code(qlib_to_tushare(code))
        
        # 判断是否为科技/成长型行业
        is_tech = is_industry(industry, ["科技", "半导体", "互联网",
                                         "新能源", "软件", "芯片", "AI",
                                         "通信", "电子", "计算机", "汽车"])

        # ==========================================
        # 核心过滤逻辑 1：规模与流动性替代指标
        # ==========================================
        # 织布机通常是那些 FCFF（自由现金流）极小的公司
        # 如果 FCFF 绝对值太小（例如只有几百万），在市场上大概率无人问津
        if not pd.isna(latest["fcff"]) and abs(latest["fcff"]) < min_fcff_abs:
            continue

        # ==========================================
        # 核心过滤逻辑 2：行业定制化判断
        # ==========================================
        failed = False

        if is_tech:
            # --- 科技类：侧重增长与毛利，放宽 ROE ---
            # 1. ROE 底线放低到 5%
            if latest["roe"] < 5.0:
                # 除非增长极快 (营收增长 > 25% 或 利润增长 > 40%)
                if not (latest["or_yoy"] > 25 or latest["netprofit_yoy"] > 40):
                    failed = True
            
            # 2. 毛利率（GPM）必须维持在高位，否则说明没有核心竞争力
            if not pd.isna(latest["gpm"]) and latest["gpm"] < 20.0:
                failed = True
                
            # 3. 现金流不能太难看（OCF增长不能长期大幅落后利润增长）
            if not pd.isna(latest["ocf_yoy"]) and latest["ocf_yoy"] < -50:
                failed = True

        else:
            # --- 传统类：侧重资本效率 (ROIC) 和 稳定性 ---
            # 1. ROE 硬门槛 10%
            if latest["roe"] < 10.0:
                failed = True
                
            # 2. 引入 ROIC (投入资本回报率)，看剔除杠杆后的真实盈利能力
            if not pd.isna(latest["roic"]) and latest["roic"] < 7.0:
                failed = True
                
            # 3. 营收不能萎缩
            if not pd.isna(latest["or_yoy"]) and latest["or_yoy"] < -5:
                failed = True

        # ==========================================
        # 核心过滤逻辑 3：通用财务安全性 (兜底)
        # ==========================================
        # 1. 负债率
        if not pd.isna(latest["debt_to_assets"]) and latest["debt_to_assets"] > debt_to_assets_max:
            failed = True
            
        # 2. 流动性安全：速动比率 (Quick Ratio) 
        # 比 current_ratio 更严格，剔除了存货，防止存货积压的小公司
        if not pd.isna(latest["quick_ratio"]) and latest["quick_ratio"] < 0.6:
            failed = True

        # 3. 剔除北交所（BJ）的小票 —— 织布机的重灾区
        # 如果你确实不想看织布机，北交所 80% 的票都可以直接过滤
        if code.startswith("BJ"):
            # 对北交所执行极高门槛，或者直接剔除
            if latest["fcff"] < 50_000_000: # 北交所公司 FCFF 必须大于 5000万 才看
                failed = True

        if not failed:
            passed_codes.append(code)

    logger.info(f"财务精筛完成 | 最终入选: {len(passed_codes)} | 过滤比例: {(1 - len(passed_codes)/len(codes))*100:.1f}%")
    return passed_codes

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
    codes = filter_stocks_by_finance(
        codes,
        start_time=(today - relativedelta(months=6)).strftime('%Y-%m-%d'),
        end_time=end_dt,
        # or_yoy_min=5.0,
        # current_ratio_min=0.9,
        # debt_to_assets_max=80.0
        # TODO: 其他可调参数  
    )

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