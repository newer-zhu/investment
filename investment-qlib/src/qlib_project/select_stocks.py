import sys
from pathlib import Path
from datetime import datetime
import qlib
from qlib.data import D
import pandas as pd
from dateutil.relativedelta import relativedelta
from constants import DATA_PATH, FINANCE_PATH, TECH_KEYWORDS
# 导入你的行业工具
from utils.filter_stocks import get_industry_from_cache
from typing import Iterable, List
from utils.logger import logger
from utils.util import is_industry, qlib_to_tushare, get_latest_financial_row
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
    debt_to_assets_max: float = 85.0,
    current_ratio_min: float = 0.8,
    # 新增：最小现金流规模（用于替代市值筛选，单位：元）
    # 假设 1000万 是一个极小公司的门槛
    min_fcff_abs: float = 2_000_000.0 
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
        # 核心过滤逻辑 1：规模底线（防止完全没流动性）
        # ==========================================
        # 短线只需要避开“三无”微型票
        if not pd.isna(latest["fcff"]) and abs(latest["fcff"]) < min_fcff_abs:
            if not pd.isna(latest["interestdebt"]) and latest["interestdebt"] < 5_000_000:
                continue # 既没钱也没债，这种票多半是僵尸股

        # ==========================================
        # 核心过滤逻辑 2：【重点】短线科技/成长股逻辑
        # ==========================================
        if is_tech:
            # 1. 营收增长是短线灵魂 (放宽对利润的要求，宁要规模不要微利)
            # 只要营收增长 > 15%，ROE 是负的也能接受
            if latest["or_yoy"] < 15.0 and latest["roe"] < 3.0:
                failed = True
            
            # 2. 拒绝“低端组装” (即使是科技，毛利低于12%也难有爆发力)
            if not pd.isna(latest["gpm"]) and latest["gpm"] < 12.0:
                failed = True
            
            # 3. 动态趋势：如果营收在加速 (or_yoy > roe_yoy)，说明在抢市场，优先保留
            if latest["or_yoy"] > 30.0:
                failed = False # 营收暴力增长，豁免其他财务瑕疵

        # ==========================================
        # 核心过滤逻辑 3：传统股逻辑 (依然保持稳健，防止短线踩坑)
        # ==========================================
        else:
            if latest["roe"] < 7.0: # 稍微下调一点，从10%降到7%
                failed = True
            if not pd.isna(latest["or_yoy"]) and latest["or_yoy"] < -10:
                failed = True

        # ==========================================
        # 核心过滤逻辑 4：安全性兜底 (短线策略只需看爆雷风险)
        # ==========================================
        # 负债率：除非极高(>85%)，否则不轻易杀掉高杠杆扩张的科技股
        if not pd.isna(latest["debt_to_assets"]) and latest["debt_to_assets"] > debt_to_assets_max:
            failed = True

        # 剔除北交所 (短线如果不想玩织布机，这一行必须留着)
        if code.startswith("BJ"):
             # 北交所必须是顶尖成长才看，否则流动性不支持短线
            if latest["or_yoy"] < 50.0: 
                failed = True

        if not failed:
            passed_codes.append(code)

    logger.info(f"短线精筛完成 | 留存: {len(passed_codes)} | 过滤比例降低，攻击性提升")
    return passed_codes

from typing import List, Dict


def filter_stocks_by_finance_tech_short(
    codes: List[str],
    start_time: str,
    end_time: str
) -> List[str]:
    """
    科技短线专用财务过滤 (V3.0 宽松排雷版)
    原则：除非确定是雷，否则不轻易过滤；利用额外字段对研发、商誉、资金链进行兜底。
    """
    logger.info(f"科技短线财务过滤开始 | 初始股票数: {len(codes)}")

    qlib.init(provider_uri=str(FINANCE_PATH))

    # Qlib 基础字段
    fields = [
        "$roe", "$grossprofit_margin", "$or_yoy", 
        "$debt_to_assets", "$quick_ratio", "$ocf_yoy"
    ]

    df = D.features(instruments=codes, fields=fields, start_time=start_time, end_time=end_time)
    if df is None or df.empty: return codes

    df.columns = ["roe", "gpm", "or_yoy", "debt_to_assets", "quick_ratio", "ocf_yoy"]
    passed = []

    for code, df_code in df.groupby(level="instrument"):
        df_code = df_code.droplevel("instrument")
        df_valid = df_code.dropna(how="all")

        # 1. 基础判断：完全没数据的（如新股）直接放行，短线不看过去，看未来
        if df_valid.empty:
            passed.append(code)
            continue

        latest = df_valid.iloc[-1]
        prev = df_valid.iloc[-2] if len(df_valid) >= 2 else latest
        
        # 2. 从 CSV 提取最新原始数据（解决 NaN 问题）
        # 这里调用之前定义的 get_latest_financial_row 方法
        extra = get_latest_financial_row(code)

        # 行业判断
        industry = get_industry_by_code(qlib_to_tushare(code))
        if not is_industry(industry, tuple(TECH_KEYWORDS)):
            continue

        failed = False

        # ==========================================
        # 🟢 核心排雷 1：财务欺诈与回款风险 (利用额外字段)
        # ==========================================
        # 营收含金量：销售商品收到的现金 / 营业收入
        # 如果这个比例极低 (<0.5)，说明营收很可能是凑的或者是纯打欠条
        revenue = extra.get('revenue', latest.get('revenue', 0))
        c_sale = extra.get('c_fr_sale_sg', 0)
        if revenue > 0 and c_sale > 0:
            if (c_sale / revenue) < 0.5:
                failed = True

        # ==========================================
        # 🟢 核心排雷 2：商誉与资产虚高 (利用额外字段)
        # ==========================================
        # 科技股最怕年报商誉减值“大洗澡”。如果商誉占总资产 > 40%，短线风险极大
        total_assets = extra.get('total_assets', 0)
        goodwill = extra.get('goodwill', 0)
        if total_assets > 0 and goodwill > 0:
            if (goodwill / total_assets) > 0.4:
                failed = True

        # ==========================================
        # 🟢 核心排雷 3：伪科技识别 (研发强度)
        # ==========================================
        # 如果研发投入 rd_exp 明确存在且极低 (<2%)，则认为不是真科技。
        # 注意：如果数据缺失则放行（宽松原则）
        rd_exp = extra.get('rd_exp', 0)
        if revenue > 0 and rd_exp > 0:
            if (rd_exp / revenue) < 0.02:
                failed = True

        # ==========================================
        # 🟢 核心排雷 4：生存底线 (债务与流动性)
        # ==========================================
        # 负债率：放宽到 90% (科技股允许高杠杆抢市场)
        if not pd.isna(latest["debt_to_assets"]) and latest["debt_to_assets"] > 90:
            failed = True

        # 现金流枯竭：账面现金 money_cap 几乎为 0，且负债极高
        money_cap = extra.get('money_cap', 0)
        if money_cap is not None and money_cap < 1_000_000: # 现金不足100万
            if latest["debt_to_assets"] > 80:
                failed = True

        # ==========================================
        # 🟢 核心排雷 5：趋势性塌陷
        # ==========================================
        # 只有在营收和盈利同时出现“断崖式”下跌且无好转迹象时才过滤
        if (not pd.isna(latest["or_yoy"]) and latest["or_yoy"] < -50 and 
            not pd.isna(latest["roe"]) and latest["roe"] < -20):
            failed = True

        # ==========================================
        # 🟢 宽松补偿：如果营收暴力增长，豁免以上部分财务瑕疵
        # ==========================================
        if not pd.isna(latest["or_yoy"]) and latest["or_yoy"] > 50:
            failed = False 

        if not failed:
            passed.append(code)

    logger.info(f"科技短线排雷完成 | 入选: {len(passed)} | 过滤比例: {(1 - len(passed)/len(codes))*100:.1f}%")
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
    # codes = filter_by_industry(codes, banned_industries=("军工", "国防","银行"))
    # print(f"行业过滤后数量: {len(codes)}")
    
    # (3) 价格过滤
    codes = filter_by_price(codes, min_price=5.0)
    print(f"价格过滤后数量: {len(codes)}")
    
    # (4) 财务指标过滤
    # codes = filter_stocks_by_finance(
    #     codes,
    #     start_time=(today - relativedelta(months=6)).strftime('%Y-%m-%d'),
    #     end_time=end_dt,
    #     # or_yoy_min=5.0,
    #     # current_ratio_min=0.9,
    #     # debt_to_assets_max=80.0
    #     # TODO: 其他可调参数  
    # )
    
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