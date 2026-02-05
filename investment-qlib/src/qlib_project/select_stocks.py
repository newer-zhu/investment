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
from typing import List
from utils.logger import logger
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

def filter_by_industry(codes, banned_industries=("军工", "国防")):
    """
    根据行业关键字过滤股票
    """
    filtered = []
    for code in codes:
        # 统一处理代码格式，提取纯数字部分用于查询缓存
        pure_code = "".join(filter(str.isdigit, code))
        try:
            industry = get_industry_from_cache(pure_code)
        except Exception:
            # 缓存异常则跳过该股
            continue

        if not industry:
            continue

        # 命中黑名单行业，排除
        if any(bad in industry for bad in banned_industries):
            continue

        filtered.append(code)
    return filtered

def filter_by_price(
    codes,
    min_price=5.0,
    min_amount_avg=30_000,
    max_amp=0.15,
    max_vwap_gap=0.05,
    max_amount_cv=1.5,
):
    """
    量价多维过滤（无未来函数）
    """
    if not codes:
        return []

    try:
        cal = qlib.data.D.calendar()
        if len(cal) == 0:
            return codes

        last_day = cal[-1]

        fields = [
            "$close / $factor",          # 真实价格
            "$high / $factor",
            "$low / $factor",
            "$vwap / $factor",
            "Mean($amount, 5)",
            "Std($amount, 5)",
        ]

        df = qlib.data.D.features(
            codes, fields,
            start_time=last_day,
            end_time=last_day
        )

        if df is None or df.empty:
            return codes

        df.columns = [
            "price",
            "high",
            "low",
            "vwap",
            "amount_mean",
            "amount_std",
        ]

        amp = (df["high"] - df["low"]) / df["price"]
        vwap_gap = abs(df["price"] - df["vwap"]) / df["vwap"]
        amount_cv = df["amount_std"] / df["amount_mean"]

        mask = (
            (df["price"] > min_price) &
            (df["amount_mean"] > min_amount_avg) &
            (amp < max_amp) &
            (vwap_gap < max_vwap_gap) &
            (amount_cv < max_amount_cv)
        )

        filtered = (
            df[mask]
            .index
            .get_level_values("instrument")
            .unique()
            .tolist()
        )

        print(
            f"📊 量价过滤: {len(codes)} → {len(filtered)} "
            f"(剔除 {len(codes) - len(filtered)})"
        )

        return filtered

    except Exception as e:
        print(f"❌ 筛选异常: {e}")
        return codes


def filter_stocks_by_finance(
    codes: List[str],
    start_time: str,
    end_time: str,
    roe_min: float = 10.0,
    gross_margin_min: float = 20.0,
    or_yoy_min: float = 0.0,
    debt_to_assets_max: float = 70.0,
    current_ratio_min: float = 1.0,
) -> List[str]:
    """
    使用财务指标对股票做初步过滤（极宽松版）
    规则：
    - 有值 → 判断
    - 无值 → 忽略
    - 只要出现明确不达标 → 剔除
    """

    logger.info(f"开始财务初筛，股票数: {len(codes)}")

    qlib.init(provider_uri=str(FINANCE_PATH))

    fields = [
        "$roe",
        # "$gross_margin",
        "$or_yoy",
        "$debt_to_assets",
        "$current_ratio",
    ]

    df = D.features(
        instruments=codes,
        fields=fields,
        start_time=start_time,
        end_time=end_time,
    )

    df.columns = [
        "roe",
        # "grossprofit_margin",
        "or_yoy",
        "debt_to_assets",
        "current_ratio",
    ]

    passed_codes = []
    no_data_cnt = 0
    filtered_cnt = 0

    for code, df_code in df.groupby(level="instrument"):
        df_code = df_code.droplevel("instrument")

        df_valid = df_code.dropna(how="all")
        if df_valid.empty:
            no_data_cnt += 1
            # 财务全空：放行 or 剔除？
            # 👉 建议放行，交给后面的模型
            passed_codes.append(code)
            continue

        latest = df_valid.iloc[-1]

        failed = False

        if not pd.isna(latest["roe"]) and latest["roe"] < roe_min:
            failed = True

        if not pd.isna(latest["or_yoy"]) and latest["or_yoy"] < or_yoy_min:
            failed = True

        if not pd.isna(latest["debt_to_assets"]) and latest["debt_to_assets"] > debt_to_assets_max:
            failed = True

        if not pd.isna(latest["current_ratio"]) and latest["current_ratio"] < current_ratio_min:
            failed = True

        if failed:
            filtered_cnt += 1
        else:
            passed_codes.append(code)

    logger.info(
        f"财务筛选完成 | 总数: {len(codes)} | "
        f"无数据放行: {no_data_cnt} | "
        f"被筛掉: {filtered_cnt} | "
        f"通过: {len(passed_codes)}"
    )

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
    
        # (3) 价格过滤
    codes = filter_by_price(codes, min_price=5.0)
    print(f"价格过滤后数量: {len(codes)}")
    
    # (4) 财务指标过滤
    codes = filter_stocks_by_finance(
        codes,
        start_time=(today - relativedelta(years=1)).strftime('%Y-%m-%d'),
        end_time=end_dt,
        roe_min=8.0,
        or_yoy_min=5.0,
        current_ratio_min=0.9,
        debt_to_assets_max=80.0
        # TODO: 其他可调参数  
    )
    
    # (2) 行业过滤
    # codes = filter_by_industry(codes, banned_industries=("军工", "国防","银行"))
    # print(f"行业过滤后数量: {len(codes)}")
    


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