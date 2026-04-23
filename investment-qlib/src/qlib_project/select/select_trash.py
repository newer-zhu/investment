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
    from constants import DATA_PATH
except ImportError:
    from qlib_project.constants import DATA_PATH
# 导入你的行业工具
from utils.filter_stocks import get_industry_from_cache
from typing import Iterable, List
from utils.logger import logger
from utils.util import is_industry, qlib_to_tushare, get_latest_financial_row
from utils.api import get_industry_by_code, get_name_by_code
# ================= 路径配置 =================

OUTPUT_PATH = DATA_PATH / "instruments" / "my_trash_pool.txt"

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

def filter_by_price_retail(
    codes: List[str],
    min_price=2.0,       # 剔除面值退市危险股 (低于2块钱的雷太多)
    max_price=20.0,      # 散户甜点区：超过 20-30 块，散户接盘意愿断崖式下跌
    min_amount_avg=8000.0, # 近20日均成交额 > 8000万 (没有量就没有流动性溢价)
    min_amp=0.04,        # 【核心新增】必须有波动！日均振幅 < 4% 的死鱼一律不要
):
    """
    散户票专用量价精筛：锁定低价、高活跃度、高波动的游资散户主战场
    """
    if not codes:
        return []

    try:
        cal = qlib.data.D.calendar()
        if len(cal) == 0: return codes
        last_day = cal[-1]

        # 计算近 20 天的均值特征，避免单日突发数据的干扰
        fields = [
            "$close / $factor",                 # 真实收盘价
            "Mean(($high - $low) / $close, 20)", # 近20日平均振幅 (关键！)
            "Mean($amount, 20) ",               # 近20日均成交额 (万元)
        ]

        df = qlib.data.D.features(
            codes, fields,
            start_time=last_day,
            end_time=last_day
        )

        if df is None or df.empty: return codes

        df.columns = ["real_price", "avg_amp", "amount_mean"]

        mask = (
            (df["real_price"] > min_price) & 
            (df["real_price"] <= max_price) &         # 锁定低价股区间
            (df["amount_mean"] > min_amount_avg) &    # 剔除无人问津的冷门股
            (df["avg_amp"] > min_amp)                 # 剔除织布机，留下上蹿下跳的妖股苗子
        )

        filtered = df[mask].index.get_level_values("instrument").unique().tolist()
        
        print(f"✅ 散户票量价精筛完成: {len(codes)} -> {len(filtered)} (已锁定低价高活标的)")
        return filtered

    except Exception as e:
        print(f"❌ filter_by_price_retail 异常: {e}")
        return codes


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
    
    # (2) 价格过滤
    codes = filter_by_price_retail(codes)

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