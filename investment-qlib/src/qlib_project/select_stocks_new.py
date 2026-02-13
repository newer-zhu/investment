import qlib
from qlib.data import D
import pandas as pd
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta
from constants import DATA_PATH

OUTPUT_PATH = DATA_PATH / "instruments" / "my_filtered_pool.txt"

def get_vectorized_filtered_codes(start_dt, end_dt):
    """
    逻辑：先拉取数值特征，再利用 Pandas 向量化处理代码字符串
    """
    # 1. 定义数值特征 (Qlib 表达式)
    fields = [
        "$close / $factor",           # 真实价格
        "Mean($amount, 20)",          # 20日均成交额
        "($high - $low) / $close",    # 振幅
    ]
    col_names = ["real_price", "amount_avg", "amplitude"]

    # 2. 一次性获取所有股票数据
    df = D.features(D.instruments(market='all'), fields, start_time=start_dt, end_time=end_dt)
    if df.empty:
        return []
    
    df.columns = col_names

    # 3. 仅保留最近一个交易日的数据进行筛选
    last_date = df.index.get_level_values('datetime').max()
    df = df.xs(last_date, level='datetime').copy()

    # 4. 【向量化字符串过滤】使用 Pandas 的 .str.contains 或 .str.startswith
    # 获取索引中的 instrument 字符串
    inst_series = df.index.get_level_values('instrument').str.upper()

    # 定义排除逻辑
    # 排除：科创板(688), 创业板(300, 301), 北交所(BJ), ST(如果代码包含ST), 指数(000/399开头)
    is_not_tech_board = ~inst_series.str.contains('SH688|SZ300|SZ301|BJ')
    is_not_index = ~inst_series.str.contains('SH000|SZ399')
    is_sh_sz = inst_series.str.startswith('SH') | inst_series.str.startswith('SZ')

    # 5. 数值逻辑过滤
    mask = (
        is_not_tech_board & 
        is_not_index & 
        is_sh_sz &
        (df["real_price"] > 5.0) &
        (df["amount_avg"] > 5000) & # 5000万
        (df["amplitude"] < 0.15)
    )

    return df[mask].index.get_level_values('instrument').unique().tolist()

def main():
    qlib.init(provider_uri=str(DATA_PATH))
    
    # 稍微拉长一点窗口确保能抓到最近一个交易日
    today = datetime.today()
    start_dt = (today - relativedelta(days=15)).strftime('%Y-%m-%d')
    end_dt = today.strftime('%Y-%m-%d')

    print(f"🚀 正在执行 Pandas 向量化筛选...")

    try:
        codes = get_vectorized_filtered_codes(start_dt, end_dt)
        
        if codes:
            OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
            with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
                for code in codes:
                    f.write(f"{code}\n")
            print(f"✅ 筛选完成! 剩余主板个股: {len(codes)} 只")
        else:
            print("⚠️ 未找到符合条件的股票。")
            
    except Exception as e:
        print(f"❌ 运行出错: {e}")

if __name__ == "__main__":
    main()