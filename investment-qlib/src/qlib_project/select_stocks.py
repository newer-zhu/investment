from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta
from utils.filter_stocks import get_industry_from_cache
import sys
import qlib
from qlib.data import D
import pandas as pd
from datetime import datetime, timedelta

PROJECT_ROOT = Path(__file__).resolve().parents[2]
QLIB_PATH = PROJECT_ROOT / "data" / "source"
DATA_PATH = QLIB_PATH / "instruments" / "all.txt"
OUTPUT_PATH= QLIB_PATH / "instruments" / "my_filtered_pool.txt"

current_dir = Path(__file__).resolve().parent
if str(current_dir) not in sys.path:
    sys.path.append(str(current_dir))

def load_recent_instruments(
    universe_file: Path,
    recent_months: int = 1,
):
    """
    读取 universe 文件
    条件：end_date 在 [today - recent_months, today] 之间
    """
    today = datetime.today().date()
    start_date = today - relativedelta(months=recent_months)

    codes = []

    with open(universe_file, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue

            code, _, end_date = line.strip().split()

            end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()

            if start_date <= end_dt <= today:
                codes.append(code)

    return codes

def filter_mainboard_stocks(codes):
    """
    过滤掉：
    - 创业板 (300 / 301)
    - 科创板 (688 / 689)
    - 新三板 (8xxxx)
    - 京交所 (BJ)
    """
    filtered = []

    for code in codes:
        code_upper = code.upper()

        # 京交所（直接排）
        if code_upper.startswith("BJ"):
            continue

        # 提取 6 位纯数字
        pure = "".join(filter(str.isdigit, code_upper)).zfill(6)

        # 创业板 / 科创板 / 新三板
        if pure.startswith(("300", "301", "688", "689", "8")):
            continue

        filtered.append(code)

    return filtered

def filter_by_price(
    codes,
    lookback_days=5,
    min_close_price=3.0,
):
    # 1. 安全获取日期：取数据库里存在的最新交易日
    all_calendar = D.calendar()
    last_trade_day = all_calendar[-1] 
    # 往前推 lookback_days 个交易日
    start_trade_day = all_calendar[-max(len(all_calendar), lookback_days)]

    fields = [
        "$close", 
        "$close / $factor", 
    ]
    
    # 2. 获取数据
    df = D.features(
        instruments=codes,
        fields=fields,
        start_time=start_trade_day,
        end_time=last_trade_day,
    )

    if df.empty:
        return df

    # 3. 稳妥地修改列名 (通过 rename 而不是直接覆盖)
    # Qlib 返回的列名通常是 fields 里的原字符串
    df.columns = ["close", "raw_close"]

    # 4. 过滤：我们通常只看“最新”一天的价格是否低于阈值
    # 如果只看最后一天：
    last_day_df = df.groupby('instrument').last()
    filtered_df = last_day_df[last_day_df['raw_close'] > min_close_price]

    return filtered_df

def filter_by_industry(codes, banned_industries=("军工",)):
    """
    根据行业关键字过滤股票
    依赖外部:
        get_industry_from_cache(code) -> str | None
    """
    filtered = []

    for code in codes:
        try:
            industry = get_industry_from_cache(code[2:])
        except Exception:
            # 缓存异常 / 查不到，直接跳过或保留，看你策略
            continue

        if not industry:
            continue

        # 命中黑名单行业，排除
        if any(bad in industry for bad in banned_industries):
            continue

        filtered.append(code)

    return filtered

def main():
    qlib.init(provider_uri=QLIB_PATH)
    universe_file = Path(DATA_PATH)

    codes = load_recent_instruments(
        universe_file=universe_file,
        recent_months=1,
    )
    codes = filter_mainboard_stocks(codes)
    # codes = filter_by_industry(
    #     codes,
    #     banned_industries=("军工","国防"))
    codes = filter_by_price(
        codes,
        lookback_days=1,
        min_close_price=5.0,
    )

    # 1. 从 Qlib 获取所有股票的原始日期配置
    # D.instruments('all') 返回的是 dict: {code: [[start_date, end_date], ...]}
    all_insts = D.instruments(market='all').list_instruments()
    
    # 2. 提取过滤后的代码
    filtered_codes = codes.index.unique().tolist()
    
    final_data = []
    
    for code in filtered_codes:
        if code in all_insts:
            # 获取该代码在 all.txt 中的日期范围
            # 注意：有些票可能有多段日期，通常取第一段即可 [0]
            start_dt, end_dt = all_insts[code][0]
            
            # 转换为字符串格式 YYYY-MM-DD
            start_str = start_dt.strftime('%Y-%m-%d')
            end_str = end_dt.strftime('%Y-%m-%d')
            
            final_data.append([code, start_str, end_str])
        else:
            # 万一在 all 里面没找到（理论上不会），给个默认值
            final_data.append([code, '2020-01-01', '2099-12-31'])

    # 3. 转换为 DataFrame 并保存为 Qlib 标准的 Tab 分隔格式
    res_df = pd.DataFrame(final_data)
    res_df.to_csv(
        OUTPUT_PATH, 
        sep='\t', 
        header=False, 
        index=False
    )
    print(f"已按原始日期范围保存至: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()