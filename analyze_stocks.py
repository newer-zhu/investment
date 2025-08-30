import os
import sys
import datetime
import pandas as pd
import akshare as ak

OUTPUT_FOLDER = "output"
FILENAME_PREFIX = "picked_stocks"


def find_csv_for_today_or_latest() -> str | None:
    today_name = f"{FILENAME_PREFIX}_{datetime.date.today().strftime('%Y%m%d')}.csv"
    today_path = os.path.join(OUTPUT_FOLDER, today_name)
    if os.path.exists(today_path):
        return today_path

    if not os.path.isdir(OUTPUT_FOLDER):
        return None

    candidates = [
        os.path.join(OUTPUT_FOLDER, f)
        for f in os.listdir(OUTPUT_FOLDER)
        if f.startswith(FILENAME_PREFIX) and f.endswith(".csv")
    ]
    if not candidates:
        return None
    return max(candidates, key=os.path.getmtime)


def analyze_row(row: pd.Series):
    """
    在这里填写对单只股票的分析逻辑。
    row 包含 CSV 的一行，例如列：代码、名称、价格、今日涨跌、总市值、年初至今涨跌幅、行业。
    你可以替换下面的示例打印为你的实际分析与处理。
    """
    code = str(row.get("代码"))
        
    # 获取今天日期
    end_date = datetime.date.today().strftime("%Y%m%d")

    # 往前推 180 天（大约 6 个月）
    start_date = (datetime.date.today() - datetime.timedelta(days=180)).strftime("%Y%m%d")
    if not macd_filter(code, start_date, end_date):
        return False
    return True


def macd_filter(symbol: str, start_date: str, end_date: str, adjust: str = "qfq") -> bool:
    """
    判断股票在给定区间内是否满足：
    1. MACD > 0
    2. 最近出现过金叉（DIF 上穿 DEA）
    """
    # 获取历史行情
    df = ak.stock_zh_a_hist(symbol=symbol, period="daily",
                            start_date=start_date, end_date=end_date, adjust=adjust)
    df.rename(columns={"日期": "date", "收盘": "close"}, inplace=True)
    df["close"] = df["close"].astype(float)

    # 计算MACD
    short, long, m = 12, 26, 9
    df["EMA12"] = df["close"].ewm(span=short, adjust=False).mean()
    df["EMA26"] = df["close"].ewm(span=long, adjust=False).mean()
    df["DIF"] = df["EMA12"] - df["EMA26"]
    df["DEA"] = df["DIF"].ewm(span=m, adjust=False).mean()
    df["MACD"] = 2 * (df["DIF"] - df["DEA"])
    # 计算 MA20
    df["MA20"] = df["close"].rolling(window=20).mean()

    # 条件1: MACD > 0 且 MACD < 0.2 且 收盘价 > MA20
    cond1 = df.iloc[-1]["MACD"] > 0 and df.iloc[-1]["MACD"] < 0.2 and df.iloc[-1]["close"] > df.iloc[-1]["MA20"]
    # 条件2: 最近是否有金叉
    df["golden_cross"] = (df["DIF"] > df["DEA"]) & (df["DIF"].shift(1) <= df["DEA"].shift(1))
    cond2 = df["golden_cross"].iloc[-10:].any()  # 例如只看最近10天是否有金叉

    # 计算 RSI (14日)
    delta = df["close"].diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)

    roll_up = up.rolling(window=14).mean()
    roll_down = down.rolling(window=14).mean()
    RS = roll_up / roll_down
    df["RSI"] = 100 - (100 / (1 + RS))
    
    # 条件3: RSI 区间控制
    cond3 = 35 < df.iloc[-1]["RSI"] < 75
    
    return  cond1 and cond2 and cond3

def analyze_csv(path: str):
    if not os.path.exists(path):
        print(f"文件不存在: {path}")
        return
    df = pd.read_csv(path)
    if df.empty:
        print("CSV 文件为空。")
        return

    print(f"开始分析文件: {os.path.basename(path)}，共 {len(df)} 条")
    passed_indices = []
    for idx, row in df.iterrows():
        code = str(row.get("代码"))
        name = row.get("名称")
        price = row.get("价格")
        ytd = row.get("年初至今涨跌幅")
        industry = row.get("行业")
        if analyze_row(row):
            passed_indices.append(idx)
            print(f"result: {code} {name} | 行业: {industry} | 价格: {price} | 年内: {ytd}")

    # 覆盖写回：仅保留满足条件的记录
    if passed_indices:
        filtered = df.loc[passed_indices].reset_index(drop=True)
        # 转换并按 年初至今涨跌幅 升序排序
        if "年初至今涨跌幅" in filtered.columns:
            filtered["年初至今涨跌幅"] = pd.to_numeric(filtered["年初至今涨跌幅"], errors="coerce")
            filtered = filtered.sort_values(by="年初至今涨跌幅", ascending=True).reset_index(drop=True)
        filtered.to_csv(path, index=False, encoding="utf-8-sig")
        print(f"已覆盖写回: {path}，保留 {len(filtered)} 条")
    else:
        print("没有满足条件的股票，原文件不做修改。")


def find_previous_csv_path() -> str | None:
    """找到今天之前最新的一份 picked_stocks_*.csv"""
    if not os.path.isdir(OUTPUT_FOLDER):
        return None
    today_name = f"{FILENAME_PREFIX}_{datetime.date.today().strftime('%Y%m%d')}.csv"
    candidates = [
        os.path.join(OUTPUT_FOLDER, f)
        for f in os.listdir(OUTPUT_FOLDER)
        if f.startswith(FILENAME_PREFIX) and f.endswith(".csv") and f != today_name
    ]
    if not candidates:
        return None
    # 取最近修改的一个
    return max(candidates, key=os.path.getmtime)


def find_today_cache_path() -> str:
    today_str = datetime.date.today().strftime("%Y-%m-%d")
    return os.path.join("cache", f"quote_cache_{today_str}.csv")


def _parse_percent_series(s: pd.Series) -> pd.Series:
    if s.dtype == object:
        s = s.astype(str).str.replace('%', '', regex=False)
    return pd.to_numeric(s, errors="coerce")


def append_prev_portfolio_avg_to_today(today_output_csv_path: str):
    """
    读取上一份 output CSV（昨天或更早），以其代码集为组合，
    用今天 cache/quote_cache_YYYY-MM-DD.csv 中的“涨跌幅”计算组合平均涨跌，
    并将摘要行追加到今天 output CSV 的最后一行。
    """
    prev_path = find_previous_csv_path()
    today_cache_path = find_today_cache_path()
    if not prev_path or not os.path.exists(today_output_csv_path) or not os.path.exists(today_cache_path):
        print("缺少上一份/今日输出或今日缓存CSV，跳过组合均值追加。")
        return

    try:
        prev_df = pd.read_csv(prev_path)
        today_output_df = pd.read_csv(today_output_csv_path)
        cache_df = pd.read_csv(today_cache_path, dtype={"代码": str})
    except Exception as e:
        print(f"读取 CSV 失败: {e}")
        return

    if prev_df.empty or today_output_df.empty or "代码" not in prev_df.columns or "代码" not in cache_df.columns:
        print("CSV 列不完整或为空，跳过组合均值追加。")
        return

    prev_codes = set(prev_df["代码"].astype(str).tolist())
    cache_df["代码"] = cache_df["代码"].astype(str)
    join_df = cache_df[cache_df["代码"].isin(prev_codes)].copy()

    if join_df.empty or "涨跌幅" not in join_df.columns:
        print("今日缓存 CSV 中缺少匹配代码或列‘涨跌幅’，跳过组合均值追加。")
        return

    join_df["涨跌幅"] = _parse_percent_series(join_df["涨跌幅"])  # 转为数值（单位：%）
    avg_rise = join_df["涨跌幅"].mean()

    # 仅打印结果，不写回 CSV
    print(f"上一期组合代码数: {len(prev_codes)}；今日平均涨跌幅（%）: {avg_rise}")


def get_prev_portfolio_avg_message() -> str:
    """
    计算上一份组合在今日的平均涨跌幅，并返回一行可展示的文本。
    不依赖传入路径，自动查找上一份 output 和今日 cache。
    找不到数据时返回空字符串。
    """
    prev_path = find_previous_csv_path()
    today_cache_path = find_today_cache_path()
    if not prev_path or not os.path.exists(today_cache_path):
        return ""
    try:
        prev_df = pd.read_csv(prev_path)
        cache_df = pd.read_csv(today_cache_path, dtype={"代码": str})
    except Exception:
        return ""
    if prev_df.empty or "代码" not in prev_df.columns or "代码" not in cache_df.columns:
        return ""
    prev_codes = set(prev_df["代码"].astype(str).tolist())
    cache_df["代码"] = cache_df["代码"].astype(str)
    join_df = cache_df[cache_df["代码"].isin(prev_codes)].copy()
    if join_df.empty or "涨跌幅" not in join_df.columns:
        return ""
    join_df["涨跌幅"] = _parse_percent_series(join_df["涨跌幅"])  # % → 数值
    avg_rise = join_df["涨跌幅"].mean()
    return f"上一期组合代码数: {len(prev_codes)}；今日平均涨跌幅（%）: {avg_rise:.2f}"



if __name__ == "__main__":
    # 用法：python analyze_stocks.py [可选: CSV路径]
    csv_path = sys.argv[1] if len(sys.argv) > 1 else find_csv_for_today_or_latest()
    if not csv_path:
        print("未找到 CSV 文件，请先运行选股导出或手动指定路径。")
        sys.exit(1)
    analyze_csv(csv_path)
    # 追加上一期组合的今日平均涨跌（读取 cache/ 当日行情）
    append_prev_portfolio_avg_to_today(csv_path)


