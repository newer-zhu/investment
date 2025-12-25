import os
import sys
import datetime
import pandas as pd
import akshare as ak
from logger import logger

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
    return os.path.join("cache", "market", f"quote_cache_{today_str}.csv")


def _parse_percent_series(s: pd.Series) -> pd.Series:
    if s.dtype == object:
        s = s.astype(str).str.replace('%', '', regex=False)
    return pd.to_numeric(s, errors="coerce")


# def append_prev_portfolio_avg_to_today(today_output_csv_path: str):
    """
    读取上一份 output CSV（昨天或更早），以其代码集为组合，
    用今天 cache/quote_cache_YYYY-MM-DD.csv 中的“涨跌幅”计算组合平均涨跌，
    并将摘要行追加到今天 output CSV 的最后一行。
    """
    prev_path = find_previous_csv_path()
    today_cache_path = find_today_cache_path()
    if not prev_path or not os.path.exists(today_output_csv_path) or not os.path.exists(today_cache_path):
        logger.warning("缺少上一份/今日输出或今日缓存CSV，跳过组合均值追加。")
        return

    try:
        prev_df = pd.read_csv(prev_path)
        today_output_df = pd.read_csv(today_output_csv_path)
        cache_df = pd.read_csv(today_cache_path, dtype={"代码": str})
    except Exception as e:
        logger.error(f"读取 CSV 失败: {e}", exc_info=True)
        return

    if prev_df.empty or today_output_df.empty or "代码" not in prev_df.columns or "代码" not in cache_df.columns:
        logger.warning("CSV 列不完整或为空，跳过组合均值追加。")
        return

    prev_codes = set(prev_df["代码"].astype(str).tolist())
    cache_df["代码"] = cache_df["代码"].astype(str)
    join_df = cache_df[cache_df["代码"].isin(prev_codes)].copy()

    if join_df.empty or "涨跌幅" not in join_df.columns:
        logger.warning("今日缓存 CSV 中缺少匹配代码或列'涨跌幅'，跳过组合均值追加。")
        return

    join_df["涨跌幅"] = _parse_percent_series(join_df["涨跌幅"])  # 转为数值（单位：%）
    avg_rise = join_df["涨跌幅"].mean()

    # 仅打印结果，不写回 CSV
    logger.info(f"上一期组合代码数: {len(prev_codes)}；今日平均涨跌幅（%）: {avg_rise:.2f}")


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
        prev_df = pd.read_csv(prev_path).head(10)  # ✅ 只保留前10行
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
        logger.error("未找到 CSV 文件，请先运行选股导出或手动指定路径。")
        sys.exit(1)

    # 追加上一期组合的今日平均涨跌（读取 cache/ 当日行情）
    # append_prev_portfolio_avg_to_today(csv_path)


