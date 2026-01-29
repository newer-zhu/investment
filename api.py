import akshare as ak
import pandas as pd
import os
import datetime
from logger import logger
from utils import get_prev_trade_date, find_first_missing_trade_date



def get_stock_history(
    symbol: str,
    start_date: str,
    end_date: str,
    adjust: str = "qfq"
) -> pd.DataFrame:
    """
    获取股票历史行情（严格交易日连续性保证版）
    """

    cache_dir = os.path.join("cache", "history")
    os.makedirs(cache_dir, exist_ok=True)
    cache_file = os.path.join(cache_dir, f"{symbol}_history.csv")

    start_dt = pd.to_datetime(start_date, format="%Y%m%d")
    end_dt = pd.to_datetime(end_date, format="%Y%m%d")
    today_dt = pd.Timestamp.now().normalize()

    trade_calendar = get_prev_trade_date()
    trade_calendar = trade_calendar[
        (trade_calendar >= start_dt) & (trade_calendar <= min(end_dt, today_dt))
    ]

    df_cached = None
    fetch_start_dt = start_dt
    fetch_end_dt = min(end_dt, today_dt)

    # ======================
    # 读取缓存
    # ======================
    if os.path.exists(cache_file):
        try:
            df_cached = pd.read_csv(cache_file)
            df_cached["date"] = pd.to_datetime(df_cached["date"])
            df_cached = df_cached.sort_values("date").reset_index(drop=True)

            if not df_cached.empty:
                cached_dates = df_cached["date"]

                # 1️⃣ 检查交易日连续性
                missing_dt = find_first_missing_trade_date(
                    cached_dates, trade_calendar
                )

                if missing_dt is not None:
                    logger.warning(
                        f"{symbol} 缓存交易日断裂，从 {missing_dt.date()} 开始回补"
                    )
                    fetch_start_dt = missing_dt
                else:
                    # 2️⃣ 缓存连续，检查是否覆盖目标区间
                    cached_start = cached_dates.min()
                    cached_end = cached_dates.max()

                    if cached_start <= start_dt and cached_end >= fetch_end_dt:
                        df_result = df_cached[
                            (df_cached["date"] >= start_dt) &
                            (df_cached["date"] <= fetch_end_dt)
                        ].copy()
                        return df_result.reset_index(drop=True)

                    # 3️⃣ 只补最后一段
                    fetch_start_dt = cached_end + pd.Timedelta(days=1)

        except Exception as e:
            logger.warning(f"读取缓存失败，重新全量获取: {e}")
            df_cached = None

    # ======================
    # 拉取 API 数据
    # ======================
    if fetch_start_dt > fetch_end_dt:
        # 理论上不应该发生
        fetch_start_dt = fetch_end_dt

    logger.info(
        f"获取 {symbol} 行情: {fetch_start_dt.date()} → {fetch_end_dt.date()}"
    )

    try:
        df_new = ak.stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=fetch_start_dt.strftime("%Y%m%d"),
            end_date=fetch_end_dt.strftime("%Y%m%d"),
            adjust=adjust,
        )

        if df_new.empty:
            logger.warning(f"{symbol} API 返回空数据")
            return df_cached if df_cached is not None else pd.DataFrame()

        df_new.rename(
            columns={"日期": "date", "收盘": "close"},
            inplace=True
        )
        df_new["date"] = pd.to_datetime(df_new["date"])
        df_new["close"] = pd.to_numeric(df_new["close"], errors="coerce")
        df_new = df_new.dropna(subset=["close"])
        df_new = df_new.sort_values("date").reset_index(drop=True)

        # ======================
        # 合并 + 去重
        # ======================
        if df_cached is not None and not df_cached.empty:
            df_all = pd.concat([df_cached, df_new], ignore_index=True)
        else:
            df_all = df_new.copy()

        df_all = (
            df_all
            .drop_duplicates(subset=["date"], keep="last")
            .sort_values("date")
            .reset_index(drop=True)
        )

        # ======================
        # 最终连续性校验（兜底）
        # ======================
        final_missing = find_first_missing_trade_date(
            df_all["date"], trade_calendar
        )
        if final_missing is not None:
            logger.error(
                f"{symbol} 最终数据仍存在交易日缺口: {final_missing.date()}"
            )

        # ======================
        # 写缓存
        # ======================
        df_all.to_csv(cache_file, index=False, encoding="utf-8-sig")

        df_result = df_all[
            (df_all["date"] >= start_dt) &
            (df_all["date"] <= fetch_end_dt)
        ].copy()

        return df_result.reset_index(drop=True)

    except Exception as e:
        logger.error(f"{symbol} 历史行情获取失败: {e}", exc_info=True)
        if df_cached is not None:
            return df_cached[
                (df_cached["date"] >= start_dt) &
                (df_cached["date"] <= fetch_end_dt)
            ].copy()
        return pd.DataFrame()