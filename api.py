import akshare as ak
import pandas as pd
import os
import datetime


def get_stock_history(symbol: str, start_date: str, end_date: str, adjust: str = "qfq") -> pd.DataFrame:
    """
    获取指定股票的历史行情数据，并进行基础清洗：
    - 使用CSV缓存，避免重复请求
    - 如果已有足够历史数据，只获取最新数据并追加
    - 重命名日期/收盘列为英文
    - 将收盘价转为数值并去除缺失
    """
    # 创建缓存目录
    cache_dir = "cache"
    os.makedirs(cache_dir, exist_ok=True)
    
    # 使用股票代码作为文件名前缀
    cache_file = os.path.join(cache_dir, f"{symbol}_history.csv")
    
    # 转换日期字符串为datetime对象以便比较
    start_dt = pd.to_datetime(start_date, format="%Y%m%d")
    end_dt = pd.to_datetime(end_date, format="%Y%m%d")
    today_dt = pd.Timestamp.now().normalize()
    
    df_cached = None
    need_full_fetch = True
    fetch_start_date = start_date
    fetch_end_date = end_date
    
    # 检查缓存文件是否存在
    if os.path.exists(cache_file):
        try:
            df_cached = pd.read_csv(cache_file)
            df_cached["date"] = pd.to_datetime(df_cached["date"])
            
            if not df_cached.empty:
                # 检查缓存数据的日期范围
                cached_start = df_cached["date"].min()
                cached_end = df_cached["date"].max()
                
                # 如果缓存数据覆盖了所需的开始日期，且最后日期不是今天
                if cached_start <= start_dt and cached_end < today_dt:
                    # 只需要获取从缓存最后日期到今天的数据
                    fetch_start_date = (cached_end + pd.Timedelta(days=1)).strftime("%Y%m%d")
                    fetch_end_date = end_date
                    need_full_fetch = False
                elif cached_start <= start_dt and cached_end >= today_dt:
                    # 缓存数据已经足够，直接返回过滤后的数据
                    df_filtered = df_cached[
                        (df_cached["date"] >= start_dt) & 
                        (df_cached["date"] <= end_dt)
                    ].copy()
                    df_filtered = df_filtered.sort_values("date").reset_index(drop=True)
                    return df_filtered
        except Exception as e:
            print(f"[WARNING] 读取缓存文件 {cache_file} 失败: {e}")
            df_cached = None
    
    # 从API获取数据
    try:
        df_new = ak.stock_zh_a_hist(symbol=symbol, period="daily",
                                    start_date=fetch_start_date, end_date=fetch_end_date, adjust=adjust)
        # 重命名日期列，保留所有其他列（如成交量等）
        df_new.rename(columns={"日期": "date", "收盘": "close"}, inplace=True)
        df_new["date"] = pd.to_datetime(df_new["date"])
        df_new["close"] = pd.to_numeric(df_new["close"], errors="coerce")
        df_new = df_new.dropna(subset=["close"]).reset_index(drop=True)
        
        if df_new.empty:
            # 如果新数据为空，返回缓存数据（如果存在）
            if df_cached is not None and not df_cached.empty:
                df_filtered = df_cached[
                    (df_cached["date"] >= start_dt) & 
                    (df_cached["date"] <= end_dt)
                ].copy()
                return df_filtered.sort_values("date").reset_index(drop=True)
            return pd.DataFrame()
        
        # 合并缓存数据和新数据
        if df_cached is not None and not df_cached.empty and not need_full_fetch:
            # 追加新数据到缓存
            df_combined = pd.concat([df_cached, df_new], ignore_index=True)
        else:
            # 首次获取或需要全量更新
            df_combined = df_new.copy()
        
        # 去重（按日期），保留最新的
        df_combined = df_combined.drop_duplicates(subset=["date"], keep="last")
        # 按日期排序
        df_combined = df_combined.sort_values("date").reset_index(drop=True)
        
        # 保存到CSV
        try:
            df_combined.to_csv(cache_file, index=False, encoding="utf-8-sig")
        except Exception as e:
            print(f"[WARNING] 保存缓存文件 {cache_file} 失败: {e}")
        
        # 返回请求日期范围内的数据
        df_result = df_combined[
            (df_combined["date"] >= start_dt) & 
            (df_combined["date"] <= end_dt)
        ].copy()
        
        return df_result.sort_values("date").reset_index(drop=True)
        
    except Exception as e:
        print(f"[ERROR] 获取股票 {symbol} 历史数据失败: {e}")
        # 如果API调用失败，尝试返回缓存数据
        if df_cached is not None and not df_cached.empty:
            df_filtered = df_cached[
                (df_cached["date"] >= start_dt) & 
                (df_cached["date"] <= end_dt)
            ].copy()
            return df_filtered.sort_values("date").reset_index(drop=True)
        return pd.DataFrame()
