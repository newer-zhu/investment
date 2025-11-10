import akshare as ak
import pandas as pd
from api import get_stock_history
from tqdm import tqdm, trange
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import datetime
from typing import Dict, Any
from utils import parse_number, safe_get, is_industry, get_latest_quarter, load_config_from_ini
import tushare as ts
from time import sleep

ts.set_token('fbf2b6227d7a9df2ca8e3cc18c29cd38be4929bcd77dee4b95092f4f')
pro = ts.pro_api()
# 全局资金上限（单位：元）
MAX_FUNDS = float(load_config_from_ini("strategy").get("max_funds", 20000))
# 股票行业信息
# 代码,名称,最新价,涨跌幅,涨跌额,成交量,成交额,振幅,最高,最低,今开,昨收,量比,换手率,市盈率-动态,市净率,总市值,流通市值,涨速,5分钟涨跌,60日涨跌幅,年初至今涨跌幅
INFO_CACHE = {}  
# 资金流和换手率缓存
# 股票代码	int64	-
# 最新价	float64	-
# 涨跌幅	object	注意单位: %
# 换手率	object	-
# 流入资金	object	注意单位: 元
# 流出资金	object	注意单位: 元
# 净额	object	注意单位: 元
# 成交额	object	注意单位: 元
FUND_FLOW_DICT = {}
# 所有A股行情
QUOTE_DICT = {}
HALF_YEAR_HIGH_SET = set()
# 量价齐跌
ljqd_blacklist = set()

"""
加载连续量价齐跌的黑名单股票到全局 set
"""
def load_ljqd_blacklist(min_days=3, min_turnover=20):
    global ljqd_blacklist
    try:
        df = ak.stock_rank_ljqd_ths()

        df.rename(columns={
            "股票代码": "code",
            "量价齐跌天数": "days",
            "累计换手率": "turnover",
        }, inplace=True)

        # 转换数据类型
        df["days"] = df["days"].astype(int)
        df["turnover"] = parse_number(df["turnover"])

        # 过滤条件：连续天数 ≥ min_days 
        blacklist = df[(df["days"] >= min_days)]["code"].tolist()

        ljqd_blacklist = set(blacklist)
        print(f"[INFO] 已加载 {len(ljqd_blacklist)} 只量价齐跌股票到黑名单")

    except Exception as e:
        print(f"[ERROR] 加载量价齐跌黑名单失败: {e}")

"""初始化新高股票集合，只请求一次接口"""
def init_half_year_high(symbol: str = "历史新高"):
    global HALF_YEAR_HIGH_SET
    try:
        df = ak.stock_rank_cxg_ths(symbol=symbol)
        HALF_YEAR_HIGH_SET = set(df["股票代码"].astype(str).tolist())
        print(f"{symbol} 股票数量: {len(HALF_YEAR_HIGH_SET)}")
    except Exception as e:
        print(f"获取 {symbol} 数据失败: {e}")
        HALF_YEAR_HIGH_SET = set()

"""初始化资金流和换手率缓存，只调用一次接口，缓存所有字段"""
def init_fund_flow_cache():
    global FUND_FLOW_DICT

    FUND_FLOW_DICT.clear()

    try:
        # 拉取3日排行资金流数据（可改成 3日排行 / 5日排行 / 20日排行）
        df = ak.stock_fund_flow_individual(symbol="3日排行")

        drop_cols = {"序号", "股票简称"}  # 不需要的列

        for _, row in df.iterrows():
            code = str(row["股票代码"]).zfill(6)
            row_dict = {}
            for col in df.columns:
                if col in drop_cols:
                    continue
                val = row[col]
                # 数字或字符串数值统一解析
                row_dict[col] = parse_number(val) if isinstance(val, (str, int, float)) else val

            FUND_FLOW_DICT[code] = row_dict

        print(f"资金流缓存初始化完成，共 {len(FUND_FLOW_DICT)} 条记录")
    except Exception as e:
        print(f"初始化资金流缓存失败: {e}")

    """初始化全局行情缓存，每天只请求一次接口"""

def ts_code_to_code(ts_code: str) -> str:
    """将tushare的ts_code (如000001.SZ) 转换为6位数字代码 (如000001)"""
    return ts_code.split('.')[0]

def code_to_ts_code(code: str, market: str = None) -> str:
    """将6位数字代码转换为tushare的ts_code"""
    if market:
        return f"{code}.{market}"
    # 根据代码前缀判断市场
    if code.startswith(('600', '601', '603', '605', '688', '689')):
        return f"{code}.SH"  # 上海
    elif code.startswith(('000', '001', '002', '003', '300', '301')):
        return f"{code}.SZ"  # 深圳
    else:
        return f"{code}.SH"  # 默认上海

def build_quote_dict_from_hist(code: str, name: str, hist_df: pd.DataFrame) -> dict:
    """从历史数据构建QUOTE_DICT条目"""
    if hist_df.empty:
        return None
    
    latest = hist_df.iloc[-1]
    # 计算年初至今涨跌幅
    current_year_start = datetime.date.today().replace(month=1, day=1).strftime("%Y%m%d")
    year_start_price = None
    if len(hist_df) > 0:
        year_start_rows = hist_df[hist_df['trade_date'].astype(str) >= current_year_start]
        if len(year_start_rows) > 0:
            year_start_price = float(year_start_rows.iloc[0].get('close', 0))
    current_price = float(latest.get('close', 0)) if pd.notna(latest.get('close')) else 0
    ytd_pct_chg = ((current_price - year_start_price) / year_start_price * 100) if year_start_price and year_start_price > 0 else 0
    
    # 成交额单位转换：tushare返回的是千元，转换为元
    amount = float(latest.get('amount', 0)) * 1000 if pd.notna(latest.get('amount')) else 0
    
    return {
        "代码": code,
        "名称": name,
        "最新价": current_price,
        "涨跌幅": float(latest.get('pct_chg', 0)) if pd.notna(latest.get('pct_chg')) else 0,
        "涨跌额": float(latest.get('change', 0)) if pd.notna(latest.get('change')) else 0,
        "成交量": float(latest.get('vol', 0)) * 100 if pd.notna(latest.get('vol')) else 0,  # tushare返回的是手，转换为股
        "成交额": amount,
        "最高": float(latest.get('high', 0)) if pd.notna(latest.get('high')) else 0,
        "最低": float(latest.get('low', 0)) if pd.notna(latest.get('low')) else 0,
        "今开": float(latest.get('open', 0)) if pd.notna(latest.get('open')) else 0,
        "昨收": float(latest.get('pre_close', 0)) if pd.notna(latest.get('pre_close')) else 0,
        "换手率": 0,  # 需要从其他接口获取，暂时设为0
        "流通市值": 0,  # 需要从其他接口获取，暂时设为0
        "总市值": 0,  # 需要从其他接口获取，暂时设为0
        "年初至今涨跌幅": ytd_pct_chg,
        "市盈率-动态": 0,  # 需要从其他接口获取，暂时设为0
        "市净率": 0,  # 需要从其他接口获取，暂时设为0
    }

def init_quote_dict():
    global QUOTE_DICT
    
    cache_dir = "cache"
    history_cache_dir = os.path.join(cache_dir, "history")
    os.makedirs(cache_dir, exist_ok=True)
    os.makedirs(history_cache_dir, exist_ok=True)
    
    # 计算3年前的日期
    end_date = datetime.date.today().strftime("%Y%m%d")
    start_date = (datetime.date.today() - datetime.timedelta(days=3*365)).strftime("%Y%m%d")
    
    print("获取股票列表...")
    try:
        # 获取所有上市股票列表
        stock_basic_df = pro.stock_basic(exchange='', list_status='L', 
                                         fields='ts_code,symbol,name,area,industry,market')
        print(f"获取到 {len(stock_basic_df)} 只上市股票")
    except Exception as e:
        print(f"获取股票列表失败: {e}")
        # 回退：从数据库读取已有数据的全部 ts_code
        try:
            from db_utils import DbUtils
            db_fallback = DbUtils()
            stock_basic_df = db_fallback.query_df("SELECT DISTINCT ts_code FROM stock_data")
            if stock_basic_df is None or stock_basic_df.empty:
                print("数据库中未找到任何历史 ts_code，终止初始化。")
                return
            # 填充最小必要列，后续会根据 ts_code 生成 code，并且行业等字段后续不强依赖
            stock_basic_df['symbol'] = stock_basic_df['ts_code'].apply(ts_code_to_code)
            stock_basic_df['name'] = ""
            stock_basic_df['area'] = ""
            stock_basic_df['industry'] = ""
            stock_basic_df['market'] = ""
            print(f"从数据库回退获取到 {len(stock_basic_df)} 个 ts_code")
        except Exception as ex:
            print(f"数据库回退获取 ts_code 失败: {ex}")
            return
    
    print("获取ST股票列表...")
    try:
        # 获取ST股票列表
        st_df = pro.stock_st(ts_code='', fields='ts_code,name')
        st_codes = set(st_df['ts_code'].apply(ts_code_to_code).astype(str))
        print(f"获取到 {len(st_codes)} 只ST股票")
    except Exception as e:
        print(f"获取ST股票列表失败: {e}")
        st_codes = set()
    
    # 排除ST股票，只保留A股（排除创业板300/301、科创板688/689、新三板8开头）
    stock_basic_df['code'] = stock_basic_df['ts_code'].apply(ts_code_to_code)
    stock_basic_df = stock_basic_df[~stock_basic_df['code'].isin(st_codes)]
    # 排除创业板、科创板、新三板
    mask = ~stock_basic_df['code'].str.startswith(('300', '301', '688', '689', '8'))
    stock_basic_df = stock_basic_df[mask].reset_index(drop=True)
    print(f"排除ST和特殊板块后，剩余 {len(stock_basic_df)} 只股票")
    
    # 缓存行业信息到INFO_CACHE
    print("缓存股票行业信息...")
    global INFO_CACHE
    for _, row in stock_basic_df.iterrows():
        code = row['code']
        industry = row.get('industry', '')
        if industry and pd.notna(industry):
            INFO_CACHE[code] = industry
    
    # 缓存每只股票的历史数据（过去3年）
    print("开始缓存股票历史数据...")
    QUOTE_DICT.clear()
    cached_count = 0
    failed_count = 0
    
    from db_utils import DbUtils
    db = DbUtils()

    for idx, row in tqdm(stock_basic_df.iterrows(), total=len(stock_basic_df), desc="缓存历史数据"):
        ts_code = row['ts_code']
        code = row['code']
        name = row['name']
        
        try:
            # 查询该 ts_code 在库中已存在的最大交易日
            try:
                existing_df = db.query_df(
                    "SELECT DATE_FORMAT(MAX(trade_date), '%Y%m%d') AS max_date FROM stock_data WHERE ts_code = :ts_code",
                    {"ts_code": ts_code},
                )
                existing_end_date = str(existing_df.iloc[0]["max_date"]) if not existing_df.empty else None
            except Exception as e:
                existing_end_date = None
                if failed_count < 5:
                    print(f"查询 {code} 最新交易日失败: {e}")

            # 拉取并写入增量或全量数据
            fetch_start = None
            if existing_end_date and existing_end_date != "None":
                if existing_end_date < end_date:
                    # 从现有数据的下一天开始获取
                    fetch_start = str(int(existing_end_date) + 1)
            else:
                fetch_start = start_date

            if fetch_start is not None and fetch_start <= end_date:
                try:
                    new_df = pro.daily(ts_code=ts_code, start_date=fetch_start, end_date=end_date)
                    sleep(1) # 避免请求过于频繁
                    if new_df is not None and not new_df.empty and 'trade_date' in new_df.columns:
                        # 将 trade_date 转换为日期对象以匹配数据库 DATE 类型
                        # 同时确保列名与表结构一致
                        df_to_write = new_df.copy()
                        # Tushare 返回的 trade_date 为 'YYYYMMDD' 字符串，转换为日期
                        df_to_write['trade_date'] = pd.to_datetime(df_to_write['trade_date'], format="%Y%m%d").dt.date
                        # 写入（upsert）
                        db.upsert_df(
                            df=df_to_write[['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount']],
                            table_name="stock_data",
                            key_columns=["ts_code", "trade_date"],
                        )
                except Exception as e:
                    if failed_count < 10:
                        print(f"获取/写入 {code}({name}) 历史数据失败: {e}")
            
            # 构建QUOTE_DICT条目
            # 仅从数据库读取最新交易日的数据以构建行情，避免占用过多内存
            try:
                hist_df = db.query_df(
                    """
                    SELECT s.ts_code,
                           DATE_FORMAT(s.trade_date, '%Y%m%d') AS trade_date,
                           s.`open`, s.`high`, s.`low`, s.`close`, s.`pre_close`, s.`change`, s.`pct_chg`, s.`vol`, s.`amount`
                    FROM stock_data s
                    JOIN (
                        SELECT ts_code AS ts, MAX(trade_date) AS max_date
                        FROM stock_data
                        WHERE ts_code = :ts_code
                    ) t
                      ON s.ts_code = t.ts AND s.trade_date = t.max_date
                    """,
                    {"ts_code": ts_code},
                )
            except Exception as e:
                hist_df = None
                if failed_count < 5:
                    print(f"读取 {code} 历史数据失败: {e}")

            if hist_df is not None and not hist_df.empty and 'trade_date' in hist_df.columns:
                # 确保 trade_date 为字符串（YYYYMMDD）以兼容后续处理
                hist_df['trade_date'] = hist_df['trade_date'].astype(str)
                quote_entry = build_quote_dict_from_hist(code, name, hist_df.sort_values('trade_date'))
                if quote_entry:
                    QUOTE_DICT[code] = quote_entry
                    cached_count += 1
                else:
                    failed_count += 1
            else:
                failed_count += 1
        except Exception as e:
            failed_count += 1
            if failed_count <= 10:
                print(f"处理 {code}({name}) 时发生错误: {e}")
    
    print(f"历史数据缓存完成: 成功 {cached_count} 只，失败 {failed_count} 只")
    print(f"QUOTE_DICT 已加载 {len(QUOTE_DICT)} 只股票的行情数据")
        
    init_fund_flow_cache()  
    init_half_year_high()
    load_ljqd_blacklist()

def get_industry_from_cache(code):
    """获取股票行业信息，带缓存"""
    if code in INFO_CACHE:
        return INFO_CACHE[code]

    try:
        df_info = ak.stock_individual_info_em(symbol=code)
        industry_row = df_info[df_info["item"] == "行业"]
        if not industry_row.empty:
            industry = industry_row["value"].iloc[0]
        else:
            industry = None
    except:
        industry = None

    INFO_CACHE[code] = industry
    return industry

# 突破上涨的股票
def load_up_trend_stocks(option="30日均线"):
    df = ak.stock_rank_xstp_ths(symbol = option)
    df = df.rename(columns={"股票代码": "code"})
    return df[['code']]


def load_filter_lists(in_stock):
    # 向上突破A股
    stock_list = load_up_trend_stocks()

    # ST 股
    try:
        st_codes = set(ak.stock_zh_a_st_em()['代码'].astype(str))
    except Exception:
        st_codes = set()

    # 停牌股
    try:
        suspension_codes = set(ak.news_trade_notify_suspend_baidu()['股票代码'].astype(str))
    except Exception:
        suspension_codes = set()

    # 排除 创业板(300/301)、科创板(688/689)、新三板(8开头)
    mask = ~stock_list['code'].str.startswith(('300', '301', '688', '689', '8'))
    stock_list = stock_list[mask]

    # 黑名单集合
    excluded_codes = (
        st_codes
        | suspension_codes
        | set(map(str, HALF_YEAR_HIGH_SET))
        | set(map(str, ljqd_blacklist))
    )
    stock_list = stock_list[~stock_list['code'].isin(excluded_codes)]

    # === 加速资金过滤：把 QUOTE_DICT 转成 DataFrame merge ===
    quote_df = pd.DataFrame.from_dict(QUOTE_DICT, orient="index")
    quote_df = quote_df.reset_index().rename(columns={"index": "code"})
    stock_list = stock_list.merge(quote_df, on="code", how="left")

    # 资金条件过滤
    stock_list = stock_list[
        (stock_list["最新价"] * 100 <= MAX_FUNDS / 3)
        & (stock_list["最新价"] >= 5)
        & (stock_list["成交额"] >= 50_000_000)
    ]

    # === 行业过滤：一次性批量获取行业信息 ===
    industries = {code: get_industry_from_cache(code) for code in stock_list['code']}
    stock_list["industry"] = stock_list["code"].map(industries)

    industry_blacklist = ["国防", "军工", "钢铁","贵金属"]
    stock_list = stock_list[~stock_list["industry"].apply(lambda x: is_industry(x, industry_blacklist))]

    stock_list = stock_list.reset_index(drop=True)
    return stock_list[["code"]]

# 动态换手率判断
def get_dynamic_turnover_threshold(free_float_mkt_cap):
    """根据流通市值返回换手率阈值（百分比）"""
    if free_float_mkt_cap <= 50e8:  # 小盘
        return 0.15
    elif free_float_mkt_cap <= 200e8:  # 中盘
        return 0.08
    else:  # 大盘
        return 0.03

def calculate_total_score(fundamental_score: float, technical_score: float,
                          weight_f: float = 0.6, weight_t: float = 0.4) -> float:
    return round(weight_f * fundamental_score + weight_t * technical_score, 2)


"""筛选单只股票"""
def check_stock(code):
    steps = ["获取数据", "基本面评分", "技术面评分", "计算总分"]

    # 预先声明变量，避免作用域问题
    row, industry = None, None
    fundamental_score, technical_score, total_score = None, None, None
    price = None
    # 日期范围（最近半年）
    end_date = datetime.date.today().strftime("%Y%m%d")
    start_date = (datetime.date.today() - datetime.timedelta(days=180)).strftime("%Y%m%d")

    for step in steps:
        tqdm.write(f"[{code}] {step}...")
        if step == "获取数据":
            # 从缓存行情中取数据
            row = QUOTE_DICT.get(code)
            if not row:
                return None

            price = row["最新价"]
            turnover_rate = row["换手率"]

            # 换手率判断
            free_float_mkt_cap = row.get("流通市值", 0)
            dynamic_thr = get_dynamic_turnover_threshold(free_float_mkt_cap)
            fund_data = FUND_FLOW_DICT.get(code, {})
            if fund_data.get("连续换手率", 0) < dynamic_thr * 3 or turnover_rate < dynamic_thr:
                return None

            # 行业
            industry = get_industry_from_cache(code)

        elif step == "基本面评分":
            fundamental_score = calculate_fundamental_score(code, industry)

        elif step == "技术面评分":
            technical_score = calculate_technical_score(code, start_date, end_date)

        elif step == "计算总分":
            total_score = calculate_total_score(fundamental_score, technical_score)

    return {
        "代码": code,
        "名称": row["名称"],
        "价格": price,
        "今日涨跌": row["涨跌幅"],
        "总市值": row["总市值"],
        "年初至今涨跌幅": row["年初至今涨跌幅"],
        "行业": industry,
        "基本面评分": fundamental_score,
        "技术面评分": technical_score,
        "总分": total_score
    }

"""多线程选股"""
def pick_stocks_multithread(max_workers=20, strategy="a"):
    stock_list = load_filter_lists(strategy)
    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(check_stock, code) for code in stock_list['code'].tolist()]

        # 在每次执行一个任务后更新进度条
        for future in tqdm(as_completed(futures), total=len(stock_list), desc="选股中"):
            result = future.result()
            if result:
                results.append(result)

    return pd.DataFrame(results)

def calculate_technical_score(symbol: str, start_date: str, end_date: str, adjust: str = "qfq") -> float:
    """
    计算技术面评分 (0-100)，综合：
    - MACD 状态与金叉
    - 均线多头排列（MA5/10/20/60）
    - RSI 合理区间
    - 布林带位置
    - 成交量放大（量比近5/10）
    - 波底刚上翘（低位反转信号）
    """
    # 获取历史行情
    try:
        df = get_stock_history(symbol=symbol, start_date=start_date, end_date=end_date, adjust=adjust)
        if df.empty:
            return 0.0
    except Exception as e:
        print(f"Error getting stock history for {symbol}: {e}")
        return 0.0

    # ============ 技术指标 ============
    # MACD
    short, long, m = 12, 26, 9
    df["EMA12"] = df["close"].ewm(span=short, adjust=False).mean()
    df["EMA26"] = df["close"].ewm(span=long, adjust=False).mean()
    df["DIF"] = df["EMA12"] - df["EMA26"]
    df["DEA"] = df["DIF"].ewm(span=m, adjust=False).mean()
    df["MACD"] = 2 * (df["DIF"] - df["DEA"])

    # 均线
    for w in [5, 10, 20, 60]:
        df[f"MA{w}"] = df["close"].rolling(window=w).mean()

    # RSI (14日)
    delta = df["close"].diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    roll_up = up.rolling(window=14).mean()
    roll_down = down.rolling(window=14).mean()
    RS = roll_up / roll_down
    df["RSI"] = 100 - (100 / (1 + RS))

    # 布林带（20, 2）
    df["STD20"] = df["close"].rolling(window=20).std()
    df["BB_MID"] = df["MA20"]
    df["BB_UP"] = df["BB_MID"] + 2 * df["STD20"]
    df["BB_LOW"] = df["BB_MID"] - 2 * df["STD20"]

    # 成交量与量比
    vol_col = "成交量" if "成交量" in df.columns else None
    if vol_col:
        df["VOL5"] = pd.to_numeric(df[vol_col], errors="coerce").rolling(window=5).mean()
        df["VOL10"] = pd.to_numeric(df[vol_col], errors="coerce").rolling(window=10).mean()
        last_vol = pd.to_numeric(df.iloc[-1][vol_col], errors="coerce")
        vol_ratio = 0.0
        base = max(df.iloc[-1]["VOL5"] or 0, df.iloc[-1]["VOL10"] or 0)
        if pd.notna(last_vol) and pd.notna(base) and base > 0:
            vol_ratio = float(last_vol) / float(base)
    else:
        vol_ratio = 0.0

    # 最新一日
    last = df.iloc[-1]
    close = float(last["close"])
    macd = float(last.get("MACD", 0) or 0)
    ma5 = float(last.get("MA5", 0) or 0)
    ma10 = float(last.get("MA10", 0) or 0)
    ma20 = float(last.get("MA20", 0) or 0)
    ma60 = float(last.get("MA60", 0) or 0)
    rsi = float(last.get("RSI", 50) or 50)
    bb_mid = float(last.get("BB_MID", ma20) or ma20)
    bb_low = float(last.get("BB_LOW", ma20) or ma20)

    # 金叉检测（最近10天）
    df["golden_cross"] = (df["DIF"] > df["DEA"]) & (df["DIF"].shift(1) <= df["DEA"].shift(1))
    has_gc = bool(df["golden_cross"].iloc[-10:].any())

    # ============ 评分 ============
    score = 0.0

    # MACD 与金叉 (20)
    if macd > 0:
        score += 12
    if has_gc:
        score += 8

    # 均线多头 (25)
    if ma5 > ma10 > ma20 > ma60:
        score += 25
    elif ma5 > ma10 > ma20:
        score += 16
    elif ma5 > ma10:
        score += 8

    # RSI 区间 (15)
    if 40 <= rsi <= 70:
        score += 15
    elif 30 <= rsi <= 80:
        score += 8

    # 价格相对布林带 (10)
    if close > bb_mid:
        score += 10
    elif close > bb_low:
        score += 5

    # 量比 (20)
    if vol_ratio >= 1.8:
        score += 20
    elif vol_ratio >= 1.4:
        score += 14
    elif vol_ratio >= 1.2:
        score += 8

    # 波底刚上翘 (15)
    recent = df.iloc[-5:]
    near_bottom = close <= (df["close"].rolling(20).min().iloc[-1] * 1.1)
    rising = all(recent["close"].diff().iloc[-3:] > 0)
    ma5_up = df["MA5"].iloc[-1] > df["MA5"].iloc[-2]
    if near_bottom and rising and ma5_up:
        score += 15
        
    return float(min(100.0, max(0.0, score)))


def get_fundamental_data(code: str) -> Dict[str, Any]:
    """获取基本面数据，返回详细指标字典"""
    try:
        df = ak.stock_financial_abstract_ths(symbol=code)
        if df.empty:
            return {}
        
        latest = df.iloc[-1]  # 取最新季度
        
        # 基础财务指标
        net_profit = parse_number(safe_get(latest, "净利润"))
        roe = parse_number(safe_get(latest, "净资产收益率"))
        gross_margin = parse_number(safe_get(latest, "销售毛利率"))
        net_profit_growth = parse_number(safe_get(latest, "净利润同比增长率"))
        revenue_growth = parse_number(safe_get(latest, "营业总收入同比增长率"))
        debt_ratio = parse_number(safe_get(latest, "资产负债率"))
        current_ratio = parse_number(safe_get(latest, "流动比率"))
        
        row = QUOTE_DICT.get(code)
        pe_ratio = row.get("市盈率-动态", 0)
        pb_ratio = row.get("市净率", 0)

        data_out: Dict[str, Any] = {
            "net_profit": net_profit,
            "roe": roe,
            "gross_margin": gross_margin,
            "net_profit_growth": net_profit_growth,
            "revenue_growth": revenue_growth,
            "debt_ratio": debt_ratio,
            "current_ratio": current_ratio,
            "pe_ratio": pe_ratio,
            "pb_ratio": pb_ratio
        }
        return data_out
        
    except Exception as e:
        print(f"{code} 财务基本面数据获取失败: {e}")
        return {}
   
"""计算基本面评分 (0-100)"""
def calculate_fundamental_score(code: str, industry: str) -> float:
    """计算基本面评分 (0-100)，并融合原有硬性筛选逻辑为早退条件。
    行业用于决定不同阈值（科技成长 vs 传统）。
    """
    fundamental_data = get_fundamental_data(code)
    if not fundamental_data:
        return 0

    # 读取关键指标，缺失默认为 0
    def gv(key: str, default: float = 0.0) -> float:
        try:
            return float(fundamental_data.get(key, default))
        except Exception:
            return default

    net_profit = gv("net_profit", 0)
    roe = gv("roe", 0)
    gross_margin = gv("gross_margin", 0)
    net_profit_growth = gv("net_profit_growth", 0)
    revenue_growth = gv("revenue_growth", 0)
    debt_ratio = gv("debt_ratio", 0)
    current_ratio = gv("current_ratio", 0)
    pe_ratio = gv("pe_ratio", 0)

    # 行业类别：科技成长股 vs 传统行业
    is_tech = is_industry(industry, ["科技", "半导体", "互联网", "新能源", "软件", "芯片", "AI", "通信"])

    # 硬性门槛：不满足则直接 0 分
    if is_tech:
        if revenue_growth < 0.05:
            return 0
        if net_profit < 0 and net_profit_growth < -0.1:
            return 0
        if debt_ratio > 0.8:
            return 0
        # 科技股不过度限制毛利率/流动比率
    else:
        if net_profit <= 0:
            return 0
        if gross_margin < 0.1:
            return 0
        if net_profit_growth < 0:
            return 0
        if revenue_growth < 0.02:
            return 0
        if debt_ratio > 0.7:
            return 0
        if current_ratio < 0.8:
            return 0

    # 通过硬性门槛后，计算分数构成
    score = 0.0

    # 盈利能力 (30分)
    if roe > 0.18:
        score += 30
    elif roe > 0.12:
        score += 24
    elif roe > 0.08:
        score += 18
    elif roe > 0.05:
        score += 10

    # 成长性 (30分) —— 科技权重略高
    growth_weight = 30 if is_tech else 25
    growth_score = 0
    if revenue_growth > 0.25 and net_profit_growth > 0.25:
        growth_score = growth_weight
    elif revenue_growth > 0.15 and net_profit_growth > 0.15:
        growth_score = growth_weight * 0.8
    elif revenue_growth > 0.08 and net_profit_growth > 0.08:
        growth_score = growth_weight * 0.6
    elif revenue_growth > 0 and net_profit_growth > 0:
        growth_score = growth_weight * 0.4
    score += growth_score

    # 估值合理性 (20分) —— 科技给更宽容的区间
    valuation_weight = 20
    if pe_ratio > 0:
        if is_tech:
            if pe_ratio < 30:
                score += valuation_weight
            elif pe_ratio < 45:
                score += valuation_weight * 0.75
            elif pe_ratio < 60:
                score += valuation_weight * 0.5
            else:
                score += valuation_weight * 0.25
        else:
            if pe_ratio < 20:
                score += valuation_weight
            elif pe_ratio < 30:
                score += valuation_weight * 0.75
            elif pe_ratio < 50:
                score += valuation_weight * 0.5
            else:
                score += valuation_weight * 0.25

    # 财务健康度 (20分)
    if debt_ratio < 0.3 and current_ratio > 1.5:
        score += 20
    elif debt_ratio < 0.5 and current_ratio > 1.0:
        score += 15
    elif debt_ratio < 0.7 and current_ratio > 0.8:
        score += 10

    # 限制总分不超过 100
    return float(min(100.0, max(0.0, score)))


def save_and_print_picked(picked: pd.DataFrame, prefix="picked_stocks", folder="output"):
    """
    打印并导出选中的股票列表
    :param picked: DataFrame 股票数据
    :param prefix: 文件名前缀
    :param folder: 保存目录
    """
    if picked is None or picked.empty:
        print("没有选中的股票。")
        return

    # 打印
    print("初步选中的股票：")
    print(picked)

    # 文件名加日期
    today_str = datetime.date.today().strftime("%Y%m%d")
    filename = f"{prefix}_{today_str}.csv"

    # 确保目录存在
    os.makedirs(folder, exist_ok=True)
    filepath = os.path.join(folder, filename)

    # 导出
    picked.to_csv(filepath, index=False, encoding="utf-8-sig")
    print(f"已导出文件：{filepath}")

if __name__ == "__main__":
    # 禁止 requests 走代理

    pd.set_option("display.max_rows", None)
    init_quote_dict()  # 初始化

    picked = pick_stocks_multithread( max_workers=10, strategy="b")
    picked = picked.drop_duplicates(subset="代码").reset_index(drop=True)
    picked = picked.sort_values(by="总分", ascending=False).reset_index(drop=True)

    save_and_print_picked(picked)
    
    
