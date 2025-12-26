import akshare as ak
import pandas as pd
from api import get_stock_history
from tqdm import tqdm, trange
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import datetime
from typing import Dict, Any
from utils import parse_number, safe_get, is_industry, get_latest_quarter, load_config_from_ini
from logger import logger


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
        logger.info(f"已加载 {len(ljqd_blacklist)} 只量价齐跌股票到黑名单")

    except Exception as e:
        logger.error(f"加载量价齐跌黑名单失败: {e}", exc_info=True)

"""初始化新高股票集合，只请求一次接口"""
def init_half_year_high(symbol: str = "历史新高"):
    global HALF_YEAR_HIGH_SET
    try:
        df = ak.stock_rank_cxg_ths(symbol=symbol)
        HALF_YEAR_HIGH_SET = set(df["股票代码"].astype(str).tolist())
        logger.info(f"{symbol} 股票数量: {len(HALF_YEAR_HIGH_SET)}")
    except Exception as e:
        logger.error(f"获取 {symbol} 数据失败: {e}", exc_info=True)
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

        logger.info(f"资金流缓存初始化完成，共 {len(FUND_FLOW_DICT)} 条记录")
    except Exception as e:
        logger.error(f"初始化资金流缓存失败: {e}", exc_info=True)

    """初始化全局行情缓存，每天只请求一次接口"""

def init_quote_dict():
    global QUOTE_DICT

    today_str = pd.Timestamp.now().strftime("%Y-%m-%d")
    market_cache_dir = os.path.join("cache", "market")
    os.makedirs(market_cache_dir, exist_ok=True)   # 确保 cache/market 文件夹存在
    CACHE_FILE = os.path.join(market_cache_dir, f"quote_cache_{today_str}.csv")

    if os.path.exists(CACHE_FILE):
        logger.info("使用本地缓存行情数据")
        quote_df = pd.read_csv(CACHE_FILE, dtype={"代码": str})
    else:
        logger.info("本地缓存无效，联网拉取行情数据...")
        quote_df = ak.stock_sh_a_spot_em()
        quote_df.to_csv(CACHE_FILE, index=False)
        logger.info(f"行情数据拉取完成，共 {len(quote_df)} 条记录，已保存到缓存")

    logger.info(f"正在处理行情数据，共 {len(quote_df)} 条...")
    today_dt = pd.Timestamp.now().normalize()
    
    for _, row in tqdm(quote_df.iterrows(), total=len(quote_df), desc="加载行情数据", leave=False):
        code = row["代码"]
        QUOTE_DICT[code] = {
            col: parse_number(row[col]) if col not in ["代码", "名称"] else row[col]
            for col in quote_df.columns
        }
        
        # 将今日数据追加到对应的历史缓存文件中
        try:
            history_cache_dir = os.path.join("cache", "history")
            os.makedirs(history_cache_dir, exist_ok=True)
            history_file = os.path.join(history_cache_dir, f"{code}_history.csv")
            
            # 构建今日的历史数据行
            today_data = {
                "date": today_dt,
                "股票代码": code,
                "开盘": parse_number(row.get("今开", 0)),
                "close": parse_number(row.get("最新价", 0)),
                "最高": parse_number(row.get("最高", 0)),
                "最低": parse_number(row.get("最低", 0)),
                "成交量": parse_number(row.get("成交量", 0)),
                "成交额": parse_number(row.get("成交额", 0)),
                "振幅": parse_number(row.get("振幅", 0)),
                "涨跌幅": parse_number(row.get("涨跌幅", 0)),
                "涨跌额": parse_number(row.get("涨跌额", 0)),
                "换手率": parse_number(row.get("换手率", 0)),
            }
            
            # 检查历史文件是否存在
            if os.path.exists(history_file):
                # 读取现有数据
                df_history = pd.read_csv(history_file)
                df_history["date"] = pd.to_datetime(df_history["date"])
                
                # 检查今天是否已经有数据
                if df_history["date"].max() < today_dt:
                    # 追加今日数据
                    df_new = pd.DataFrame([today_data])
                    df_combined = pd.concat([df_history, df_new], ignore_index=True)
                    df_combined = df_combined.sort_values("date").reset_index(drop=True)
                    df_combined.to_csv(history_file, index=False, encoding="utf-8-sig")
                # 如果今天已有数据，不重复追加
            else:
                # 创建新文件
                df_new = pd.DataFrame([today_data])
                df_new.to_csv(history_file, index=False, encoding="utf-8-sig")
        except Exception as e:
            # 单个股票追加失败不影响整体流程
            logger.debug(f"追加股票 {code} 今日数据到历史缓存失败: {e}")
    
    logger.info(f"行情数据加载完成，共 {len(QUOTE_DICT)} 只股票，今日数据已同步到历史缓存")
        
    logger.info("开始初始化资金流缓存...")
    init_fund_flow_cache()  
    logger.info("开始初始化历史新高股票...")
    init_half_year_high()
    logger.info("开始加载量价齐跌黑名单...")
    load_ljqd_blacklist()
    logger.info("所有初始化完成")
    

"""获取股票行业信息，带CSV缓存"""
def get_industry_from_cache(code):
    # 首次调用时，从CSV加载缓存
    if not INFO_CACHE:
        industry_cache_dir = os.path.join("cache", "industry")
        os.makedirs(industry_cache_dir, exist_ok=True)
        cache_file = os.path.join(industry_cache_dir, "stock_industry_cache.csv")
        
        if os.path.exists(cache_file):
            try:
                df_cache = pd.read_csv(cache_file, dtype={"code": str})
                for _, row in df_cache.iterrows():
                    INFO_CACHE[row["code"]] = row["industry"] if pd.notna(row["industry"]) else None
            except Exception as e:
                logger.warning(f"加载行业缓存失败: {e}")
    
    # 检查内存缓存
    if code in INFO_CACHE:
        return INFO_CACHE[code]

    # 如果不在缓存中，调用API获取
    try:
        df_info = ak.stock_individual_info_em(symbol=code)
        industry_row = df_info[df_info["item"] == "行业"]
        if not industry_row.empty:
            industry = industry_row["value"].iloc[0]
        else:
            industry = None
    except Exception as e:
        logger.warning(f"获取 {code} 行业信息失败: {e}")
        industry = None

    # 更新内存缓存
    INFO_CACHE[code] = industry
    
    # 保存到CSV
    industry_cache_dir = os.path.join("cache", "industry")
    os.makedirs(industry_cache_dir, exist_ok=True)
    cache_file = os.path.join(industry_cache_dir, "stock_industry_cache.csv")
    
    try:
        # 读取现有数据或创建新的DataFrame
        if os.path.exists(cache_file):
            df_cache = pd.read_csv(cache_file, dtype={"code": str})
            # 如果code已存在，更新；否则追加
            if code in df_cache["code"].values:
                df_cache.loc[df_cache["code"] == code, "industry"] = industry
            else:
                df_cache = pd.concat([df_cache, pd.DataFrame({"code": [code], "industry": [industry]})], ignore_index=True)
        else:
            df_cache = pd.DataFrame({"code": [code], "industry": [industry]})
        
        # 保存到CSV
        df_cache.to_csv(cache_file, index=False, encoding="utf-8-sig")
    except Exception as e:
        logger.warning(f"保存行业缓存失败: {e}")
    
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
    logger.debug("加载ST股列表...")
    try:
        st_codes = set(ak.stock_zh_a_st_em()['代码'].astype(str))
        logger.debug(f"加载ST股完成，共 {len(st_codes)} 只")
    except Exception as e:
        logger.warning(f"加载ST股失败: {e}")
        st_codes = set()

    # 停牌股
    logger.debug("加载停牌股列表...")
    try:
        suspension_codes = set(ak.news_trade_notify_suspend_baidu()['股票代码'].astype(str))
        logger.debug(f"加载停牌股完成，共 {len(suspension_codes)} 只")
    except Exception as e:
        logger.warning(f"加载停牌股失败: {e}")
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
    logger.info(f"开始批量获取行业信息，共 {len(stock_list)} 只股票...")
    industries = {}
    for code in tqdm(stock_list['code'], desc="获取行业信息", leave=False):
        industries[code] = get_industry_from_cache(code)
    stock_list["industry"] = stock_list["code"].map(industries)
    logger.debug(f"行业信息获取完成")

    industry_blacklist = ["国防", "军工", "钢铁","贵金属"]
    stock_list = stock_list[~stock_list["industry"].apply(lambda x: is_industry(x, industry_blacklist))]

    stock_list = stock_list.reset_index(drop=True)
    logger.info(f"筛选完成，剩余 {len(stock_list)} 只股票")
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

def calculate_total_score(
    fundamental_score: float,
    technical_score: float,
    weight_f: float = 0.65,
    weight_t: float = 0.35
) -> float:
    """
    A股风格总评分：
    - 基本面为核心（底座）
    - 技术面作为加速/抑制因子
    - 适合股票池初筛
    """

    # 基本分（线性）
    base = weight_f * fundamental_score + weight_t * technical_score

    # 技术面调节因子（不对称）
    if technical_score >= 75:
        factor = 1.05   # 强趋势加速
    elif technical_score >= 60:
        factor = 1.02
    elif technical_score >= 45:
        factor = 1.0    # 中性
    elif technical_score >= 30:
        factor = 0.95   # 弱势压制
    else:
        factor = 0.9    # 明显走弱

    total = base * factor
    return round(min(100.0, max(0.0, total)), 2)


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
        logger.debug(f"[{code}] {step}...")
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
    logger.info(f"开始多线程选股，线程数: {max_workers}, 策略: {strategy}")
    stock_list = load_filter_lists(strategy)
    logger.info(f"待筛选股票数量: {len(stock_list)}")
    
    if stock_list.empty:
        logger.warning("股票列表为空，无法进行选股")
        return pd.DataFrame()
    
    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(check_stock, code) for code in stock_list['code'].tolist()]

        # 在每次执行一个任务后更新进度条
        for future in tqdm(as_completed(futures), total=len(stock_list), desc="选股中", unit="只"):
            result = future.result()
            if result:
                results.append(result)

    logger.info(f"选股完成，共选出 {len(results)} 只符合条件的股票")
    return pd.DataFrame(results)

def calculate_technical_score(symbol: str, start_date: str, end_date: str, adjust: str = "qfq") -> float:
    """
    A股风格技术面评分 (0-100)
    用于初步股票池筛选，强调可参与性与风险过滤
    - 使用前复权价格（qfq）
    - 目的：判断趋势、结构与形态
    - 不用于收益回测或精确择时
    """
    try:
        df = get_stock_history(symbol=symbol, start_date=start_date, end_date=end_date, adjust=adjust)
        if df.empty or len(df) < 60:
            return 0.0
    except Exception:
        return 0.0

    # === MACD ===
    df["EMA12"] = df["close"].ewm(span=12, adjust=False).mean()
    df["EMA26"] = df["close"].ewm(span=26, adjust=False).mean()
    df["DIF"] = df["EMA12"] - df["EMA26"]
    df["DEA"] = df["DIF"].ewm(span=9, adjust=False).mean()
    df["MACD"] = 2 * (df["DIF"] - df["DEA"])

    # === 均线 ===
    for w in [5, 10, 20, 60]:
        df[f"MA{w}"] = df["close"].rolling(w).mean()

    # === RSI ===
    delta = df["close"].diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    rs = up.rolling(14).mean() / down.rolling(14).mean()
    df["RSI"] = 100 - (100 / (1 + rs))

    # === 布林带 ===
    df["STD20"] = df["close"].rolling(20).std()
    df["BB_MID"] = df["MA20"]
    df["BB_UP"] = df["BB_MID"] + 2 * df["STD20"]
    df["BB_LOW"] = df["BB_MID"] - 2 * df["STD20"]

    # === 成交量 ===
    vol_col = "成交量" if "成交量" in df.columns else None
    vol_ratio = 0.0
    if vol_col:
        vol = pd.to_numeric(df[vol_col], errors="coerce")
        base = max(vol.rolling(5).mean().iloc[-1] or 0,
                   vol.rolling(10).mean().iloc[-1] or 0)
        if base > 0:
            vol_ratio = float(vol.iloc[-1]) / base

    last = df.iloc[-1]
    prev = df.iloc[-2]

    close = float(last["close"])
    ma5, ma10, ma20, ma60 = last["MA5"], last["MA10"], last["MA20"], last["MA60"]
    rsi = float(last["RSI"] or 50)
    macd = float(last["MACD"] or 0)
    bb_mid, bb_low, bb_up = last["BB_MID"], last["BB_LOW"], last["BB_UP"]

    score = 0.0

    # ===== 1️⃣ 趋势状态（只要“不差”即可）=====
    if ma5 > ma10 > ma20:
        score += 12
    elif ma5 > ma10:
        score += 6

    # 过度拉升直接降权（A股非常重要）
    if close > bb_up * 1.02:
        score *= 0.7

    # ===== 2️⃣ MACD：只奖励“刚转强”=====
    macd_turn_up = macd > 0 and prev["MACD"] <= 0
    if macd_turn_up:
        score += 15
    elif macd > 0:
        score += 6

    # ===== 3️⃣ RSI：回落不破 & 重新抬头 =====
    if 35 <= rsi <= 65:
        score += 12
    elif rsi < 30:
        score += 6   # 超跌反弹预期
    elif rsi > 80:
        score *= 0.6  # 情绪过热惩罚

    # ===== 4️⃣ 量能：必须与位置绑定 =====
    near_mid_low = close <= bb_mid * 1.02
    if vol_ratio >= 1.5 and near_mid_low:
        score += 20
    elif vol_ratio >= 1.2 and near_mid_low:
        score += 12
    elif vol_ratio >= 2.0:
        score *= 0.7  # 高位爆量，反而危险

    # ===== 5️⃣ 低位启动（你原来的“波底上翘”，更 A 股化）=====
    recent = df.iloc[-5:]
    price_rising = recent["close"].diff().iloc[-3:].gt(0).all()
    ma5_up = df["MA5"].iloc[-1] > df["MA5"].iloc[-2]
    near_recent_low = close <= df["close"].rolling(20).min().iloc[-1] * 1.12

    if near_recent_low and price_rising and ma5_up:
        score += 18

    return float(min(100.0, max(0.0, score)))

def get_fundamental_data(code: str) -> Dict[str, Any]:
    """获取基本面数据，返回详细指标字典，带CSV缓存（每月自动刷新）"""
    # 创建缓存目录
    financial_cache_dir = os.path.join("cache", "financial")
    os.makedirs(financial_cache_dir, exist_ok=True)
    
    # 使用股票代码作为文件名前缀
    cache_file = os.path.join(financial_cache_dir, f"{code}_financial.csv")
    
    # 检查缓存文件是否存在且是否需要刷新（每月刷新一次）
    need_refresh = False
    df = None
    
    if os.path.exists(cache_file):
        try:
            # 检查文件修改时间，如果超过1个月则刷新
            file_mtime = os.path.getmtime(cache_file)
            file_time = datetime.datetime.fromtimestamp(file_mtime)
            time_diff = datetime.datetime.now() - file_time
            
            # 如果缓存超过30天，需要刷新
            if time_diff.days > 30:
                need_refresh = True
                logger.info(f"{code} 财务缓存已超过30天，刷新数据...")
            else:
                # 缓存仍然有效，加载缓存数据
                df = pd.read_csv(cache_file)
                if df.empty:
                    df = None
                    need_refresh = True
        except Exception as e:
            logger.warning(f"读取财务缓存文件 {cache_file} 失败: {e}")
            df = None
            need_refresh = True
    else:
        need_refresh = True
    
    # 如果缓存不存在或需要刷新，从API获取
    if need_refresh:
        try:
            df = ak.stock_financial_abstract_ths(symbol=code)
            if df.empty:
                return {}
            
            # 保存到CSV
            try:
                df.to_csv(cache_file, index=False, encoding="utf-8-sig")
                logger.debug(f"{code} 财务数据已保存到缓存")
            except Exception as e:
                logger.warning(f"保存财务缓存文件 {cache_file} 失败: {e}")
        except Exception as e:
            logger.error(f"{code} 财务基本面数据获取失败: {e}")
            return {}
    
    if df.empty:
        return {}
    
    try:
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
        pe_ratio = row.get("市盈率-动态", 0) if row else 0
        pb_ratio = row.get("市净率", 0) if row else 0

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
        logger.error(f"{code} 财务基本面数据处理失败: {e}", exc_info=True)
        return {}
   
def calculate_fundamental_score(code: str, industry: str) -> float:
    """A股风格：基本面安全度 + 可参与度评分（0-100）
    用于初步股票池筛选，不做精细定价
    """
    fundamental_data = get_fundamental_data(code)
    if not fundamental_data:
        return 0.0

    def gv(key: str, default: float = 0.0) -> float:
        try:
            return float(fundamental_data.get(key, default))
        except Exception:
            return default

    net_profit = gv("net_profit")
    roe = gv("roe")
    gross_margin = gv("gross_margin")
    net_profit_growth = gv("net_profit_growth")
    revenue_growth = gv("revenue_growth")
    debt_ratio = gv("debt_ratio")
    current_ratio = gv("current_ratio")
    pe_ratio = gv("pe_ratio")

    is_tech = is_industry(
        industry,
        ["科技", "半导体", "互联网", "新能源", "软件", "芯片", "AI", "通信"]
    )

    # ===== 硬性早退：排雷优先 =====
    if is_tech:
        # 科技股：允许波动，但不能失控
        if revenue_growth < -0.08:
            return 0.0
        if net_profit < 0 and net_profit_growth < -0.3:
            return 0.0
        if debt_ratio > 0.75:
            return 0.0
    else:
        # 传统行业：必须活得稳
        if net_profit <= 0:
            return 0.0
        if revenue_growth < -0.05:
            return 0.0
        if debt_ratio > 0.7:
            return 0.0
        if current_ratio < 0.7:
            return 0.0
        if gross_margin < 0.08:
            return 0.0

    score = 0.0

    # ===== 盈利质量（弱区分，不追极致）=====
    if roe >= 0.15:
        score += 18
    elif roe >= 0.10:
        score += 15
    elif roe >= 0.06:
        score += 10
    elif roe >= 0.03:
        score += 5

    # ===== 成长性（容忍波动，惩罚断档）=====
    if revenue_growth > 0.20 and net_profit_growth > 0.20:
        score += 25
    elif revenue_growth > 0.10:
        score += 18
    elif revenue_growth > 0:
        score += 12
    elif revenue_growth > -0.05:
        score += 6

    # ===== 财务安全（兜底逻辑）=====
    if debt_ratio < 0.35 and current_ratio > 1.2:
        score += 20
    elif debt_ratio < 0.55 and current_ratio > 0.9:
        score += 12
    elif debt_ratio < 0.7:
        score += 6

    # ===== 估值：只做惩罚，不做奖励 =====
    if pe_ratio > 0:
        if is_tech and pe_ratio > 80:
            score *= 0.75
        elif not is_tech and pe_ratio > 50:
            score *= 0.6

    return float(min(100.0, max(0.0, score)))

def save_and_print_picked(picked: pd.DataFrame, prefix="picked_stocks", folder="output"):
    """
    打印并导出选中的股票列表
    :param picked: DataFrame 股票数据
    :param prefix: 文件名前缀
    :param folder: 保存目录
    """
    if picked is None or picked.empty:
        logger.warning("没有选中的股票")
        return

    # 打印
    logger.info("初步选中的股票：")
    logger.info(f"\n{picked.to_string()}")

    # 文件名加日期
    today_str = datetime.date.today().strftime("%Y%m%d")
    filename = f"{prefix}_{today_str}.csv"

    # 确保目录存在
    os.makedirs(folder, exist_ok=True)
    filepath = os.path.join(folder, filename)

    # 导出
    picked.to_csv(filepath, index=False, encoding="utf-8-sig")
    logger.info(f"已导出文件：{filepath}")

if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("开始执行选股程序")
    logger.info("=" * 60)
    
    pd.set_option("display.max_rows", None)
    
    logger.info("初始化全局数据...")
    init_quote_dict()  # 初始化

    logger.info("开始选股流程...")
    picked = pick_stocks_multithread(max_workers=5, strategy="b")
    
    if not picked.empty:
        logger.info("处理选股结果...")
        picked = picked.drop_duplicates(subset="代码").reset_index(drop=True)
        picked = picked.sort_values(by="总分", ascending=False).reset_index(drop=True)
        logger.info(f"去重和排序完成，最终选出 {len(picked)} 只股票")

    save_and_print_picked(picked)
    
    logger.info("=" * 60)
    logger.info("选股程序执行完成")
    logger.info("=" * 60)
    
    
