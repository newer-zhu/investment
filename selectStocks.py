import akshare as ak
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import datetime
import time
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from utils import parse_number, safe_get, is_industry, get_latest_quarter, load_config_from_ini

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 全局配置
config = load_config_from_ini("strategy")
MAX_FUNDS = float(config.get("max_funds", 20000))
MAX_STOCKS_PER_SECTOR = int(config.get("max_stocks_per_sector", 3))  # 每个行业最多选股数
MIN_MARKET_CAP = float(config.get("min_market_cap", 50e8))  # 最小市值 50亿
MAX_MARKET_CAP = float(config.get("max_market_cap", 2000e8))  # 最大市值 2000亿

# 缓存优化
INFO_CACHE = {}
FUND_FLOW_DICT = {}
QUOTE_DICT = {}
HALF_YEAR_HIGH_SET = set()
ljqd_blacklist = set()

# 新增：技术指标缓存
TECH_INDICATORS_CACHE = {}

@dataclass
class StockScore:
    """股票评分数据结构"""
    code: str
    name: str
    price: float
    score: float
    technical_score: float
    fundamental_score: float
    momentum_score: float
    risk_score: float
    industry: str
    market_cap: float
    turnover_rate: float
    pe_ratio: float
    pb_ratio: float
    debt_ratio: float
    roe: float
    revenue_growth: float
    net_profit_growth: float

"""
加载连续量价齐跌的黑名单股票到全局 set
"""
def load_ljqd_blacklist(min_days=3):
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

def init_quote_dict():
    global QUOTE_DICT

    today_str = pd.Timestamp.now().strftime("%Y-%m-%d")
    cache_dir = "cache"
    os.makedirs(cache_dir, exist_ok=True)   # 确保 cache 文件夹存在
    CACHE_FILE = os.path.join(cache_dir, f"quote_cache_{today_str}.csv")

    if os.path.exists(CACHE_FILE):
        print("使用本地缓存行情数据")
        quote_df = pd.read_csv(CACHE_FILE, dtype={"代码": str})
    else:
        print("本地缓存无效，联网拉取行情数据...")
        quote_df = ak.stock_sh_a_spot_em()
        quote_df.to_csv(CACHE_FILE, index=False)

    for _, row in quote_df.iterrows():
        code = row["代码"]
        QUOTE_DICT[code] = {
            col: parse_number(row[col]) if col not in ["代码", "名称"] else row[col]
            for col in quote_df.columns
        }

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

def get_technical_indicators(code: str) -> Dict:
    """获取技术指标，带缓存"""
    if code in TECH_INDICATORS_CACHE:
        return TECH_INDICATORS_CACHE[code]
    
    try:
        # 获取K线数据
        df = ak.stock_zh_a_hist(symbol=code, period="daily", adjust="qfq")
        if len(df) < 60:
            return {}
        
        # 计算技术指标
        close = df['收盘'].values
        volume = df['成交量'].values
        
        # 移动平均线
        ma5 = pd.Series(close).rolling(5).mean().iloc[-1]
        ma10 = pd.Series(close).rolling(10).mean().iloc[-1]
        ma20 = pd.Series(close).rolling(20).mean().iloc[-1]
        ma60 = pd.Series(close).rolling(60).mean().iloc[-1]
        
        # RSI
        delta = pd.Series(close).diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs)).iloc[-1]
        
        # MACD
        ema12 = pd.Series(close).ewm(span=12).mean()
        ema26 = pd.Series(close).ewm(span=26).mean()
        macd = ema12.iloc[-1] - ema26.iloc[-1]
        
        # 布林带
        bb_middle = pd.Series(close).rolling(20).mean().iloc[-1]
        bb_std = pd.Series(close).rolling(20).std().iloc[-1]
        bb_upper = bb_middle + 2 * bb_std
        bb_lower = bb_middle - 2 * bb_std
        
        # 成交量指标
        volume_ma5 = pd.Series(volume).rolling(5).mean().iloc[-1]
        volume_ratio = volume[-1] / volume_ma5 if volume_ma5 > 0 else 1
        
        indicators = {
            'ma5': ma5, 'ma10': ma10, 'ma20': ma20, 'ma60': ma60,
            'rsi': rsi, 'macd': macd, 'bb_upper': bb_upper, 'bb_lower': bb_lower,
            'volume_ratio': volume_ratio, 'current_price': close[-1]
        }
        
        TECH_INDICATORS_CACHE[code] = indicators
        return indicators
        
    except Exception as e:
        logger.warning(f"获取技术指标失败 {code}: {e}")
        return {}

def calculate_technical_score(code: str) -> float:
    """计算技术面评分 (0-100)"""
    indicators = get_technical_indicators(code)
    if not indicators:
        return 0
    
    score = 0
    
    # 均线多头排列 (30分)
    if (indicators['ma5'] > indicators['ma10'] > indicators['ma20'] > indicators['ma60']):
        score += 30
    elif (indicators['ma5'] > indicators['ma10'] > indicators['ma20']):
        score += 20
    elif (indicators['ma5'] > indicators['ma10']):
        score += 10
    
    # RSI 适中 (20分)
    if 30 <= indicators['rsi'] <= 70:
        score += 20
    elif 20 <= indicators['rsi'] <= 80:
        score += 10
    
    # MACD 金叉 (20分)
    if indicators['macd'] > 0:
        score += 20
    
    # 布林带位置 (15分)
    price = indicators['current_price']
    if price > indicators['bb_middle']:
        score += 15
    elif price > indicators['bb_lower']:
        score += 8
    
    # 成交量放大 (15分)
    if indicators['volume_ratio'] > 1.5:
        score += 15
    elif indicators['volume_ratio'] > 1.2:
        score += 8
    
    return score

def calculate_momentum_score(code: str, quote_data: Dict) -> float:
    """计算动量评分 (0-100)"""
    score = 0
    
    # 年初至今涨幅 (40分)
    ytd_change = quote_data.get("年初至今涨跌幅", 0)
    if 0.1 <= ytd_change <= 0.5:  # 10%-50%涨幅最佳
        score += 40
    elif 0.05 <= ytd_change <= 0.8:
        score += 25
    elif ytd_change > 0:
        score += 10
    
    # 今日涨幅 (30分)
    today_change = quote_data.get("涨跌幅", 0)
    if 0.02 <= today_change <= 0.07:  # 2%-7%涨幅最佳
        score += 30
    elif 0.01 <= today_change <= 0.1:
        score += 20
    elif today_change > 0:
        score += 10
    
    # 换手率 (30分)
    turnover = quote_data.get("换手率", 0)
    if 0.05 <= turnover <= 0.15:  # 5%-15%换手率最佳
        score += 30
    elif 0.03 <= turnover <= 0.2:
        score += 20
    elif turnover > 0.02:
        score += 10
    
    return score

def calculate_risk_score(code: str, quote_data: Dict, fundamental_data: Dict) -> float:
    """计算风险评分 (0-100，分数越低风险越小)"""
    risk_score = 0
    
    # 市值风险 (25分)
    market_cap = quote_data.get("总市值", 0)
    if market_cap < 50e8:  # 小于50亿
        risk_score += 25
    elif market_cap < 100e8:  # 50-100亿
        risk_score += 15
    elif market_cap > 1000e8:  # 大于1000亿
        risk_score += 10
    
    # 估值风险 (25分)
    pe_ratio = fundamental_data.get("pe_ratio", 0)
    if pe_ratio > 50:
        risk_score += 25
    elif pe_ratio > 30:
        risk_score += 15
    elif pe_ratio > 0:
        risk_score += 5
    
    # 负债风险 (25分)
    debt_ratio = fundamental_data.get("debt_ratio", 0)
    if debt_ratio > 0.7:
        risk_score += 25
    elif debt_ratio > 0.5:
        risk_score += 15
    elif debt_ratio > 0.3:
        risk_score += 5
    
    # 流动性风险 (25分)
    turnover = quote_data.get("换手率", 0)
    if turnover < 0.02:
        risk_score += 25
    elif turnover < 0.05:
        risk_score += 15
    elif turnover < 0.1:
        risk_score += 5
    
    return risk_score

# 突破上涨的股票
def load_up_trend_stocks(option="60日均线"):
    df = ak.stock_rank_xstp_ths(symbol = option)
    df = df.rename(columns={"股票代码": "code"})
    return df[['code']]

def load_filter_lists(in_stock):
    # 向上突破A股
    stock_list = load_up_trend_stocks()
    
    # ST 股列表
    try:
        st_df = ak.stock_zh_a_st_em()
        st_codes = set(st_df['代码'].tolist())
    except Exception as e:
        st_codes = set()

    # 停牌股票
    try:
        suspend_df = ak.news_trade_notify_suspend_baidu()
        suspension_codes = set(suspend_df['股票代码'].tolist())
    except Exception as e:
        suspension_codes = set()

    # 直接剔除的股票
    mask = ~(
        stock_list['code'].str.startswith('300') |
        stock_list['code'].str.startswith('301') |
        stock_list['code'].str.startswith('688') |
        stock_list['code'].str.startswith('8')
    )

    stock_list = stock_list[mask]

    # 排除 ST 与 停牌股票
    excluded_codes = st_codes | suspension_codes
    stock_list = stock_list[~stock_list['code'].isin(excluded_codes)].reset_index(drop=True)

    return stock_list


def get_fundamental_data(code: str, industry: str) -> Dict:
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
        
        # 估值指标
        try:
            pe_df = ak.stock_a_pe_ths(symbol=code)
            pe_ratio = parse_number(pe_df.iloc[0]["市盈率"]) if not pe_df.empty else 0
        except:
            pe_ratio = 0
            
        try:
            pb_df = ak.stock_a_pb_ths(symbol=code)
            pb_ratio = parse_number(pb_df.iloc[0]["市净率"]) if not pb_df.empty else 0
        except:
            pb_ratio = 0
        
        return {
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
        
    except Exception as e:
        logger.warning(f"{code} 基本面数据获取失败: {e}")
        return {}

def calculate_fundamental_score(code: str, industry: str) -> float:
    """计算基本面评分 (0-100)"""
    fundamental_data = get_fundamental_data(code, industry)
    if not fundamental_data:
        return 0
    
    score = 0
    
    # 盈利能力 (30分)
    roe = fundamental_data.get("roe", 0)
    if roe > 0.15:
        score += 30
    elif roe > 0.1:
        score += 20
    elif roe > 0.05:
        score += 10
    
    # 成长性 (25分)
    revenue_growth = fundamental_data.get("revenue_growth", 0)
    net_profit_growth = fundamental_data.get("net_profit_growth", 0)
    
    if revenue_growth > 0.2 and net_profit_growth > 0.2:
        score += 25
    elif revenue_growth > 0.1 and net_profit_growth > 0.1:
        score += 20
    elif revenue_growth > 0.05 and net_profit_growth > 0.05:
        score += 15
    elif revenue_growth > 0 and net_profit_growth > 0:
        score += 10
    
    # 估值合理性 (25分)
    pe_ratio = fundamental_data.get("pe_ratio", 0)
    if 0 < pe_ratio < 20:
        score += 25
    elif 0 < pe_ratio < 30:
        score += 20
    elif 0 < pe_ratio < 50:
        score += 15
    elif pe_ratio > 0:
        score += 5
    
    # 财务健康度 (20分)
    debt_ratio = fundamental_data.get("debt_ratio", 0)
    current_ratio = fundamental_data.get("current_ratio", 0)
    
    if debt_ratio < 0.3 and current_ratio > 1.5:
        score += 20
    elif debt_ratio < 0.5 and current_ratio > 1.0:
        score += 15
    elif debt_ratio < 0.7 and current_ratio > 0.8:
        score += 10
    
    return score

def check_fundamental(code: str, industry: str) -> bool:
    """基本面筛选（简化版，用于快速过滤）"""
    fundamental_data = get_fundamental_data(code, industry)
    if not fundamental_data:
        return False
    
    net_profit = fundamental_data.get("net_profit", 0)
    revenue_growth = fundamental_data.get("revenue_growth", 0)
    debt_ratio = fundamental_data.get("debt_ratio", 0)
    
    if is_industry(industry, ["科技", "半导体", "互联网", "新能源", "软件", "芯片", "AI", "通信"]):
        # 科技成长股逻辑（牛市版）
        if revenue_growth < 0.05:  # 营收增速放宽到 5%
            return False
        if net_profit < 0 and fundamental_data.get("net_profit_growth", 0) < -0.1:  
            return False  # 亏损可以接受，但不能大幅恶化
        if debt_ratio > 0.8:  
            return False  # 牛市容忍更高杠杆
        return True
    else:
        # 传统行业逻辑（牛市版）
        if net_profit <= 0:
            return False  # 传统行业最好还是要赚钱
        if fundamental_data.get("gross_margin", 0) < 0.1:  
            return False  # 放宽毛利率
        if fundamental_data.get("net_profit_growth", 0) < 0:  
            return False  # 牛市可以接受持平，但不接受下降
        if revenue_growth < 0.02:  
            return False  # 营收至少正增长
        if debt_ratio > 0.7:  
            return False  # 传统行业不建议太高杠杆
        if fundamental_data.get("current_ratio", 0) < 0.8:  
            return False  # 放宽流动比率
        return True  

# 动态换手率判断
def get_dynamic_turnover_threshold(free_float_mkt_cap):
    """根据流通市值返回换手率阈值（百分比）"""
    if free_float_mkt_cap <= 50e8:  # 小盘
        return 0.15
    elif free_float_mkt_cap <= 200e8:  # 中盘
        return 0.08
    else:  # 大盘
        return 0.03

  
def check_stock(code: str, start_date: str, min_vol_ratio: float) -> Optional[StockScore]:
    """筛选单只股票，返回评分对象"""
    # 排除新高
    if code in HALF_YEAR_HIGH_SET:
        return None
    # 排除量价齐跌
    if code in ljqd_blacklist:
        return None  

    # 从缓存行情中取数据
    row = QUOTE_DICT.get(code)
    if not row:
        return None
    
    # 行业判断
    industry = get_industry_from_cache(code)
    if is_industry(industry, ["国防","军工","有色","金属","煤炭","钢铁"]):
        return None
    
    # 基础过滤
    price = row["最新价"]
    market_cap = row["总市值"]
    turnover_rate = row["换手率"]
    
    # 价格和市值过滤
    if price * 100 > MAX_FUNDS/2 or price < 5:  # 超出资金限制
        return None
    
    if market_cap < MIN_MARKET_CAP or market_cap > MAX_MARKET_CAP:
        return None
    
    # 成交额过滤
    if row["成交额"] < 50_000_000:  # 小于 5000 万剔除
        return None

    # 换手率判断
    free_float_mkt_cap = row.get("流通市值", 0)
    dynamic_thr = get_dynamic_turnover_threshold(free_float_mkt_cap)
    fund_data = FUND_FLOW_DICT.get(code, {})
    if fund_data.get("连续换手率", 0) < dynamic_thr*3 or turnover_rate < dynamic_thr:
        return None
    
    # 基本面筛选
    if not check_fundamental(code, industry):
        return None
    
    # 获取详细数据
    fundamental_data = get_fundamental_data(code, industry)
    
    # 计算各项评分
    technical_score = calculate_technical_score(code)
    fundamental_score = calculate_fundamental_score(code, industry)
    momentum_score = calculate_momentum_score(code, row)
    risk_score = calculate_risk_score(code, row, fundamental_data)
    
    # 综合评分 (技术面30% + 基本面30% + 动量20% + 风险控制20%)
    total_score = (
        technical_score * 0.3 +
        fundamental_score * 0.3 +
        momentum_score * 0.2 +
        (100 - risk_score) * 0.2  # 风险分数越低越好
    )
    
    return StockScore(
        code=code,
        name=row["名称"],
        price=price,
        score=total_score,
        technical_score=technical_score,
        fundamental_score=fundamental_score,
        momentum_score=momentum_score,
        risk_score=risk_score,
        industry=industry or "未知",
        market_cap=market_cap,
        turnover_rate=turnover_rate,
        pe_ratio=fundamental_data.get("pe_ratio", 0),
        pb_ratio=fundamental_data.get("pb_ratio", 0),
        debt_ratio=fundamental_data.get("debt_ratio", 0),
        roe=fundamental_data.get("roe", 0),
        revenue_growth=fundamental_data.get("revenue_growth", 0),
        net_profit_growth=fundamental_data.get("net_profit_growth", 0)
    )

def pick_stocks_multithread(start_date="2025-01-01", min_vol_ratio=1.5, max_workers=20, strategy="a"):
    """多线程选股"""
    stock_list = load_filter_lists(strategy)
    results = []

    logger.info(f"开始选股，共 {len(stock_list)} 只股票待筛选")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                check_stock, code,
                start_date, min_vol_ratio
            )
            for code in stock_list['code'].tolist()
        ]

        # 在每次执行一个任务后更新进度条
        for future in tqdm(as_completed(futures), total=len(stock_list), desc="选股中"):
            result = future.result()
            if result:
                results.append(result)

    logger.info(f"选股完成，共筛选出 {len(results)} 只股票")
    return results

def optimize_portfolio(stock_scores: List[StockScore], max_stocks: int = 10) -> List[StockScore]:
    """投资组合优化 - 行业分散 + 评分排序"""
    if not stock_scores:
        return []
    
    # 按行业分组
    industry_groups = {}
    for stock in stock_scores:
        industry = stock.industry
        if industry not in industry_groups:
            industry_groups[industry] = []
        industry_groups[industry].append(stock)
    
    # 每个行业选择评分最高的股票
    selected_stocks = []
    for industry, stocks in industry_groups.items():
        # 按综合评分排序
        sorted_stocks = sorted(stocks, key=lambda x: x.score, reverse=True)
        # 每个行业最多选择指定数量的股票
        selected_stocks.extend(sorted_stocks[:MAX_STOCKS_PER_SECTOR])
    
    # 最终按评分排序，选择前N只
    final_stocks = sorted(selected_stocks, key=lambda x: x.score, reverse=True)[:max_stocks]
    
    return final_stocks

def create_portfolio_report(stocks: List[StockScore]) -> pd.DataFrame:
    """创建投资组合报告"""
    if not stocks:
        return pd.DataFrame()
    
    data = []
    for stock in stocks:
        data.append({
            "代码": stock.code,
            "名称": stock.name,
            "价格": stock.price,
            "综合评分": round(stock.score, 2),
            "技术评分": round(stock.technical_score, 2),
            "基本面评分": round(stock.fundamental_score, 2),
            "动量评分": round(stock.momentum_score, 2),
            "风险评分": round(stock.risk_score, 2),
            "行业": stock.industry,
            "总市值(亿)": round(stock.market_cap / 1e8, 2),
            "换手率(%)": round(stock.turnover_rate * 100, 2),
            "PE": round(stock.pe_ratio, 2) if stock.pe_ratio > 0 else "N/A",
            "PB": round(stock.pb_ratio, 2) if stock.pb_ratio > 0 else "N/A",
            "ROE(%)": round(stock.roe * 100, 2),
            "营收增长(%)": round(stock.revenue_growth * 100, 2),
            "净利润增长(%)": round(stock.net_profit_growth * 100, 2)
        })
    
    df = pd.DataFrame(data)
    
    # 添加统计信息
    if not df.empty:
        logger.info(f"投资组合统计:")
        logger.info(f"平均评分: {df['综合评分'].mean():.2f}")
        logger.info(f"平均PE: {df[df['PE'] != 'N/A']['PE'].mean():.2f}")
        logger.info(f"平均ROE: {df['ROE(%)'].mean():.2f}%")
        logger.info(f"行业分布: {df['行业'].value_counts().to_dict()}")
    
    return df


def save_and_print_picked(picked: pd.DataFrame, prefix="picked_stocks", folder="output"):
    """
    打印并导出选中的股票列表
    :param picked: DataFrame 股票数据
    :param prefix: 文件名前缀
    :param folder: 保存目录
    """
    if picked is None or picked.empty:
        logger.warning("没有选中的股票。")
        return

    # 打印
    logger.info("选中的股票：")
    print(picked.to_string(index=False))

    # 文件名加日期
    today_str = datetime.date.today().strftime("%Y%m%d")
    filename = f"{prefix}_{today_str}.csv"

    # 确保目录存在
    os.makedirs(folder, exist_ok=True)
    filepath = os.path.join(folder, filename)

    # 导出
    picked.to_csv(filepath, index=False, encoding="utf-8-sig")
    logger.info(f"已导出文件：{filepath}")

def check_market_conditions() -> Dict:
    """检查市场环境，为策略调整提供依据"""
    try:
        # 获取上证指数数据
        sh_index = ak.stock_zh_index_spot()
        sh_data = sh_index[sh_index['代码'] == '000001'].iloc[0]
        
        # 获取北向资金数据
        try:
            north_flow = ak.stock_hsgt_north_net_flow_in()
            today_flow = north_flow.iloc[0]['value'] if not north_flow.empty else 0
        except:
            today_flow = 0
        
        # 获取市场情绪指标
        try:
            fear_greed = ak.stock_zh_a_spot_em()
            up_count = len(fear_greed[fear_greed['涨跌幅'] > 0])
            down_count = len(fear_greed[fear_greed['涨跌幅'] < 0])
            market_sentiment = up_count / (up_count + down_count) if (up_count + down_count) > 0 else 0.5
        except:
            market_sentiment = 0.5
        
        return {
            'sh_change': sh_data.get('涨跌幅', 0),
            'north_flow': today_flow,
            'market_sentiment': market_sentiment,
            'is_bull_market': sh_data.get('涨跌幅', 0) > 0.01,  # 上证涨幅>1%视为牛市
            'is_volatile': abs(sh_data.get('涨跌幅', 0)) > 0.02  # 涨幅>2%视为波动较大
        }
    except Exception as e:
        logger.warning(f"市场环境检查失败: {e}")
        return {
            'sh_change': 0,
            'north_flow': 0,
            'market_sentiment': 0.5,
            'is_bull_market': False,
            'is_volatile': False
        }

def adjust_strategy_for_market(market_conditions: Dict) -> Dict:
    """根据市场环境调整策略参数"""
    strategy_params = {
        'max_stocks': 10,
        'min_score': 60,
        'max_risk_score': 40,
        'sector_weight': 0.3
    }
    
    if market_conditions.get('is_bull_market', False):
        # 牛市策略：更激进
        strategy_params.update({
            'max_stocks': 15,
            'min_score': 50,
            'max_risk_score': 50,
            'sector_weight': 0.4
        })
        logger.info("检测到牛市环境，采用激进策略")
    elif market_conditions.get('is_volatile', False):
        # 波动市场：更保守
        strategy_params.update({
            'max_stocks': 8,
            'min_score': 70,
            'max_risk_score': 30,
            'sector_weight': 0.2
        })
        logger.info("检测到波动市场，采用保守策略")
    else:
        # 震荡市场：中性策略
        logger.info("检测到震荡市场，采用中性策略")
    
    return strategy_params

if __name__ == "__main__":
    # 设置pandas显示选项
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", None)
    
    start_time = time.time()
    
    # 检查市场环境
    market_conditions = check_market_conditions()
    strategy_params = adjust_strategy_for_market(market_conditions)
    
    logger.info("开始初始化数据...")
    init_quote_dict()  # 初始化全局行情
    init_fund_flow_cache()  
    init_half_year_high()
    load_ljqd_blacklist()
    
    logger.info("开始选股...")
    stock_scores = pick_stocks_multithread(
        start_date="2025-08-01", 
        min_vol_ratio=1.5, 
        max_workers=15, 
        strategy="b"
    )
    
    # 过滤低评分股票
    filtered_stocks = [s for s in stock_scores if s.score >= strategy_params['min_score']]
    logger.info(f"评分过滤后剩余 {len(filtered_stocks)} 只股票")
    
    # 投资组合优化
    optimized_stocks = optimize_portfolio(
        filtered_stocks, 
        max_stocks=strategy_params['max_stocks']
    )
    
    # 创建报告
    if optimized_stocks:
        report_df = create_portfolio_report(optimized_stocks)
        save_and_print_picked(report_df, prefix="optimized_portfolio")
    else:
        logger.warning("没有符合条件的股票")
    
    end_time = time.time()
    logger.info(f"选股完成，耗时 {end_time - start_time:.2f} 秒")
    
    
