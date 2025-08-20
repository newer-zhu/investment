import akshare as ak
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import datetime
from utils import parse_number, safe_get, is_industry, get_latest_quarter


# 全局资金上限（单位：元）
MAX_FUNDS = 20000
# 股票行业信息
INFO_CACHE = {}  
# 资金流和换手率缓存
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

"""初始化半年新高股票集合，只请求一次接口"""
def init_half_year_high():
    global HALF_YEAR_HIGH_SET
    try:
        df = ak.stock_rank_cxg_ths(symbol="半年新高")
        HALF_YEAR_HIGH_SET = set(df["股票代码"].astype(str).tolist())
        print(f"半年新高股票数量: {len(HALF_YEAR_HIGH_SET)}")
    except Exception as e:
        print(f"获取半年新高数据失败: {e}")
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

# 突破上涨的股票
def load_up_trend_stocks(option="10日均线"):
    df = ak.stock_rank_xstp_ths(symbol = option)
    df = df.rename(columns={"股票代码": "code"})
    return df[['code']]

def load_filter_lists(in_stock):
    """加载 ST 股、停牌股、军工股列表"""
    if in_stock == "a":
        # 所有 A 股
        stock_list = ak.stock_info_a_code_name()
    elif in_stock == "b":
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


def check_fundamental(code, industry):
    """用最新季度的基本面核心指标筛选股票"""
    try:
        df = ak.stock_financial_abstract_ths(symbol=code)
        latest = df.iloc[-1]  # 取最新季度

        net_profit = parse_number(safe_get(latest, "净利润"))
        roe = parse_number(safe_get(latest, "净资产收益率"))
        gross_margin = parse_number(safe_get(latest, "销售毛利率"))
        net_profit_growth = parse_number(safe_get(latest, "净利润同比增长率"))
        revenue_growth = parse_number(safe_get(latest, "营业总收入同比增长率"))
        debt_ratio = parse_number(safe_get(latest, "资产负债率"))
        current_ratio = parse_number(safe_get(latest, "流动比率"))

        if is_industry(industry, ["科技", "半导体", "互联网", "新能源", "软件", "芯片", "AI", "通信"]):
            # 科技成长股逻辑
            if revenue_growth < 0.1:  # 营收增速要大
                return False
            if net_profit < 0 and net_profit_growth < 0:
                return False  # 亏损不能扩大
            if debt_ratio > 0.7:
                return False
            return True
        else:
            # 传统行业逻辑
            if net_profit <= 0:
                return False
            if gross_margin < 0.15:
                return False
            if net_profit_growth < 0.05:
                return False
            if revenue_growth < 0.05:
                return False
            if debt_ratio > 0.6:
                return False
            if current_ratio < 1:
                return False

        if roe < 0.05:
            return False
            
        return True
    except Exception as e:
        print(f"{code} 基本面数据异常: {e}")
        return False  

# 动态换手率判断
def get_dynamic_turnover_threshold(free_float_mkt_cap):
    """根据流通市值返回换手率阈值（百分比）"""
    if free_float_mkt_cap <= 50e8:  # 小盘
        return 0.15
    elif free_float_mkt_cap <= 200e8:  # 中盘
        return 0.07
    else:  # 大盘
        return 0.03

  
"""筛选单只股票"""
def check_stock(code, start_date, min_vol_ratio):
    # 排除半年新高
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
    if is_industry(industry, ["国防","军工","有色","金属","煤炭"]):
        return None
    
    if not check_fundamental(code, industry):
        return None

    
    price = row["最新价"]
    turnover_rate = row["换手率"]

    # 资金判断
    if price * 100 > MAX_FUNDS/2 or price < 5 or price > 30:  # 超出资金限制
        return None
    
    # 成交额过滤
    if row["成交额"] < 50_000_000:  # 小于 5000 万剔除
        return None

    # 换手率判断
    free_float_mkt_cap = row.get("流通市值", 0)
    dynamic_thr = get_dynamic_turnover_threshold(free_float_mkt_cap)
    fund_data = FUND_FLOW_DICT.get(code, {})
    if fund_data.get("连续换手率", 0) < dynamic_thr:
        return None

    return {
        "代码": code,
        "名称": row["名称"],
        "价格": price,
        "今日涨跌": row["涨跌幅"],
        "总市值": row["总市值"],
        "年初至今涨跌幅": row["年初至今涨跌幅"],
        "行业": industry
    }

"""多线程选股"""
def pick_stocks_multithread(start_date="2025-01-01", min_vol_ratio=1.5, max_workers=20, strategy="a"):
    stock_list = load_filter_lists(strategy)
    results = []

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

    return pd.DataFrame(results)


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
    print("选中的股票：")
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
    pd.set_option("display.max_rows", None)
    init_quote_dict()  # 初始化全局行情
    init_fund_flow_cache()  
    init_half_year_high()
    load_ljqd_blacklist()

    picked = pick_stocks_multithread(start_date="2025-08-01", min_vol_ratio=1.5, max_workers=15, strategy="b")
    picked = picked.drop_duplicates(subset="代码").reset_index(drop=True)
    picked = picked.sort_values(by="年初至今涨跌幅", ascending=True).reset_index(drop=True)

    save_and_print_picked(picked)
    
    
