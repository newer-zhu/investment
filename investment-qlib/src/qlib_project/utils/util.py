# utils.py
from __future__ import annotations
import os
import smtplib
import configparser
import datetime
import akshare as ak
import pandas as pd
import holidays
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from utils.logger import logger
import time
from functools import lru_cache

OUTPUT_FOLDER = "output"
FILENAME_PREFIX = "picked_stocks"
LAST_CALL_TIME = 0
MIN_INTERVAL = 1.5  # 秒

import pandas as pd
from pathlib import Path
from typing import Dict, Optional
from constants import SOURCE_PATH
def get_latest_financial_row(
    code: str, 
    bs_dir: str= SOURCE_PATH / "cache" / "balance", 
    is_dir: str= SOURCE_PATH / "cache" / "income", 
    cf_dir: str= SOURCE_PATH / "cache" / "cashflow"
) -> Dict[str, any]:
    """
    从三大表 CSV 目录中提取指定股票的最新财务特征
    :param code: 股票代码, 如 "600435.SH"
    :param bs_dir: 资产负债表 CSV 存放目录
    :param is_dir: 利润表 CSV 存放目录
    :param cf_dir: 现金流量表 CSV 存放目录
    """
    # 统一转换文件名格式 (假设为 600435.SH.csv)
    file_name = f"{code}.csv"
    
    # 我们关心的排雷核心字段
    target_fields = {
        'bs': ['end_date', 'total_assets', 'goodwill', 'accounts_receiv', 'money_cap', 'total_cur_liab'],
        'is': ['end_date', 'revenue', 'rd_exp', 'n_income_attr_p'],
        'cf': ['end_date', 'c_fr_sale_sg', 'n_cashflow_act']
    }
    
    result = {}

    def _read_latest_from_dir(directory: str, fields: list) -> Optional[pd.Series]:
        path = Path(directory) / file_name
        if not path.exists():
            return None
        
        try:
            # 只读取需要的列以节省内存
            df = pd.read_csv(path, usecols=lambda x: x in fields or x == 'end_date')
            if df.empty: return None
            
            # 按报告期排序，取最新一条记录
            # 如果你的数据里有 ann_date (公告日)，建议按 ann_date 排序更符合实盘逻辑
            df['end_date'] = pd.to_datetime(df['end_date'])
            return df.sort_values('end_date').iloc[-1]
        except Exception as e:
            # print(f"读取 {path} 出错: {e}")
            return None

    # 分别从三个文件夹抓取
    bs_row = _read_latest_from_dir(bs_dir, target_fields['bs'])
    is_row = _read_latest_from_dir(is_dir, target_fields['is'])
    cf_row = _read_latest_from_dir(cf_dir, target_fields['cf'])

    # 合并数据到字典
    if bs_row is not None: result.update(bs_row.to_dict())
    if is_row is not None: result.update(is_row.to_dict())
    if cf_row is not None: result.update(cf_row.to_dict())

    return result


def throttle():
    global LAST_CALL_TIME
    now = time.time()
    sleep_time = MIN_INTERVAL - (now - LAST_CALL_TIME)
    if sleep_time > 0:
        time.sleep(sleep_time)
    LAST_CALL_TIME = time.time()
    
def _to_qlib_instrument(code: str) -> str:
    """
    600000 -> SH600000
    000001 -> SZ000001
    """
    code = str(code).strip()
    code = "".join(filter(str.isdigit, code)).zfill(6)
    prefix = "SH" if code.startswith("6") else "SZ"
    return prefix + code

def load_config_from_ini(section: str,
                         path: str | None = None,
                         config_path_env: str = "EMAIL_JOB_CONFIG",
                         default_path: str = "config.ini") -> dict:
    """
    通用的 INI 配置读取函数，返回指定 section 下的键值字典（去除空值）。
    优先级：显式 path > 环境变量 EMAIL_JOB_CONFIG > 默认 config.ini
    """
    if path is None:
        path = os.getenv(config_path_env, default_path)
    if not os.path.exists(path):
        return {}
    parser = configparser.ConfigParser()
    parser.read(path, encoding="utf-8")
    if not parser.has_section(section):
        return {}
    values = {k: v for k, v in parser.items(section) if v is not None and v != ""}
    return values

import pandas as pd

def selected_stocks_to_html(selected_stocks: list[dict]) -> str:
    """
    将 selected_stocks 列表（英文字段）转成 HTML 表格
    :param selected_stocks: list of dict
    :return: HTML 字符串
    """
    if not selected_stocks:
        return "<p>No stock matched the conditions.</p>"
    
    df = pd.DataFrame(selected_stocks)
    
    # 显示列和顺序（对应你的英文字段）
    show_cols = [
        "code", "name", "pct_change", "turnover", "volume_ratio", 
        "circulating_value", "amount", "amplitude", "speed", 
        "five_min_change", "sixty_day_change", "pe_ratio", "pb_ratio", 
        "fundamental_score", "technical_score", "total_score"
    ]
    df = df[[c for c in show_cols if c in df.columns]]
    
    # 格式化金额列（以亿为单位）
    if "amount" in df.columns:
        df["amount"] = df["amount"].apply(lambda x: f"{x/1e8:.2f}B")
    if "circulating_value" in df.columns:
        df["circulating_value"] = df["circulating_value"].apply(lambda x: f"{x/1e8:.2f}B")
    
    # 转成 HTML
    table_html = df.to_html(index=False, border=0, escape=False)
    
    # 添加样式
    style = """
    <style>
      table { border-collapse: collapse; width: 100%; }
      th, td { border: 1px solid #e5e7eb; padding: 6px 10px; text-align: center; font-family: Arial, Helvetica, sans-serif; font-size: 13px; }
      th { background: #f3f4f6; }
      td:first-child { font-family: Consolas, 'Courier New', monospace; }
    </style>
    """
    return style + table_html



def send_email(subject: str, body: str, to_email: str,
               from_email: str, from_password: str,
               smtp_server: str = "smtp.gmail.com", smtp_port: int = 587,
               content_type: str = "plain"):
    """
    发送邮件通知

    :param subject: 邮件主题
    :param body: 邮件正文
    :param to_email: 收件人邮箱
    :param from_email: 发件人邮箱
    :param from_password: 发件人邮箱的授权码/密码
    :param smtp_server: SMTP服务器，默认 Gmail
    :param smtp_port: SMTP端口，默认 587
    """
    try:
        # 构建邮件
        message = MIMEMultipart()
        message['From'] = from_email
        message['To'] = to_email
        message['Subject'] = Header(subject, 'utf-8')
        
        # 添加正文（plain 或 html）
        subtype = 'html' if content_type.lower() == 'html' else 'plain'
        message.attach(MIMEText(body, subtype, 'utf-8'))
        
        # 连接 SMTP
        if smtp_port == 465:
            # SSL 连接（QQ邮箱等）
            server = smtplib.SMTP_SSL(smtp_server, smtp_port)
        else:
            # TLS 连接（Gmail等）
            server = smtplib.SMTP(smtp_server, smtp_port)
            server.starttls()  # 安全传输
        
        server.login(from_email, from_password)
        
        # 发送邮件
        server.sendmail(from_email, [to_email], message.as_string())
        server.quit()
        
        logger.info(f"邮件发送成功：{from_email} -> {to_email}")

    except Exception as e:
        logger.error(f"邮件发送失败：{from_email} -> {to_email}: {e}", exc_info=True)

def parse_number(s):
    if s is None:
        return 0.0
    if isinstance(s, float) or isinstance(s, int):
        return float(s)
    if not isinstance(s, str):
        s = str(s)
    s = s.strip().replace(",", "")
    
    try:
        if s.endswith("%"):
            return float(s.replace("%", "")) / 100.0
        if "万" in s or "亿" in s:
            s = s.replace("万", "*1e4").replace("亿", "*1e8")
            return float(eval(s))
        return float(s)
    except:
        return 0.0


def safe_get(df, field):
    val = df.get(field)
    if val is None:
        return 0
    return val

@lru_cache(maxsize=1024)
def is_industry(
    industry: str,
    keywords: tuple[str, ...]
) -> bool:

    if not industry:
        return False

    industry = industry.lower()

    return any(
        k.lower() in industry
        for k in keywords
    )


def format_symbol(code: str) -> str:
    """
    将纯数字证券代码转换成雪球接口要求的格式
    :param code: 纯数字证券代码，例如 "600000", "000001", "300750"
    :return: 格式化后的 symbol，例如 "SH600000", "SZ000001", "SZ300750"
    """
    code = str(code).zfill(6)  # 保证6位
    if code.startswith(("60", "68")):  # 沪市主板 & 科创板
        return f"SH{code}"
    elif code.startswith(("00", "30")):  # 深市主板 & 创业板
        return f"SZ{code}"
    else:
        raise ValueError(f"未知代码前缀: {code}")

def get_latest_quarter() -> str:
    """
    获取A股能查到的最新财报季度 (YYYYQ)
    考虑财报发布时间延迟
    """
    today = datetime.date.today()
    year = today.year
    month = today.month
    day = today.day

    if month < 5:  
        # 5月前 → 年报能查，1季报大多数公司还没全出
        return f"{year-1}4"
    elif month < 9:  
        # 5-8月 → 一季报能查，中报大多数公司还没全出
        return f"{year}1"
    elif month < 11:  
        # 9-10月 → 中报能查，三季报还没全出
        return f"{year}2"
    else:  
        # 11月以后 → 三季报能查
        return f"{year}3"

def is_trading_day(date=None):
    """判断给定日期是否为交易日"""
    if date is None:
        date = datetime.date.today()
    
    # 首先判断是否为周末
    if date.weekday() >= 5:  # 5是周六，6是周日
        return False
    
    # 判断是否为节假日（这里使用中国节假日）
    cn_holidays = holidays.China()
    if date in cn_holidays:
        return False
    
    return True


def find_csv_for_today_or_latest() -> str | None:
    """Return today's CSV path if it exists; otherwise the most recent matching CSV; else None."""
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


def csv_to_html_table(path: str) -> str:
    df = pd.read_csv(path)
       # 只保留前 10 行
    df = df.head(10)
    if df.empty:
        return "<p>文件存在，但没有选中的股票。</p>"
    # 仅展示常用列并确保代码是字符串，便于复制
    # preferred_cols = ["代码", "名称", "价格", "今日涨跌", "总市值", "年初至今涨跌幅", "行业"]
    # show_cols = [c for c in preferred_cols if c in df.columns]
    # if show_cols:
    #     df = df[show_cols]
    if "代码" in df.columns:
        df["代码"] = df["代码"].astype(str)

    # 转为 HTML 表格，居中显示，便于复制
    table_html = df.to_html(index=False, border=0, escape=False)
    style = """
    <style>
      table { border-collapse: collapse; width: 100%; }
      th, td { border: 1px solid #e5e7eb; padding: 8px 10px; text-align: center; font-family: Arial, Helvetica, sans-serif; font-size: 13px; }
      th { background: #f3f4f6; }
      td:first-child { font-family: Consolas, 'Courier New', monospace; }
    </style>
    """
    return style + table_html


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


def get_prev_trade_date(today_dt: pd.Timestamp) -> pd.Timestamp:
    """
    使用 akshare 交易日历，获取上一个交易日
    """
    trade_df = ak.tool_trade_date_hist_sina()
    trade_df["trade_date"] = pd.to_datetime(trade_df["trade_date"])

    trade_dates = trade_df["trade_date"].sort_values().reset_index(drop=True)

    if today_dt not in set(trade_dates):
        raise ValueError(f"{today_dt.date()} 不在交易日历中")

    idx = trade_dates[trade_dates == today_dt].index[0]
    if idx == 0:
        raise ValueError("没有上一个交易日")

    return trade_dates.iloc[idx - 1]

def find_first_missing_trade_date(dates: pd.Series, trade_calendar: pd.Series):
    """
    给定已有日期序列，找出第一个缺失的交易日
    """
    date_set = set(dates)
    for d in trade_calendar:
        if d < dates.min():
            continue
        if d > dates.max():
            break
        if d not in date_set:
            return d
    return None


def load_stock_pool(txt_path: str):
    with open(txt_path, "r", encoding="utf-8") as f:
        instruments = [
            line.strip()
            for line in f
            if line.strip()
        ]
    if not isinstance(instruments, (list, tuple)):
        instruments = list(instruments)
    return instruments

def qlib_to_tushare(code: str) -> str:
    """
    SH600000 -> 600000.SH
    SZ000001 -> 000001.SZ
    """
    code = code.strip().upper()
    if code.startswith("SH"):
        return code[2:] + ".SH"
    if code.startswith("SZ"):
        return code[2:] + ".SZ"
    raise ValueError(f"Unknown code format: {code}")


def tushare_to_qlib(code: str) -> str:
    """
    600000.SH -> SH600000
    000001.SZ -> SZ000001
    """
    if "." not in str(code):
        return code
    num, exch = code.split(".")
    return f"{exch.upper()}{num}"
