# utils.py
# utils.py
import os
import smtplib
import configparser
import datetime

import pandas as pd
import holidays
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header


OUTPUT_FOLDER = "output"
FILENAME_PREFIX = "picked_stocks"

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
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()  # 安全传输
        server.login(from_email, from_password)
        
        # 发送邮件
        server.sendmail(from_email, [to_email], message.as_string())
        server.quit()
        
        print(f"邮件发送成功：{from_email} -> {to_email}")

    except Exception as e:
        print(f"邮件发送失败：{from_email} -> {to_email}: {e}")

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

def is_industry(industry: str, keywords: list[str]) -> bool:
    """
    判断行业是否属于给定的关键词列表（模糊匹配）

    :param industry: 行业名称（字符串）
    :param keywords: 关键词列表，例如 ["科技", "半导体", "新能源"]
    :return: True 如果行业名称包含任一关键词，否则 False
    """
    if not industry:
        return False
    return any(k in industry for k in keywords)

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