# utils.py
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header

def send_email(subject: str, body: str, to_email: str,
               from_email: str, from_password: str,
               smtp_server: str = "smtp.gmail.com", smtp_port: int = 587):
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
        
        # 添加正文
        message.attach(MIMEText(body, 'plain', 'utf-8'))
        
        # 连接 SMTP
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()  # 安全传输
        server.login(from_email, from_password)
        
        # 发送邮件
        server.sendmail(from_email, [to_email], message.as_string())
        server.quit()
        
        print("邮件发送成功！")

    except Exception as e:
        print("邮件发送失败：", e)

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


import datetime

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
