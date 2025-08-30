import os
import time
import datetime
import configparser
import subprocess
import sys
import schedule
import pandas as pd
from utils import send_email, load_config_from_ini
from analyze_stocks import get_prev_portfolio_avg_message

# Configuration (config.ini overrides env)
CONFIG_PATH = os.getenv("EMAIL_JOB_CONFIG", "config.ini")


_email_cfg = load_config_from_ini("email", CONFIG_PATH)
TO_EMAIL = _email_cfg.get("to_email", os.getenv("TO_EMAIL", "1713622254@qq.com"))
FROM_EMAIL = _email_cfg.get("from_email", os.getenv("FROM_EMAIL", "1713622254@qq.com"))
FROM_PASSWORD = _email_cfg.get("from_password", os.getenv("FROM_PASSWORD", "xjahgydqxuqtbjac"))
SMTP_SERVER = _email_cfg.get("smtp_server", os.getenv("SMTP_SERVER", "smtp.qq.com"))
SMTP_PORT = int(_email_cfg.get("smtp_port", os.getenv("SMTP_PORT", "587")))

# 支持多个发件人：from_emails、from_passwords 为逗号分隔
FROM_EMAILS = [e.strip() for e in _email_cfg.get("from_emails", "").split(",") if e.strip()] or [FROM_EMAIL]
FROM_PASSWORDS = [p.strip() for p in _email_cfg.get("from_passwords", "").split(",") if p.strip()]

# 支持多个收件人：to_emails 为逗号分隔
TO_EMAILS = [e.strip() for e in _email_cfg.get("to_emails", "").split(",") if e.strip()] or [TO_EMAIL]

OUTPUT_FOLDER = "output"
FILENAME_PREFIX = "picked_stocks"


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
    if df.empty:
        return "<p>文件存在，但没有选中的股票。</p>"
    # 仅展示常用列并确保代码是字符串，便于复制
    preferred_cols = ["代码", "名称", "价格", "今日涨跌", "总市值", "年初至今涨跌幅", "行业"]
    show_cols = [c for c in preferred_cols if c in df.columns]
    if show_cols:
        df = df[show_cols]
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


def send_daily_report():
    # 先执行选股与分析脚本，生成并筛选 CSV
    try:
        subprocess.run([sys.executable, os.path.join(os.path.dirname(__file__), "selectStocks.py")],
                       check=True)
        subprocess.run([sys.executable, os.path.join(os.path.dirname(__file__), "analyze_stocks.py")],
                       check=True)
    except Exception as e:
        print(f"执行选股/分析脚本失败: {e}")

    csv_path = find_csv_for_today_or_latest()
    prev_msg = get_prev_portfolio_avg_message()
    if not csv_path:
        intro = "未找到导出的选股文件。请先运行选股脚本生成 CSV。"
        second = f"<p>{prev_msg}</p>" if prev_msg else ""
        body = f"<p>{intro}</p>{second}"
    else:
        table_html = csv_to_html_table(csv_path)
        second = f"<p>{prev_msg}</p>" if prev_msg else ""
        body = f"<p>今日选股建议（建议持有3~5天）: 纯属个人项目，不构成任何投资建议</p>{second}{table_html}"

    subject = f"红多量化选股提醒 {datetime.date.today().isoformat()}"
    # 单一发件人，多个收件人
    for recipient in TO_EMAILS:
        try:
            send_email(
                subject=subject,
                body=body,
                to_email=recipient,
                from_email=FROM_EMAIL,
                from_password=FROM_PASSWORD,
                smtp_server=SMTP_SERVER,
                smtp_port=SMTP_PORT,
                content_type='html',
            )
            time.sleep(1)
        except Exception as e:
            print(f"发送失败: from {FROM_EMAIL} -> {recipient}: {e}")


def is_weekday(dt: datetime.datetime | None = None) -> bool:
    if dt is None:
        dt = datetime.datetime.now()
    return dt.weekday() < 5  # 0=Mon ... 4=Fri


def schedule_jobs():
    """Register weekday 22:00 jobs using schedule."""
    schedule.clear()
    schedule.every().monday.at("18:40").do(send_daily_report)
    schedule.every().tuesday.at("18:40").do(send_daily_report)
    schedule.every().wednesday.at("18:40").do(send_daily_report)
    schedule.every().thursday.at("18:40").do(send_daily_report)
    schedule.every().friday.at("18:40").do(send_daily_report)


def run_timer():
    schedule_jobs()
    while True:
        schedule.run_pending()
        time.sleep(20)


if __name__ == "__main__":
    run_timer()


