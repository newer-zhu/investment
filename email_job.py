import os
import time
import datetime
import configparser
import subprocess
import sys
import schedule
import pandas as pd
from utils import send_email, load_config_from_ini, find_csv_for_today_or_latest, csv_to_html_table
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

def send_late_suggestion(table_html):

    body = f"<p>今日尾盘选股建议（次日早盘卖出）: 注意止损！！！</p>{table_html}"

    subject = f"红多量化-尾盘提醒 {datetime.date.today().isoformat()}"

    try:
        send_email(
            subject=subject,
            body=body,
            to_email=TO_EMAIL,
            from_email=FROM_EMAIL,
            from_password=FROM_PASSWORD,
            smtp_server=SMTP_SERVER,
            smtp_port=SMTP_PORT,
            content_type='html',
        )
        time.sleep(1)
    except Exception as e:
        print(f"发送失败: from {FROM_EMAIL} -> {TO_EMAIL}: {e}")

def send_daily_report():
    # 先执行选股与分析脚本，生成并筛选 CSV
    try:
        subprocess.run([sys.executable, os.path.join(os.path.dirname(__file__), "selectStocks.py")],
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

def send_daily_report_test():
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




def schedule_jobs():
    """Register weekday 22:00 jobs using schedule."""
    schedule.clear()
    schedule.every().monday.at("15:45").do(send_daily_report)
    schedule.every().tuesday.at("15:45").do(send_daily_report)
    schedule.every().wednesday.at("15:45").do(send_daily_report)
    schedule.every().thursday.at("15:45").do(send_daily_report)
    schedule.every().friday.at("15:45").do(send_daily_report)


def run_timer():
    schedule_jobs()
    while True:
        schedule.run_pending()
        time.sleep(20)


if __name__ == "__main__":
    run_timer()


