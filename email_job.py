import os
import time
import datetime
import configparser
import schedule
import pandas as pd
from utils import send_email, load_config_from_ini

# Configuration (config.ini overrides env)
CONFIG_PATH = os.getenv("EMAIL_JOB_CONFIG", "config.ini")


_email_cfg = load_config_from_ini("email", CONFIG_PATH)
TO_EMAIL = _email_cfg.get("to_email", os.getenv("TO_EMAIL", "1713622254@qq.com"))
FROM_EMAIL = _email_cfg.get("from_email", os.getenv("FROM_EMAIL", "1713622254@qq.com"))
FROM_PASSWORD = _email_cfg.get("from_password", os.getenv("FROM_PASSWORD", "xjahgydqxuqtbjac"))
SMTP_SERVER = _email_cfg.get("smtp_server", os.getenv("SMTP_SERVER", "smtp.qq.com"))
SMTP_PORT = int(_email_cfg.get("smtp_port", os.getenv("SMTP_PORT", "587")))

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


def csv_to_text_table(path: str) -> str:
    df = pd.read_csv(path)
    if df.empty:
        return "文件存在，但没有选中的股票。"
    # Render a readable plain-text table
    return df.to_string(index=False)


def send_daily_report():
    csv_path = find_csv_for_today_or_latest()
    if not csv_path:
        body = "未找到导出的选股文件。请先运行选股脚本生成 CSV。"
    else:
        table_text = csv_to_text_table(csv_path)
        body = f"今日选股文件: {os.path.basename(csv_path)}\n\n{table_text}"

    send_email(
        subject=f"股票提醒 {datetime.date.today().isoformat()}",
        body=body,
        to_email=TO_EMAIL,
        from_email=FROM_EMAIL,
        from_password=FROM_PASSWORD,
        smtp_server=SMTP_SERVER,
        smtp_port=SMTP_PORT,
    )


def is_weekday(dt: datetime.datetime | None = None) -> bool:
    if dt is None:
        dt = datetime.datetime.now()
    return dt.weekday() < 5  # 0=Mon ... 4=Fri


def schedule_jobs():
    """Register weekday 22:00 jobs using schedule."""
    schedule.clear()
    schedule.every().monday.at("22:00").do(send_daily_report)
    schedule.every().tuesday.at("22:00").do(send_daily_report)
    schedule.every().wednesday.at("22:00").do(send_daily_report)
    schedule.every().thursday.at("22:00").do(send_daily_report)
    schedule.every().friday.at("22:00").do(send_daily_report)


def run_timer():
    schedule_jobs()
    while True:
        schedule.run_pending()
        time.sleep(20)


if __name__ == "__main__":
    run_timer()


