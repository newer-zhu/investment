import os
import time
import datetime
import configparser
import pandas as pd
from utils import send_email

# Configuration (config.ini overrides env)
CONFIG_PATH = os.getenv("EMAIL_JOB_CONFIG", "config.ini")


def load_config_from_ini(path: str) -> dict:
    values: dict[str, str] = {}
    if not os.path.exists(path):
        return values
    parser = configparser.ConfigParser()
    parser.read(path, encoding="utf-8")
    section = "email"
    if not parser.has_section(section):
        return values
    get = lambda key, fb=None: parser.get(section, key, fallback=fb)
    values.update({
        "TO_EMAIL": get("to_email"),
        "FROM_EMAIL": get("from_email"),
        "FROM_PASSWORD": get("from_password"),
        "SMTP_SERVER": get("smtp_server"),
        "SMTP_PORT": get("smtp_port"),
    })
    return {k: v for k, v in values.items() if v is not None and v != ""}


_file_cfg = load_config_from_ini(CONFIG_PATH)
TO_EMAIL = _file_cfg.get("TO_EMAIL", os.getenv("TO_EMAIL", "1713622254@qq.com"))
FROM_EMAIL = _file_cfg.get("FROM_EMAIL", os.getenv("FROM_EMAIL", "1713622254@qq.com"))
FROM_PASSWORD = _file_cfg.get("FROM_PASSWORD", os.getenv("FROM_PASSWORD", "xjahgydqxuqtbjac"))
SMTP_SERVER = _file_cfg.get("SMTP_SERVER", os.getenv("SMTP_SERVER", "smtp.qq.com"))
SMTP_PORT = int(_file_cfg.get("SMTP_PORT", os.getenv("SMTP_PORT", "587")))

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


def run_timer():
    """Run a simple weekday 22:00 timer loop."""
    last_sent_date: datetime.date | None = None
    while True:
        now = datetime.datetime.now()
        if is_weekday(now) and now.hour == 22 and now.minute == 0:
            if last_sent_date != now.date():
                try:
                    send_daily_report()
                except Exception as e:
                    print("发送失败:", e)
                last_sent_date = now.date()
                # Avoid re-sending within the same minute
                time.sleep(65)
                continue
        # Check roughly every 20 seconds
        time.sleep(20)


if __name__ == "__main__":
    run_timer()


