import os
import time
import datetime
import configparser
import subprocess
import sys
import threading
import schedule
import pandas as pd
from utils import is_trading_day,send_email, load_config_from_ini, find_csv_for_today_or_latest, selected_stocks_to_html,csv_to_html_table
from analyze_stocks import get_prev_portfolio_avg_message
import akshare as ak
from api import get_stock_history

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

# Stock list
stocks = {}

def get_stock_info(symbol: str) -> dict:
    """
    Get detailed stock info (using Snowball interface)
    :param symbol: stock code, e.g., "SH600000"
    :return: dict {item: value}
    """
    df = ak.stock_individual_spot_xq(symbol=symbol)
    info = dict(zip(df["item"], df["value"]))
    return info

def init_stocks():
    """Load stock CSV from today or latest available"""
    csv_path = find_csv_for_today_or_latest()
    now = datetime.datetime.now()
    print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] Reading stock file: {csv_path}")
    
    global stocks
    stocks = pd.read_csv(csv_path)
    print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] Successfully loaded {len(stocks)} stocks")

def get_realtime_quotes():
    """Fetch all A-share market quotes once"""
    df = ak.stock_zh_a_spot_em()
    df["代码"] = df["代码"].astype(str)
    df.set_index("代码", inplace=True)
    return df

def get_quote_for_stock(df, code: str):
    """Get single stock quote from full market DataFrame"""
    if code in df.index:
        return df.loc[code].to_dict()
    else:
        return None

def late_trading_strategy():
    """Improved trading strategy main function"""
    now = datetime.datetime.now()
    print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] Strategy started...")

    try:
        market_df = get_realtime_quotes()
        selected_stocks = []

        for _, row in stocks.iterrows():
            code = str(row["代码"]).zfill(6)
            info = get_quote_for_stock(market_df, code)
            if info is None:
                continue
            # ===== 剔除过去连续三天上涨的股票 =====
            try:
                end_date = datetime.datetime.today().strftime("%Y%m%d")
                start_date = (datetime.datetime.today() - datetime.timedelta(days=10)).strftime("%Y%m%d")
                try:
                    hist_df = get_stock_history(symbol=code, start_date=start_date, end_date=end_date)
                except Exception as e:
                    print(f"[{code}] 获取历史数据失败: {e}")
                    continue

                if len(hist_df) >= 4:
                    last4 = hist_df.tail(4).copy()
                    last4["chg"] = last4["close"].diff()
                    # 取最后三天的chg
                    last3_chg = last4["chg"].iloc[-3:]
                    if (last3_chg > 0).all():
                        # 连续三天收涨，跳过
                        print(f"[{code}] 连续三天上涨，剔除")
                        continue
            except Exception as e:
                print(f"[{code}] 历史数据检查失败: {e}")
                continue


            try:
                turnover = float(info.get("换手率", 0))
                circulating_value = float(info.get("流通市值", 0))
                volume_ratio = float(info.get("量比", 0))
                pct_change = float(info.get("涨跌幅", 0))
                amount = float(info.get("成交额", 0))
                amplitude = float(info.get("振幅", 0))
                speed = float(info.get("涨速", 0))
                five_min_change = float(info.get("5分钟涨跌", 0))
                sixty_day_change = float(info.get("60日涨跌幅", 0))
                pe_ratio = float(info.get("市盈率-动态", 0))
                pb_ratio = float(info.get("市净率", 0))

            except Exception as e:
                print(f"[{code}] Data parse error: {e}")
                continue

            # ===== Screening conditions =====
            if (
                turnover > 5
                and circulating_value < 2e11
                and volume_ratio > 1.2
                and amount > 5e8
                and amplitude > 3
                and (speed > 0 or five_min_change > 0.5)
                and sixty_day_change > 0
                and pe_ratio < 80
                and pb_ratio < 10
            ):
                selected_stocks.append({
                    "code": code,
                    "name": info.get("名称", ""),
                    "pct_change": pct_change,
                    "turnover": turnover,
                    "volume_ratio": volume_ratio,
                    "circulating_value": circulating_value,
                    "amount": amount,
                    "amplitude": amplitude,
                    "speed": speed,
                    "five_min_change": five_min_change,
                    "sixty_day_change": sixty_day_change,
                    "pe_ratio": pe_ratio,
                    "pb_ratio": pb_ratio,
                    "fundamental_score": row.get("基本面评分", None),
                    "technical_score": row.get("技术面评分", None),
                    "total_score": row.get("总分", None),
                })

        # Print results
        if selected_stocks:
            send_late_suggestion(selected_stocks_to_html(selected_stocks))
        else:
            print("No stock matched the conditions.")

    except Exception as e:
        now = datetime.datetime.now()
        print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] Strategy error: {str(e)}")

    # Check closing time
    current_time = datetime.datetime.now()
    close_time = current_time.replace(hour=15, minute=0, second=0, microsecond=0)

    if current_time >= close_time:
        print(f"[{current_time.strftime('%Y-%m-%d %H:%M:%S')}] Market closed, stop strategy")
        return False
    else:
        return True


def run_strategy_until_close():
    init_stocks()

    now = datetime.datetime.now()
    print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] Start monitoring strategy until close...")

    while True:
        should_continue = late_trading_strategy()
        if not should_continue:
            break
        time.sleep(60*5)



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



def extract_top_stocks_from_last3_files():
    """
    提取最近3个输出文件中都出现在前3位的股票详情
    """
    import glob
    from collections import Counter
    
    # 获取所有输出文件并按日期排序
    output_files = glob.glob(os.path.join("output", "picked_stocks_*.csv"))
    if len(output_files) < 3:
        return ""
    
    # 按文件名排序（包含日期）
    output_files.sort(reverse=True)
    last_3_files = output_files[:3]
    
    # 收集每个文件前3位的股票代码
    top_stocks_per_file = []
    for file_path in last_3_files:
        try:
            df = pd.read_csv(file_path)
            if len(df) >= 10:
                top_codes = df.head(10)["代码"].astype(str).tolist()
                top_stocks_per_file.append(set(top_codes))
        except Exception as e:
            print(f"读取文件 {file_path} 失败: {e}")
            continue
    
    if len(top_stocks_per_file) < 3:
        return ""
    
    # 找到在所有3个文件中都出现在前10位的股票
    common_stocks = set.intersection(*top_stocks_per_file)
    
    if not common_stocks:
        return ""
    
    # 获取这些股票的详细信息（从最新文件中）
    try:
        latest_df = pd.read_csv(last_3_files[0])
        common_stocks_details = latest_df[latest_df["代码"].astype(str).isin(common_stocks)]
        
        if common_stocks_details.empty:
            return ""
        
        # 按总分排序
        common_stocks_details = common_stocks_details.sort_values("总分", ascending=False)
        
        # 生成HTML表格
        details_html = common_stocks_details.to_html(index=False, border=0, escape=False)
        style = """
        <style>
          table { border-collapse: collapse; width: 100%; margin-top: 20px; }
          th, td { border: 1px solid #e5e7eb; padding: 8px 10px; text-align: center; font-family: Arial, Helvetica, sans-serif; font-size: 13px; }
          th { background: #f3f4f6; }
          td:first-child { font-family: Consolas, 'Courier New', monospace; }
        </style>
        """
        
        header = f"<h3>重点关注股票详情 (共{len(common_stocks_details)}只):</h3>"
        return header + style + details_html
        
    except Exception as e:
        print(f"生成连续前3位股票详情失败: {e}")
        return ""


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
        # 提取连续3天前3位的股票详情
        top3_details = extract_top_stocks_from_last3_files()
        body = f"<p>今日选股建议（建议持有3~5天）: 纯属个人项目，不构成任何投资建议</p>{second}{table_html}{top3_details}"

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
        # 提取连续3天前10位的股票详情
        top3_details = extract_top_stocks_from_last3_files()
        body = f"<p>今日选股建议（建议持有3~5天）: 纯属个人项目，不构成任何投资建议</p>{second}{table_html}{top3_details}"

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
    """(Re)register today's jobs. safe to call multiple times."""
    schedule.clear()
    now = datetime.datetime.now()
    print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] schedule_jobs() called")

    if not is_trading_day():
        print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] 非交易日，跳过任务注册")
        return

    # 让 run_strategy_until_close 在独立线程里运行，避免阻塞 schedule.run_pending()
    schedule.every().day.at("14:42").do(
        lambda: threading.Thread(target=run_strategy_until_close, daemon=True).start()
    )

    # 日报仍然可以直接注册（send_daily_report 本身是短任务）
    schedule.every().day.at("14:45").do(send_daily_report)

    # 打印当前已注册任务，便于调试
    print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] 已注册任务：")
    for j in schedule.jobs:
        print("  -", j)

def run_timer():
    # 1) 启动时先注册一次，确保程序启动当天有任务
    schedule_jobs()

    last_refresh_date = None
    while True:
        try:
            schedule.run_pending()
        except Exception as e:
            print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] schedule.run_pending 出错: {e}")

        # 每天凌晨刷新一次任务注册（避免跨日问题）
        now = datetime.datetime.now()
        if now.date() != last_refresh_date and now.hour == 0 and now.minute < 5:
            schedule_jobs()
            last_refresh_date = now.date()

        time.sleep(20)



if __name__ == "__main__":
    run_timer()


