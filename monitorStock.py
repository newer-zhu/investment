import time
import akshare as ak
from utils import selected_stocks_to_html, find_csv_for_today_or_latest, is_trading_day, format_symbol
import pandas as pd
import datetime
import schedule
from email_job import send_late_suggestion

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

def trading_strategy():
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
            # print("===== Selected stocks =====")
            # for s in selected_stocks:
            #     print(
            #         f"{s['code']} {s['name']} | pct_change: {s['pct_change']}% | "
            #         f"turnover: {s['turnover']} | volume_ratio: {s['volume_ratio']} | "
            #         f"amount: {s['amount']/1e8:.2f}亿 | amplitude: {s['amplitude']}% | "
            #         f"speed: {s['speed']} | 5min_change: {s['five_min_change']}% | "
            #         f"60d_change: {s['sixty_day_change']}% | "
            #         f"PE: {s['pe_ratio']} | PB: {s['pb_ratio']} | "
            #         f"Scores: fundamental={s['fundamental_score']} technical={s['technical_score']} total={s['total_score']}"
            #     )
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
    """Run strategy until market close"""
    if not is_trading_day():
        now = datetime.datetime.now()
        print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] Today is not a trading day, skip strategy")
        return

    now = datetime.datetime.now()
    print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] Start monitoring strategy until close...")

    while True:
        should_continue = trading_strategy()
        if not should_continue:
            break
        time.sleep(60*5)

def job():
    """Scheduled job"""
    if is_trading_day():
        now = datetime.datetime.now()
        print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] Trigger trading day job")
        run_strategy_until_close()
    else:
        now = datetime.datetime.now()
        print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] Today is not a trading day, skip job")

if __name__ == "__main__":
    init_stocks()
    schedule.every().day.at("14:30").do(job)

    print("Trading strategy scheduler started, will run every trading day at 14:30...")
    print("Press Ctrl+C to stop the program")

    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        print("\nMonitor program manually stopped")
