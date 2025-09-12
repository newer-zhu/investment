import time
import akshare as ak

def get_price(symbol: str) -> float:
    """
    获取股票当前价格
    """
    df = ak.stock_individual_spot_xq(symbol=symbol)
    # "现价" 对应的 value
    price = float(df[df["item"] == "现价"]["value"].values[0])
    return price

def monitor_stock(symbol: str, stop_loss: float, take_profit: float, interval: int = 5):
    """
    监控单只股票的止盈止损

    :param symbol: 股票代码，例如 "SH600000"
    :param stop_loss: 止损价
    :param take_profit: 止盈价
    :param interval: 轮询间隔（秒）
    """
    print(f"开始监控 {symbol}，止损价={stop_loss}，止盈价={take_profit}")
    while True:
        try:
            price = get_price(symbol)
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {symbol} 当前价: {price}")

            if price <= stop_loss:
                print(f"⚠️ 触发止损! 当前价 {price} <= {stop_loss}")
                break
            elif price >= take_profit:
                print(f"✅ 触发止盈! 当前价 {price} >= {take_profit}")
                break

            time.sleep(interval)
        except Exception as e:
            print(f"获取行情失败: {e}")
            time.sleep(interval)

if __name__ == "__main__":
    monitor_stock(symbol="SH600797", stop_loss=9.0, take_profit=11.0, interval=10)
