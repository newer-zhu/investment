import pandas as pd
import requests
import datetime
import akshare as ak
from sqlalchemy import create_engine, text

def save_to_mysql(df: pd.DataFrame, engine):

    # 重命名字段匹配数据库
    df = df.rename(columns={
        "日期": "trade_date",
        "股票代码": "stock_code",
        "开盘": "open_price",
        "收盘": "close_price",
        "最高": "high_price",
        "最低": "low_price",
        "成交量": "volume",
        "成交额": "amount",
        "振幅": "amplitude",
        "涨跌幅": "pct_change",
        "涨跌额": "price_change",
        "换手率": "turnover_rate"
    })

    # 逐行插入，避免重复
    insert_sql = """
        INSERT IGNORE INTO stock_zh_a_hist (
            trade_date, stock_code, open_price, close_price, high_price, low_price,
            volume, amount, amplitude, pct_change, price_change, turnover_rate
        )
        VALUES (
            :trade_date, :stock_code, :open_price, :close_price, :high_price, :low_price,
            :volume, :amount, :amplitude, :pct_change, :price_change, :turnover_rate
        )
    """
    with engine.begin() as conn:
        conn.execute(text(insert_sql), df.to_dict(orient="records"))
    print(f"✅ 成功插入 {len(df)} 条数据")


if __name__ == "__main__":
    # === 配置数据库连接 ===
    engine = create_engine("mysql+pymysql://root:20001030@localhost:3306/finance?charset=utf8mb4")

    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=1024)

    df = ak.stock_zh_a_hist(
        symbol="603662",
        start_date=start_date.strftime("%Y%m%d"),
        end_date=end_date.strftime("%Y%m%d"),
        adjust="qfq"
    )

    print(df.head())
    save_to_mysql(df, engine)
