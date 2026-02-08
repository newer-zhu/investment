import tushare as ts
import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_PATH = PROJECT_ROOT / "data"
token = "2f80f707c09dc4ce6c59eb215739349f7a203485b19b4401a683bb6549ec"

pro = ts.pro_api(token)

pro._DataApi__token = token # 保证有这个代码，不然不可以获取
pro._DataApi__http_url = 'http://lianghua.nanyangqiankun.top'  # 保证有这个代码，不然不可以获取

def download_stock_basic_csv(
    code: str,
    output_path: str = "stock_basic.csv"
):

    # 拉取数据
    df = pro.stock_basic(
        ts_code=code,
        name="",
        exchange="",
        market="",
        is_hs="",
        list_status="L",   # 只要上市中的
        fields=[
            "ts_code",
            "symbol",
            "name",
            "area",
            "industry",
            "cnspell",
            "market",
            "list_date",
            "delist_date",
            "exchange"
        ]
    )

    # 确保目录存在
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # 保存为 CSV（utf-8-sig，Excel 友好）
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"✅ 股票列表已保存到: {output_path}")
    print(f"📊 共 {len(df)} 条记录")

if __name__ == "__main__":
    download_stock_basic_csv(
        "",
        output_path=SOURCE_PATH / "cache" / "stock_basic.csv"
    )
