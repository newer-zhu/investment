import pandas as pd
import os
import datetime
import sys
from pathlib import Path

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
from utils import _to_qlib_instrument

def csv_to_qlib_pool(
    input_csv: str,
    output_dir: str = "investment-qlib/data/stock_pool/processed",
):
    # 1. 读原始 CSV
    df = pd.read_csv(input_csv)

    if "代码" not in df.columns:
        raise ValueError("CSV 中未找到「代码」列")

    # 2. 转换为 qlib instrument
    instruments = df["代码"].astype(str).map(_to_qlib_instrument)

    qlib_pool_df = (
        pd.DataFrame({"instrument": instruments})
        .drop_duplicates()
        .sort_values("instrument")
        .reset_index(drop=True)
    )

    # 3. 输出
    os.makedirs(output_dir, exist_ok=True)
    today = datetime.date.today().strftime("%Y-%m-%d")
    output_path = os.path.join(output_dir, f"{today}_pool.csv")

    qlib_pool_df.to_csv(output_path, index=False, encoding="utf-8")

    print(f"✅ 已生成 qlib 股票池：{output_path}")
    print(f"📊 股票数量：{len(qlib_pool_df)}")

if __name__ == "__main__":
    csv_to_qlib_pool("/mnt/f/Code/investment/output/picked_stocks_20260123.csv")
