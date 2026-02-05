import pandas as pd
import os
import datetime
import sys
from pathlib import Path

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))
from utils import _to_qlib_instrument

# def csv_to_qlib_pool(
#     input_csv: str,
#     output_dir: str = "investment-qlib/data/stock_pool/processed",
# ):
#     # 1. 读原始 CSV
#     df = pd.read_csv(input_csv)

#     if "代码" not in df.columns:
#         raise ValueError("CSV 中未找到「代码」列")

#     # 2. 转换为 qlib instrument
#     instruments = df["代码"].astype(str).map(_to_qlib_instrument)

#     qlib_pool_df = (
#         pd.DataFrame({"instrument": instruments})
#         .drop_duplicates()
#         .sort_values("instrument")
#         .reset_index(drop=True)
#     )

#     # 3. 输出
#     os.makedirs(output_dir, exist_ok=True)
#     today = datetime.date.today().strftime("%Y-%m-%d")
#     output_path = os.path.join(output_dir, f"{today}_pool.csv")

#     qlib_pool_df.to_csv(output_path, index=False, encoding="utf-8")

#     print(f"✅ 已生成 qlib 股票池：{output_path}")
#     print(f"📊 股票数量：{len(qlib_pool_df)}")

def csv_to_qlib_pool(
    input_dir: str,
    output_dir: str = "investment-qlib/data/stock_pool/processed",
):
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)

    if not input_dir.exists():
        raise ValueError(f"输入目录不存在: {input_dir}")

    # ======================
    # 1. 收集所有股票代码
    # ======================
    all_codes = []

    csv_files = list(input_dir.glob("*.csv"))
    if not csv_files:
        raise ValueError(f"目录下没有 CSV 文件: {input_dir}")

    for csv_file in csv_files:
        df = pd.read_csv(csv_file)

        if "代码" not in df.columns:
            print(f"⚠️ 跳过 {csv_file.name}（无「代码」列）")
            continue

        all_codes.extend(df["代码"].dropna().astype(str).tolist())

    if not all_codes:
        raise ValueError("未从任何 CSV 中读取到股票代码")

    # ======================
    # 2. 转换为 qlib instrument
    # ======================
    instruments = (
        pd.Series(all_codes)
        .map(_to_qlib_instrument)
        .drop_duplicates()
        .sort_values()
        .reset_index(drop=True)
    )

    qlib_pool_df = pd.DataFrame({"instrument": instruments})

    # ======================
    # 3. 输出
    # ======================
    output_dir.mkdir(parents=True, exist_ok=True)
    today = datetime.date.today().strftime("%Y-%m-%d")
    output_path = output_dir / f"{today}_pool.csv"

    qlib_pool_df.to_csv(output_path, index=False, encoding="utf-8")

    print(f"✅ 已生成 QLib 股票池: {output_path}")
    print(f"📊 股票数量: {len(qlib_pool_df)}")

if __name__ == "__main__":
    csv_to_qlib_pool("/mnt/f/Code/investment/output/")
