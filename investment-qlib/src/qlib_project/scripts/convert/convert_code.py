import pandas as pd
from pathlib import Path
from utils.util import tushare_to_qlib
from constants import SOURCE_PATH
# ======================
# 配置区
# ======================
DATA_DIR = Path(SOURCE_PATH / "cache" / "balance")  # ⬅️ 改成你的目录


# ======================
# 主逻辑
# ======================
csv_files = list(DATA_DIR.glob("*.csv"))
print(f"Found {len(csv_files)} csv files")

for csv_path in csv_files:
    try:
        df = pd.read_csv(csv_path, dtype=str)

        if "ts_code" not in df.columns:
            print(f"[SKIP] {csv_path.name}: no ts_code column")
            continue

        # 转换 code
        df["symbol"] = df["ts_code"].apply(tushare_to_qlib)

        # 删旧列
        df = df.drop(columns=["ts_code"])

        # 把 symbol 放到第一列（可选，但推荐）
        cols = ["symbol"] + [c for c in df.columns if c != "symbol"]
        df = df[cols]

        # 覆盖写回
        df.to_csv(csv_path, index=False)
        print(f"[OK] {csv_path.name}")

    except Exception as e:
        print(f"[ERROR] {csv_path.name}: {e}")
