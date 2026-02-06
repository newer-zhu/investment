import pandas as pd
from pathlib import Path

# ======================
# 配置
# ======================
INPUT_DIR = Path("/mnt/f/Code/investment/investment-qlib/data/fundamental/fina_indicator")
OUTPUT_DIR = Path("/mnt/f/Code/investment/investment-qlib/data/fundamental/features")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ======================
# 工具函数
# ======================
def tushare_to_qlib(code: str) -> str:
    """
    600000.SH -> SH600000
    000001.SZ -> SZ000001
    """
    if "." not in str(code):
        return code
    num, exch = code.split(".")
    return f"{exch.upper()}{num}"


def transform():
    for file in INPUT_DIR.glob("*.csv"):
        df = pd.read_csv(file)

        # 必须字段检查
        if not {"ts_code", "ann_date"}.issubset(df.columns):
            print(f"⚠️ 跳过 {file.name}，缺少 ts_code / ann_date")
            continue

        # ann_date → date
        df["date"] = pd.to_datetime(df["ann_date"], errors="coerce")
        df = df.dropna(subset=["date"])
        df["date"] = df["date"].dt.strftime("%Y-%m-%d")

        # ts_code → symbol
        df["symbol"] = df["ts_code"].apply(tushare_to_qlib)

        # 列顺序（仅做展示友好，不影响逻辑）
        cols = ["date", "symbol"] + [
            c for c in df.columns if c not in {"date", "symbol","ann_date","ts_code"}
        ]
        df = df[cols]

        out_file = OUTPUT_DIR / f"{tushare_to_qlib(file.stem)}.csv"
        
        df = df.sort_values(
            by=["date", "end_date"],
            ascending=[True, True]
        )

        df = df.drop_duplicates(
            subset=["symbol", "date"],
            keep="last"
        )
        df.to_csv(out_file, index=False)


if __name__ == "__main__":
    transform()
