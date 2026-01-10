from pathlib import Path
import os
import pandas as pd
import qlib
from qlib.config import REG_CN
from qlib.data.dataset import DatasetH
from qlib.contrib.data.handler import Alpha158
from qlib.contrib.model.gbdt import LGBModel
from qlib.contrib.strategy import TopkDropoutStrategy
from qlib.contrib.backtest import backtest


from qlib.contrib.data.handler import Alpha158


# ================== 路径配置 ==================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PATH = PROJECT_ROOT / "data" / "source" / "qlib_bin"
POOL_BASE_DIR = PROJECT_ROOT / "data" / "stock_pool" / "processed"
RESULT_DIR = PROJECT_ROOT / "data" / "pool_results_lgb"


# ================== 1. 初始化 qlib ==================
def init_env():
    qlib.init(
        provider_uri=str(DATA_PATH),
        region=REG_CN,
    )


# ================== 2. 读取股票池 ==================
def load_pool(pool_csv: Path) -> list[str]:
    if not pool_csv.exists():
        raise FileNotFoundError(f"股票池不存在: {pool_csv}")

    df = pd.read_csv(pool_csv, dtype=str)

    if "instrument" not in df.columns:
        raise ValueError("股票池 CSV 必须包含 instrument 列")

    instruments = df["instrument"].dropna().unique().tolist()
    return instruments


# ================== 3. 核心研究逻辑 ==================
def run_lgb_topk(
    pool_date: str,
    start_date: str,
    end_date: str,
    test_start: str,
    topk: int = 10,
):
    """
    使用指定日期的股票池运行 LightGBM + TopK
    """

    init_env()

    pool_csv = POOL_BASE_DIR / f"{pool_date}_pool.csv"
    instruments = load_pool(pool_csv)

    # -------- DataHandler --------
    handler = Alpha158(
        instruments=instruments,
        start_time=start_date,
        end_time=end_date,
    )

    # -------- Dataset --------
    dataset = DatasetH(
        handler=handler,
        segments={
            "train": (start_date, test_start),
            "test": (test_start, end_date),
        },
    )

    # -------- Model --------
    model = LGBModel(
        loss="mse",
        n_estimators=200,
        learning_rate=0.05,
        max_depth=6,
    )
    model.fit(dataset)

    # -------- Strategy --------
    strategy = TopkDropoutStrategy(
        model=model,
        dataset=dataset,
        topk=topk,
        n_drop=0,
    )

    # -------- Backtest --------
    report, positions = backtest(
        strategy=strategy,
        dataset=dataset,
    )

    # -------- 保存结果 --------
    RESULT_DIR.mkdir(parents=True, exist_ok=True)

    report_path = RESULT_DIR / f"report_{pool_date}.csv"
    pos_path = RESULT_DIR / f"positions_{pool_date}.csv"

    report.to_csv(report_path)
    positions.to_csv(pos_path)

    print(f"[OK] 回测完成: {pool_date}")
    print(f"Report: {report_path}")
    print(f"Positions: {pos_path}")

    return report, positions


# ================== 4. CLI 入口 ==================
def main():
    run_lgb_topk(
        pool_date="2025-12-24",
        start_date="2024-01-01",
        end_date="2026-01-08",
        test_start="2026-01-01",
        topk=10,
    )


if __name__ == "__main__":
    main()
