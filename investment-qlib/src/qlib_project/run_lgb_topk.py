import os
import sys
from pathlib import Path
import pandas as pd

# ================== 0. 环境兼容性修复 (必须放在最前面) ==================
# 强制设置编码，并尝试规避 joblib 的路径问题
os.environ["PYTHONIOENCODING"] = "utf-8"

# ================== 1. 配置与初始化 ==================
import qlib
from qlib.config import REG_CN, C
from qlib.data.dataset import DatasetH
from qlib.contrib.data.handler import Alpha158
from qlib.contrib.model.gbdt import LGBModel
from qlib.contrib.strategy import TopkDropoutStrategy
from qlib.backtest import backtest

# 路径配置
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PATH = PROJECT_ROOT / "data" / "source" / "qlib_bin"
POOL_BASE_DIR = PROJECT_ROOT / "data" / "stock_pool" / "processed"
RESULT_DIR = PROJECT_ROOT / "data" / "pool_results_lgb"

def init_env():
    """初始化 qlib 并强制关闭并行以绕过中文路径 Bug"""
    if not DATA_PATH.exists():
        print(f"警告: 数据目录不存在 {DATA_PATH}")
        
    qlib.init(
        provider_uri=str(DATA_PATH),
        region=REG_CN,
    )
    
    # --- 核心修复：禁用多进程 ---
    C["workers"] = 1
    C["joblib_backend"] = "sequential"
    print(f"系统初始化成功：已强制设为单进程模式以兼容中文路径。")

# ================== 2. 工具函数 ==================
def load_pool(pool_csv: Path) -> list[str]:
    if not pool_csv.exists():
        raise FileNotFoundError(f"股票池不存在: {pool_csv}")

    df = pd.read_csv(pool_csv, dtype=str)
    if "instrument" not in df.columns:
        # 尝试容错处理：如果列名是大写的 Instrument
        if "Instrument" in df.columns:
            df.rename(columns={"Instrument": "instrument"}, inplace=True)
        else:
            raise ValueError(f"CSV 缺少 'instrument' 列。当前列名: {df.columns.tolist()}")

    return df["instrument"].dropna().unique().tolist()

# ================== 3. 核心运行逻辑 ==================
def run_lgb_topk(
    pool_date: str,
    start_date: str,
    end_date: str,
    test_start: str,
    topk: int = 10,
):
    # 初始化
    init_env()

    # 加载股票池
    pool_csv = POOL_BASE_DIR / f"{pool_date}_pool.csv"
    print(f"正在读取股票池: {pool_csv.name}")
    instruments = load_pool(pool_csv)

    # -------- DataHandler (特征工程) --------
    print(f"正在准备特征 (Alpha158)... 样本数: {len(instruments)}")
    handler = Alpha158(
        instruments=instruments,
        start_time=start_date,
        end_time=end_date,
    )

    # -------- Dataset (数据集切分) --------
    dataset = DatasetH(
        handler=handler,
        segments={
            "train": (start_date, test_start),
            "test": (test_start, end_date),
        },
    )

    # -------- Model (训练 LightGBM) --------
    print("正在训练 LightGBM 模型...")
    model = LGBModel(
        loss="mse",
        n_estimators=200,
        learning_rate=0.05,
        max_depth=6,
        n_jobs=1, # 同样确保模型内部也不乱开多线程
    )
    model.fit(dataset)


# -------- 引入执行器组件 --------
    from qlib.backtest import backtest as qlib_backtest
    from qlib.backtest.executor import SimulatorExecutor

    # -------- Strategy & Executor 配置 --------
    print(f"开始回测: {test_start} 至 {end_date}")
    
    # 1. 策略
    strategy = TopkDropoutStrategy(
        model=model,
        dataset=dataset,
        topk=topk,
        n_drop=0,
    )

    # 2. 执行器 (Executor)
    # time_per_step: 调仓周期，"day" 表示日频
    # kwargs 里面可以配置交易费率等信息
    executor = SimulatorExecutor(
        time_per_step="day", 
        verbose=True
    )

    # 3. 调用修正后的 backtest
    # 注意：0.9.7 的顺序通常是 (strategy, executor, start_time, end_time)
    report, positions = qlib_backtest(
        start_time=test_start,
        end_time=end_date,
        strategy=strategy,
        executor=executor
    )
# -------- 保存结果 --------
    RESULT_DIR.mkdir(parents=True, exist_ok=True)

    report_path = RESULT_DIR / f"report_{pool_date}.csv"
    pos_path = RESULT_DIR / f"positions_{pool_date}.csv"

    # 1. 处理 report 字典
    # report 包含：'excess_return_without_cost', 'return_indicator' 等
    # 我们提取每日的具体指标 (DataFrame)
    if isinstance(report, dict):
        # 提取收益指标表格
        report_df = report.get("return_indicator", pd.DataFrame())
        if report_df.empty and "excess_return_without_cost" in report:
             report_df = report["excess_return_without_cost"]
        report_df.to_csv(report_path)
    else:
        report.to_csv(report_path)

    # 2. 处理 positions 字典
    # positions 键是日期，值是该日的持仓 DataFrame
    if isinstance(positions, dict):
        try:
            pd.concat(positions, axis=0).to_csv(pos_path)
        except Exception:
            # 如果无法直接 concat（格式不统一），则转为字符串保存以防崩溃
            with open(pos_path, "w") as f:
                f.write(str(positions))
    else:
        positions.to_csv(pos_path)

# ================== 4. 执行入口 ==================
def main():
    try:
        run_lgb_topk(
            pool_date="2025-12-24",
            start_date="2024-01-01",
            end_date="2026-01-08",
            test_start="2026-01-01",
            topk=10,
        )
    except Exception as e:
        print(f"程序运行出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()