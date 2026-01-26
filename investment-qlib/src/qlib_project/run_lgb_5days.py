import os
from pathlib import Path
import pandas as pd

# ================== 0. 环境兼容性修复 ==================
os.environ["PYTHONIOENCODING"] = "utf-8"

# ================== 1. QLib 初始化 ==================
import qlib
from qlib.config import REG_CN
from qlib.data.dataset import DatasetH
from qlib.contrib.data.handler import Alpha158
from qlib.contrib.model.gbdt import LGBModel

# --- [新增代码] 自定义 4日 预测处理器 ---
class Alpha158_4Day(Alpha158):
    def get_label_config(self):
        # 解释：Ref($close, -6) / Ref($close, -1) - 1
        # 含义：(T+6收盘价 / T+1收盘价) - 1
        # 即：如果你在明天(T+1)收盘买入，持有4个交易日后的收益率
        return (["Ref($close, -5) / Ref($close, -1) - 1"], ["LABEL0"])
# ================== 2. 路径配置 ==================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PATH = PROJECT_ROOT / "data" / "source"
POOL_BASE_DIR = PROJECT_ROOT / "data" / "stock_pool" / "processed"
RESULT_DIR = PROJECT_ROOT / "data" / "pool_predictions_lgb"

# ================== 3. 初始化函数 ==================
def init_env():
    qlib.init(
        provider_uri=str(DATA_PATH),
        region=REG_CN,
    )

def load_pool(pool_csv: Path) -> list[str]:
    df = pd.read_csv(pool_csv, dtype=str)
    if "instrument" not in df.columns:
        if "Instrument" in df.columns:
            df.rename(columns={"Instrument": "instrument"}, inplace=True)
        else:
            raise ValueError("股票池 CSV 缺少 instrument 列")
    return df["instrument"].dropna().unique().tolist()

# ================== 4. 主逻辑 ==================
def run_lgb_predict_only(
    pool_date: str,
    start_date: str,
    end_date: str,
    test_start: str,
    topk: int = 20,
):
    init_env()

    # ---------- 加载股票池 ----------
    pool_csv = POOL_BASE_DIR / f"{pool_date}_pool.csv"
    print(f"读取股票池: {pool_csv}")
    instruments = load_pool(pool_csv)

    # ---------- 特征工程 ----------
    print("构建 Alpha158 特征...")
    handler = Alpha158_4Day(
        instruments=instruments,
        start_time=start_date,
        end_time=end_date,
    )

    # ---------- Dataset ----------
    dataset = DatasetH(
        handler=handler,
        segments={
            "train": (start_date, test_start),
            "test": (test_start, end_date),
        },
    )

    # ---------- 模型 ----------
    print("训练 LightGBM...")
    model = LGBModel(
        loss="mse",
        n_estimators=600,      # 稍微增加树的数量，因为任务变难了
        learning_rate=0.005,   # [建议] 降低学习率（原来是0.01），让学习更稳健
        max_depth=5,           # [建议] 稍微降低深度（原来是6），防止死记硬背
        num_leaves=31,         # 配合 max_depth
        min_data_in_leaf=50,   # [新增] 增加叶子节点最小样本数，过滤噪音
        n_jobs=-1,
    )

    model.fit(dataset)

    # ---------- 预测 ----------
    print("生成预测分数...")
    pred = model.predict(dataset, segment="test")

    # MultiIndex → DataFrame
    pred_df = pred.reset_index()
    pred_df.columns = ["datetime", "instrument", "score"]

    # ---------- 只取 pool_date ----------
    target_date = pd.Timestamp(pool_date)
    today_pred = pred_df[pred_df["datetime"] == target_date].copy()

    if today_pred.empty:
        raise ValueError(f"{pool_date} 当天无可用预测结果")

    # ---------- 排序 ----------
    today_pred.sort_values("score", ascending=False, inplace=True)
    today_pred["rank"] = range(1, len(today_pred) + 1)
    today_pred["rank_pct"] = today_pred["score"].rank(pct=True)

    # ---------- 取 TopK ----------
    result = today_pred.head(topk)

    # ---------- 保存 ----------
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    save_path = RESULT_DIR / f"prediction_rank_{pool_date}.csv"
    result.to_csv(save_path, index=False)

    print(f"\n✅ Top {topk} 预测结果已保存：{save_path}")
    print(result.head(10))

# ================== 5. 入口 ==================
if __name__ == "__main__":
    run_lgb_predict_only(
        pool_date="2026-01-26",
        start_date="2024-05-01",
        end_date="2026-01-26",
        test_start="2025-10-01",
        topk=10,
    )
