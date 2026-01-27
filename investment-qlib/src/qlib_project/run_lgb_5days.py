import os
from pathlib import Path
import pandas as pd
import qlib
from qlib.config import REG_CN
from qlib.data.dataset import DatasetH
from qlib.contrib.data.handler import Alpha158
from qlib.contrib.model.gbdt import LGBModel

# ================== 1. 自定义 4日 预测处理器 (牛市适配) ==================
class Alpha158_4Day_Bull(Alpha158):
    def get_label_config(self):
        # 预测 T+1 收盘买入，T+5 收盘卖出的 4日收益率
        # 为了应对牛市，Label 保持为 4日收益，但在模型训练端加强泛化
        return (["Ref($close, -5) / Ref($close, -1) - 1"], ["LABEL0"])

# ================== 2. 环境与路径 ==================
os.environ["PYTHONIOENCODING"] = "utf-8"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PATH = PROJECT_ROOT / "data" / "source"
POOL_BASE_DIR = PROJECT_ROOT / "data" / "stock_pool" / "processed"
RESULT_DIR = PROJECT_ROOT / "data" / "pool_predictions_lgb"

def init_env():
    qlib.init(provider_uri=str(DATA_PATH), region=REG_CN)

def load_pool(pool_csv: Path) -> list[str]:
    df = pd.read_csv(pool_csv, dtype=str)
    col = "instrument" if "instrument" in df.columns else "Instrument"
    return df[col].dropna().unique().tolist()

# ================== 3. 主逻辑 ==================
def run_lgb_predict_bull_style(
    pool_date: str,
    start_date: str,
    end_date: str,
    test_start: str,
    topk: int = 10,
):
    init_env()

    # ---------- 加载股票池 ----------
    pool_csv = POOL_BASE_DIR / f"{pool_date}_pool.csv"
    instruments = load_pool(pool_csv)

    # ---------- 特征工程 ----------
    # 扩大训练窗口到 2024-05-01 是正确的，覆盖了不同的市场阶段
    handler = Alpha158_4Day_Bull(
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

    # ---------- 模型 (针对牛市风格适配) ----------
    print("正在训练牛市适配版 LightGBM...")
    model = LGBModel(
        loss="mse",
        n_estimators=600,
        learning_rate=0.005,  # 极低学习率，确保平滑
        max_depth=5,
        num_leaves=31,
        min_data_in_leaf=50,
        
        # --- [牛市适配新增] 正则化与随机性 ---
        lambda_l1=0.15,      # 增加 L1 正则化，减少噪音因子的干扰
        lambda_l2=0.15,      # 增加 L2 正则化，防止模型对某些暴涨股赋予过高权重
        feature_fraction=0.7, # 每次分裂只看 70% 的特征，增加模型鲁棒性
        bagging_fraction=0.7, # 增加数据随机性，防止过拟合特定行情
        bagging_freq=5,
        n_jobs=-1,
    )

    model.fit(dataset)

    # ---------- 预测与后处理 ----------
    try:
        pred = model.predict(dataset, segment="test")
    except Exception as e:
        print(f"预测失败，可能是因为 {pool_date} 没有有效特征，建议检查日期。")
        raise e

    pred_df = pred.reset_index()
    pred_df.columns = ["datetime", "instrument", "score"]

    target_date = pd.Timestamp(pool_date)
    today_pred = pred_df[pred_df["datetime"] == target_date].copy()

    if today_pred.empty:
        # 尝试回溯到上一个交易日 (如果是周末)
        print(f"⚠️ {pool_date} 无数据，请确认是否为交易日。")
        return

    # 牛市中除了看 Score，还要防止买入已经“缩量一字板”的票（无法成交）
    # 这里我们只做排序逻辑
    today_pred.sort_values("score", ascending=False, inplace=True)
    result = today_pred.head(topk)

    # ---------- 保存 ----------
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    save_path = RESULT_DIR / f"bull_prediction_{pool_date}.csv"
    result.to_csv(save_path, index=False)
    print(f"\n✅ 适配后的 Top {topk} 结果已保存：{save_path}")
    print(result)

if __name__ == "__main__":
    # 2026-01-24 是周六，建议改为 2026-01-23 (周五) 运行
    run_lgb_predict_bull_style(
        pool_date="2026-01-26", 
        start_date="2024-05-01",
        end_date="2026-01-26", # 包含 26 号是为了获取最新特征
        test_start="2025-10-01",
        topk=10,
    )