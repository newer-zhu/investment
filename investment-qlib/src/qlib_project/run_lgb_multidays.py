import os
from pathlib import Path
import pandas as pd
import numpy as np
import qlib
from qlib.config import REG_CN
from qlib.data.dataset import DatasetH
from qlib.contrib.data.handler import Alpha158
from qlib.contrib.model.gbdt import LGBModel

# ================== 1. 基础配置与动态工厂 ==================
def get_dynamic_handler(hold_days: int):
    target_ref = -(hold_days + 1)
    label_expr = f"Ref($close, {target_ref}) / Ref($close, -1) - 1"
    class DynamicAlpha158(Alpha158):
        def get_label_config(self):
            return ([label_expr], ["LABEL0"])
    return DynamicAlpha158

def get_model_params(hold_days: int):
    # 保持之前的动态调参逻辑
    configs = {
        1: {"lr": 0.01,  "l1": 0.05, "l2": 0.05, "depth": 6},
        3: {"lr": 0.005, "l1": 0.15, "l2": 0.15, "depth": 5},
        5: {"lr": 0.003, "l1": 0.25, "l2": 0.25, "depth": 4},
    }
    c = configs.get(hold_days, configs[3])
    return {
        "loss": "mse", "n_estimators": 500 + hold_days*20,
        "learning_rate": c["lr"], "max_depth": c["depth"],
        "lambda_l1": c["l1"], "lambda_l2": c["l2"],
        "feature_fraction": 0.75, "bagging_fraction": 0.75, "n_jobs": -1
    }

# ================== 2. 核心融合逻辑 ==================
def run_ensemble_predict(
    pool_date: str,
    hold_list: list = [1, 3, 5], # 同时考察这三个周期
    weights: dict = {1: 0.2, 3: 0.4, 5: 0.4}, # 权重分配：更看重中线持续性
    topk: int = 10
):
    # --- 环境初始化 ---
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
    DATA_PATH = PROJECT_ROOT / "data" / "source"
    POOL_BASE_DIR = PROJECT_ROOT / "data" / "stock_pool" / "processed"
    RESULT_DIR = PROJECT_ROOT / "data" / "pool_predictions_ensemble"
    qlib.init(provider_uri=str(DATA_PATH), region=REG_CN)

    # ---------- 加载股票池 ----------
    pool_csv = POOL_BASE_DIR / f"{pool_date}_pool.csv"
    df_pool = pd.read_csv(pool_csv, dtype=str)
    instruments = df_pool["instrument" if "instrument" in df_pool.columns else "Instrument"].dropna().unique().tolist()

    all_scores = []

    # ---------- 循环训练不同时序的模型 ----------
    for h in hold_list:
        print(f"\n训练 {h} 日时序模型...")
        DynamicHandler = get_dynamic_handler(h)
        handler = DynamicHandler(instruments=instruments, start_time="2024-05-01", end_time=pool_date)
        
        dataset = DatasetH(
            handler=handler,
            segments={"train": ("2024-05-01", "2025-10-01"), "test": ("2025-10-01", pool_date)},
        )
        
        model = LGBModel(**get_model_params(h))
        model.fit(dataset)
        
        # 预测结果
        pred = model.predict(dataset, segment="test")
        pred_df = pred.reset_index()
        pred_df.columns = ["datetime", "instrument", f"score_{h}d"]
        
        # 提取目标日期分数
        target_score = pred_df[pred_df["datetime"] == pd.Timestamp(pool_date)].copy()
        all_scores.append(target_score.set_index("instrument")[[f"score_{h}d"]])

    # ---------- 分数融合 (Ensemble) ----------
    print("\n正在进行多时序分数融合...")
    final_df = pd.concat(all_scores, axis=1)
    
    # 归一化处理（由于不同周期Label幅度不同，需先做Rank或Standardize）
    for h in hold_list:
        final_df[f"rank_{h}d"] = final_df[f"score_{h}d"].rank(pct=True)

    # 计算加权总分
    final_df["ensemble_score"] = sum(final_df[f"rank_{h}d"] * weights[h] for h in hold_list)
    final_df = final_df.sort_values("ensemble_score", ascending=False)

    # ---------- 保存结果 ----------
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    final_df.head(topk).to_csv(RESULT_DIR / f"ensemble_result_{pool_date}.csv")
    
    print(f"✅ 融合完成！Top {topk} 领涨名单已保存。")
    print(final_df[['ensemble_score'] + [f'rank_{h}d' for h in hold_list]].head(topk))

if __name__ == "__main__":
    run_ensemble_predict(pool_date="2026-01-26", hold_list=[1, 3, 5], topk=20)