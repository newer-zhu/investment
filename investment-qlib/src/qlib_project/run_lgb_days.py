import os
from pathlib import Path
import pandas as pd
import qlib
from qlib.config import REG_CN
from qlib.data.dataset import DatasetH
from qlib.contrib.data.handler import Alpha158
from qlib.contrib.model.gbdt import LGBModel

# ================== 1. 动态 Handler 工厂 ==================
def get_dynamic_handler(hold_days: int):
    """
    根据持有天数动态生成 Alpha158 处理器
    hold_days: 1-5 之间的整数
    """
    # 映射公式：T+1买入，hold_days天后卖出
    # hold=1 -> Ref(-2)/Ref(-1); hold=4 -> Ref(-5)/Ref(-1); hold=5 -> Ref(-6)/Ref(-1)
    target_ref = -(hold_days + 1)
    label_expr = f"Ref($close, {target_ref}) / Ref($close, -1) - 1"
    
    class DynamicAlpha158(Alpha158):
        def get_label_config(self):
            return ([label_expr], ["LABEL0"])
            
    return DynamicAlpha158

# ================== 2. 动态调参映射表 ==================
def get_model_params(hold_days: int):
    """
    核心逻辑：预测周期越长，噪音越大，正则化越强，学习率越低
    """
    # 基础参数
    params = {
        "loss": "mse",
        "num_leaves": 31,
        "n_jobs": -1,
    }
    
    # 动态调整部分
    configs = {
        1: {"n_estimators": 500, "lr": 0.01,  "l1": 0.05, "l2": 0.05, "depth": 6},
        2: {"n_estimators": 550, "lr": 0.008, "l1": 0.10, "l2": 0.10, "depth": 6},
        3: {"n_estimators": 600, "lr": 0.005, "l1": 0.15, "l2": 0.15, "depth": 5},
        4: {"n_estimators": 650, "lr": 0.005, "l1": 0.20, "l2": 0.20, "depth": 5},
        5: {"n_estimators": 700, "lr": 0.003, "l1": 0.25, "l2": 0.25, "depth": 4},
    }
    
    c = configs.get(hold_days, configs[3]) # 默认取 3 日配置
    
    params.update({
        "n_estimators": c["n_estimators"],
        "learning_rate": c["lr"],
        "max_depth": c["depth"],
        "lambda_l1": c["l1"],
        "lambda_l2": c["l2"],
        "feature_fraction": 0.8 - (hold_days * 0.02), # 周期越长，越要随机选择特征减少过拟合
        "bagging_fraction": 0.8 - (hold_days * 0.02),
        "min_data_in_leaf": 30 + (hold_days * 10),    # 周期越长，要求叶子节点样本越多，越稳健
    })
    return params

# ================== 3. 主预测函数 ==================
def run_dynamic_predict(
    pool_date: str,
    hold_days: int = 3,  # <--- 新增核心参数
    topk: int = 10,
    start_date: str = "2024-05-01",
    test_start: str = "2025-10-01"
):
    # 环境初始化
    os.environ["PYTHONIOENCODING"] = "utf-8"
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
    DATA_PATH = PROJECT_ROOT / "data" / "source"
    POOL_BASE_DIR = PROJECT_ROOT / "data" / "stock_pool" / "processed"
    RESULT_DIR = PROJECT_ROOT / "data" / "pool_predictions_lgb"
    
    qlib.init(provider_uri=str(DATA_PATH), region=REG_CN)

    # ---------- 加载股票池 ----------
    pool_csv = POOL_BASE_DIR / f"{pool_date}_pool.csv"
    df_pool = pd.read_csv(pool_csv, dtype=str)
    col = "instrument" if "instrument" in df_pool.columns else "Instrument"
    instruments = df_pool[col].dropna().unique().tolist()

    # ---------- 动态特征与模型配置 ----------
    print(f"🚀 任务启动: 预测 {hold_days} 日收益率 | 基准日期: {pool_date}")
    
    DynamicHandler = get_dynamic_handler(hold_days)
    handler = DynamicHandler(
        instruments=instruments,
        start_time=start_date,
        end_time=pool_date, # 截止到当前预测日
    )

    dataset = DatasetH(
        handler=handler,
        segments={"train": (start_date, test_start), "test": (test_start, pool_date)},
    )

    # 动态获取针对牛市优化的参数
    lgb_params = get_model_params(hold_days)
    print(f"📊 模型调参已完成 (Hold={hold_days}d): LR={lgb_params['learning_rate']}, L1={lgb_params['lambda_l1']}")
    
    model = LGBModel(**lgb_params)
    model.fit(dataset)

    # ---------- 预测 ----------
    pred = model.predict(dataset, segment="test")
    pred_df = pred.reset_index()
    pred_df.columns = ["datetime", "instrument", "score"]

    target_date = pd.Timestamp(pool_date)
    today_pred = pred_df[pred_df["datetime"] == target_date].copy()

    if today_pred.empty:
        print(f"❌ 警告: {pool_date} 没有预测分，请检查数据源或是否为交易日。")
        return

    # 排序与结果生成
    today_pred.sort_values("score", ascending=False, inplace=True)
    result = today_pred.head(topk)

    # ---------- 保存 ----------
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    save_name = f"pred_{hold_days}day_{pool_date}.csv"
    result.to_csv(RESULT_DIR / save_name, index=False)
    
    print(f"\n✅ 成功生成 {hold_days} 日持仓建议:")
    print(result[['instrument', 'score']].head(5))

# ================== 4. 执行入口 ==================
if __name__ == "__main__":
    run_dynamic_predict(
        pool_date="2026-01-28", 
        hold_days=2,  # 可选 1, 2, 3, 4, 5
        topk=10
    )