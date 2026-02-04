import os
import lightgbm as lgb
import pandas as pd
import numpy as np
import qlib
from pathlib import Path
from qlib.config import REG_CN
from qlib.data.dataset import DatasetH
from qlib.contrib.data.handler import Alpha158
from qlib.data import D

# ================== 0. 配置区 ==================
PROJECT_ROOT = Path(__file__).resolve().parents[2]  # 视你的层级而定

DATA_DIR = PROJECT_ROOT / "data"
FUNDAMENTAL_PATH = DATA_DIR / "fundamental/fundamental_all.csv"
BIAS_THRESHOLD = 0.15  # 偏离5日线 15% 视为危险
RISK_STOP_LOSS = -0.04 # 建议止损线

# ================== 1. 动态 Handler (纯量价) ==================
def get_dynamic_handler(hold_days: int):
    # 预测 T+hold_days 的收益
    target_ref = -(hold_days + 1)
    label_expr = f"Ref($close, {target_ref}) / Ref($close, -1) - 1"
    
    class DynamicAlpha158(Alpha158):
        def get_label_config(self):
            return ([label_expr], ["LABEL0"])
            
    return DynamicAlpha158

# ================== 2. 数据融合函数 (核心) ==================
def load_and_merge_data(dataset, segment):
    """
    从 QLib 提取量价数据，并与 CSV 财务数据强力缝合
    """
    print(f"📥 正在提取 {segment} 集的量价特征...")
    # 1. 获取 Alpha158 特征 (index: datetime, instrument)
    df_alpha = dataset.prepare(segment, col_set=["feature", "label"])
    
    # 2. 读取财务数据
    if not os.path.exists(FUNDAMENTAL_PATH):
        print("⚠️以此警告：找不到财务文件，将仅使用量价因子运行！")
        return df_alpha
        
    print("🔗 正在拼接财务因子...")
    df_fund = pd.read_csv(FUNDAMENTAL_PATH)
    df_fund['datetime'] = pd.to_datetime(df_fund['datetime'])
    # 设置索引以便 Join
    df_fund.set_index(['datetime', 'instrument'], inplace=True)
    
    # 1. flatten qlib 输出的 columns（关键）
    df_alpha = df_alpha.copy()
    df_alpha.columns = [
        b if a == "label" else f"{a}_{b}"
        for a, b in df_alpha.columns
    ]

    # 2. merge
    df_merged = (
        df_alpha
        .reset_index()
        .merge(
            df_fund.reset_index(),
            on=["datetime", "instrument"],  # 👈 注意：你这里其实可以直接双键
            how="left"
        )
        .set_index(["datetime", "instrument"])
    )
    
    # 4. 填充逻辑 (至关重要)
    # 按股票分组，用最近的财报数据向下填充 (ffill)
    # 如果某只票从未有过财报，用 0 填充
    df_merged = df_merged.groupby('instrument').ffill().fillna(0)
    
    return df_merged

# ================== 3. 动态调参 (原生 LGB 格式) ==================
def get_lgb_params(hold_days: int):
    # 针对不同持有期优化参数
    params = {
        "objective": "regression",
        "metric": "mse",
        "verbosity": -1,
        "n_jobs": -1,
        "seed": 42
    }
    
    # 动态调整：持有期越长，正则化越强
    if hold_days <= 2:
        params.update({"learning_rate": 0.008, "num_leaves": 63, "lambda_l1": 0.1, "bagging_fraction": 0.8})
    else:
        params.update({"learning_rate": 0.005, "num_leaves": 31, "lambda_l1": 0.3, "bagging_fraction": 0.7})
        
    return params

# ================== 4. 主策略流程 ==================
def run_strategy(pool_date: str, hold_days: int = 3, topk: int = 10):
    # --- 环境初始化 ---
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
    DATA_PATH = PROJECT_ROOT / "data" / "source"
    POOL_BASE_DIR = PROJECT_ROOT / "data" / "stock_pool" / "processed"
    RESULT_DIR = PROJECT_ROOT / "data" / "final_hybrid_results"
    
    qlib.init(provider_uri=str(DATA_PATH), region=REG_CN)
    
    # --- 加载股票池 ---
    pool_csv = POOL_BASE_DIR / f"{pool_date}_pool.csv"
    if not pool_csv.exists():
        print(f"❌ 错误：找不到股票池 {pool_csv}")
        return
    instruments = pd.read_csv(pool_csv, dtype=str)["instrument"].dropna().unique().tolist()

    # --- 准备数据集 ---
    print(f"🚀 启动策略: {hold_days}日周期 | 混合驱动 (量价+基本面)")
    Handler = get_dynamic_handler(hold_days)
    handler = Handler(
        instruments=instruments,
        start_time="2024-01-01", 
        # 这里的 end_time 必须往后延，防止 QLib 截断当天的特征
        end_time=str(pd.Timestamp(pool_date) + pd.Timedelta(days=5)) 
    )
    dataset = DatasetH(handler=handler, segments={"train": ("2024-01-01", "2025-10-01"), "test": ("2025-10-01", pool_date)})

    # --- 数据融合与训练 ---
    # 1. 准备训练集
    df_train = load_and_merge_data(dataset, "train")
    X_train = df_train.drop(columns=["LABEL0"])
    y_train = df_train["LABEL0"]
    
    # 2. 准备预测集 (测试集)
    df_test = load_and_merge_data(dataset, "test")
    X_test = df_test.drop(columns=["LABEL0"])
    
    # 3. 训练模型
    print(f"🧠 开始训练 LightGBM (特征数量: {X_train.shape[1]})...")
    dtrain = lgb.Dataset(X_train, label=y_train)
    params = get_lgb_params(hold_days)
    model = lgb.train(params, dtrain, num_boost_round=600)
    
    # --- 预测 ---
    print("🔮 生成预测分...")
    pred_scores = model.predict(X_test)
    
    # 整理结果
    df_result = pd.DataFrame({
        "instrument": X_test.index.get_level_values("instrument"),
        "datetime": X_test.index.get_level_values("datetime"),
        "score": pred_scores
    })
    
    # 锁定目标日期
    target_date = pd.Timestamp(pool_date)
    final_rank = df_result[df_result["datetime"] == target_date].set_index("instrument").copy()
    
    if final_rank.empty:
        print(f"❌ {pool_date} 无预测数据，请检查日期是否为交易日。")
        return

    # ================== 5. 风控模块 (Bias Filter) ==================
    print("🛡️ 执行风控检查 (Bias)...")
    
    # 使用 D.features 绕过对齐机制，强制提取当天的乖离率
    # 这里的 Mean($close, 5) 代表 5日线
    bias_df = D.features(instruments, ["$close / Mean($close, 5) - 1"], start_time=pool_date, end_time=pool_date)
    
    if not bias_df.empty:
        bias_df.columns = ["bias_5d"]
        bias_df = bias_df.reset_index().set_index("instrument")[["bias_5d"]]
        
        # 合并风控数据
        final_rank = final_rank.join(bias_df, how="inner")
        
        # 🚨 剔除逻辑
        original_count = len(final_rank)
        final_rank = final_rank[final_rank["bias_5d"] < BIAS_THRESHOLD]
        print(f"   已剔除 {original_count - len(final_rank)} 只过热股票 (Bias > {BIAS_THRESHOLD:.0%})")
    else:
        print("⚠️ 警告：无法获取风控数据，将跳过 Bias 过滤！")

    # ================== 6. 输出结果 ==================
    final_rank = final_rank.sort_values("score", ascending=False).head(topk)
    
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    save_path = RESULT_DIR / f"Hybrid_Pred_{hold_days}d_{pool_date}.csv"
    final_rank.to_csv(save_path)
    
    print("\n" + "="*50)
    print(f"✅ 最终推荐 (已剔除高位风险股)")
    print("="*50)
    # 打印前5名，包含预测分和乖离率
    print(final_rank[["score", "bias_5d"]].head(10))
    print("-" * 50)
    
    # 简单的特征重要性打印，看看财务因子有没有生效
    print("📊 因子贡献度前5名:")
    importance = pd.DataFrame({
        'feature': model.feature_name(),
        'gain': model.feature_importance(importance_type='gain')
    }).sort_values('gain', ascending=False).head(5)
    print(importance)

if __name__ == "__main__":
    # 示例：运行 2日持股策略，预测 1月28日
    run_strategy(
        pool_date="2026-01-28", 
        hold_days=2,
        topk=10
    )