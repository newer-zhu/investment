import os
from pathlib import Path
import pandas as pd
import numpy as np
import qlib
from qlib.config import REG_CN
from qlib.data.dataset import DatasetH
from qlib.contrib.data.handler import Alpha158
from qlib.contrib.model.gbdt import LGBModel
from utils.model_params_adjust import get_model_params

# ================== 1. 动态 Handler (集成风控特征) ==================
def get_dynamic_handler(hold_days: int):
    """
    hold_days: 2 (对应 Ref(-3)/Ref(-1)-1)
    """
    target_ref = -(hold_days + 1)
    label_expr = f"Ref($close, {target_ref}) / Ref($close, -1) - 1"
    
    class DynamicAlpha158WithRisk(Alpha158):
        def get_label_config(self):
            return ([label_expr], ["LABEL0"])
            
        def get_feature_config(self):
            # 继承 Alpha158 的 158 个特征
            conf = super().get_feature_config()
            # [风控指标] 计算当前价格偏离 5 日均线的程度
            conf[0].append("$close / Mean($close, 5) - 1")
            conf[1].append("bias_5d")
            return conf
            
    return DynamicAlpha158WithRisk

# ================== 3. 主预测逻辑 ==================
def run_short_term_strategy(
    pool_date: str,      # 今天日期，如 '2026-01-28'
    hold_days: int = 2,  # 预测周四买周五卖
    bias_limit: float = 0.12, # 风控：偏离 5 日线超过 12% 的不买
    topk: int = 10
):
    # --- 环境初始化 ---
    os.environ["PYTHONIOENCODING"] = "utf-8"
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
    DATA_PATH = PROJECT_ROOT / "data" / "source"
    POOL_BASE_DIR = PROJECT_ROOT / "data" / "stock_pool" / "processed"
    RESULT_DIR = PROJECT_ROOT / "data" / "short_term_predictions"
    
    qlib.init(provider_uri=str(DATA_PATH), region=REG_CN)
    target_dt = pd.Timestamp(pool_date)

    # ---------- 加载股票池 ----------
    pool_csv = POOL_BASE_DIR / f"{pool_date}_pool.csv"
    if not pool_csv.exists():
        raise FileNotFoundError(f"找不到股票池文件: {pool_csv}")
    
    df_pool = pd.read_csv(pool_csv, dtype=str)
    col = "instrument" if "instrument" in df_pool.columns else "Instrument"
    instruments = df_pool[col].dropna().unique().tolist()

    # ---------- 训练模型 ----------
    print(f"🚀 任务: {hold_days}日短线策略 | 目标买入日: {pool_date} 后一个交易日")
    
    HandlerClass = get_dynamic_handler(hold_days)
    handler = HandlerClass(
        instruments=instruments,
        start_time="2024-01-01",
        end_time=pool_date,
    )

    # 准备数据集：训练截止到去年10月，测试从去年10月至今
    dataset = DatasetH(
        handler=handler,
        segments={"train": ("2024-01-01", "2025-10-01"), "test": ("2025-10-01", pool_date)},
    )

    model = LGBModel(**get_model_params(hold_days))
    print("正在学习历史规律并构建风控特征...")
    model.fit(dataset)

    # ---------- 预测与风控过滤 ----------
    print("生成预测分数...")
    # 预测分数通常可以正常拿到，因为 Predict 不强制要求有 Label
    pred = model.predict(dataset, segment="test")
    
    # --- [核心修改] 使用底层 D.features 绕过对齐机制提取风控指标 ---
    from qlib.data import D
    print(f"正在手动提取 {pool_date} 的风控特征 (Bias)...")
    
    # 直接查原始特征库
    bias_df = D.features(
        instruments, 
        ["$close / Mean($close, 5) - 1"], 
        start_time=pool_date, 
        end_time=pool_date
    )
    
    if bias_df.empty:
        print(f"❌ 警告: 无法获取 {pool_date} 的风控特征，请检查该日是否为交易日。")
        return

    # 规范化 bias_df 格式
    bias_df.columns = ['bias_5d']
    bias_df = bias_df.reset_index().set_index('instrument')[['bias_5d']]

    # 整理预测分
    pred_df = pred.reset_index()
    pred_df.columns = ["datetime", "instrument", "score"]
    # 锁定当天的预测结果
    today_pred = pred_df[pred_df["datetime"] == pd.Timestamp(pool_date)].set_index("instrument")
    
    # 合并预测分与风控指标
    final_table = today_pred.join(bias_df, how='inner')

    # ---------- 执行风控剔除 ----------
    before_count = len(final_table)
    # 剔除涨幅过热的票 (Bias > 12%)
    final_table = final_table[final_table['bias_5d'] < bias_limit].copy()
    print(f"⚠️ 风控系统：已从 {before_count} 只候选股中剔除 {before_count - len(final_table)} 只过热个股。")

    # 重新排序并取 TopK
    final_table.sort_values("score", ascending=False, inplace=True)
    result = final_table.head(topk).reset_index()

    # ---------- 保存与展示 ----------
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    save_path = RESULT_DIR / f"T2_pred_with_risk_{pool_date}.csv"
    result.to_csv(save_path, index=False)
    
    print(f"\n✅ 选股完成！Top {topk} 名单已生成。")
    print("-" * 50)
    # 打印前 5 名进行快速预览
    print(result[['instrument', 'score', 'bias_5d']].head(10))

if __name__ == "__main__":
    run_short_term_strategy(
        pool_date="2026-01-30", 
        hold_days=2, 
        bias_limit=0.15, # 如果你觉得牛市很疯狂，可以调高到 0.15
        topk=10
    )