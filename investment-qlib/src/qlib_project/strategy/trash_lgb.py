import os
import gc
from pathlib import Path
import sys
import pandas as pd
import numpy as np
import qlib
from qlib.config import REG_CN
from qlib.data.dataset import DatasetH
from qlib.contrib.data.handler import Alpha158
from qlib.contrib.model.gbdt import LGBModel
from qlib.data import D
import matplotlib.pyplot as plt

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
SRC_ROOT = PROJECT_ROOT.parent
for path in (str(BASE_DIR), str(PROJECT_ROOT), str(SRC_ROOT)):
    if path not in sys.path:
        sys.path.insert(0, path)
from qlib_project.constants import TRASH_POOL, DATA_PATH
from qlib_project.utils.util import load_stock_pool
# ================== 1. 参数与配置 (龙回头/超跌反弹版) ==================
def get_rebound_model_params(hold_days: int):
    """
    专门针对‘极度超跌’后的‘暴力反弹’进行参数调优
    """
    params = {
        "objective": "regression",   # 必须是回归，捕捉极端涨幅
        "metric": "rmse",
        "n_jobs": -1,
        "verbosity": -1,
        "early_stopping_rounds": 50,
        "extra_trees": True,
    }
    
    # 针对反弹逻辑：树深一点以捕捉多维共振（如：偏离度+缩量+筹码）
    configs = {
        1: {"est": 800,  "lr": 0.08, "l1": 0.1, "l2": 0.5, "depth": 6},
        2: {"est": 1000, "lr": 0.06, "l1": 0.2, "l2": 1.0, "depth": 6},
        3: {"est": 1200, "lr": 0.05, "l1": 0.3, "l2": 1.5, "depth": 5},
    }
    c = configs.get(hold_days, configs[2])

    params.update({
        "n_estimators": c["est"],
        "learning_rate": c["lr"],
        "max_depth": c["depth"],
        "lambda_l1": c["l1"],
        "lambda_l2": c["l2"],
        "num_leaves": int((2 ** c["depth"]) * 0.8),
        "min_data_in_leaf": 20, # 允许模型学习较小的反弹样本集
        "feature_fraction": 0.8,
        "bagging_fraction": 0.8,
        "bagging_freq": 1,
    })
    return params

# ================== 2. 动态 Handler (反弹特征注入) ==================
def get_rebound_handler(hold_days: int, refined_fields=None, refined_names=None):
    label_expr = "If(Ref($high, -1) > Ref($high, -2), Ref($high, -1), Ref($high, -2)) / $close - 1"
    
    class ReboundAlpha158(Alpha158):
        def get_label_config(self):
            return ([label_expr], ["LABEL_REBOUND"])
            
        def get_feature_config(self):
            extra_fields = [
                "$close / Mean($close, 5) - 1", 
                "$close / Mean($close, 20) - 1",
                "$amount / Mean($amount, 5)", 
                "($high - $low) / $close"
            ]
            extra_names = ["bias_5d", "bias_20d", "vol_ratio", "amp_1d"]

            if refined_fields and refined_names:
                return refined_fields + extra_fields, refined_names + extra_names
            
            conf = super().get_feature_config()
            conf[0].extend(extra_fields)
            conf[1].extend(extra_names)
            return conf

        def get_learn_processors(self):
            return [{"class": "ConfigSectionProcessor", "kwargs": {"fillna_label": True, "clip_label_outlier": False}}]
            
    return ReboundAlpha158
# ================== 3. 特征精炼工具 ==================
def refine_features(model, dataset, top_k=50):
    importance = model.model.feature_importance(importance_type='gain')
    fields, names = dataset.handler.get_feature_config()
    df = pd.DataFrame({'name': names, 'field': fields, 'imp': importance}).sort_values('imp', ascending=False)
    top_df = df.head(top_k)
    return top_df['field'].tolist(), top_df['name'].tolist()

# ================== 4. 主策略执行函数 ==================
def run_rebound_strategy(
    pool_date: str,
    stock_pool: list,
    data_path: str,
    hold_days: int = 2,
    topk: int = 8
):
    # --- 初始化 ---
    qlib.init(provider_uri=data_path, region=REG_CN)
    start_train = "2018-01-01"
    test_start = "2025-10-01" # 根据你数据实际情况调整
    valid_start = (pd.Timestamp(test_start) - pd.Timedelta(days=90)).strftime('%Y-%m-%d')
    
    # --- 第一阶段：全量特征训练 ---
    print("🚀 阶段 1：全量特征探测...")
    HandlerV1 = get_rebound_handler(hold_days)
    instruments = load_stock_pool(stock_pool)
    hd_v1 = HandlerV1(instruments=instruments, start_time=start_train, end_time=pool_date)
    ds_v1 = DatasetH(handler=hd_v1, segments={
        "train": (start_train, valid_start),
        "valid": (valid_start, test_start),
        "test": (test_start, pool_date)
    })
    model_v1 = LGBModel(**get_rebound_model_params(hold_days))
    model_v1.fit(ds_v1)
    
    new_fields, new_names = refine_features(model_v1, ds_v1, top_k=60)
    del ds_v1, model_v1
    gc.collect()

    # --- 第二阶段：精炼特征拟合 ---
    print("🚀 阶段 2：精炼特征训练...")
    HandlerV2 = get_rebound_handler(hold_days, new_fields, new_names)
    hd_v2 = HandlerV2(instruments=instruments, start_time=start_train, end_time=pool_date)
    ds_v2 = DatasetH(handler=hd_v2, segments={
        "train": (start_train, valid_start),
        "valid": (valid_start, test_start),
        "test": (test_start, pool_date)
    })
    model_v2 = LGBModel(**get_rebound_model_params(hold_days))
    model_v2.fit(ds_v2)

    # --- 预测与超跌过滤 ---
    pred_df = model_v2.predict(ds_v2, segment="test")
    if isinstance(pred_df, pd.Series): pred_df = pred_df.to_frame(name="score")
    
    # 提取 pool_date 当天的预测值
    target_date = pd.Timestamp(pool_date)
    today_pred = pred_df.loc[target_date] if target_date in pred_df.index.get_level_values(0) else None
    
    if today_pred is None:
        print(f"❌ {pool_date} 没有预测数据。")
        return

# ---------- 手动提取截面风控特征 (采用你习惯的稳健写法) ----------
    print(f"🛠️ 正在应用【缩量超跌】形态过滤...")
    risk_fields = [
        "$close / Mean($close, 5) - 1", 
        "$close / Mean($close, 20) - 1", 
        "$amount / Mean($amount, 5)", 
        "($high - $low) / $close"
    ]
    
    # 1. 获取原始数据
    risk_df_raw = D.features(instruments, risk_fields, start_time=pool_date, end_time=pool_date)
    
    if risk_df_raw.empty:
        print(f"❌ 警告: 无法获取 {pool_date} 的风控特征。可能原因是该日非交易日或数据未更新。")
        return

    # 2. 核心修复：效仿你成功的写法，重置索引并只保留 instrument，彻底避开 Timestamp 匹配问题
    risk_df = risk_df_raw.reset_index()
    risk_df.columns = ['datetime', 'instrument', 'bias_5d', 'bias_20d', 'vol_ratio', 'amp_1d']
    risk_df = risk_df.set_index('instrument')[['bias_5d', 'bias_20d', 'vol_ratio', 'amp_1d']]

    # ---------- 选股合成 ----------
    # 同样，确保 today_pred 也是以 instrument 为索引
    today_pred = pred_df[pred_df.index.get_level_values('datetime') == pd.Timestamp(pool_date)].copy()
    today_pred = today_pred.reset_index().set_index('instrument')
    
    # 3. 强力合并
    final_table = today_pred[["score"]].join(risk_df, how='inner')
    print(f"过滤前总数: {len(final_table)}")
    print(f"Bias_5d 均值: {final_table['bias_5d'].mean():.4f}")
    print(f"Vol_Ratio 均值: {final_table['vol_ratio'].mean():.4f}")
    mask = (
        # 降低门槛：只要在5日线下就行，或者稍微跌破一点
        (final_table['bias_5d'] < 0) &     
        
        # 放宽缩量要求：只要不是那种极其恐怖的巨量（比如 > 2.0）就行
        (final_table['vol_ratio'] < 1.2) &     
        
        # 中期趋势稍微放宽
        (final_table['bias_20d'] > -0.10) &    
        
        # 振幅要求降低
        (final_table['amp_1d'] > 0.02)         
    )
    
    candidates = final_table[mask].sort_values("score", ascending=False)
    result = candidates.head(topk)

    print(f"\n🎯 {pool_date} 阻击名单 (Top {topk}):")
    print(result[['score', 'bias_5d', 'vol_ratio']])
    
    # 保存结果
    RESULT_DIR = PROJECT_ROOT / "data" / "short_term_predictions"
    RESULT_DIR.mkdir(parents=True, exist_ok=True)

    result.to_csv(RESULT_DIR / f"rebound_picks_{pool_date}.csv")
    return result

if __name__ == "__main__":
    run_rebound_strategy(
        pool_date="2026-04-22", 
        stock_pool=TRASH_POOL, 
        data_path=DATA_PATH,
        hold_days=2,
        topk=6
    )