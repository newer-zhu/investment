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

# 路径配置
BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
SRC_ROOT = PROJECT_ROOT.parent
for path in (str(BASE_DIR), str(PROJECT_ROOT), str(SRC_ROOT)):
    if path not in sys.path:
        sys.path.insert(0, path)

from qlib_project.constants import TRASH_POOL, DATA_PATH
from qlib_project.utils.util import load_stock_pool

# ================== 1. 模型参数配置 ==================
def get_rebound_model_params(hold_days: int):
    """
    针对超跌反弹的轻量化拟合参数
    """
    return {
        "objective": "regression",
        "metric": "rmse",
        "n_jobs": -1,
        "verbosity": -1,
        "early_stopping_rounds": 50,
        "extra_trees": True,
        "n_estimators": 1000,
        "learning_rate": 0.05,
        "max_depth": 6,
        "lambda_l1": 0.1,
        "lambda_l2": 0.2,
        "num_leaves": 40,
    }

# ================== 2. 反弹专用数据处理器 ==================
def get_rebound_handler(hold_days: int, refined_fields=None, refined_names=None):
    # 目标：未来2日内最高价相对当前收盘的收益（博取盘中冲高）
    label_expr = "If(Ref($high, -1) > Ref($high, -2), Ref($high, -1), Ref($high, -2)) / $close - 1"
    
    class ReboundAlpha158(Alpha158):
        def get_label_config(self):
            return ([label_expr], ["LABEL_REBOUND"])
            
        def get_feature_config(self):
            # 显式注入：乖离率、量比、振幅
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

# ================== 3. 特征重要性提取 ==================
def refine_features(model, dataset, top_k=60):
    importance = model.model.feature_importance(importance_type='gain')
    fields, names = dataset.handler.get_feature_config()
    df = pd.DataFrame({'name': names, 'field': fields, 'imp': importance}).sort_values('imp', ascending=False)
    return df.head(top_k)['field'].tolist(), df.head(top_k)['name'].tolist()

# ================== 4. 主策略函数 ==================
def run_rebound_strategy(
    pool_date: str,
    stock_pool: list,
    data_path: str,
    hold_days: int = 2,
    topk: int = 8
):
    # --- A. 初始化与数据准备 ---
    qlib.init(provider_uri=data_path, region=REG_CN)
    start_train = "2018-01-01"
    test_start = "2025-10-01"
    valid_start = (pd.Timestamp(test_start) - pd.Timedelta(days=90)).strftime('%Y-%m-%d')
    instruments = load_stock_pool(stock_pool)
    
    # --- B. 阶段 1：全特征初步训练 ---
    print("🚀 阶段 1：扫描全量特征（寻找反弹信号共振点）...")
    ds_v1 = DatasetH(
        handler=get_rebound_handler(hold_days)(instruments=instruments, start_time=start_train, end_time=pool_date),
        segments={"train": (start_train, valid_start), "valid": (valid_start, test_start), "test": (test_start, pool_date)}
    )
    model_v1 = LGBModel(**get_rebound_model_params(hold_days))
    model_v1.fit(ds_v1)
    
    refined_fields, refined_names = refine_features(model_v1, ds_v1, top_k=60)
    del ds_v1, model_v1
    gc.collect()

    # --- C. 阶段 2：精炼特征二次拟合 ---
    print("🚀 阶段 2：精炼特征训练（锁定高胜率组合）...")
    ds_v2 = DatasetH(
        handler=get_rebound_handler(hold_days, refined_fields, refined_names)(instruments=instruments, start_time=start_train, end_time=pool_date),
        segments={"train": (start_train, valid_start), "valid": (valid_start, test_start), "test": (test_start, pool_date)}
    )
    model_v2 = LGBModel(**get_rebound_model_params(hold_days))
    model_v2.fit(ds_v2)

    # --- D. 预测结果与形态过滤 ---
    pred_df = model_v2.predict(ds_v2, segment="test")
    if isinstance(pred_df, pd.Series): pred_df = pred_df.to_frame(name="score")

    # 1. 提取预测集（确保日期对齐）
    pred_reset = pred_df.reset_index()
    if len(pred_reset.columns) == 3:
        pred_reset.columns = ['datetime', 'instrument', 'score']
    pred_reset['instrument'] = pred_reset['instrument'].astype(str).str.upper().str.strip()
    today_pred = pred_reset[pred_reset['datetime'] == pd.Timestamp(pool_date)].copy()

    if today_pred.empty:
        print(f"❌ 警告: {pool_date} 当天无预测数据。")
        return

    # 2. 提取行情截面特征进行形态验证
    print(f"🛠️ 正在应用【超跌 + 衰竭】形态过滤...")
    risk_fields = [
        "$close / Mean($close, 5) - 1", 
        "$close / Mean($close, 20) - 1", 
        "$amount / Mean($amount, 5)", 
        "($high - $low) / $close"
    ]
    risk_df_raw = D.features(instruments, risk_fields, start_time=pool_date, end_time=pool_date)
    
    if risk_df_raw.empty:
        print(f"❌ 警告: 无法获取 {pool_date} 的行情风控数据。")
        return

    # 3. 稳健合并
    risk_reset = risk_df_raw.reset_index()
    risk_reset['instrument'] = risk_reset['instrument'].astype(str).str.upper().str.strip()
    
    # 动态识别计算出的特征列
    feat_cols = [c for c in risk_reset.columns if c not in ['datetime', 'instrument']]
    
    final_table = today_pred.set_index('instrument')[['score']].join(
        risk_reset.set_index('instrument')[feat_cols], how='inner'
    )
    
    # 重命名方便过滤逻辑阅读
    final_table.columns = ['score', 'bias_5d', 'bias_20d', 'vol_ratio', 'amp_1d']

    # --- E. 核心风控逻辑：寻找跌到位后的缩量点 ---
    mask = (
        (final_table['bias_5d'] < 0) &         # 状态：在均线下方
        (final_table['vol_ratio'] < 1.1) &     # 状态：缩量，说明抛压衰竭
        (final_table['bias_20d'] > -0.15) &    # 状态：中期趋势未崩坏（防A字杀）
        (final_table['amp_1d'] > 0.015)        # 状态：保持基本活跃度
    )
    
    candidates = final_table[mask].sort_values("score", ascending=False)
    result = candidates.head(topk)

    print(f"\n✅ {pool_date} 阻击名单 (Join 数: {len(final_table)}, 选出: {len(result)}):")
    if not result.empty:
        print(result[['score', 'bias_5d', 'vol_ratio']])
        # 保存结果
        RESULT_DIR = PROJECT_ROOT / "data" / "rebound_predictions"
        RESULT_DIR.mkdir(parents=True, exist_ok=True)
        result.to_csv(RESULT_DIR / f"rebound_picks_{pool_date}.csv")
    else:
        print("⚠️ 今日无匹配形态的个股，请耐心等待市场回调至缩量点。")

    return result

if __name__ == "__main__":
    run_rebound_strategy(
        pool_date="2026-04-22", 
        stock_pool=TRASH_POOL, 
        data_path=DATA_PATH,
        hold_days=2,
        topk=6
    )