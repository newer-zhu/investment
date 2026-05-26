import os
import gc
from pathlib import Path
import pandas as pd
import numpy as np
import qlib
from qlib.config import REG_CN
from qlib.data.dataset import DatasetH
from qlib.contrib.data.handler import Alpha158
from qlib.contrib.model.gbdt import LGBModel
from qlib.data import D
import matplotlib.pyplot as plt

# 假设这些是你项目里的自定义模块
from utils.model_params_adjust import get_model_params
from constants import FILTERED_POOL, DATA_PATH
from utils.util import load_stock_pool

# ================== 1. 特征提取与精炼工具 ==================
def get_refined_feature_config(model, dataset, top_k=50):
    importance = model.model.feature_importance(importance_type='gain')
    handler = dataset.handler
    orig_fields, orig_names = handler.get_feature_config()
    
    df_importance = pd.DataFrame({
        'name': orig_names,
        'field': orig_fields,
        'importance': importance
    }).sort_values(by='importance', ascending=False)
    
    plt.figure(figsize=(10, 8))
    df_importance.head(20).set_index('name')['importance'].plot(kind='barh')
    plt.title(f"Top 20 Features (Gain)")
    plt.savefig("feature_importance.png")
    plt.close()
    
    top_df = df_importance.head(top_k)
    return top_df['field'].tolist(), top_df['name'].tolist()

# ================== 2. 动态 Handler (支持特征注入) ==================
def get_refined_handler(hold_days: int, refined_fields=None, refined_names=None):
    target_ref = -(hold_days + 1)
    # 标签逻辑：hold_days=2 T+1开盘买入，T+3收盘卖出
    label_expr = f"Ref($close, {target_ref}) / Ref($open, -1) - 1"
    
    class DynamicRefinedAlpha158(Alpha158):
        def get_label_config(self):
            return ([label_expr], ["LABEL0"])
            
        def get_feature_config(self):
            if refined_fields and refined_names:
                # 别忘了加上你自定义的 bias_5d 风控特征
                fields = refined_fields + ["$close / Mean($close, 5) - 1"]
                names = refined_names + ["bias_5d"]
                return fields, names
            
            conf = super().get_feature_config()
            conf[0].append("$close / Mean($close, 5) - 1")
            conf[1].append("bias_5d")
            return conf

        def get_learn_processors(self):
            return [{"class": "ConfigSectionProcessor", "kwargs": {"fillna_label": True, "clip_label_outlier": True}}]
            
    return DynamicRefinedAlpha158

# ================== 3. 主预测逻辑 ==================
def run_short_term_strategy(
    pool_date: str,      
    hold_days: int = 2,  
    bias_limit: float = 0.12, 
    topk: int = 10,
    start_date: str = "2018-01-01", 
    test_start: str = "2025-10-01"
):
    os.environ["PYTHONIOENCODING"] = "utf-8"
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
    RESULT_DIR = PROJECT_ROOT / "data" / "short_term_predictions"
    
    qlib.init(provider_uri=str(DATA_PATH), region=REG_CN)
    target_dt = pd.Timestamp(pool_date)
    instruments = load_stock_pool(FILTERED_POOL)

    valid_start = pd.Timestamp(test_start) - pd.Timedelta(days=90) 
    valid_start_str = valid_start.strftime('%Y-%m-%d')
    
    # 第一阶段：全量特征探测
    HandlerClass_v1 = get_refined_handler(hold_days=hold_days)
    handler_v1 = HandlerClass_v1(instruments=instruments, start_time=start_date, end_time=pool_date)
    dataset_v1 = DatasetH(
        handler=handler_v1,
        segments={"train": (start_date, valid_start_str), "valid": (valid_start_str, test_start), "test": (test_start, pool_date)},
    )
    model_v1 = LGBModel(**get_model_params(hold_days))
    model_v1.fit(dataset_v1)

    new_fields, new_names = get_refined_feature_config(model_v1, dataset_v1, top_k=50)
    print(f"✅ 已精选出 {len(new_fields)} 个黄金特征...")

    del dataset_v1, handler_v1
    gc.collect() 
    
    # 第二阶段：精炼特征训练
    HandlerClass_v2 = get_refined_handler(hold_days=hold_days, refined_fields=new_fields, refined_names=new_names)
    handler_v2 = HandlerClass_v2(instruments=instruments, start_time=start_date, end_time=pool_date)
    dataset_v2 = DatasetH(
        handler=handler_v2,
        segments={"train": (start_date, valid_start_str), "valid": (valid_start_str, test_start), "test": (test_start, pool_date)},
    )
    model_v2 = LGBModel(**get_model_params(hold_days))
    model_v2.fit(dataset_v2)

    # ---------- 预测与 IC 计算 ----------
    print("生成预测分数...")
    pred_df = model_v2.predict(dataset_v2, segment="test")
    
    # [核心修复1] 强制将可能出现的 Series 转回 DataFrame，否则 reset_index 后无法指定 columns
    if isinstance(pred_df, pd.Series):
        pred_df = pred_df.to_frame(name="score")

    pred_df = pred_df.reset_index()
    # 确保列名一致性
    if pred_df.shape[1] == 3:
        pred_df.columns = ["datetime", "instrument", "score"]
    else:
        # 如果 predict 意外带了 label 列，这里做兼容
        pred_df.columns = ["datetime", "instrument", "score"] + [f"col_{i}" for i in range(pred_df.shape[1]-3)]

    # [新增] 保存全量测试集预测结果供回测使用 
    pred_path = RESULT_DIR / "full_test_predictions.pkl"
    pred_df.to_pickle(pred_path)
    print(f"✅ 全量预测数据已保存至: {pred_path} (用于回测)")
    
    # ---------- 提取 test 段真实 LABEL (计算 IC) ----------
    print("提取 test 段真实 LABEL...")
    try:
        label_df = dataset_v2.prepare(segments="test", col_set="label")
        label_df = label_df.reset_index()
        # 兼容处理列名，确保第二列之后是标签
        label_col_name = label_df.columns[-1]
        label_df.rename(columns={label_col_name: "LABEL0"}, inplace=True)

        # 合并 score + label
        # 即使是 2-08 执行，merge(inner) 会自动保留 test 段中有标签的历史日期，计算出 IC
        pred_with_label = pd.merge(
            pred_df[["datetime", "instrument", "score"]],
            label_df[["datetime", "instrument", "LABEL0"]],
            on=["datetime", "instrument"],
            how="inner"
        ).set_index(["datetime", "instrument"])

        if not pred_with_label.empty:
            print(f"✅ 成功合并预测与 LABEL，共 {len(pred_with_label)} 条样本")
            from ic_eval import calc_ic_rank_ic
            summary, ic_ts, rank_ic_ts = calc_ic_rank_ic(pred_with_label)
            print("===== IC / Rank IC 统计 =====")
            for k, v in summary.items():
                print(f"{k}: {v:.4f}" if isinstance(v, float) else f"{k}: {v}")
        else:
            print("ℹ️ 当前 test 段中尚无已实现的 Label（正常现象：全部为未来日期）")

    except Exception as e:
        print(f"⚠️ IC / Rank IC 计算跳过 (原因: {repr(e)})")

    # ---------- 风控指标提取 (D.features) ----------
    print(f"正在手动提取 {pool_date} 的风控特征 (Bias)...")
    bias_df = D.features(
        instruments, 
        ["$close / Mean($close, 5) - 1"], 
        start_time=pool_date, 
        end_time=pool_date
    )
    
    if bias_df.empty:
        print(f"❌ 警告: 无法获取 {pool_date} 的风控特征。")
        return

    bias_df.columns = ['bias_5d']
    bias_df = bias_df.reset_index().set_index('instrument')[['bias_5d']]

    # ---------- 选股合成 ----------
    # 锁定当天的预测结果
    today_pred = pred_df[pred_df["datetime"] == pd.Timestamp(pool_date)].copy()
    today_pred = today_pred.set_index("instrument")
    
    # [核心修复2] 使用 join 前确保是 DataFrame
    final_table = today_pred[["score"]].join(bias_df, how='inner')

    # 执行风控剔除
    before_count = len(final_table)
    final_table = final_table[final_table['bias_5d'] < bias_limit].copy()
    print(f"⚠️ 风控系统：已从 {before_count} 只个股中剔除 {before_count - len(final_table)} 只过热股。")

    final_table.sort_values("score", ascending=False, inplace=True)
    result = final_table.head(topk).reset_index()

    # 保存展示
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    save_path = RESULT_DIR / f"T2_pred_with_risk_{pool_date}.csv"
    result.to_csv(save_path, index=False)
    print(f"结果已保存至: {save_path}")
    print(result[['instrument', 'score', 'bias_5d']].head(topk))

if __name__ == "__main__":
    run_short_term_strategy(
        pool_date="2026-05-25", 
        hold_days=3, 
        bias_limit=0.10, 
        topk=5
    )