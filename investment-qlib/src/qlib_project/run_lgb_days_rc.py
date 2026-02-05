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
from constants import FILTERED_POOL, DATA_PATH
from utils.util import load_stock_pool
import matplotlib.pyplot as plt
import gc
# ================== 1. 动态 Handler (集成风控特征) ==================
def get_dynamic_handler(hold_days: int):
    """
    hold_days: 2 (对应 Ref(-3)/Ref(-1)-1)
    """
    target_ref = -(hold_days + 1)
    # T+1开盘 buy，T+hold_days 收盘卖出
    label_expr = f"Ref($close, {target_ref}) / Ref($open, -1) - 1"
    
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

# ================== 1. 特征提取与精炼工具 ==================
def get_refined_feature_config(model, dataset, top_k=50):
    """
    自动提取特征重要性并返回对应的 (表达式, 名称)
    """
    # 获取特征重要性
    importance = model.model.feature_importance(importance_type='gain')
    
    # 获取原始特征名称和对应的表达式
    # Alpha158 的特征存储在 handler 的数据加载器中
    handler = dataset.handler
    orig_fields, orig_names = handler.get_feature_config()
    
    # 构造重要性表格
    df_importance = pd.DataFrame({
        'name': orig_names,
        'field': orig_fields,
        'importance': importance
    }).sort_values(by='importance', ascending=False)
    
    # 自动可视化 (不阻塞版本)
    plt.figure(figsize=(10, 8))
    df_importance.head(20).set_index('name')['importance'].plot(kind='barh')
    plt.title(f"Top 20 Features (Gain)")
    plt.savefig("feature_importance.png") # 保存图片，不卡主程序
    plt.close() # 释放内存
    
    top_df = df_importance.head(top_k)
    return top_df['field'].tolist(), top_df['name'].tolist()

# ================== 2. 动态 Handler (支持特征注入) ==================
def get_refined_handler(hold_days: int, refined_fields=None, refined_names=None):
    target_ref = -(hold_days + 1)
    label_expr = f"Ref($close, {target_ref}) / Ref($close, -1) - 1"
    
    class DynamicRefinedAlpha158(Alpha158):
        def get_label_config(self):
            return ([label_expr], ["LABEL0"])
            
        def get_feature_config(self):
            # 如果提供了精炼列表，则只使用精炼后的
            if refined_fields and refined_names:
                # 别忘了加上你自定义的 bias_5d 风控特征
                fields = refined_fields + ["$close / Mean($close, 5) - 1"]
                names = refined_names + ["bias_5d"]
                return fields, names
            
            # 否则返回原始 Alpha158 特征 + bias_5d
            conf = super().get_feature_config()
            conf[0].append("$close / Mean($close, 5) - 1")
            conf[1].append("bias_5d")
            return conf

        # 依然保留你之前的标准化逻辑
        def get_learn_processors(self):
            return [{"class": "ConfigSectionProcessor", "kwargs": {"fillna_label": True, "clip_label_outlier": True}}]
            
    return DynamicRefinedAlpha158

# ================== 3. 主预测逻辑 ==================
def run_short_term_strategy(
    pool_date: str,      # 今天日期，如 '2026-01-28'
    hold_days: int = 2,  # 预测周四买周五卖
    bias_limit: float = 0.12, # 风控：偏离 5 日线超过 12% 的不买
    topk: int = 10,
    start_date: str = "2005-01-01", # 建议训练集拉长，增加泛化性
    test_start: str = "2025-10-01"
):
    # --- 环境初始化 ---
    os.environ["PYTHONIOENCODING"] = "utf-8"
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
    RESULT_DIR = PROJECT_ROOT / "data" / "short_term_predictions"
    
    qlib.init(provider_uri=str(DATA_PATH), region=REG_CN)
    target_dt = pd.Timestamp(pool_date)

    # ---------- 加载股票池 ----------
    instruments = load_stock_pool(FILTERED_POOL)

    # ---------- 训练模型 ----------
    print(f"🚀 任务: {hold_days}日短线策略 | 目标买入日: {pool_date} 后一个交易日")
    
    # HandlerClass = get_dynamic_handler(hold_days)
    # handler = HandlerClass(
    #     instruments=instruments,
    #     start_time=start_date,
    #     end_time=pool_date,
    # )

    valid_start = pd.Timestamp(test_start) - pd.Timedelta(days=90) 
    valid_start_str = valid_start.strftime('%Y-%m-%d')
    
    # 第一阶段：全量特征探测
    HandlerClass_v1 = get_refined_handler(hold_days=2)
    handler_v1 = HandlerClass_v1(
        instruments=instruments, 
        start_time=start_date, 
        end_time=pool_date
    )

    dataset_v1 = DatasetH(
        handler=handler_v1,
        segments={
            "train": (start_date, valid_start_str),      # 真正的训练集
            "valid": (valid_start_str, test_start),       # 验证集：用于早停监控
            "test": (test_start, pool_date)               # 测试集
        },
    )

    model_v1 = LGBModel(**get_model_params(hold_days))
    model_v1.fit(dataset_v1)

    # 自动精炼
    new_fields, new_names = get_refined_feature_config(model_v1, dataset_v1, top_k=50)
    print(f"✅ 已精选出 {len(new_fields)} 个黄金特征，准备进入第二阶段训练...")

    del dataset_v1
    del handler_v1
    gc.collect()  # 强制进行垃圾回收
    print("🧹 第一阶段原始数据已清理，内存已释放。")
    
    HandlerClass_v2 = get_refined_handler(hold_days=2, refined_fields=new_fields, refined_names=new_names)
    handler_v2 = HandlerClass_v2(
            instruments=instruments,
            start_time=start_date,
            end_time=pool_date
        )
    
    dataset_v2 = DatasetH(
        handler=handler_v2,
        segments={
            "train": (start_date, valid_start_str),      # 真正的训练集
            "valid": (valid_start_str, test_start),       # 验证集：用于早停监控
            "test": (test_start, pool_date)               # 测试集
        },
    )

    model_v2 = LGBModel(**get_model_params(hold_days))
    model_v2.fit(dataset_v2)
    # ---------- 预测与风控过滤 ----------
    print("生成预测分数...")
    pred_df  = model_v2.predict(dataset_v2, segment="test")
    pred_df  = pred_df.reset_index()
    pred_df.columns = ["datetime", "instrument", "score"]

    # ---------- 提取 test 段真实 LABEL ----------
    print("提取 test 段真实 LABEL...")

    try:
        label_df = dataset_v2.prepare(
            segments="test",
            col_set="label"
        )
        label_df = label_df.reset_index()
        label_df.columns = ["datetime", "instrument", "LABEL0"]

        # ---------- 合并 score + label ----------
        pred_with_label = pd.merge(
            pred_df,
            label_df,
            on=["datetime", "instrument"],
            how="inner"
        ).set_index(["datetime", "instrument"])

        print(f"✅ 成功合并预测与 LABEL，共 {len(pred_with_label)} 条样本")

        # ---------- 计算 IC / Rank IC ----------
        from ic_eval import calc_ic_rank_ic
        summary, ic_ts, rank_ic_ts = calc_ic_rank_ic(pred_with_label)

        print("===== IC / Rank IC 统计 =====")
        for k, v in summary.items():
            print(f"{k}: {v:.4f}" if isinstance(v, float) else f"{k}: {v}")

        print("\n最近 10 个交易日 Rank IC：")
        print(rank_ic_ts.tail(10))

    except Exception as e:
        print("⚠️ IC / Rank IC 计算失败，不影响后续选股流程")
        print(f"原因: {repr(e)}")
    
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

    # 如果是 4 列（score + label）
    if pred_df.shape[1] == 4:
        pred_df.columns = ["datetime", "instrument", "score", "label"]

    # 如果是 3 列（只有 score）
    elif pred_df.shape[1] == 3:
        pred_df.columns = ["datetime", "instrument", "score"]

    else:
        raise ValueError(f"Unexpected pred_df shape: {pred_df.shape}")
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
    print(f"结果已保存至: {save_path}")
    
    print(f"\n✅ 选股完成！Top {topk} 名单已生成。")
    print("-" * 50)
    # 打印前 5 名进行快速预览
    print(result[['instrument', 'score', 'bias_5d']].head(10))

if __name__ == "__main__":
    run_short_term_strategy(
        pool_date="2026-02-05", 
        hold_days=2, 
        bias_limit=0.10, # 如果你觉得牛市很疯狂，可以调高到 0.15
        topk=10
    )