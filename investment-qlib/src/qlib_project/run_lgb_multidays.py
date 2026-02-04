import os
from pathlib import Path
import pandas as pd
import numpy as np
import qlib
import sys
from qlib.config import REG_CN
from qlib.data.dataset import DatasetH
from qlib.contrib.data.handler import Alpha158
from qlib.contrib.model.gbdt import LGBModel
from utils.model_params_adjust import get_model_params

current_dir = Path(__file__).resolve().parent
if str(current_dir) not in sys.path:
    sys.path.append(str(current_dir))
# ================== 1. 动态 Handler (包含避雷指标) ==================
def get_dynamic_handler(hold_days: int):
    target_ref = -(hold_days + 1)
    label_expr = f"Ref($close, {target_ref}) / Ref($close, -1) - 1"
    
    # 增加 bias_5d 指标：(当前收盘价 / 5日均线) - 1
    # 表达式解释：$close / Mean($close, 5) - 1
    class DynamicAlpha158WithBias(Alpha158):
        def get_label_config(self):
            return ([label_expr], ["LABEL0"])
            
        def get_feature_config(self):
            # 获取 Alpha158 原有特征并加入 5日乖离率
            conf = super().get_feature_config()
            conf[0].append("$close / Mean($close, 5) - 1")
            conf[1].append("bias_5d")
            return conf
            
    return DynamicAlpha158WithBias

# ================== 3. 核心融合与避雷函数 ==================
def run_final_strategy(
    pool_date: str,
    hold_list: list = [1, 3, 5],
    weights: dict = {1: 0.2, 3: 0.4, 5: 0.4},
    bias_threshold: float = 0.15, # 偏离 5日线 15% 强制剔除
    topk: int = 10
):
    # --- 环境配置 ---
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
    DATA_PATH = PROJECT_ROOT / "data" / "source"
    POOL_BASE_DIR = PROJECT_ROOT / "data" / "stock_pool" / "processed"
    RESULT_DIR = PROJECT_ROOT / "data" / "final_strategy_results"
    qlib.init(provider_uri=str(DATA_PATH), region=REG_CN)

    # ---------- 加载股票池 ----------
    pool_csv = POOL_BASE_DIR / f"{pool_date}_pool.csv"
    # instruments = pd.read_csv(pool_csv, dtype=str)["instrument"].dropna().unique().tolist()
    # TODO: 默认使用 csi300 指数成分股作为备选池
    instruments="csi300"
    all_scores = []
    bias_data = None

    # ---------- 多时序训练与预测 ----------
    for h in hold_list:
        print(f"\n[时序训练] 正在处理 {h} 日周期...")
        HandlerClass = get_dynamic_handler(h)
        handler = HandlerClass(instruments=instruments, start_time="2024-05-01", end_time=pool_date)
        
        # 提取数据 (包含特征以便获取 bias_5d)
        dataset = DatasetH(
            handler=handler,
            segments={"train": ("2024-05-01", "2025-10-01"), "test": ("2025-10-01", pool_date)},
        )
        
        model = LGBModel(**get_model_params(h))
        model.fit(dataset)
        
        # 预测
        pred = model.predict(dataset, segment="test")
        pred_df = pred.reset_index()
        pred_df.columns = ["datetime", "instrument", f"score_{h}d"]
        
        # 提取目标日期分数
        target_score = pred_df[pred_df["datetime"] == pd.Timestamp(pool_date)].copy()
        all_scores.append(target_score.set_index("instrument")[[f"score_{h}d"]])

        # 顺便从 dataset 获取最后一天的 bias_5d (只需取一次)
        if bias_data is None:
            feature_df = dataset.prepare(segments="test", col_set="feature")
            
            # 1. 检查索引名字，确定 datetime 所在的层级
            if 'datetime' in feature_df.index.names:
                # 获取数据中实际存在的最新日期
                available_dates = feature_df.index.get_level_values('datetime').unique()
                latest_date = available_dates.max()
                print(f"--- 自动识别最新日期: {latest_date.date()} ---")
                
                # 2. 使用 xs 方法提取。xs 会自动处理层级问题
                # level='datetime' 确保无论它是第一层还是第二层都能找准
                last_date_features = feature_df.xs(latest_date, level='datetime')
                
                # 3. 此时得到的 last_date_features 索引就是 instrument
                # 我们直接提取 bias_5d 列
                if 'bias_5d' in last_date_features.columns:
                    bias_data = last_date_features[['bias_5d']].copy()
                else:
                    # 容错：如果找不到列名，尝试根据位置取最后一列（因为你在 handler 里是最后 append 的）
                    bias_data = last_date_features.iloc[:, [-1]]
                    bias_data.columns = ['bias_5d']
            else:
                raise ValueError("feature_df 索引中未找到 'datetime' 层级，请检查数据结构")
    # ---------- 分数融合与风控过滤 ----------
    print("\n[融合风控] 正在计算综合得分并执行避雷过滤...")
    final_df = pd.concat(all_scores + [bias_data], axis=1)
    
    # 排名归一化
    for h in hold_list:
        final_df[f"rank_{h}d"] = final_df[f"score_{h}d"].rank(pct=True)

    # 综合得分
    final_df["ensemble_score"] = sum(final_df[f"rank_{h}d"] * weights[h] for h in hold_list)
    
    # --- 避雷过滤逻辑 ---
    # 记录过滤前的数量
    before_count = len(final_df)
    # 剔除 bias_5d > 15% 的股票
    final_df = final_df[final_df['bias_5d'] < bias_threshold].copy()
    after_count = len(final_df)
    print(f"⚠️ 已从名单中剔除 {before_count - after_count} 只过热股票 (偏离度 > {bias_threshold*100}%)")

    # 最终排序
    final_df = final_df.sort_values("ensemble_score", ascending=False)
    result = final_df.head(topk)

    # ---------- 保存结果 ----------
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    save_path = RESULT_DIR / f"final_recommendation_{pool_date}.csv"
    result.to_csv(save_path)
    print(f"\n✅ 结果已保存至: {save_path}")
    
    print(f"\n✅ 选股完成！Top {topk} 策略建议已生成。")
    print(result[['ensemble_score', 'bias_5d'] + [f'rank_{h}d' for h in hold_list]])

if __name__ == "__main__":
    run_final_strategy(
        pool_date="2026-01-30", 
        hold_list=[1, 3, 5], 
        bias_threshold=0.15, # 15% 阈值
        topk=10
    )