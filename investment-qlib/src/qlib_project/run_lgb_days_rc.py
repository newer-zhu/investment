import os
from pathlib import Path
import pandas as pd
import qlib
from qlib.config import REG_CN
from qlib.data.dataset import DatasetH
from qlib.contrib.data.handler import Alpha158
from qlib.contrib.model.gbdt import LGBModel
from qlib.data import D

# 保持原有关联模块不变
from utils.model_params_adjust import get_model_params
from constants import FILTERED_POOL, DATA_PATH
from utils.util import load_stock_pool

def run_short_term_strategy(
    pool_date: str,      
    hold_days: int = 3,  
    bias_limit: float = 0.10, 
    topk: int = 5,
    start_date: str = "2018-01-01", 
    test_start: str = "2025-10-01"
):
    os.environ["PYTHONIOENCODING"] = "utf-8"
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
    RESULT_DIR = PROJECT_ROOT / "data" / "short_term_predictions"
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    
    # 初始化 Qlib 与加载股票池
    qlib.init(provider_uri=str(DATA_PATH), region=REG_CN)
    instruments = load_stock_pool(FILTERED_POOL)

    # 验证集动态切分 (提前90天)
    valid_start = pd.Timestamp(test_start) - pd.Timedelta(days=90) 
    valid_start_str = valid_start.strftime('%Y-%m-%d')
    
    # 转换为截面相对收益率
    label_expr = f"Ref($close, -{hold_days}) / Ref($open, -1) - 1"
    
    # 动态构建 Handler (集成 Alpha158 + 自定义风控特征)
    class ShortTermAlpha158(Alpha158):
        def get_label_config(self):
            return ([label_expr], ["LABEL0"])
            
        def get_feature_config(self):
            conf = super().get_feature_config()
            conf[0].append("$close / Mean($close, 5) - 1")
            conf[1].append("bias_5d")
            return conf
        
        def get_learn_processors(self):
            return [
                {"class": "DropnaLabel"},
                # 对 label 这一列（fields_group="label"）进行截面 Z-Score 标准化
                # 这样既去除了大盘波动的绝对影响（减去均值），又缩放了方差，彻底解决 IC 为负的问题
                {"class": "CSZScoreProcessor", "kwargs": {"fields_group": "label"}}
            ]


    print("🚀 正在构建数据集并提取特征...")
    handler = ShortTermAlpha158(instruments=instruments, start_time=start_date, end_time=pool_date)
    dataset = DatasetH(
        handler=handler,
        segments={
            "train": (start_date, valid_start_str), 
            "valid": (valid_start_str, test_start), 
            "test": (test_start, pool_date)
        },
    )
    
    print("🚂 正在训练 LightGBM 模型...")
    model = LGBModel(**get_model_params(hold_days))
    model.fit(dataset)

    # ---------- 预测结果格式化 ----------
    print("📊 生成测试集预测分数...")
    pred_df = model.predict(dataset, segment="test")
    
    if isinstance(pred_df, pd.Series):
        pred_df = pred_df.to_frame(name="score")
    pred_df = pred_df.reset_index()
    
    if pred_df.shape[1] == 3:
        pred_df.columns = ["datetime", "instrument", "score"]
    else:
        pred_df.columns = ["datetime", "instrument", "score"] + [f"col_{i}" for i in range(pred_df.shape[1]-3)]

    # 保存全量预测结果供后续标准回测使用
    pred_path = RESULT_DIR / "full_test_predictions.pkl"
    pred_df.to_pickle(pred_path)
    print(f"✅ 全量预测数据已保存至: {pred_path}")
    
    # ---------- 提取 test 段真实 LABEL (计算 IC) ----------
    try:
        label_df = dataset.prepare(segments="test", col_set="label").reset_index()
        label_df.rename(columns={label_df.columns[-1]: "LABEL0"}, inplace=True)

        pred_with_label = pd.merge(
            pred_df[["datetime", "instrument", "score"]],
            label_df[["datetime", "instrument", "LABEL0"]],
            on=["datetime", "instrument"],
            how="inner"
        ).set_index(["datetime", "instrument"])

        if not pred_with_label.empty:
            from ic_eval import calc_ic_rank_ic
            summary, _, _ = calc_ic_rank_ic(pred_with_label)
            print("\n===== IC / Rank IC 统计 =====")
            for k, v in summary.items():
                print(f"{k}: {v:.4f}" if isinstance(v, float) else f"{k}: {v}")
        else:
            print("\nℹ️ 当前 test 段最近几日尚无已实现的 Label（属于未到期未来的正常现象）")
    except Exception as e:
        print(f"\n⚠️ IC 计算跳过 (原因: {repr(e)})")

    # ---------- 目标交易日选股与风控过滤 ----------
    print(f"\n🔍 正在执行 {pool_date} 当日选股与风控锁定...")
    
    # 提取 pool_date 当天的预测分数
    today_pred = pred_df[pred_df["datetime"] == pd.Timestamp(pool_date)].copy()
    if today_pred.empty:
        print(f"❌ 错误: 未能在测试集中找到 {pool_date} 的预测分数，请检查数据完整性。")
        return
    today_pred.set_index("instrument", inplace=True)

    # 手动提取当日收盘后的 Bias 风控特征
    bias_df = D.features(
        instruments, 
        ["$close / Mean($close, 5) - 1"], 
        start_time=pool_date, 
        end_time=pool_date
    )
    
    if bias_df.empty:
        print(f"❌ 警告: 无法获取 {pool_date} 的风控特征，跳过风控过滤。")
        final_table = today_pred[["score"]].copy()
    else:
        bias_df.columns = ['bias_5d']
        bias_df = bias_df.reset_index().set_index('instrument')[['bias_5d']]
        final_table = today_pred[["score"]].join(bias_df, how='inner')
        
        # 执行乖离率风控剔除
        before_count = len(final_table)
        final_table = final_table[final_table['bias_5d'] < bias_limit].copy()
        print(f"⚠️ 风控提示：已从未未来触发过热（Bias >= {bias_limit}）的 {before_count} 只个股中剔除 {before_count - len(final_table)} 只。")

    # 排序并输出 TopK
    final_table.sort_values("score", ascending=False, inplace=True)
    result = final_table.head(topk).reset_index()

    # 保存今日信号
    save_path = RESULT_DIR / f"T2_pred_with_risk_{pool_date}.csv"
    result.to_csv(save_path, index=False)
    print(f"✅ 今日选股策略信号已保存至: {save_path}")
    print("\n===== 今日最终推荐买入池 =====")
    print(result[['instrument', 'score', 'bias_5d'] if 'bias_5d' in result.columns else ['instrument', 'score']])

if __name__ == "__main__":
    run_short_term_strategy(
        pool_date="2026-05-27", 
        hold_days=3, 
        bias_limit=0.10, 
        topk=5
    )
