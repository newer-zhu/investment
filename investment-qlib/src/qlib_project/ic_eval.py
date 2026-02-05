import pandas as pd
import numpy as np
from scipy.stats import spearmanr

def calc_ic_rank_ic(pred: pd.DataFrame, label_col: str = "LABEL0"):
    """
    计算 IC / Rank IC（按交易日横截面）
    
    pred: DataFrame，index 为 (datetime, instrument)
          必须包含：
            - score : 模型预测分数
            - LABEL0: 真实未来收益
    """
    df = pred.copy().reset_index()
    df["datetime"] = pd.to_datetime(df["datetime"])

    ic_list = []
    rank_ic_list = []

    for dt, g in df.groupby("datetime"):
        if g[label_col].isna().all():
            continue
        if g["score"].nunique() <= 1:
            continue

        ic = g["score"].corr(g[label_col], method="pearson")
        rank_ic = g["score"].corr(g[label_col], method="spearman")

        ic_list.append(ic)
        rank_ic_list.append(rank_ic)

    ic_series = pd.Series(ic_list, name="IC")
    rank_ic_series = pd.Series(rank_ic_list, name="RankIC")

    summary = {
        "IC_mean": ic_series.mean(),
        "IC_std": ic_series.std(),
        "RankIC_mean": rank_ic_series.mean(),
        "RankIC_std": rank_ic_series.std(),
        "IC_IR": ic_series.mean() / ic_series.std() if ic_series.std() != 0 else np.nan,
        "RankIC_IR": rank_ic_series.mean() / rank_ic_series.std() if rank_ic_series.std() != 0 else np.nan,
        "num_days": len(ic_series)
    }

    return summary, ic_series, rank_ic_series


# ================== 用法示例 ==================
# pred = model.predict(dataset, segment="test")
# pred 必须能拿到 LABEL0
# qlib 默认 predict(test) 返回的 DataFrame index 是 (datetime, instrument)

# summary, ic_ts, rank_ic_ts = calc_ic_rank_ic(pred)

# print("===== IC / Rank IC 统计 =====")
# for k, v in summary.items():
#     print(f"{k}: {v:.4f}" if isinstance(v, float) else f"{k}: {v}")

# # 查看最近 10 天
# print("\n最近 10 个交易日 Rank IC：")
# print(rank_ic_ts.tail(10))
