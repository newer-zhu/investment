import os
from pathlib import Path
import pandas as pd
import numpy as np
import lightgbm as lgb  # 引入原生 lightgbm

# ================== 0. 环境兼容性修复 ==================
os.environ["PYTHONIOENCODING"] = "utf-8"

# ================== 1. QLib 初始化 ==================
import qlib
from qlib.config import REG_CN
from qlib.contrib.data.handler import Alpha158
from qlib.data import D

# ================== 2. 路径配置 ==================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PATH = PROJECT_ROOT / "data" / "source"
POOL_BASE_DIR = PROJECT_ROOT / "data" / "stock_pool" / "processed"
RESULT_DIR = PROJECT_ROOT / "data" / "pool_predictions_lgb"

# ================== 3. 初始化函数 ==================
def init_env():
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"QLib 数据路径不存在: {DATA_PATH}")
    qlib.init(provider_uri=str(DATA_PATH), region=REG_CN)

def load_pool(pool_csv: Path) -> list[str]:
    if not pool_csv.exists():
        raise FileNotFoundError(f"股票池文件不存在: {pool_csv}")
    df = pd.read_csv(pool_csv, dtype=str)
    df.columns = df.columns.str.strip().str.lower()
    if "instrument" not in df.columns:
        raise ValueError("股票池 CSV 缺少 instrument 列")
    return df["instrument"].dropna().unique().tolist()

# ================== 4. 修复：标签生成函数 (对齐 MultiIndex) ==================
def get_label_data(instruments: list[str], start_date: str, end_date: str, future_days: int = 5):
    """
    计算未来 N 日收益率，并转换为与 QLib 特征对齐的 MultiIndex Series
    """
    print(f"正在计算未来 {future_days} 日收益率标签...")
    
    # 批量获取收盘价 (比循环单只股票快得多)
    df = D.features(instruments, ['$close'], start_time=start_date, end_time=end_date)
    
    # df 是 MultiIndex (instrument, datetime)，我们需要先 unstack 方便 shift
    price_df = df['$close'].unstack(level='instrument')
    
    # 计算收益率：(未来价格 - 当前价格) / 当前价格
    # shift(-n) 会把未来的数据向上平移，即当前行有了未来的数据
    future_ret = (price_df.shift(-future_days) - price_df) / price_df
    
    # 重新堆叠回 (datetime, instrument) 格式，并改名
    label_series = future_ret.stack().reindex(df.index)
    label_series.name = "label"
    
    return label_series

# ================== 5. 主逻辑 ==================
def run_lgb_predict_weekly(
    pool_date: str,
    predict_date: str,
    start_date: str,
    end_date: str,
    test_start: str,
    topk: int = 20,
    holding_days: int = 5
):
    init_env()

    # ---------- 加载股票池 ----------
    pool_csv = POOL_BASE_DIR / f"{pool_date}_pool.csv"
    instruments = load_pool(pool_csv)
    print(f"股票池数量: {len(instruments)} 支")

    # ---------- 1. 获取特征 (X) ----------
    print("构建 Alpha158 特征...")
    # Alpha158 处理器会自动提取特征
    handler = Alpha158(
        instruments=instruments,
        start_time=start_date,
        end_time=end_date,
    )
    # fetch() 返回 DataFrame，索引为 (datetime, instrument)
    feature_df = handler.fetch()

    # ---------- 2. 获取标签 (Y) ----------
    # 注意：计算 label 需要稍微多一点的数据以计算 shift，所以结束时间可以稍微延后，
    # 但由于你是用历史数据训练，end_date 必须是已知的历史日期
    label_series = get_label_data(instruments, start_date=start_date, end_date=end_date, future_days=holding_days)

    # ---------- 3. 数据合并与清洗 ----------
    print("合并特征与标签...")
    # join 自动对齐索引 (datetime, instrument)
    data = feature_df.join(label_series, how='inner')
    
    # 去除包含空值的行 (特征或标签为空都不能用于训练)
    data = data.dropna()
    
    if data.empty:
        raise ValueError("合并后数据为空，请检查日期范围或股票池数据是否完整")

    # ---------- 4. 切分训练集与测试集 ----------
    # reset_index 以便按列筛选日期
    data_reset = data.reset_index()
    train_mask = (data_reset["datetime"] >= pd.Timestamp(start_date)) & (data_reset["datetime"] < pd.Timestamp(test_start))
    # 注意：预测用的数据只需要特征，这里我们切分出 Test 集用于评估或验证
    # 实际预测是针对 predict_date 那一天的
    
    X_train = data_reset.loc[train_mask, feature_df.columns]
    y_train = data_reset.loc[train_mask, "label"]
    
    print(f"训练集大小: {len(X_train)}")

    # ---------- 5. 训练原生 LightGBM ----------
    print("训练 LightGBM (Native)...")
    model = lgb.LGBMRegressor(
        objective='mse',
        n_estimators=300,
        learning_rate=0.02,
        max_depth=6,
        n_jobs=-1,
        random_state=42,
        verbose=-1
    )
    
    model.fit(X_train, y_train)

    # ---------- 6. 预测 (针对特定日期) ----------
    print(f"生成 {predict_date} 的预测分数...")
    
    # 我们需要 predict_date 当天的特征数据。
    # 由于 handler 可能已经加载了该日期 (如果 end_date 涵盖了 predict_date)
    # 如果 predict_date 在 end_date 之外，你需要单独拉取。
    # 假设 predict_date 在 handler 的范围内：
    
    target_date = pd.Timestamp(predict_date)
    
    # 从 feature_df (原始特征全集) 中提取目标日期的特征
    # 注意：这里不需要 label，只需要特征
    if target_date in feature_df.index.get_level_values("datetime"):
        X_pred = feature_df.loc[target_date]
    else:
        print(f"⚠️ {predict_date} 不在特征数据范围内，尝试使用 pool_date: {pool_date}")
        target_date = pd.Timestamp(pool_date)
        if target_date in feature_df.index.get_level_values("datetime"):
            X_pred = feature_df.loc[target_date]
        else:
            raise ValueError(f"无法找到 {predict_date} 或 {pool_date} 的特征数据")

    # 预测
    scores = model.predict(X_pred)
    
    # 构建结果 DataFrame
    result = pd.DataFrame({
        "datetime": target_date,
        "instrument": X_pred.index, # X_pred 的 index 是 instrument (因为 loc[date] 去掉了 date 层级)
        "score": scores
    })

    # ---------- 7. 排序与保存 ----------
    result.sort_values("score", ascending=False, inplace=True)
    result["rank"] = range(1, len(result) + 1)
    
    # 取 TopK
    top_result = result.head(topk)

    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    save_path = RESULT_DIR / f"prediction_rank_{predict_date}_weekly.csv"
    top_result.to_csv(save_path, index=False, encoding="utf-8-sig")

    print(f"\n✅ Top {topk} 预测结果已保存：{save_path}")
    print(top_result.head(10))

# ================== 6. 入口 ==================
if __name__ == "__main__":
    run_lgb_predict_weekly(
        pool_date="2026-01-23",     
        predict_date="2026-01-23",  
        start_date="2025-01-01",
        end_date="2026-01-23", # 确保这里包含用于生成当天特征的数据
        test_start="2025-11-01",
        topk=10,
        holding_days=5 # 设置为你想要的持股周期
    )