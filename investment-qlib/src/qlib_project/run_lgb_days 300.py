import os
from pathlib import Path
import pandas as pd
import qlib
from qlib.config import REG_CN
from qlib.data.dataset import DatasetH
from qlib.contrib.data.handler import Alpha158
from qlib.contrib.model.gbdt import LGBModel
from utils.model_params_adjust import get_model_params
# ================== 1. 动态 Handler 工厂 ==================
def get_dynamic_handler(hold_days: int):
    """
    根据持有天数动态生成 Alpha158 处理器
    """
    # 计算公式：(未来第N天的收盘价 / 明天的开盘价) - 1 
    # Qlib 中 Ref(close, -2) 通常指 T+1 的价格
    target_ref = -(hold_days + 1)
    label_expr = f"Ref($close, {target_ref}) / Ref($close, -1) - 1"
    
    class DynamicAlpha158(Alpha158):
        def get_label_config(self):
            return ([label_expr], ["LABEL0"])
            
    return DynamicAlpha158

# ================== 2. 动态调参映射表 ==================

# ================== 3. 主预测函数 ==================
def run_dynamic_predict(
    pool_date: str,
    hold_days: int = 3,
    topk: int = 10,
    start_date: str = "2024-04-01", # 建议训练集拉长，增加泛化性
    test_start: str = "2025-04-01"
):
    # 环境初始化
    os.environ["PYTHONIOENCODING"] = "utf-8"
    
    # --- 路径修正：直接指向你的 source 目录 ---
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
    DATA_PATH = PROJECT_ROOT /  "data" / "source" 
    RESULT_DIR = PROJECT_ROOT / "data" / "pool_predictions_lgb"
    
    # 初始化 Qlib
    qlib.init(provider_uri=str(DATA_PATH), region=REG_CN)

    print(f"🚀 模式：CSI300 股票池 | 预测 {hold_days} 日收益 | 基准日: {pool_date}")
    
    # ---------- 1. 配置数据处理器 ----------
    # 直接使用 "csi300" 字符串，Qlib 会自动去 instruments/csi300.txt 查找
    market = "csi300"
    
    DynamicHandler = get_dynamic_handler(hold_days)
    handler = DynamicHandler(
        instruments=market,
        start_time=start_date,
        end_time=pool_date,
    )

    # ---------- 2. 构建数据集 ----------
    dataset = DatasetH(
        handler=handler,
        segments={
            "train": (start_date, test_start), 
            "test": (test_start, pool_date)
        },
    )

    # ---------- 3. 训练模型 ----------
    lgb_params = get_model_params(hold_days)
    model = LGBModel(**lgb_params)
    model.fit(dataset)

    # ---------- 4. 预测与过滤 ----------
    pred = model.predict(dataset, segment="test")
    
    # 转换为 DataFrame 处理
    pred_df = pred.reset_index()
    pred_df.columns = ["datetime", "instrument", "score"]

    # 只取 pool_date 当天的数据
    target_date = pd.Timestamp(pool_date)
    today_pred = pred_df[pred_df["datetime"] == target_date].copy()

    if today_pred.empty:
        # 如果当天没预测值，尝试找最近的一个交易日
        available_dates = pred_df["datetime"].unique()
        last_date = available_dates[-1] if len(available_dates) > 0 else "None"
        print(f"❌ 警告: {pool_date} 无数据。最新可用预测日为: {last_date}")
        return

    # 排序
    today_pred.sort_values("score", ascending=False, inplace=True)
    result = today_pred.head(topk)

    # ---------- 5. 保存结果 ----------
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    save_name = f"pred_csi300_{hold_days}day_{pool_date}.csv"
    result.to_csv(RESULT_DIR / save_name, index=False)
    
    print(f"\n✅ 沪深300 - {hold_days}日持仓建议 ({pool_date}):")
    print(result[['instrument', 'score']])

if __name__ == "__main__":
    run_dynamic_predict(
        pool_date="2026-02-03", 
        hold_days=3,
        topk=10
    )