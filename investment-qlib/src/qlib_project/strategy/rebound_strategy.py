import os
import gc
import json
import pickle
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
from qlib_project.utils.util import load_stock_pool, send_email, load_config_from_ini
from qlib_project.utils.email_report_utils import generate_rebound_report_html

# ================== 0. 存储路径配置 ==================
MODEL_DIR = PROJECT_ROOT / "data" / "models" / "rebound"
MODEL_DIR.mkdir(parents=True, exist_ok=True)
MODEL_FILE = MODEL_DIR / "lgb_rebound_model.pkl"
FEATURE_FILE = MODEL_DIR / "rebound_refined_features.json"

# ================== 1. 模型参数配置 ==================
def get_rebound_model_params(hold_days: int):
    """针对超跌反弹的实盘鲁棒型参数"""
    return {
        "objective": "regression",
        "metric": "rmse",
        "n_jobs": -1,
        "verbosity": -1,
        "early_stopping_rounds": 100, # 给模型更多观察期
        "extra_trees": True,          # 继续保持，这有助于抗过拟合
        "n_estimators": 1000,
        
        # --- 核心调整项 ---
        "learning_rate": 0.01,        # 显著调低，慢工出细活
        "max_depth": 3,               # 强制弱模型！超跌反弹的核心逻辑通常很简单
        "num_leaves": 8,              # 配合 depth=3，限制叶子节点数
        
        "lambda_l1": 1.5,             # 显著提高 L1，剔除无效特征
        "lambda_l2": 2.0,             # 显著提高 L2，平滑系数
        
        "bagging_fraction": 0.8,      # 每次迭代只用 80% 的数据，增加随机性
        "feature_fraction": 0.8,      # 每次迭代只用 80% 的特征，防止单一特征绑架模型
        "bagging_freq": 5,
        "min_data_in_leaf": 50,       # 确保每个叶子节点有足够样本，防止学到孤例
    }


# ================== 2. 反弹专用数据处理器 ==================
def get_rebound_handler(hold_days: int, refined_fields=None, refined_names=None):
    label_expr = "If(Ref($high, -1) > Ref($high, -2), Ref($high, -1), Ref($high, -2)) / $close - 1"
    
    class ReboundAlpha158(Alpha158):
        def get_label_config(self):
            return ([label_expr], ["LABEL_REBOUND"])
            
        def get_feature_config(self):
            extra_fields = [
                "$close / Mean($close, 5) - 1",          # bias_5d
                "$close / Mean($close, 20) - 1",         # bias_20d
                "$amount / Mean($amount, 5)",            # vol_ratio
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


# ================== 4. 任务A: 周末离线训练 (每周执行一次) ==================
def train_weekly(train_end_date: str, stock_pool: list, data_path: str, hold_days: int = 2):
    print(f"\n================ 开始执行每周模型训练 ({train_end_date}) ================")
    qlib.init(provider_uri=data_path, region=REG_CN)
    start_train = "2018-01-01"
    
    # 实盘建议只留最近 30-60 天作为验证集，让模型“记忆”更贴近当前的行情
    valid_start = (pd.Timestamp(train_end_date) - pd.Timedelta(days=60)).strftime('%Y-%m-%d')
    
    # 在实盘训练中，test 的 segment 其实不参与训练，设为 train_end_date 即可
    # 真正的预测是在训练完后调用 model.predict(ds_v1)
    segments = {
        "train": (start_train, valid_start),
        "valid": (valid_start, train_end_date),
        "test": (train_end_date, train_end_date) 
    }
    stock_in = load_stock_pool(stock_pool)

    # 阶段 1：全量特征训练
    print("🚀 [阶段 1/3]：扫描全量特征（寻找反弹信号共振点）...")
    ds_v1 = DatasetH(
        handler=get_rebound_handler(hold_days)(instruments=stock_in, start_time=start_train, end_time=train_end_date),
        segments=segments
    )
    model_v1 = LGBModel(**get_rebound_model_params(hold_days))
    model_v1.fit(ds_v1)
    
    refined_fields, refined_names = refine_features(model_v1, ds_v1, top_k=60)
    del ds_v1, model_v1
    gc.collect()

    # 阶段 2：精炼特征二次拟合
    print("🚀 [阶段 2/3]：精炼特征训练（锁定高胜率组合）...")
    ds_v2 = DatasetH(
        handler=get_rebound_handler(hold_days, refined_fields, refined_names)(instruments=stock_in, start_time=start_train, end_time=train_end_date),
        segments=segments
    )
    model_v2 = LGBModel(**get_rebound_model_params(hold_days))
    model_v2.fit(ds_v2)
        
    # ---------- 预测与 IC 计算 ----------
    pred_df = model_v2.predict(ds_v2, segment="test")
    
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
    RESULT_DIR = PROJECT_ROOT / "data" / "short_term_predictions"
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    pred_path = RESULT_DIR / "full_test_predictions.pkl"
    pred_df.to_pickle(pred_path)
    print(f"✅ 全量预测数据已保存至: {pred_path} (用于回测)")
    
    # ---------- 提取 test 段真实 LABEL (计算 IC) ----------
    print("提取 test 段真实 LABEL...")
    try:
        label_df = ds_v2.prepare(segments="test", col_set="label")
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
        
    # 阶段 3：持久化保存
    print("🚀 [阶段 3/3]：保存模型与特征组合...")
    # 保存特征列表
    with open(FEATURE_FILE, 'w', encoding='utf-8') as f:
        json.dump({"fields": refined_fields, "names": refined_names}, f, ensure_ascii=False)
    
    # 保存模型 (Qlib 的 LGBModel 支持 pickle)
    with open(MODEL_FILE, 'wb') as f:
        pickle.dump(model_v2, f)
        
    print(f"✅ 模型训练完成！")
    print(f"💾 特征路径: {FEATURE_FILE}")
    print(f"💾 模型路径: {MODEL_FILE}")


# ================== 5. 任务B: 每日推断 (每日盘后执行) ==================
def predict_daily(pool_date: str, stock_pool: list, data_path: str, hold_days: int = 2, topk: int = 8):
    print(f"\n================ 开始执行每日预测 ({pool_date}) ================")
    qlib.init(provider_uri=data_path, region=REG_CN)
    stock_in = load_stock_pool(stock_pool)

    # --- A. 市场环境过滤 ---
    print("🌍 正在判断市场环境...")
    market_df = D.features(
        instruments=["SH000300"],  # 沪深300
        fields=["$close", "Mean($close, 20)"],
        start_time=pool_date,
        end_time=pool_date
    )

    if market_df.empty:
        print(f"❌ 无法获取 {pool_date} 市场数据，跳过交易")
        return pd.DataFrame()

    market_df = market_df.reset_index()
    close = market_df.iloc[0]["$close"]
    ma20 = market_df.iloc[0]["Mean($close, 20)"]
    
    breadth_df = D.features(instruments=stock_in, fields=["$close", "Ref($close, 1)"], start_time=pool_date, end_time=pool_date).reset_index()
    breadth = (breadth_df["$close"] > breadth_df["Ref($close, 1)"]).mean()

    print(f"📊 市场状态: 沪深300 close={close:.2f}, MA20={ma20:.2f}, 市场广度={breadth:.1%}")

    if close < ma20 or breadth < 0.4:
        print("⚠️ 当前市场弱势（跌破MA20 或 广度不佳），停止开仓")
        return pd.DataFrame()
    else:
        print("✅ 市场环境健康，允许执行反弹策略")

    # --- B. 加载模型与特征 ---
    if not MODEL_FILE.exists() or not FEATURE_FILE.exists():
        print("❌ 未找到预训练模型或特征文件，请先运行 train_weekly()！")
        return pd.DataFrame()

    with open(FEATURE_FILE, 'r', encoding='utf-8') as f:
        features_dict = json.load(f)
        refined_fields = features_dict['fields']
        refined_names = features_dict['names']

    with open(MODEL_FILE, 'rb') as f:
        model_loaded = pickle.load(f)
    print("✅ 预训练模型加载成功！")

    # --- C. 构建推断数据集 (为了计算 MA20 等特征，start_time 前推 60 天) ---
    print("⏱️ 正在构建今日推断特征...")
    lookback_start = (pd.Timestamp(pool_date) - pd.Timedelta(days=60)).strftime('%Y-%m-%d')
    
    ds_infer = DatasetH(
        handler=get_rebound_handler(hold_days, refined_fields, refined_names)(instruments=stock_in, start_time=lookback_start, end_time=pool_date),
        segments={"test": (pool_date, pool_date)}  # 只对今天进行预测
    )

    # --- D. 预测结果 ---
    pred_df = model_loaded.predict(ds_infer, segment="test")
    if isinstance(pred_df, pd.Series): pred_df = pred_df.to_frame(name="score")

    pred_reset = pred_df.reset_index()
    if len(pred_reset.columns) == 3:
        pred_reset.columns = ['datetime', 'instrument', 'score']
    pred_reset['instrument'] = pred_reset['instrument'].astype(str).str.upper().str.strip()
    today_pred = pred_reset[pred_reset['datetime'] == pd.Timestamp(pool_date)].copy()

    if today_pred.empty:
        print(f"❌ 警告: {pool_date} 当天无预测得分。")
        return pd.DataFrame()

    # --- E. 形态过滤与合并 ---
    print(f"🛠️ 正在应用【超跌 + 衰竭】形态过滤...")
    risk_fields = [
        "$close / Mean($close, 5) - 1", 
        "$close / Mean($close, 20) - 1", 
        "$amount / Mean($amount, 5)", 
        "($high - $low) / $close"
    ]
    risk_df_raw = D.features(stock_in, risk_fields, start_time=pool_date, end_time=pool_date)
    
    if risk_df_raw.empty:
        print(f"❌ 警告: 无法获取 {pool_date} 的行情风控数据。")
        return pd.DataFrame()

    risk_reset = risk_df_raw.reset_index()
    risk_reset['instrument'] = risk_reset['instrument'].astype(str).str.upper().str.strip()
    feat_cols = [c for c in risk_reset.columns if c not in ['datetime', 'instrument']]
    
    final_table = today_pred.set_index('instrument')[['score']].join(
        risk_reset.set_index('instrument')[feat_cols], how='inner'
    )
    
    final_table.columns = ['score', 'bias_5d', 'bias_20d', 'vol_ratio', 'amp_1d']

    mask = (
        (final_table['bias_5d'] < -0.04) &      
        (final_table['vol_ratio'] < 1.2) &      
        (final_table['bias_20d'] > -0.2) &      
        (final_table['amp_1d'] > 0.01) 
    )
    
    candidates = final_table[mask].sort_values("score", ascending=False)
    result = candidates.head(topk)

    # --- F. 输出与邮件发送 ---
    print(f"\n✅ {pool_date} 阻击名单 (有效标的: {len(final_table)}, 触发买点: {len(result)}):")
    if not result.empty:
        print(result[['score', 'bias_5d', 'vol_ratio']])
        
        # 结果存盘
        RESULT_DIR = PROJECT_ROOT / "data" / "rebound_predictions"
        RESULT_DIR.mkdir(parents=True, exist_ok=True)
        result_path = RESULT_DIR / f"rebound_picks_{pool_date}.csv"
        result.to_csv(result_path)
        print(f"💾 结果已保存至: {result_path}")
        
        # 发送邮件
        try:
            config_path = PROJECT_ROOT.parent.parent.parent / "config.ini"
            _email_cfg = load_config_from_ini("email", str(config_path))
            TO_EMAILS = [e.strip() for e in _email_cfg.get("to_emails", "").split(",") if e.strip()] or [_email_cfg.get("to_email", "")]
            FROM_EMAIL = _email_cfg.get("from_email", "")
            FROM_PASSWORD = _email_cfg.get("from_password", "")
            SMTP_SERVER = _email_cfg.get("smtp_server", "smtp.qq.com")
            SMTP_PORT = int(_email_cfg.get("smtp_port", "587"))
            
            html_body = generate_rebound_report_html(pool_date, result, len(final_table))
            subject = f"🚀 超跌反弹策略报告 - {pool_date}"
            
            for to_email in TO_EMAILS:
                send_email(
                    subject=subject, body=html_body, to_email=to_email,
                    from_email=FROM_EMAIL, from_password=FROM_PASSWORD,
                    smtp_server=SMTP_SERVER, smtp_port=SMTP_PORT, content_type="html"
                )
            print("📧 邮件报告已发送成功！")
        except Exception as e:
            print(f"❌ 邮件发送失败: {e}")
    else:
        print("⚠️ 今日无匹配形态的个股，耐心等待市场回调至缩量点。")

    return result

# ================== 6. 运行入口 ==================
if __name__ == "__main__":
    # 为了方便测试，你可以通过传入命令行参数 sys.argv 控制执行逻辑
    # 例如：python rebound_strategy.py --mode=train --date=2026-05-01
    #       python rebound_strategy.py --mode=predict --date=2026-05-06
    
    import argparse
    parser = argparse.ArgumentParser(description="超跌反弹策略：训练与预测分离")
    parser.add_argument("--mode", type=str, choices=["train", "predict"], default="predict", help="执行模式：train 或 predict")
    parser.add_argument("--date", type=str, default="2026-05-06", help="目标日期，格式 YYYY-MM-DD")
    args = parser.parse_args()

    if args.mode == "train":
        # 周末运行：执行模型训练与特征降维保存
        train_weekly(
            train_end_date=args.date,
            stock_pool=TRASH_POOL,
            data_path=DATA_PATH,
            hold_days=2
        )
    else:
        # 工作日运行：仅执行推断逻辑，极大提升运行速度
        result = predict_daily(
            pool_date=args.date,
            stock_pool=TRASH_POOL,
            data_path=DATA_PATH,
            hold_days=2,
            topk=6
        )