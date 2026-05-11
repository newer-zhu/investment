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

# ================== 基础路径配置 ==================
BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
SRC_ROOT = PROJECT_ROOT.parent
for path in (str(BASE_DIR), str(PROJECT_ROOT), str(SRC_ROOT)):
    if path not in sys.path:
        sys.path.insert(0, path)

from qlib_project.constants import TRASH_POOL, DATA_PATH
from qlib_project.utils.util import load_stock_pool, send_email, load_config_from_ini
from qlib_project.utils.email_report_utils import generate_rebound_report_html

# 存储路径
MODEL_DIR = PROJECT_ROOT / "data" / "models" / "rebound"
MODEL_DIR.mkdir(parents=True, exist_ok=True)
MODEL_FILE = MODEL_DIR / "lgb_rebound_model.pkl"
FEATURE_FILE = MODEL_DIR / "rebound_refined_features.json"

# ================== 1. 改进的模型参数配置 ==================
def get_rebound_model_params(hold_days: int, feature_names: list = None):
    """
    针对超跌反弹定制：
    1. huber 损失：对妖股的暴涨暴跌不敏感，只抓普遍规律。
    2. monotone_constraints：强制模型理解“跌得多反弹强”。
    """
    params = {
        "objective": "huber",          # 鲁棒回归，抗离群点
        "hubert_delta": 0.1,
        "metric": "mae",
        "n_jobs": -1,
        "verbosity": -1,
        "early_stopping_rounds": 80,
        "extra_trees": True,
        "n_estimators": 1000,
        "learning_rate": 0.015,        # 慢速学习
        "max_depth": 3,                # 极浅树，防止背诵行情
        "num_leaves": 7,
        "lambda_l1": 2.0,              # 强 L1 正则，筛选特征
        "lambda_l2": 2.0,
        "bagging_fraction": 0.7,
        "feature_fraction": 0.7,
        "bagging_freq": 1,
        "min_data_in_leaf": 30,
    }

    # 自动注入单调性约束
    if feature_names:
        constraints = []
        for name in feature_names:
            if "bias" in name.lower(): 
                constraints.append(-1) # 乖离率越小(越负)，得分越高
            elif "vol_ratio" in name.lower() or "amp" in name.lower():
                constraints.append(1)  # 振幅/量比越大，得分越高
            else:
                constraints.append(0)
        params["monotone_constraints"] = constraints

    return params


def signal_ic(score, label):
    """Compute daily IC and rank IC for signal scores and labels."""
    df = pd.concat([score.rename("score"), label.rename("label")], axis=1).dropna()
    if df.empty:
        return pd.DataFrame({"ic": [], "rank_ic": []})

    if isinstance(df.index, pd.MultiIndex):
        if "datetime" in df.index.names:
            group = df.groupby(level="datetime")
        else:
            group = df.groupby(level=0)
    elif "datetime" in df.columns:
        group = df.groupby("datetime")
    else:
        group = [(None, df)]

    ic_list = []
    rank_ic_list = []
    for _, sub in group:
        if len(sub) < 2:
            ic_list.append(np.nan)
            rank_ic_list.append(np.nan)
            continue
        ic_list.append(sub["score"].corr(sub["label"]))
        rank_ic_list.append(sub["score"].rank().corr(sub["label"].rank()))
    return pd.DataFrame({"ic": ic_list, "rank_ic": rank_ic_list})

# ================== 2. 数据处理器 ==================
def get_rebound_handler(hold_days: int, refined_fields=None, refined_names=None):
    # 预测目标：未来2日最高价相对于今日收盘的涨幅（捕捉反弹瞬间）
    label_expr = "If(Ref($high, -1) > Ref($high, -2), Ref($high, -1), Ref($high, -2)) / $close - 1"
    
    class ReboundAlpha158(Alpha158):
        def get_label_config(self):
            return ([label_expr], ["LABEL_REBOUND"])
            
        def get_feature_config(self):
            # 基础反弹逻辑因子
            extra_fields = [
                "$close / Mean($close, 5) - 1", 
                "$close / Mean($close, 20) - 1", 
                "$amount / Mean($amount, 5)", 
                "($high - $low) / $close"
            ]
            extra_names = ["bias_5d", "bias_20d", "vol_ratio", "amp_1d"]

            if refined_fields and refined_names:
                return refined_fields, refined_names
            
            conf = super().get_feature_config()
            conf[0].extend(extra_fields)
            conf[1].extend(extra_names)
            return conf

        def get_learn_processors(self):
            return [
                {"class": "ConfigSectionProcessor", "kwargs": {"fillna_label": True, "clip_label_outlier": True}},
                {"class": "CSZScoreNorm", "kwargs": {"fields_group": "feature"}} # 横向标准化
            ]
            
    return ReboundAlpha158

# ================== 3. 特征精炼 (杜绝泄漏版) ==================
def refine_features_no_leakage(handler_obj, segments, top_k=50):
    """
    为了防止泄漏，我们新创建一个临时的简单模型，只在 train segment 上训练。
    """
    ds = DatasetH(handler=handler_obj, segments=segments)
    # 快速训练一个评估模型
    tmp_model = LGBModel(n_estimators=200, learning_rate=0.1, max_depth=3, verbosity=-1)
    tmp_model.fit(ds)
    
    importance = tmp_model.model.feature_importance(importance_type='gain')
    fields, names = handler_obj.get_feature_config()
    
    df = pd.DataFrame({'name': names, 'field': fields, 'imp': importance}).sort_values('imp', ascending=False)
    
    # 强制保留核心反弹因子
    core_names = ["bias_5d", "bias_20d", "vol_ratio", "amp_1d"]
    refined_df = df[~df['name'].isin(core_names)].head(top_k - len(core_names))
    final_df = pd.concat([df[df['name'].isin(core_names)], refined_df])
    
    return final_df['field'].tolist(), final_df['name'].tolist()

# ================== 4. 每周离线训练任务 ==================
def train_weekly(train_end_date: str, stock_pool: list, data_path: str, hold_days: int = 2):
    print(f"\n================ 启动防御型模型训练 ({train_end_date}) ================")
    qlib.init(provider_uri=data_path, region=REG_CN)
    
    # 应对 Regime Shift: 仅使用 2021 年以后的数据
    start_train = "2021-01-01" 
    valid_start = (pd.Timestamp(train_end_date) - pd.Timedelta(days=120)).strftime('%Y-%m-%d')
    
    segments = {
        "train": (start_train, (pd.Timestamp(valid_start) - pd.Timedelta(days=1)).strftime('%Y-%m-%d')),
        "valid": (valid_start, (pd.Timestamp(train_end_date) - pd.Timedelta(days=1)).strftime('%Y-%m-%d')),
        "test": (train_end_date, train_end_date) 
    }
    stock_in = load_stock_pool(stock_pool)

    # --- 阶段 1：特征精炼（防泄漏模式） ---
    print("🚀 [阶段 1]：特征初筛（仅基于历史 Train 段）...")
    base_handler = get_rebound_handler(hold_days)(instruments=stock_in, start_time=start_train, end_time=train_end_date)
    refined_fields, refined_names = refine_features_no_leakage(base_handler, segments, top_k=45)
    
    # --- 阶段 2：最终模型训练 ---
    print(f"🚀 [阶段 2]：精炼拟合（保留 {len(refined_names)} 个核心特征）...")
    final_handler = get_rebound_handler(hold_days, refined_fields, refined_names)(
        instruments=stock_in, start_time=start_train, end_time=train_end_date
    )
    ds_final = DatasetH(handler=final_handler, segments=segments)
    
    # 注入特征名以启用单调约束
    model_params = get_rebound_model_params(hold_days, refined_names)
    final_model = LGBModel(**model_params)
    final_model.fit(ds_final)
    
    # --- 阶段 3：效能评估 (验证集 IC) ---
    # valid_pred = final_model.predict(ds_final, segment="valid")
    # valid_label = ds_final.prepare(segments="valid", col_set="label")
    # if isinstance(valid_pred, pd.Series): valid_pred = valid_pred.to_frame("score")
    
    # valid_combined = pd.concat([valid_pred, valid_label], axis=1).dropna()
    # if not valid_combined.empty:

    #     ic = signal_ic(valid_combined.iloc[:, 0], valid_combined.iloc[:, 1])
    #     print(f"📈 验证集 (最近120天) IC: {ic['ic'].mean():.4f}, Rank IC: {ic['rank_ic'].mean():.4f}")

    # --- 阶段 4：保存 ---
    final_model.to_pickle(str(MODEL_FILE))
    with open(FEATURE_FILE, 'w', encoding='utf-8') as f:
        json.dump({"fields": refined_fields, "names": refined_names}, f, ensure_ascii=False)
    
    print(f"✅ 模型训练完成！路径: {MODEL_FILE}")

# ================== 5. 每日推断任务 ==================
def predict_daily(pool_date: str, stock_pool: list, data_path: str, hold_days: int = 2, topk: int = 6):
    print(f"\n================ 执行每日阻击预测 ({pool_date}) ================")
    qlib.init(provider_uri=data_path, region=REG_CN)
    stock_in = load_stock_pool(stock_pool)
    
    print(f"DEBUG: 检查股票池样本: {stock_in[:5]}")
    check_d = D.features(stock_in[:10], ["$close"], start_time=pool_date, end_time=pool_date)

    if check_d.empty:
        print("‼️ 结论：你的本地Qlib数据库里没有 " + pool_date + " 的数据，请先同步数据！")
    
    # 1. 环境风控：大盘必须企稳，广度必须尚可
    market_df = D.features(["SH000300"], ["$close", "Mean($close, 20)"], start_time=pool_date, end_time=pool_date)
    if not market_df.empty:
        row = market_df.iloc[0]
        if row["$close"] < row["Mean($close, 20)"]:
            print("⚠️ 市场处于 MA20 下方，系统性风险高，不建议开仓。")
            # return pd.DataFrame() # 根据你的风险偏好决定是否强制中断

    # 2. 加载
    if not MODEL_FILE.exists():
        print("❌ 模型文件不存在。")
        return pd.DataFrame()

    with open(FEATURE_FILE, 'r', encoding='utf-8') as f:
        feat_conf = json.load(f)
    
    with open(MODEL_FILE, 'rb') as f:
        model = pickle.load(f)

    # 3. 预测
    lookback = (pd.Timestamp(pool_date) - pd.Timedelta(days=60)).strftime('%Y-%m-%d')
    handler = get_rebound_handler(hold_days, feat_conf['fields'], feat_conf['names'])(
        instruments=stock_in, start_time=lookback, end_time=pool_date
    )
    ds = DatasetH(handler=handler, segments={"test": (pool_date, pool_date)})
    
    pred_df = model.predict(ds, segment="test")
    if isinstance(pred_df, pd.Series): pred_df = pred_df.to_frame("score")
    
    # 强制规范化预测结果的索引
    scores = pred_df.reset_index()
    scores = scores[scores['datetime'] == pd.Timestamp(pool_date)].copy()
    scores['instrument'] = scores['instrument'].astype(str).str.upper().str.strip()
    scores = scores.set_index('instrument')

    print(f"DEBUG: 预测得分表(scores)样本数: {len(scores)}")

    # --- E. 行情风控数据清洗 ---
    risk_fields = ["$close/Mean($close,5)-1", "$close/Mean($close,20)-1", "$amount/Mean($amount,5)", "($high-$low)/$close"]
    risk_df_raw = D.features(stock_in, risk_fields, start_time=pool_date, end_time=pool_date)
    
    if risk_df_raw.empty:
        print(f"❌ 错误: D.features 无法获取字段数据。")
        return pd.DataFrame()

    # 重点：使用 xs 提取特定日期的所有股票，此时索引会自动变成 instrument
    try:
        # 如果 Qlib 返回的是 (datetime, instrument)
        risk_data = risk_df_raw.xs(pd.Timestamp(pool_date), level='datetime')
    except KeyError:
        # 如果 Qlib 返回的是 (instrument, datetime)
        risk_data = risk_df_raw.xs(pd.Timestamp(pool_date), level=1)

    # 规范化列名和索引
    risk_data.columns = ['bias_5d', 'bias_20d', 'vol_ratio', 'amp_1d']
    risk_data.index = risk_data.index.astype(str).str.upper().str.strip()

    print(f"DEBUG: 行情风控表(risk_data)样本数: {len(risk_data)}")
    if len(risk_data) > 0:
        print(f"DEBUG: 修正后的 Risk ID 示例: '{risk_data.index[0]}'")

    # --- F. 合并 ---
    final = scores[['score']].join(risk_data[['bias_5d', 'bias_20d', 'vol_ratio', 'amp_1d']], how='inner')
    print(f"DEBUG: 参与形态过滤的股票总数 (Join后): {len(final)}")
    # 过滤条件：5日乖离率够低，且成交量没有异常放大（防止下跌放量承接不住）
    mask = (final['bias_5d'] < -0.035) & (final['vol_ratio'] < 1.3)
    result = final[mask].sort_values('score', ascending=False).head(topk)

    print(f"\n✅ {pool_date} 阻击名单 (Join 数: {len(final)}, 选出: {len(result)}):")
    if not result.empty:
        print(result[['score', 'bias_5d', 'vol_ratio']])
        # 保存结果
        RESULT_DIR = PROJECT_ROOT / "data" / "rebound_predictions"
        RESULT_DIR.mkdir(parents=True, exist_ok=True)
        result_path = RESULT_DIR / f"rebound_picks_{pool_date}.csv"
        result.to_csv(result_path)
        print(f"💾 结果已保存至: {result_path}")
        
        # 发送邮件报告
        try:
            config_path = PROJECT_ROOT.parent.parent.parent / "config.ini"  # /mnt/f/Code/investment/config.ini
            print(f"🔍 配置文件路径: {config_path}")
            _email_cfg = load_config_from_ini("email", str(config_path))
            TO_EMAILS = [e.strip() for e in _email_cfg.get("to_emails", "").split(",") if e.strip()] or [_email_cfg.get("to_email", "")]
            FROM_EMAIL = _email_cfg.get("from_email", "")
            FROM_PASSWORD = _email_cfg.get("from_password", "")
            SMTP_SERVER = _email_cfg.get("smtp_server", "smtp.qq.com")
            SMTP_PORT = int(_email_cfg.get("smtp_port", "587"))
            
            print(f"📧 邮件配置 - 发件人: {FROM_EMAIL}")
            print(f"📧 邮件配置 - 密码: {FROM_PASSWORD[:4]}****")  # 只显示前4位
            print(f"📧 邮件配置 - 收件人: {TO_EMAILS}")
            print(f"📧 邮件配置 - SMTP: {SMTP_SERVER}:{SMTP_PORT}")
            
            # 生成 HTML 报告
            html_body = generate_rebound_report_html(pool_date, result, len(final))
            
            subject = f"🚀 超跌反弹策略报告 - {pool_date}"
            
            for to_email in TO_EMAILS:
                send_email(
                    subject=subject,
                    body=html_body,
                    to_email=to_email,
                    from_email=FROM_EMAIL,
                    from_password=FROM_PASSWORD,
                    smtp_server=SMTP_SERVER,
                    smtp_port=SMTP_PORT,
                    content_type="html"
                )
            print("📧 邮件报告已发送成功！")
        except Exception as e:
            print(f"❌ 邮件发送失败: {e}")
    else:
        print("⚠️ 今日无匹配形态的个股，请耐心等待市场回调至缩量点。")

    return result

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, choices=["train", "predict"], default="predict")
    parser.add_argument("--date", type=str, default=pd.Timestamp.now().strftime('%Y-%m-%d'))
    args = parser.parse_args()

    if args.mode == "train":
        train_weekly(args.date, TRASH_POOL, DATA_PATH)
    else:
        predict_daily(args.date, TRASH_POOL, DATA_PATH)