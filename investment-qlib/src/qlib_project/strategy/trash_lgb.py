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
from qlib_project.utils.util import load_stock_pool, send_email, load_config_from_ini
from qlib_project.utils.email_report_utils import generate_rebound_report_html
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
    stock_in = load_stock_pool(stock_pool)
    # ================== 市场环境过滤 ==================
    print("🌍 正在判断市场环境...")

    market_df = D.features(
        instruments=["SH000300"],  # 沪深300
        fields=["$close", "Mean($close, 20)"],
        start_time=pool_date,
        end_time=pool_date
    )

    if market_df.empty:
        print("❌ 无法获取市场数据，跳过交易")
        return

    market_df = market_df.reset_index()
    close = market_df.iloc[0]["$close"]
    ma20 = market_df.iloc[0]["Mean($close, 20)"]
    
    breadth_df = D.features(
    instruments=stock_in,
    fields=["$close", "Ref($close, 1)"],
    start_time=pool_date,
    end_time=pool_date
    ).reset_index()
    breadth = (breadth_df["$close"] > breadth_df["Ref($close, 1)"]).mean()

    print(f"📊 市场状态: close={close:.2f}, MA20={ma20:.2f}")

    # 🔥 核心判断
    if close < ma20 or breadth < 0.4:
        print("⚠️ 当前市场弱势（跌破MA20），停止开仓")
        return
    else:
        print("✅ 市场环境健康，允许执行反弹策略")
    

    #阶段 1：全特征初步训练 ---
    print("🚀 阶段 1：扫描全量特征（寻找反弹信号共振点）...")
    ds_v1 = DatasetH(
        handler=get_rebound_handler(hold_days)(instruments=stock_in, start_time=start_train, end_time=pool_date),
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
        handler=get_rebound_handler(hold_days, refined_fields, refined_names)(instruments=stock_in, start_time=start_train, end_time=pool_date),
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
    risk_df_raw = D.features(stock_in, risk_fields, start_time=pool_date, end_time=pool_date)
    
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
    
    final_table.columns = [
        'score', 
        'bias_5d', 
        'bias_20d', 
        'vol_ratio', 
        'amp_1d'
    ]

    # --- E. 核心风控逻辑：寻找跌到位后的缩量点 ---
    mask = (
        (final_table['bias_5d'] < -0.04) &      # 放宽
        (final_table['vol_ratio'] < 1.2) &      # 放宽
        (final_table['bias_20d'] > -0.2) &      # 放宽
        (final_table['amp_1d'] > 0.01) 
    )
    
    print(final_table[['bias_5d', 'vol_ratio', 'bias_20d', 'amp_1d']].describe())
    
    candidates = final_table[mask].sort_values("score", ascending=False)
    result = candidates.head(topk)

    print(f"\n✅ {pool_date} 阻击名单 (Join 数: {len(final_table)}, 选出: {len(result)}):")
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
            html_body = generate_rebound_report_html(pool_date, result, len(final_table))
            
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
    # 运行策略
    result = run_rebound_strategy(
        pool_date="2026-05-06",
        stock_pool=TRASH_POOL,
        data_path=DATA_PATH,
        hold_days=2,
        topk=6
    )
