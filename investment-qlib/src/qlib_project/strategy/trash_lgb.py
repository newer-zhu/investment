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
                "$close / Mean($close, 5) - 1", 
                "$close / Mean($close, 20) - 1",
                "$amount / Mean($amount, 5)", 
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

# ================== 5. 生成 HTML 报告 ==================
def generate_rebound_report_htmlb(pool_date: str, result: pd.DataFrame, total_candidates: int) -> str:
    """
    生成专业的量化报告 HTML
    """
    num_selected = len(result)
    
    # 格式化表格
    df_display = result.reset_index()[['instrument', 'score', 'bias_5d', 'bias_20d', 'vol_ratio', 'amp_1d']].copy()
    df_display.columns = ['股票代码', '预测得分', '5日乖离率', '20日乖离率', '量比', '振幅']
    
    # 格式化数值
    df_display['预测得分'] = df_display['预测得分'].round(4)
    df_display['5日乖离率'] = (df_display['5日乖离率'] * 100).round(2).astype(str) + '%'
    df_display['20日乖离率'] = (df_display['20日乖离率'] * 100).round(2).astype(str) + '%'
    df_display['量比'] = df_display['量比'].round(2)
    df_display['振幅'] = (df_display['振幅'] * 100).round(2).astype(str) + '%'
    
    table_html = df_display.to_html(index=False, border=0, escape=False)
    
    # HTML 模板
    html = f"""
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>超跌反弹策略报告</title>
        <style>
            body {{
                font-family: 'Microsoft YaHei', 'PingFang SC', 'Hiragino Sans GB', sans-serif;
                line-height: 1.6;
                color: #333;
                max-width: 800px;
                margin: 0 auto;
                padding: 20px;
                background-color: #f8f9fa;
            }}
            .header {{
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 30px;
                border-radius: 10px;
                text-align: center;
                margin-bottom: 30px;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            }}
            .header h1 {{
                margin: 0;
                font-size: 28px;
                font-weight: 300;
            }}
            .header p {{
                margin: 10px 0 0 0;
                opacity: 0.9;
            }}
            .summary {{
                background: white;
                padding: 25px;
                border-radius: 8px;
                margin-bottom: 25px;
                box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
            }}
            .summary h2 {{
                color: #2c3e50;
                border-bottom: 2px solid #3498db;
                padding-bottom: 10px;
                margin-top: 0;
            }}
            .stats {{
                display: flex;
                justify-content: space-around;
                margin: 20px 0;
            }}
            .stat {{
                text-align: center;
            }}
            .stat .number {{
                font-size: 32px;
                font-weight: bold;
                color: #3498db;
            }}
            .stat .label {{
                color: #7f8c8d;
                font-size: 14px;
                margin-top: 5px;
            }}
            .table-container {{
                background: white;
                border-radius: 8px;
                overflow: hidden;
                box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
            }}
            th {{
                background: #34495e;
                color: white;
                padding: 15px 10px;
                text-align: center;
                font-weight: 500;
                font-size: 14px;
            }}
            td {{
                padding: 12px 10px;
                text-align: center;
                border-bottom: 1px solid #ecf0f1;
                font-size: 13px;
            }}
            tr:nth-child(even) {{
                background-color: #f8f9fa;
            }}
            tr:hover {{
                background-color: #e8f4fd;
            }}
            .footer {{
                text-align: center;
                margin-top: 30px;
                color: #7f8c8d;
                font-size: 12px;
            }}
            .strategy-info {{
                background: #ecf0f1;
                padding: 15px;
                border-radius: 6px;
                margin-bottom: 20px;
            }}
            .strategy-info h3 {{
                margin-top: 0;
                color: #2c3e50;
            }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>🚀 超跌反弹策略报告</h1>
            <p>报告日期：{pool_date} | 生成时间：{pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
        
        <div class="summary">
            <h2>📊 策略执行概况</h2>
            <div class="stats">
                <div class="stat">
                    <div class="number">{total_candidates}</div>
                    <div class="label">候选股票数</div>
                </div>
                <div class="stat">
                    <div class="number">{num_selected}</div>
                    <div class="label">入选股票数</div>
                </div>
                <div class="stat">
                    <div class="number">{(num_selected/total_candidates*100):.1f}%</div>
                    <div class="label">入选比例</div>
                </div>
            </div>
            
            <div class="strategy-info">
                <h3>🎯 策略逻辑说明</h3>
                <p>基于 LightGBM 模型预测未来2日最高价相对当前收盘的收益，筛选出具备超跌反弹潜力的股票。</p>
                <ul>
                    <li>✅ 5日乖离率 < 0：股价位于均线下方</li>
                    <li>✅ 量比 < 1.1：缩量表明抛压衰竭</li>
                    <li>✅ 20日乖离率 > -15%：中期趋势未完全崩坏</li>
                    <li>✅ 振幅 > 1.5%：保持基本活跃度</li>
                </ul>
            </div>
        </div>
        
        <div class="table-container">
            <table>
                {table_html}
            </table>
        </div>
        
        <div class="footer">
            <p>⚠️ 投资有风险，入市需谨慎。本报告仅供参考，不构成投资建议。</p>
            <p>Generated by Qlib Quant Strategy Engine</p>
        </div>
    </body>
    </html>
    """
    return html

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
    instruments = load_stock_pool(stock_pool)
    
    # --- B. 阶段 1：全特征初步训练 ---
    print("🚀 阶段 1：扫描全量特征（寻找反弹信号共振点）...")
    ds_v1 = DatasetH(
        handler=get_rebound_handler(hold_days)(instruments=instruments, start_time=start_train, end_time=pool_date),
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
        handler=get_rebound_handler(hold_days, refined_fields, refined_names)(instruments=instruments, start_time=start_train, end_time=pool_date),
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
    risk_df_raw = D.features(instruments, risk_fields, start_time=pool_date, end_time=pool_date)
    
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
    
    # 重命名方便过滤逻辑阅读
    final_table.columns = ['score', 'bias_5d', 'bias_20d', 'vol_ratio', 'amp_1d']

    # --- E. 核心风控逻辑：寻找跌到位后的缩量点 ---
    mask = (
        (final_table['bias_5d'] < -0.05) &         # 状态：在均线下方
        (final_table['vol_ratio'] < 1.1) &     # 状态：缩量，说明抛压衰竭
        (final_table['bias_20d'] > -0.15) &    # 状态：中期趋势未崩坏（防A字杀）
        (final_table['amp_1d'] > 0.015)        # 状态：保持基本活跃度
    )
    
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
        pool_date="2026-04-30",
        stock_pool=TRASH_POOL,
        data_path=DATA_PATH,
        hold_days=2,
        topk=6
    )
