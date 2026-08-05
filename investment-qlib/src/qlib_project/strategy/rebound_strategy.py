import os
import gc
import json
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
from pandas.tseries.offsets import BDay

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
from qlib_project.utils.backend_score_sender import build_backend_records_from_result, save_scores_to_backend

# ================== 存储路径 ==================
MODEL_DIR = PROJECT_ROOT / "data" / "models" / "rebound"
MODEL_DIR.mkdir(parents=True, exist_ok=True)
MODEL_FILE = MODEL_DIR / "lgb_rebound_model.pkl"
FEATURE_FILE = MODEL_DIR / "rebound_refined_features.json"

# ================== 共享因子表达式 (Handler 与 predict_daily 共用，避免重复定义) ==================
_REBOUND_EXTRA_FIELDS = [
    "$close / Mean($close, 5) - 1",          # 5日均线乖离率
    "$close / Mean($close, 20) - 1",         # 20日均线乖离率
    "$amount / Mean($amount, 5)",            # 5日成交额比（量能变化）
    "($high - $low) / $close",               # 日内振幅
    "($close - $open) / ($high - $low + 1e-12)",  # 实体K线比例
]
_REBOUND_EXTRA_NAMES = ["bias_5d", "bias_20d", "vol_ratio", "amp_1d", "body_ratio"]

# predict_daily 风控只需要前4个因子（bias_5d, bias_20d, vol_ratio, amp_1d）
_REBOUND_RISK_FIELDS = _REBOUND_EXTRA_FIELDS[:4]
_REBOUND_RISK_NAMES  = _REBOUND_EXTRA_NAMES[:4]

# ================== Qlib 初始化 (模块级，只执行一次) ==================
_qlib_initialized = False


def _ensure_qlib_init(data_path: str = None):
    """惰性初始化 Qlib，确保 data_path 与调用方一致。"""
    global _qlib_initialized
    if not _qlib_initialized:
        provider = data_path or DATA_PATH
        qlib.init(provider_uri=provider, region=REG_CN)
        _qlib_initialized = True
    else:
        # 已初始化，但允许切换到不同的 data_path（如 train 用不同数据源）
        from qlib.config import C
        current = C.get_data_path()
        target = str(data_path or DATA_PATH)
        if current != target:
            qlib.init(provider_uri=target, region=REG_CN)


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
        "min_data_in_leaf": 15,
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


def signal_ic(score, label) -> pd.DataFrame:
    """
    计算每日 IC 和 Rank IC。
    Qlib 内置的 evaluate 模块不提供 IC 计算，因此自己实现。
    适配 Qlib 标准 MultiIndex: (datetime, instrument)。
    """
    df = pd.concat([score.rename("score"), label.rename("label")], axis=1).dropna()
    if df.empty:
        return pd.DataFrame({"ic": [], "rank_ic": []})

    # Qlib 标准格式: MultiIndex (datetime, instrument)
    if isinstance(df.index, pd.MultiIndex):
        group = df.groupby(level=0)  # level=0 总是 datetime
    else:
        # 兜底: 单层索引按日期列分组
        if "datetime" in df.columns:
            group = df.groupby("datetime")
        else:
            return pd.DataFrame({"ic": [df["score"].corr(df["label"])],
                                 "rank_ic": [df["score"].rank().corr(df["label"].rank())]})

    ic_list, rank_ic_list = [], []
    for _, sub in group:
        if len(sub) < 2:
            ic_list.append(np.nan)
            rank_ic_list.append(np.nan)
        else:
            ic_list.append(sub["score"].corr(sub["label"]))
            rank_ic_list.append(sub["score"].rank().corr(sub["label"].rank()))
    return pd.DataFrame({"ic": ic_list, "rank_ic": rank_ic_list})

# ================== 2. 数据处理器 ==================
def get_rebound_handler(hold_days: int = 2, refined_fields=None, refined_names=None):
    # 预测目标：T日收盘买入，持仓 hold_days 天后在 T+hold_days 日收盘卖出的收益率
    # 注意：Ref($close, -N) 在 Qlib 中代表未来第 N 个交易日的收盘价
    label_expr = f"Ref($close, -{hold_days}) / $close - 1"
    
    class ReboundAlpha158(Alpha158):
        def get_label_config(self):
            return ([label_expr], ["LABEL_REBOUND"])
            
        def get_feature_config(self):
            # 如果传入了精炼后的特征，直接使用
            if refined_fields and refined_names:
                return refined_fields, refined_names

            conf = super().get_feature_config()
            conf[0].extend(_REBOUND_EXTRA_FIELDS)
            conf[1].extend(_REBOUND_EXTRA_NAMES)
            return conf

        def get_learn_processors(self):
            return [
                {"class": "ConfigSectionProcessor", "kwargs": {"fillna_label": True, "clip_label_outlier": True}},
                {"class": "CSZScoreNorm", "kwargs": {"fields_group": "feature"}}
            ]
            
    return ReboundAlpha158


def refine_features_no_leakage(handler_obj, segments, top_k=45):
    """
    无泄漏特征筛选：在 Train 段内部按 8:2 切分内部验证集，评估 Out-of-Fold 真实重要性。
    """
    train_start, train_end = segments["train"]
    train_start_dt = pd.to_datetime(train_start)
    train_end_dt = pd.to_datetime(train_end)
    split_dt = train_start_dt + (train_end_dt - train_start_dt) * 0.8
    
    # 构造内部 Segments，彻底隔绝外部真实 Valid/Test 阶段
    tmp_segments = {
        "train": (train_start, split_dt.strftime("%Y-%m-%d")),
        "valid": ((split_dt + pd.Timedelta(days=1)).strftime("%Y-%m-%d"), train_end)
    }
    ds = DatasetH(handler=handler_obj, segments=tmp_segments)
    
    tmp_model = LGBModel(
        n_estimators=300, 
        learning_rate=0.05, 
        max_depth=3, 
        subsample=0.8,
        colsample_bytree=0.6,        # 强制特征采样，防止强特征遮蔽次强特征
        early_stopping_rounds=30,    # 在内部 valid 上有效触发 early-stopping
        verbosity=-1
    )
    tmp_model.fit(ds)
    
    importance = tmp_model.model.feature_importance(importance_type='gain')
    fields, names = handler_obj.get_feature_config()
    
    df = pd.DataFrame({'name': names, 'field': fields, 'imp': importance}).sort_values('imp', ascending=False)
    
    # 补齐完整的 5 大核心反弹因子
    core_names = ["bias_5d", "bias_20d", "vol_ratio", "amp_1d", "body_ratio"]
    existing_core = [n for n in core_names if n in df['name'].values]
    
    non_core_k = max(0, top_k - len(existing_core))
    refined_df = df[~df['name'].isin(existing_core)].head(non_core_k)
    final_df = pd.concat([df[df['name'].isin(existing_core)], refined_df])
    
    return final_df['field'].tolist(), final_df['name'].tolist()


# ================== 4. 每周离线训练任务 ==================
def train_weekly(test_start_date: str, test_end_date: str, stock_pool: list, data_path: str, hold_days: int = 2):
    print(f"\n================ 启动防御型模型训练 (预测区间: {test_start_date} 至 {test_end_date}) ================")
    _ensure_qlib_init(data_path)
    
    # 2. 获取真实的交易日历列表 (涵盖 2021 到 2027，确保覆盖当前 2026 年及后续)
    cal = D.calendar(start_time="2021-01-01", end_time="2027-12-31")
    
    # 3. 锁定测试集开始日期在日历中的索引
    test_start_dt = pd.Timestamp(test_start_date)
    # 如果输入的 test_start_date 恰好是假期，searchsorted 会自动指向假期后的第一个交易日
    test_start_idx = np.searchsorted(cal, test_start_dt)
    
    # 4. 严格按照交易日坐标轴（Index）往前推，彻底拉开 hold_days + 1 的安全隔离带
    # 验证集结束日：测试集开始前第 (hold_days + 1) 个交易日
    valid_end_idx = test_start_idx - (hold_days + 1)
    
    # 验证集长度：用交易日来衡量更精准，80个交易日大约相当于 4 个月的自然日
    valid_len_trades = 80 
    valid_start_idx = valid_end_idx - valid_len_trades
    
    # 训练集结束日：验证集开始前第 (hold_days + 1) 个交易日
    train_end_idx = valid_start_idx - (hold_days + 1)
    
    # 5. 将索引还原为标准的日期字符串
    start_train = "2021-01-01" 
    train_end_date_actual = cal[train_end_idx].strftime('%Y-%m-%d')
    valid_start = cal[valid_start_idx].strftime('%Y-%m-%d')
    valid_end = cal[valid_end_idx].strftime('%Y-%m-%d')
    
    # 严格对齐时间分段
    segments = {
        "train": (start_train, train_end_date_actual), 
        "valid": (valid_start, valid_end),
        "test": (test_start_date, test_end_date) 
    }
    
    print(f"📅 [数据划分诊断]：")
    print(f"   - Train 期间: {segments['train'][0]} ~ {segments['train'][1]}")
    print(f"   - 🛡️ 隔离带 1 : 间隔 {valid_start_idx - train_end_idx} 个真实交易日")
    print(f"   - Valid 期间: {segments['valid'][0]} ~ {segments['valid'][1]}")
    print(f"   - 🛡️ 隔离带 2 : 间隔 {test_start_idx - valid_end_idx} 个真实交易日")
    print(f"   - Test  期间: {segments['test'][0]} ~ {segments['test'][1]}")

    # ================= 后续逻辑保持不变 =================
    stock_in = load_stock_pool(stock_pool)

    # --- 阶段 1：特征精炼（防泄漏模式） ---
    print("🚀 [阶段 1]：特征初筛（仅基于历史 Train 段）...")
    base_handler = get_rebound_handler(hold_days)(instruments=stock_in, start_time=start_train, end_time=train_end_date_actual)
    refined_fields, refined_names = refine_features_no_leakage(base_handler, segments, top_k=45)
    
    # --- 阶段 2：最终模型训练 ---
    print(f"🚀 [阶段 2]：精炼拟合（保留 {len(refined_names)} 个核心特征）...")
    final_handler = get_rebound_handler(hold_days, refined_fields, refined_names)(
        instruments=stock_in, start_time=start_train, end_time=test_end_date
    )
    ds_final = DatasetH(handler=final_handler, segments=segments)
    
    model_params = get_rebound_model_params(hold_days, refined_names)
    final_model = LGBModel(**model_params)
    final_model.fit(ds_final)
    
    # --- 阶段 3：效能评估 ---
    valid_pred = final_model.predict(ds_final, segment="valid")
    valid_label = ds_final.prepare(segments="valid", col_set="label")
    if isinstance(valid_pred, pd.Series): valid_pred = valid_pred.to_frame("score")
    
    valid_combined = pd.concat([valid_pred, valid_label], axis=1).dropna()
    if not valid_combined.empty:
        ic = signal_ic(valid_combined.iloc[:, 0], valid_combined.iloc[:, 1])
        print(f"📈 验证集 IC: {ic['ic'].mean():.4f}, Rank IC: {ic['rank_ic'].mean():.4f}")

    # --- 阶段 4：生成测试集信号并保存 ---
    print(f"🚀 [阶段 4]：正在生成回测区间的预测信号 ({test_start_date} ~ {test_end_date})...")
    test_pred = final_model.predict(ds_final, segment="test")
    if isinstance(test_pred, pd.Series): test_pred = test_pred.to_frame("score")
        
    SIGNAL_FILE = MODEL_FILE.parent / "lgb_rebound_pred.pkl"
    print(f"📊 预测信号生成完毕，总行数: {len(test_pred)}")
    
    test_pred.to_pickle(str(SIGNAL_FILE))
    final_model.to_pickle(str(MODEL_FILE))
    with open(FEATURE_FILE, 'w', encoding='utf-8') as f:
        json.dump({"fields": refined_fields, "names": refined_names}, f, ensure_ascii=False)
    
    print(f"✅ 模型与信号全部训练准备完成！\n")


# ================== 5. 每日推断任务 ==================
def predict_daily(pool_date: str, stock_pool: list, data_path: str, hold_days: int = 2, topk: int = 6):
    print(f"\n================ 执行每日阻击预测 ({pool_date}) ================")
    _ensure_qlib_init(data_path)
    stock_in = load_stock_pool(stock_pool)
    
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
    
    model = LGBModel.load(MODEL_FILE)

    # 3. 预测
    lookback = (pd.Timestamp(pool_date) - pd.Timedelta(days=120)).strftime('%Y-%m-%d')
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

    # --- E. 行情风控数据清洗 (使用共享常量，与 Handler 保持一致) ---
    risk_df_raw = D.features(stock_in, _REBOUND_RISK_FIELDS, start_time=pool_date, end_time=pool_date)
    
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
    risk_data.columns = _REBOUND_RISK_NAMES
    risk_data.index = risk_data.index.astype(str).str.upper().str.strip()

    print(f"DEBUG: 行情风控表(risk_data)样本数: {len(risk_data)}")
    if len(risk_data) > 0:
        print(f"DEBUG: 修正后的 Risk ID 示例: '{risk_data.index[0]}'")

    # --- F. 合并 + 自适应筛选 ---
    final = scores[['score']].join(risk_data[['bias_5d', 'bias_20d', 'vol_ratio', 'amp_1d']], how='inner')
    print(f"DEBUG: 参与形态过滤的股票总数 (Join后): {len(final)}")

    # 两层筛选替代固定阈值：
    # 1. 质量门: 排除成交量异常放大的股票（放量杀跌难以判断承接力）
    candidates = final[final['vol_ratio'] < 1.5].copy()
    print(f"DEBUG: 量比<1.5 后剩余: {len(candidates)} 只")

    # 2. 自适应超跌: 取候选池中偏5d最"超跌"的底部分位，而非硬阈值
    #    上涨市取相对最弱的，下跌市自然收敛到真正的超跌股
    bias_threshold = candidates['bias_5d'].quantile(0.30)  # 底部 30%
    oversold = candidates[candidates['bias_5d'] < bias_threshold]
    print(f"DEBUG: bias_5d 底部30%阈值={bias_threshold:.4f}, 剩余: {len(oversold)} 只")

    result = oversold.sort_values('score', ascending=False).head(topk)

    print(f"\n✅ {pool_date} 阻击名单 (Join 数: {len(final)}, 选出: {len(result)}):")
    if not result.empty:
        print(result[['score', 'bias_5d', 'vol_ratio']])

        backend_records = build_backend_records_from_result(result, pool_date)
        backend_response = save_scores_to_backend(backend_records)
        if backend_response.get("success"):
            print(f"📤 后端批量保存成功，计数: {backend_response.get('count', len(backend_records))}")
        else:
            print(f"⚠️ 后端批量保存未成功: {backend_response.get('message', 'unknown error')}")

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
    # 用于每日阻击推断的日期
    parser.add_argument("--date", type=str, default=pd.Timestamp.now().strftime('%Y-%m-%d'))
    # 用于控制回测信号生成区间的起止日期
    parser.add_argument("--test_start", type=str, default="2025-12-15", help="回测的开始日期")
    parser.add_argument("--test_end", type=str, default="2026-05-15", help="回测的结束日期")
    args = parser.parse_args()

    if args.mode == "train":
        # 如果是跑回测模型训练，严格使用 test_start 和 test_end 来划定信号生成区间
        train_weekly(
            test_start_date=args.test_start, 
            test_end_date=args.test_end, 
            stock_pool=TRASH_POOL, 
            data_path=DATA_PATH, 
            hold_days=2
        )
    else:
        # 如果是跑实盘或单日预测，依旧沿用单日 date 参数
        predict_daily(args.date, TRASH_POOL, DATA_PATH)