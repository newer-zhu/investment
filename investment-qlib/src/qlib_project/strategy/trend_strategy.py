"""
趋势跟踪策略：动量延续
与超跌反弹相反——追涨杀跌，买已经涨的赌继续涨
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np
import qlib
from qlib.config import REG_CN
from qlib.data.dataset import DatasetH
from qlib.contrib.model.gbdt import LGBModel
from qlib.data import D

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
SRC_ROOT = PROJECT_ROOT.parent
for path in (str(BASE_DIR), str(PROJECT_ROOT), str(SRC_ROOT)):
    if path not in sys.path:
        sys.path.insert(0, path)

from qlib_project.constants import (
    DATA_PATH, CONFIG_PATH, BASE_POOL,
    TREND_MODEL_DIR, TREND_SIGNAL_FILE, TREND_PREDICTIONS_DIR,
)
from qlib_project.utils.util import load_stock_pool, send_email, load_config_from_ini
from qlib_project.utils.email_report_utils import generate_trend_report_html
from qlib_project.utils.backend_score_sender import build_backend_records_from_result, save_scores_to_backend
from qlib_project.select.select_trend import filter_by_trend, get_mainboard_universe

TREND_MODEL_DIR.mkdir(parents=True, exist_ok=True)

_qlib_initialized = False


def _ensure_qlib_init(data_path=None):
    global _qlib_initialized
    if not _qlib_initialized:
        qlib.init(provider_uri=data_path or DATA_PATH, region=REG_CN)
        _qlib_initialized = True


def _limit_up_threshold(code: str) -> float:
    """按板块返回涨停涨幅阈值(留少量舍入余量)"""
    c = code.upper()
    # 创业板 / 科创板: 20%
    if c.startswith(("SZ300", "SZ301", "SH688", "SH689")):
        return 0.198
    # 北交所: 30%
    if c.startswith(("BJ4", "BJ8", "BJ9")):
        return 0.298
    # 主板(60/00/001/002/003): 10%
    return 0.098


def _features_single_day(codes, fields, trade_date, names):
    """安全取单日行情: 处理 D.features 的 MultiIndex (instrument, datetime),
    返回以 instrument 为索引、列名为 names 的 DataFrame。与 backtest_trend 同款。

    注意: 必须按 fields 传入顺序重命名列 —— D.features 返回的列即 fields 顺序,
    而 columns.difference() 会返回排序后的列, 若按其 zip 重命名会错位(历史 bug)。
    """
    if not codes:
        return pd.DataFrame(columns=names)
    df = D.features(codes, fields, start_time=trade_date, end_time=trade_date)
    if df is None or df.empty:
        return pd.DataFrame(columns=names)
    df = df.reset_index()
    df["instrument"] = df["instrument"].astype(str).str.upper()
    # D.features 列顺序 == fields 顺序: 去掉前两个索引列后按原序重命名
    field_cols = list(df.columns)[2:len(names) + 2]
    df = df.rename(columns=dict(zip(field_cols, names)))
    return df.set_index("instrument")[names]


def apply_trend_buy_filter(scores, asof_date, topk=2, min_price=1.0, vol_ratio_max=1.5):
    """回测/实盘共用的趋势买入过滤 (单一事实来源, 与 backtest_trend 的买入门完全一致)。

    scores: 当日信号 (Series 或含 'score' 列的 DataFrame), index 为 instrument。
    asof_date: 决策基准日 (用该日收盘价做过滤, 无前视)。
    返回: (topk_result, n_candidates)。
      - topk_result: 已按 score 降序截取 topk 的 DataFrame, 含
        score/bias_5d/bias_20d/close_price/vol_ratio/ret_real
      - n_candidates: 通过质量门(价格/量比/涨停剔除)的候选数
    """
    # 兼容 MultiIndex (datetime, instrument): 取 instrument 层作为股票代码索引
    if isinstance(scores.index, pd.MultiIndex):
        scores = scores.copy()
        scores.index = scores.index.get_level_values(-1)
    instruments = list(scores.index)
    feats = _features_single_day(
        instruments,
        [
            "$close / Mean($close, 5) - 1",
            "$close / Mean($close, 20) - 1",
            "$close",
            "$amount / Mean($amount, 5)",
            "($close / Ref($close, 1)) * (Ref($factor, 1) / $factor) - 1",
        ],
        asof_date,
        ["bias_5d", "bias_20d", "close_price", "vol_ratio", "ret_real"],
    )
    if feats.empty:
        return pd.DataFrame(columns=["score", "bias_5d", "bias_20d",
                                     "close_price", "vol_ratio", "ret_real"]), 0
    sc = scores["score"] if isinstance(scores, pd.DataFrame) else scores.rename("score")
    combined = pd.DataFrame({"score": sc}).join(feats, how="inner")
    if combined.empty:
        return combined, 0

    # --- 质量门: 价格下限 / 量比 / 剔除当日涨停(买不进) ---
    lim = combined.index.map(_limit_up_threshold)
    lim = pd.Series(lim.values, index=combined.index)
    candidates = combined[
        (combined["close_price"] > min_price) &
        (combined["vol_ratio"] < vol_ratio_max) &
        (combined["ret_real"] < (lim - 0.002))
    ].copy()
    if candidates.empty:
        return candidates, 0

    # --- 趋势门: 5日乖离顶部分位 + 决策日仍收涨(动量未断) ---
    top_q = candidates["bias_5d"].quantile(0.70)
    safe = candidates[
        (candidates["bias_5d"] > max(top_q, 0.02)) &
        (candidates["ret_real"] > 0)
    ]
    ranked = safe["score"].dropna().sort_values(ascending=False)
    return safe.loc[ranked.head(topk).index], len(candidates)


# ================== 纯动量 Feature Handler ==================
# 不再继承 Alpha158（含大量均值回归因子如 RSV/RANK/RSI/SUMP）,
# 改为直接继承 DataHandlerLP，只使用动量/趋势类因子。
from qlib.data.dataset.handler import DataHandlerLP
from qlib.data.dataset.processor import Processor
from qlib.utils import get_callable_kwargs
from qlib.data.dataset import processor as processor_module
from inspect import getfullargspec

_DEFAULT_TREND_LEARN_PROCESSORS = [
    {"class": "DropnaLabel"},
    {"class": "CSZScoreNorm", "kwargs": {"fields_group": "label"}},
]

# 经典动量突破因子（纯动量，无均值回归）
_TREND_MOMENTUM_FIELDS = [
    # --- 1. 价格动量 (ROC) ---
    "$close / Ref($close, 5) - 1",
    "$close / Ref($close, 10) - 1",
    "$close / Ref($close, 20) - 1",
    "$close / Ref($close, 30) - 1",
    "$close / Ref($close, 60) - 1",
    # --- 2. 均线多头排列度 ---
    "Mean($close, 5) / Mean($close, 10) - 1",
    "Mean($close, 5) / Mean($close, 20) - 1",
    "Mean($close, 10) / Mean($close, 20) - 1",
    "Mean($close, 10) / Mean($close, 30) - 1",
    "Mean($close, 20) / Mean($close, 60) - 1",
    # --- 3. 趋势强度 (斜率 & R²) ---
    "Slope($close, 10) / $close",
    "Slope($close, 20) / $close",
    "Rsquare($close, 10)",
    "Rsquare($close, 20)",
    # --- 4. 波动率 ---
    "Std($close, 5) / $close",
    "Std($close, 10) / $close",
    "Std($close, 20) / $close",
    # --- 5. 量能趋势 ---
    "$volume / Mean($volume, 5)",
    "$volume / Mean($volume, 10)",
    "$volume / Mean($volume, 20)",
    # --- 6. 量价配合 ---
    "Corr($close, Log($volume+1), 10)",
    "Corr($close, Log($volume+1), 20)",
    # --- 7. 趋势一致性 (涨跌天数差) ---
    "Mean($close > Ref($close, 1), 5)",
    "Mean($close > Ref($close, 1), 10)",
    "Mean($close > Ref($close, 1), 20)",
    "Mean($close > Ref($close, 1), 5) - Mean($close < Ref($close, 1), 5)",
    "Mean($close > Ref($close, 1), 10) - Mean($close < Ref($close, 1), 10)",
    # --- 8. 动量突破 (距N日高点的距离) ---
    "$close / Max($high, 10) - 1",
    "$close / Max($high, 20) - 1",
    "$close / Max($high, 60) - 1",
    # --- 9. 动量加速度 ---
    "(Ref($close, -5) - Ref($close, -10)) / Ref($close, -10)",
    "(Ref($close, -10) - Ref($close, -20)) / Ref($close, -20)",
    # --- 10. 上影线 & 实体比例 (趋势质量) ---
    "($high - $close) / $close",
    "($close - $open) / ($high - $low + 1e-12)",
    # --- 11. K线形态 ---
    "($close - $open) / $open",
    "($high - $low) / $open",
    "($high - Greater($open, $close)) / $open",
    "(Less($open, $close) - $low) / $open",
    # --- 12. 原始价格 (5日窗口, 归一化) ---
    "$open / $close",
    "$high / $close",
    "$low / $close",
    "Ref($open, 1) / $close",
    "Ref($high, 1) / $close",
    "Ref($low, 1) / $close",
    "Ref($open, 2) / $close",
    "Ref($high, 2) / $close",
    "Ref($low, 2) / $close",
    # --- 13. 原始成交量 ---
    "Ref($volume, 1) / ($volume + 1e-12)",
    "Ref($volume, 2) / ($volume + 1e-12)",
    "Ref($volume, 3) / ($volume + 1e-12)",
]

_TREND_MOMENTUM_NAMES = [
    # 1. ROC
    "ROC5", "ROC10", "ROC20", "ROC30", "ROC60",
    # 2. MA crossover
    "MA5_10", "MA5_20", "MA10_20", "MA10_30", "MA20_60",
    # 3. Slope & R²
    "SLOPE10", "SLOPE20", "RSQR10", "RSQR20",
    # 4. Volatility
    "STD5", "STD10", "STD20",
    # 5. Volume trend
    "VOL_RATIO5", "VOL_RATIO10", "VOL_RATIO20",
    # 6. Price-Volume correlation
    "CORR10", "CORR20",
    # 7. Trend consistency
    "UP_PCT5", "UP_PCT10", "UP_PCT20", "UP_DN_DIFF5", "UP_DN_DIFF10",
    # 8. Breakout
    "NEAR_HIGH10", "NEAR_HIGH20", "NEAR_HIGH60",
    # 9. Acceleration
    "ROC_ACCEL5", "ROC_ACCEL10",
    # 10. Candle quality
    "UPPER_SHADOW", "BODY_RATIO",
    # 11. Kbar
    "RET_1D", "RANGE_1D", "KUP", "KLOW",
    # 12. Raw price
    "OPEN0", "HIGH0", "LOW0", "OPEN1", "HIGH1", "LOW1", "OPEN2", "HIGH2", "LOW2",
    # 13. Raw volume
    "VOL1", "VOL2", "VOL3",
]


def _check_transform_proc(proc_l, fit_start_time, fit_end_time):
    """与 Alpha158 相同的 processor 预处理逻辑"""
    new_l = []
    for p in proc_l:
        if not isinstance(p, Processor):
            klass, pkwargs = get_callable_kwargs(p, processor_module)
            args = getfullargspec(klass).args
            if "fit_start_time" in args and "fit_end_time" in args:
                if fit_start_time is not None and fit_end_time is not None:
                    pkwargs.update({"fit_start_time": fit_start_time, "fit_end_time": fit_end_time})
            proc_config = {"class": klass.__name__, "kwargs": pkwargs}
            if isinstance(p, dict) and "module_path" in p:
                proc_config["module_path"] = p["module_path"]
            new_l.append(proc_config)
        else:
            new_l.append(p)
    return new_l


class MomentumTrendHandler(DataHandlerLP):
    """纯动量 Feature Handler，不含任何均值回归因子。

    与 Alpha158 的区别：
    - 去掉 RSV/RANK/SUMP/SUMN/SUMD (RSI 类均值回归)
    - 去掉 QTLU/QTLD/MAX/MIN (支撑阻力)
    - 去掉 RESI (残差回归)
    - 去掉 IMAX/IMIN/IMXD (Aroon 反转)
    - 增加动量突破、加速度、均线多头排列等纯趋势因子
    """

    def __init__(
        self,
        instruments="csi500",
        start_time=None,
        end_time=None,
        freq="day",
        infer_processors=None,
        learn_processors=None,
        fit_start_time=None,
        fit_end_time=None,
        process_type=DataHandlerLP.PTYPE_A,
        filter_pipe=None,
        inst_processors=None,
        label_expr=None,
        label_name=None,
        **kwargs,
    ):
        if infer_processors is None:
            infer_processors = []
        if learn_processors is None:
            learn_processors = _DEFAULT_TREND_LEARN_PROCESSORS

        infer_processors = _check_transform_proc(infer_processors, fit_start_time, fit_end_time)
        learn_processors = _check_transform_proc(learn_processors, fit_start_time, fit_end_time)

        data_loader = {
            "class": "QlibDataLoader",
            "kwargs": {
                "config": {
                    "feature": (_TREND_MOMENTUM_FIELDS, _TREND_MOMENTUM_NAMES),
                    "label": (
                        kwargs.pop("label", [label_expr]),
                        kwargs.pop("label_name", [label_name or "LABEL_TREND"]),
                    ),
                },
                "filter_pipe": filter_pipe,
                "freq": freq,
                "inst_processors": inst_processors,
            },
        }
        super().__init__(
            instruments=instruments,
            start_time=start_time,
            end_time=end_time,
            data_loader=data_loader,
            infer_processors=infer_processors,
            learn_processors=learn_processors,
            process_type=process_type,
            **kwargs,
        )


def get_trend_handler(hold_days=5, refined_fields=None, refined_names=None):
    """创建趋势策略 Handler。feat_conf 用于精炼特征回传。"""
    label_expr = f"Ref($open, -{hold_days+1}) / Ref($open, -1) - 1"

    class TrendHandler(MomentumTrendHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, label_expr=label_expr, label_name="LABEL_TREND", **kwargs)

        def get_feature_config(self):
            if refined_fields and refined_names:
                return refined_fields, refined_names
            return _TREND_MOMENTUM_FIELDS, _TREND_MOMENTUM_NAMES

    return TrendHandler


# ================== 模型参数 ==================
def get_trend_model_params(feature_names=None):
    params = {
        "objective": "regression",
        "metric": "rmse",
        "n_jobs": -1,
        "verbosity": -1,
        "early_stopping_rounds": 50,
        "n_estimators": 500,
        "learning_rate": 0.03,
        "max_depth": 4,
        "num_leaves": 15,
        "lambda_l1": 0.5,
        "lambda_l2": 0.5,
        "bagging_fraction": 0.8,
        "feature_fraction": 0.8,
        "min_data_in_leaf": 20,
    }
    return params


# ================== 信号 IC ==================
def signal_ic(score, label) -> pd.DataFrame:
    df = pd.concat([score.rename("score"), label.rename("label")], axis=1).dropna()
    if df.empty:
        return pd.DataFrame({"ic": [], "rank_ic": []})
    if isinstance(df.index, pd.MultiIndex):
        group = df.groupby(level=0)
    elif "datetime" in df.columns:
        group = df.groupby("datetime")
    else:
        return pd.DataFrame({"ic": [df["score"].corr(df["label"])],
                             "rank_ic": [df["score"].rank().corr(df["label"].rank())]})
    ic_list, rk_list = [], []
    for _, sub in group:
        if len(sub) < 2:
            ic_list.append(np.nan); rk_list.append(np.nan)
        else:
            ic_list.append(sub["score"].corr(sub["label"]))
            rk_list.append(sub["score"].rank().corr(sub["label"].rank()))
    return pd.DataFrame({"ic": ic_list, "rank_ic": rk_list})


# ================== 单周滚动周期 (roll 与 predict 共用) ==================
def _run_week_cycle(base_pool, ws, we, test_end, hold_days, cal):
    """单周完整流程: 每周重新趋势选股 + 滚动训练当周模型 + 预测当周, 无前视。

    roll 回测与 predict 实盘都调用它, 保证信号来源与池子刷新逻辑完全一致。
    ws/we: 与 roll 相同的周窗口边界 (周一 与 下周一前一天/回测结束日)。
    test_end: 测试段截断日 (与 roll 的 min(cal[we_idx], test_end) 一致)。
    返回 dict (ws/we/cycle_pool/as_of_date/segments/pred); 无有效窗口时返回 None。
    """
    ws_idx = np.searchsorted(cal, ws)
    we_idx = min(np.searchsorted(cal, we), len(cal) - 1)   # 日期可能超出数据末尾, 钳制防越界
    if ws_idx >= len(cal):
        return None
    aws = cal[ws_idx]
    awe = min(cal[we_idx], pd.Timestamp(test_end))
    if aws > awe:
        return None

    ws_str = aws.strftime('%Y-%m-%d')
    we_str = awe.strftime('%Y-%m-%d')

    valid_end_idx = ws_idx - (hold_days + 2)
    valid_start_idx = valid_end_idx - 80
    train_end_idx = valid_start_idx - (hold_days + 2)
    if train_end_idx < 0:
        return None

    # ---- 每周重新选股 (实盘无前视) ----
    # 用"本周开始前最近交易日"作筛选基准, 再结合滚动训练段(也都在本周之前)训练模型。
    as_of_idx = max(ws_idx - 1, 0)
    as_of_date = cal[as_of_idx]
    cycle_pool = filter_by_trend(base_pool, as_of_date=as_of_date)
    if not cycle_pool:
        cycle_pool = base_pool   # 筛选异常/为空时兜底

    segments = {
        "train": ("2021-01-01", cal[train_end_idx].strftime('%Y-%m-%d')),
        "valid": (cal[valid_start_idx].strftime('%Y-%m-%d'),
                  cal[valid_end_idx].strftime('%Y-%m-%d')),
        "test": (ws_str, we_str),
    }

    handler_cls = get_trend_handler(hold_days)
    handler = handler_cls(instruments=cycle_pool,
                          start_time="2021-01-01",
                          end_time=segments["test"][1])  # 必须覆盖到 test 段
    ds = DatasetH(handler=handler, segments=segments)
    model = LGBModel(**get_trend_model_params())
    model.fit(ds)

    pred = model.predict(ds, segment="test")
    if isinstance(pred, pd.Series):
        pred = pred.to_frame("score")
    return {
        "ws": ws_str, "we": we_str,
        "cycle_pool": cycle_pool,
        "as_of_date": as_of_date,
        "segments": segments,
        "pred": pred,
    }


# ================== 滚动训练 ==================
def roll_train(test_start, test_end, stock_pool_path, data_path, hold_days=5):
    print(f"\n{'='*60}")
    print(f"📈 趋势滚动训练: {test_start} → {test_end}")
    print(f"{'='*60}")
    _ensure_qlib_init(data_path)

    cal = D.calendar(start_time="2021-01-01", end_time="2027-12-31")
    test_start_dt = pd.Timestamp(test_start)
    test_end_dt = pd.Timestamp(test_end)

    week_starts = pd.date_range(start=test_start_dt, end=test_end_dt, freq="W-MON")
    if len(week_starts) == 0:
        week_starts = [test_start_dt]

    all_predictions = []
    # 候选池: 优先用传入的基础池(仅主板/行业过滤, 无动态量价过滤), 否则退回全主板。
    # 每个周期在此候选内按"该周期开始时点"的行情重新做趋势筛选 → 模拟实盘每周刷新股票池。
    if stock_pool_path and Path(stock_pool_path).exists():
        base_pool = load_stock_pool(stock_pool_path)
        print(f"  📦 基础候选池: {len(base_pool)} 只 (来自 {Path(stock_pool_path).name})")
    else:
        base_pool = get_mainboard_universe(
            cal[0].strftime('%Y-%m-%d'), cal[-1].strftime('%Y-%m-%d'))
        print(f"  📦 基础候选池: {len(base_pool)} 只 (全主板)")

    for i, ws in enumerate(week_starts):
        if i + 1 < len(week_starts):
            we = week_starts[i + 1] - pd.Timedelta(days=1)
        else:
            we = test_end_dt

        cycle = _run_week_cycle(base_pool, ws, we, test_end_dt, hold_days, cal)
        if cycle is None:
            continue
        print(f"  [{i+1}/{len(week_starts)}] {cycle['ws']}~{cycle['we']}: "
              f"趋势池 {len(cycle['cycle_pool'])} 只 "
              f"(基准 {pd.Timestamp(cycle['as_of_date']).date()})")
        all_predictions.append(cycle["pred"])
        print(f"       {cycle['ws']}~{cycle['we']}: {len(cycle['pred'])} 条信号")

    final_pred = pd.concat(all_predictions)
    final_pred = final_pred[~final_pred.index.duplicated(keep='first')].sort_index()

    final_pred.to_pickle(str(TREND_SIGNAL_FILE))
    print(f"\n✅ 趋势滚动训练完成: {len(final_pred)} 条 → {TREND_SIGNAL_FILE}")


# ================== 实盘单日预测 ==================
def predict_day(date_str, stock_pool_path, data_path, hold_days=5, topk=6,
                use_market_filter=True):
    """实盘单日推荐: 与 roll 回测同一逻辑 (当周重训 + 每周重筛池 + 回测同款过滤)。

    输出 = backtest_trend 在对应执行日看到的买入候选:
      - 用"目标日期权威信号所在周"的模型(训练数据只到本周之前, 无前视)
      - 股票池 = 基础池 + 本周开始时点 filter_by_trend
      - 过滤 = apply_trend_buy_filter (与回测买入门完全一致)
      - 大盘<MA20 暂停买入 (与回测 use_market_filter 一致)
    """
    _ensure_qlib_init(data_path)

    cal = D.calendar(start_time="2021-01-01", end_time="2027-12-31")
    target = pd.Timestamp(date_str)

    # 基础候选池: 与 roll 相同口径
    if stock_pool_path and Path(stock_pool_path).exists():
        base_pool = load_stock_pool(stock_pool_path)
        print(f"  📦 基础候选池: {len(base_pool)} 只 (来自 {Path(stock_pool_path).name})")
    else:
        base_pool = get_mainboard_universe(
            cal[0].strftime('%Y-%m-%d'), cal[-1].strftime('%Y-%m-%d'))
        print(f"  📦 基础候选池: {len(base_pool)} 只 (全主板)")

    # 数据源可能滞后: 回退到最近有特征数据的交易日 (决策基准日 asof)
    _recent = [d for d in cal if d <= target][-5:]
    if not _recent:
        _recent = list(cal)[-5:]
    asof_date = None
    for _day in reversed(_recent):
        _probe = D.features(base_pool[:20], ["$close"], start_time=_day, end_time=_day)
        if _probe is not None and not _probe.empty:
            asof_date = _day
            if _day != target:
                print(f"⚠️ {date_str} 无特征数据, 回退到 {_day.date()}")
            break
    if asof_date is None:
        print("❌ 无法获取行情数据 (目标日期无特征数据)")
        return None
    print(f"\n📈 趋势预测, 决策基准日: {pd.Timestamp(asof_date).date()}")

    # 大盘 MA20 风控 (与回测 use_market_filter 一致): 大盘<MA20 暂停买入
    if use_market_filter:
        mkt = D.features(["SH000300"], ["$close", "Mean($close, 20)"],
                         start_time=asof_date, end_time=asof_date)
        if mkt is not None and not mkt.empty:
            row = mkt.iloc[0]
            if row["$close"] < row["Mean($close, 20)"]:
                print(f"⚠️ 大盘在 MA20 下方 ({pd.Timestamp(asof_date).date()}), 暂停买入 (与回测一致)")
                return None
        else:
            print("⚠️ 无法获取大盘数据, 跳过 MA20 风控")

    # 定位"权威信号"所在周 (与 roll 一致):
    # roll 的周测试段为 [周一, 下周一], 周一那天的信号由"上一周模型"产出(dedup keep='first'),
    # 其余日期由"本周模型"产出。这里复现同样的周归属, 保证 predict 与 roll 信号同源。
    monday_of = asof_date - pd.Timedelta(days=asof_date.weekday())
    if asof_date == monday_of:
        anchor = monday_of - pd.Timedelta(days=7)   # 周一 → 上一周模型
    else:
        anchor = monday_of                          # 非周一 → 本周模型
    next_monday = anchor + pd.Timedelta(days=7)

    cycle = _run_week_cycle(
        base_pool, anchor, next_monday - pd.Timedelta(days=1),
        test_end=next_monday, hold_days=hold_days, cal=cal,
    )
    if cycle is None:
        print("❌ 当周无有效训练/预测窗口 (数据不足)")
        return None
    pred = cycle["pred"]

    # 取 <= asof 的最新信号 (与回测 'latest signal <= asof' 一致)
    sig = pred[pred.index.get_level_values("datetime") <= asof_date]
    if sig.empty:
        print("❌ 目标日期无可用信号")
        return None
    latest_sig_day = sig.index.get_level_values("datetime").max()
    sig_day = sig[sig.index.get_level_values("datetime") == latest_sig_day]
    day_scores = sig_day["score"]
    day_scores.index = sig_day.index.get_level_values("instrument")

    result, n_candidates = apply_trend_buy_filter(
        day_scores, asof_date=asof_date, topk=topk)
    if result.empty:
        print("❌ 过滤后无候选 (当日无符合条件的趋势股)")
        return None

    result = result.copy()
    result.index = result.index.astype(str).str.upper().str.strip()
    result = result.sort_values("score", ascending=False).head(topk)
    pick_date = pd.Timestamp(asof_date).strftime('%Y-%m-%d')

    print(f"\n✅ 趋势推荐 Top{len(result)} ({pick_date}):")
    print(result[['score', 'bias_5d', 'bias_20d', 'vol_ratio']].to_string())

    # 保存
    TREND_PREDICTIONS_DIR.mkdir(parents=True, exist_ok=True)
    out_csv = TREND_PREDICTIONS_DIR / f"trend_picks_{pick_date}.csv"
    result[['score', 'bias_5d', 'bias_20d', 'vol_ratio']].to_csv(out_csv)
    print(f"\n💾 已保存至: {out_csv}")

    # 后端批量保存
    backend_records = build_backend_records_from_result(result, pick_date)
    backend_response = save_scores_to_backend(backend_records)
    if backend_response.get("success"):
        print(f"📤 后端批量保存成功，计数: {backend_response.get('count', len(backend_records))}")
    else:
        print(f"⚠️ 后端批量保存未成功: {backend_response.get('message', 'unknown error')}")

    # 发送邮件报告
    try:
        print(f"🔍 配置文件路径: {CONFIG_PATH}")
        _email_cfg = load_config_from_ini("email", str(CONFIG_PATH))
        TO_EMAILS = [e.strip() for e in _email_cfg.get("to_emails", "").split(",") if e.strip()] or [_email_cfg.get("to_email", "")]
        FROM_EMAIL = _email_cfg.get("from_email", "")
        FROM_PASSWORD = _email_cfg.get("from_password", "")
        SMTP_SERVER = _email_cfg.get("smtp_server", "smtp.qq.com")
        SMTP_PORT = int(_email_cfg.get("smtp_port", "587"))

        print(f"📧 邮件配置 - 发件人: {FROM_EMAIL}")
        print(f"📧 邮件配置 - 收件人: {TO_EMAILS}")
        print(f"📧 邮件配置 - SMTP: {SMTP_SERVER}:{SMTP_PORT}")

        html_body = generate_trend_report_html(pick_date, result, n_candidates)
        subject = f"📈 趋势跟踪策略报告 - {pick_date}"

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

    return result


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, choices=["roll", "predict"], default="predict")
    parser.add_argument("--date", type=str, default=pd.Timestamp.now().strftime('%Y-%m-%d'))
    parser.add_argument("--test_start", type=str, default="2026-02-01")
    parser.add_argument("--test_end", type=str, default="2026-07-20")
    parser.add_argument("--hold_days", type=int, default=5)
    parser.add_argument("--topk", type=int, default=6)
    parser.add_argument("--market-filter", type=int, default=1, help="大盘MA20风控(1开0关, 与回测一致)")
    args = parser.parse_args()

    if args.mode == "roll":
        # 基础池传 BASE_POOL(仅主板/行业过滤, 不含动态量价过滤):
        # roll 内部每周按当时行情重新趋势选股; 若文件不存在则自动退回全主板候选。
        roll_train(args.test_start, args.test_end, BASE_POOL, DATA_PATH, args.hold_days)
    elif args.mode == "predict":
        # ===== 单日预测模式: 与 roll 回测同一逻辑 =====
        # 每次运行训练"目标日期权威信号所在周"的模型(训练数据只到本周之前), 无前视。
        predict_day(args.date, BASE_POOL, DATA_PATH, args.hold_days, args.topk,
                    use_market_filter=bool(args.market_filter))
