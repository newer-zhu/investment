"""
趋势跟踪策略：动量延续
与超跌反弹相反——追涨杀跌，买已经涨的赌继续涨
"""
import json
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
    DATA_PATH, CONFIG_PATH, TREND_POOL, BASE_POOL,
    TREND_MODEL_DIR, TREND_MODEL_FILE, TREND_FEATURE_FILE,
    TREND_SIGNAL_FILE, TREND_PREDICTIONS_DIR,
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


# ================== 训练 ==================
def train_once(test_start, test_end, stock_pool_path, data_path, hold_days=5):
    print(f"\n{'='*60}")
    print(f"📈 趋势跟踪模型训练: {test_start} → {test_end}")
    print(f"{'='*60}")
    _ensure_qlib_init(data_path)

    cal = D.calendar(start_time="2021-01-01", end_time="2027-12-31")
    test_start_dt = pd.Timestamp(test_start)
    test_start_idx = np.searchsorted(cal, test_start_dt)
    valid_end_idx = test_start_idx - (hold_days + 2)
    valid_start_idx = valid_end_idx - 80
    train_end_idx = valid_start_idx - (hold_days + 2)

    segments = {
        "train": ("2021-01-01", cal[train_end_idx].strftime('%Y-%m-%d')),
        "valid": (cal[valid_start_idx].strftime('%Y-%m-%d'),
                  cal[valid_end_idx].strftime('%Y-%m-%d')),
        "test": (test_start, test_end),
    }
    print(f"  Train: {segments['train'][0]} ~ {segments['train'][1]}")
    print(f"  Valid: {segments['valid'][0]} ~ {segments['valid'][1]}")
    print(f"  Test:  {segments['test'][0]} ~ {segments['test'][1]}")

    stock_in = load_stock_pool(stock_pool_path)
    print(f"  📦 股票池: {len(stock_in)} 只")

    handler_cls = get_trend_handler(hold_days)
    handler = handler_cls(instruments=stock_in, start_time="2021-01-01",
                          end_time=segments["test"][1])  # 必须覆盖到 test 段
    ds = DatasetH(handler=handler, segments=segments)

    model = LGBModel(**get_trend_model_params())
    model.fit(ds)

    # IC
    pred = model.predict(ds, segment="valid")
    if isinstance(pred, pd.Series):
        pred = pred.to_frame("score")
    label = ds.prepare(segments="valid", col_set="label")
    combined = pd.concat([pred, label], axis=1).dropna()
    if not combined.empty:
        ic = signal_ic(combined.iloc[:, 0], combined.iloc[:, 1])
        print(f"  📈 Valid IC: {ic['ic'].mean():.4f}, Rank IC: {ic['rank_ic'].mean():.4f}")

    # 预测
    test_pred = model.predict(ds, segment="test")
    if isinstance(test_pred, pd.Series):
        test_pred = test_pred.to_frame("score")
    print(f"  ✅ 生成 {len(test_pred)} 条信号")

    return test_pred, model


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

        ws_idx = np.searchsorted(cal, ws)
        we_idx = np.searchsorted(cal, we)
        if ws_idx >= len(cal):
            continue
        aws = cal[ws_idx]
        awe = min(cal[we_idx], pd.Timestamp(test_end))
        if aws > awe:
            continue

        ws_str = aws.strftime('%Y-%m-%d')
        we_str = awe.strftime('%Y-%m-%d')

        valid_end_idx = ws_idx - (hold_days + 2)
        valid_start_idx = valid_end_idx - 80
        train_end_idx = valid_start_idx - (hold_days + 2)
        if train_end_idx < 0:
            continue

        # ---- 每周重新选股 (实盘无前视) ----
        # 周一开盘前只能拿到上周五(及更早)的数据, 所以用"本周开始前最近交易日"作筛选基准,
        # 再结合滚动训练段(也都在本周之前)训练模型 → 预测本周。池子每周动态进出。
        as_of_idx = max(ws_idx - 1, 0)
        cycle_pool = filter_by_trend(base_pool, as_of_date=cal[as_of_idx])
        if not cycle_pool:
            cycle_pool = base_pool   # 筛选异常/为空时兜底
        print(f"  [{i+1}/{len(week_starts)}] {ws_str}~{we_str}: "
              f"趋势池 {len(cycle_pool)} 只 (基准 {pd.Timestamp(cal[as_of_idx]).date()})")

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
        all_predictions.append(pred)
        print(f"       {ws_str}~{we_str}: {len(pred)} 条信号")

    final_pred = pd.concat(all_predictions)
    final_pred = final_pred[~final_pred.index.duplicated(keep='first')].sort_index()

    final_pred.to_pickle(str(TREND_SIGNAL_FILE))
    print(f"\n✅ 趋势滚动训练完成: {len(final_pred)} 条 → {TREND_SIGNAL_FILE}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, choices=["train", "roll", "predict"], default="predict")
    parser.add_argument("--date", type=str, default=pd.Timestamp.now().strftime('%Y-%m-%d'))
    parser.add_argument("--test_start", type=str, default="2026-02-01")
    parser.add_argument("--test_end", type=str, default="2026-07-20")
    parser.add_argument("--hold_days", type=int, default=5)
    parser.add_argument("--topk", type=int, default=6)
    args = parser.parse_args()

    if args.mode == "roll":
        # 基础池传 BASE_POOL(仅主板/行业过滤, 不含动态量价过滤):
        # roll 内部每周按当时行情重新趋势选股; 若文件不存在则自动退回全主板候选。
        roll_train(args.test_start, args.test_end, BASE_POOL, DATA_PATH, args.hold_days)
    elif args.mode == "predict":
        # ===== 单日预测模式 =====
        _ensure_qlib_init()
        stock_in = load_stock_pool(TREND_POOL)
        # 数据源可能滞后: 指定日期未必已入库(如日期在日历中但特征数据缺失)。
        # 回退到最近有特征数据的交易日, 避免取空数据导致推荐失效。
        _cal = D.calendar()
        _target = pd.Timestamp(args.date)
        _recent = [d for d in _cal if d <= _target][-5:]
        if not _recent:
            _recent = list(_cal)[-5:]
        for _day in reversed(_recent):
            _probe = D.features(stock_in[:20], ["$close"], start_time=_day, end_time=_day)
            if _probe is not None and not _probe.empty:
                if _day != _target:
                    print(f"⚠️ {args.date} 无特征数据, 回退到 {_day.date()}")
                args.date = _day.strftime('%Y-%m-%d')
                break
        print(f"\n📈 趋势预测 ({args.date}), 股票池: {len(stock_in)} 只")

        if not TREND_MODEL_FILE.exists():
            print("❌ 模型不存在，请先训练: python trend_strategy.py --mode=train")
        else:
            model = LGBModel.load(TREND_MODEL_FILE)
            feat_conf = None
            if TREND_FEATURE_FILE.exists():
                with open(TREND_FEATURE_FILE, 'r') as f:
                    feat_conf = json.load(f)

            lookback = (pd.Timestamp(args.date) - pd.Timedelta(days=120)).strftime('%Y-%m-%d')
            # 特征配置必须与训练时一致：有 feat_conf 用精炼后的，否则用全量 Alpha158
            handler_cls = get_trend_handler(
                args.hold_days,
                feat_conf['fields'] if feat_conf else None,
                feat_conf['names'] if feat_conf else None
            )
            handler = handler_cls(instruments=stock_in, start_time=lookback, end_time=args.date)
            ds = DatasetH(handler=handler, segments={"test": (args.date, args.date)})

            pred = model.predict(ds, segment="test")
            if isinstance(pred, pd.Series):
                pred = pred.to_frame("score")
            scores = pred.reset_index()
            scores = scores[scores['datetime'] == pd.Timestamp(args.date)].copy()
            scores['instrument'] = scores['instrument'].astype(str).str.upper().str.strip()
            scores = scores.set_index('instrument')

            # 行情风控: D.features 获取当天的乖离、量比、上影线
            # 含真实日涨幅(ret_real, 用 factor 还原实际价), 用于剔除涨停/一字板
            risk_fields = [
                "$close / Mean($close, 5) - 1",
                "$close / Mean($close, 20) - 1",
                "$amount / Mean($amount, 5)",
                "($high - $close) / $close",
                "($close / Ref($close, 1)) * (Ref($factor, 1) / $factor) - 1",
            ]
            risk_names = ["bias_5d", "bias_20d", "vol_ratio", "upper_shadow", "ret_real"]
            risk_df = D.features(stock_in, risk_fields, start_time=args.date, end_time=args.date)
            if not risk_df.empty:
                risk_df.columns = risk_names
                try:
                    risk_df = risk_df.xs(pd.Timestamp(args.date), level='datetime')
                except KeyError:
                    risk_df = risk_df.xs(pd.Timestamp(args.date), level=1)
                risk_df.index = risk_df.index.astype(str).str.upper().str.strip()

                final = scores[['score']].join(risk_df[['bias_5d', 'bias_20d', 'vol_ratio', 'ret_real']], how='inner')
                # 剔除无法买入的标的: 当日收盘涨停/一字板(次日开盘大概率顶一字, 买不进)
                _lim = final.index.map(_limit_up_threshold)
                _lim = pd.Series(_lim.values, index=final.index)
                final = final[final['ret_real'] < (_lim - 0.002)]
                # 趋势筛选：5日乖离为正（多头）+ 放量确认（1.2~5倍均量，剔除无量跟风和极端爆量）
                candidates = final[(final['bias_5d'] > 0) & (final['vol_ratio'] > 1.2) & (final['vol_ratio'] < 5.0)]
                result = candidates.sort_values('score', ascending=False).head(args.topk)

                print(f"\n✅ 趋势推荐 Top{args.topk} ({args.date}):")
                print(result[['score', 'bias_5d', 'bias_20d', 'vol_ratio']].to_string())

                # 保存
                TREND_PREDICTIONS_DIR.mkdir(parents=True, exist_ok=True)
                result.to_csv(TREND_PREDICTIONS_DIR / f"trend_picks_{args.date}.csv")
                print(f"\n💾 已保存至: {TREND_PREDICTIONS_DIR / f'trend_picks_{args.date}.csv'}")

                # 后端批量保存
                backend_records = build_backend_records_from_result(result, args.date)
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

                    # 生成 HTML 报告
                    html_body = generate_trend_report_html(args.date, result, len(final))

                    subject = f"📈 趋势跟踪策略报告 - {args.date}"

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
                print("❌ 无法获取行情数据")
                print(f"   说明: qlib D.features 返回空数据；risk_df.shape={risk_df.shape}")
                print("   可能原因: 该日期在当前数据源中缺少行情，或股票池当天无可用标的。")
    else:
        # train mode
        test_pred, model = train_once(
            args.test_start, args.test_end, TREND_POOL, DATA_PATH, args.hold_days
        )
        test_pred.to_pickle(str(TREND_SIGNAL_FILE))
        model.to_pickle(str(TREND_MODEL_FILE))
        print(f"\n✅ 模型和信号已保存")
