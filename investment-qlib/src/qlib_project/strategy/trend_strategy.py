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
from qlib.contrib.data.handler import Alpha158
from qlib.contrib.model.gbdt import LGBModel
from qlib.data import D

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
SRC_ROOT = PROJECT_ROOT.parent
for path in (str(BASE_DIR), str(PROJECT_ROOT), str(SRC_ROOT)):
    if path not in sys.path:
        sys.path.insert(0, path)

from qlib_project.constants import DATA_PATH
from qlib_project.utils.util import load_stock_pool

TREND_POOL = DATA_PATH / "instruments" / "my_trend_pool.txt"
MODEL_DIR = PROJECT_ROOT / "data" / "models" / "trend"
MODEL_DIR.mkdir(parents=True, exist_ok=True)
MODEL_FILE = MODEL_DIR / "lgb_trend_model.pkl"
FEATURE_FILE = MODEL_DIR / "trend_refined_features.json"

_qlib_initialized = False


def _ensure_qlib_init(data_path=None):
    global _qlib_initialized
    if not _qlib_initialized:
        qlib.init(provider_uri=data_path or DATA_PATH, region=REG_CN)
        _qlib_initialized = True


# ================== 趋势定制 Handler ==================
_TREND_EXTRA_FIELDS = [
    "$close / Mean($close, 5) - 1",            # 5日乖离
    "$close / Mean($close, 20) - 1",           # 20日乖离
    "$amount / Mean($amount, 5)",              # 量比
    "($high - $close) / $close",               # 上影线（趋势股怕冲高回落）
    "Mean($close, 10) / Mean($close, 20)",     # 均线多头排列度
]
_TREND_EXTRA_NAMES = ["bias_5d", "bias_20d", "vol_ratio", "upper_shadow", "ma_bull"]


def get_trend_handler(hold_days=5, refined_fields=None, refined_names=None):
    label_expr = f"Ref($close, -{hold_days}) / $close - 1"

    class TrendAlpha158(Alpha158):
        def get_label_config(self):
            return ([label_expr], ["LABEL_TREND"])

        def get_feature_config(self):
            if refined_fields and refined_names:
                return refined_fields, refined_names
            conf = super().get_feature_config()
            conf[0].extend(_TREND_EXTRA_FIELDS)
            conf[1].extend(_TREND_EXTRA_NAMES)
            return conf

        def get_learn_processors(self):
            return [
                {"class": "ConfigSectionProcessor",
                 "kwargs": {"fillna_label": True, "clip_label_outlier": True}},
                {"class": "CSZScoreNorm", "kwargs": {"fields_group": "feature"}},
            ]
    return TrendAlpha158


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
    valid_end_idx = test_start_idx - (hold_days + 1)
    valid_start_idx = valid_end_idx - 80
    train_end_idx = valid_start_idx - (hold_days + 1)

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
    base_pool = load_stock_pool(stock_pool_path)

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

        valid_end_idx = ws_idx - (hold_days + 1)
        valid_start_idx = valid_end_idx - 80
        train_end_idx = valid_start_idx - (hold_days + 1)
        if train_end_idx < 0:
            continue

        segments = {
            "train": ("2021-01-01", cal[train_end_idx].strftime('%Y-%m-%d')),
            "valid": (cal[valid_start_idx].strftime('%Y-%m-%d'),
                      cal[valid_end_idx].strftime('%Y-%m-%d')),
            "test": (ws_str, we_str),
        }

        handler_cls = get_trend_handler(hold_days)
        handler = handler_cls(instruments=base_pool,
                              start_time="2021-01-01",
                              end_time=segments["test"][1])  # 必须覆盖到 test 段
        ds = DatasetH(handler=handler, segments=segments)
        model = LGBModel(**get_trend_model_params())
        model.fit(ds)

        pred = model.predict(ds, segment="test")
        if isinstance(pred, pd.Series):
            pred = pred.to_frame("score")
        all_predictions.append(pred)
        print(f"  [{i+1}/{len(week_starts)}] {ws_str}~{we_str}: {len(pred)} 条信号")

    final_pred = pd.concat(all_predictions)
    final_pred = final_pred[~final_pred.index.duplicated(keep='first')].sort_index()

    SIGNAL_FILE = MODEL_FILE.parent / "lgb_trend_pred.pkl"
    final_pred.to_pickle(str(SIGNAL_FILE))
    print(f"\n✅ 趋势滚动训练完成: {len(final_pred)} 条 → {SIGNAL_FILE}")


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
        roll_train(args.test_start, args.test_end, TREND_POOL, DATA_PATH, args.hold_days)
    elif args.mode == "predict":
        # ===== 单日预测模式 =====
        _ensure_qlib_init()
        stock_in = load_stock_pool(TREND_POOL)
        print(f"\n📈 趋势预测 ({args.date}), 股票池: {len(stock_in)} 只")

        if not MODEL_FILE.exists():
            print("❌ 模型不存在，请先训练: python trend_strategy.py --mode=train")
        else:
            model = LGBModel.load(MODEL_FILE)
            feat_conf = None
            if FEATURE_FILE.exists():
                with open(FEATURE_FILE, 'r') as f:
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

            # 行情风控
            risk_df = D.features(stock_in, _TREND_EXTRA_FIELDS[:4], start_time=args.date, end_time=args.date)
            if not risk_df.empty:
                risk_df.columns = _TREND_EXTRA_NAMES[:4]
                try:
                    risk_df = risk_df.xs(pd.Timestamp(args.date), level='datetime')
                except KeyError:
                    risk_df = risk_df.xs(pd.Timestamp(args.date), level=1)
                risk_df.index = risk_df.index.astype(str).str.upper().str.strip()

                final = scores[['score']].join(risk_df[['bias_5d', 'bias_20d', 'vol_ratio']], how='inner')
                # 趋势筛选：偏5d>1%、量比<1.5、score降序
                candidates = final[(final['bias_5d'] > 0.01) & (final['vol_ratio'] < 1.5)]
                result = candidates.sort_values('score', ascending=False).head(args.topk)

                print(f"\n✅ 趋势推荐 Top{args.topk} ({args.date}):")
                print(result[['score', 'bias_5d', 'bias_20d', 'vol_ratio']].to_string())

                # 保存
                out_dir = PROJECT_ROOT / "data" / "trend_predictions"
                out_dir.mkdir(parents=True, exist_ok=True)
                result.to_csv(out_dir / f"trend_picks_{args.date}.csv")
                print(f"\n💾 已保存至: {out_dir / f'trend_picks_{args.date}.csv'}")
            else:
                print("❌ 无法获取行情数据")
    else:
        # train mode
        test_pred, model = train_once(
            args.test_start, args.test_end, TREND_POOL, DATA_PATH, args.hold_days
        )
        SIGNAL_FILE = MODEL_FILE.parent / "lgb_trend_pred.pkl"
        test_pred.to_pickle(str(SIGNAL_FILE))
        model.to_pickle(str(MODEL_FILE))
        print(f"\n✅ 模型和信号已保存")
