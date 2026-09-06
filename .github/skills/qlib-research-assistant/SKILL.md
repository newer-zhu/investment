---
name: qlib-research-assistant
description: "Quantitative research assistant for the investment-qlib project (Python 3.9+, Microsoft Qlib 0.9.7). Use when the user asks to: (1) check, validate, or update Qlib bin data (data/source/qlib_bin, HS300/CSI500 or custom main-board pool); (2) generate or modify model training code — LightGBM, XGBoost, GRU or other Qlib models; (3) run a strategy backtest and produce a report/equity curve; (4) analyze backtest results, IC/RankIC and propose strategy improvements. Covers lookahead-bias-free decision timing, adjusted-vs-real price conventions, limit-up/down rejection handling, actual-fill accounting, and this repo's single-source-of-truth rule parameters. Triggers: qlib data check, train LightGBM/XGBoost/GRU, backtest trend strategy, analyze equity curve, improve strategy, daily predict, IC analysis."
argument-hint: "Task (data / train / backtest / analyze) + optional dates or parameters"
---

# Qlib Research Assistant (investment-qlib)

Personal quantitative research copilot for the `investment-qlib` project. It turns research requests
("check the data", "train a model", "backtest", "why is the PnL bad?") into correct, repo-consistent,
**lookahead-bias-free** Qlib code and analysis.

## When to Use
- User asks to inspect/verify/repair/update Qlib market data (calendar drift, missing days, universe files).
- User asks to write or modify training code: LightGBM / XGBoost / GRU / NN, feature handlers, labels, rolling retrain.
- User asks to run or extend a backtest and explain the resulting report/equity curve.
- User asks to analyze metrics (IC, RankIC, drawdown, win rate) and suggest concrete improvements.
- User asks about the daily pipeline, `predict_day`, or consistency between backtest and live signals.

## Project Map (workspace root = `investment/`)
| Thing | Path |
|---|---|
| Qlib project root | `investment-qlib/` |
| Market data (provider_uri) | `investment-qlib/data/source/qlib_bin` (`calendars/`, `features/`, `instruments/`) |
| Universe / stock-pool files | `investment-qlib/data/source/qlib_bin/instruments/*.txt` |
| Models + signal files | `investment-qlib/data/models/trend/` (`lgb_trend_pred.pkl` = `TREND_SIGNAL_FILE`) |
| Paths & **single source of truth rules** | `investment-qlib/src/qlib_project/constants.py` (`TREND_RULES`) |
| Strategy & training | `investment-qlib/src/qlib_project/strategy/trend_strategy.py`, `rebound_strategy.py`, `position_manager.py` |
| Stock selection | `investment-qlib/src/qlib_project/select/select_trend.py` |
| Backtest | `investment-qlib/src/qlib_project/backtest/backtest_trend.py` (canonical), `back_test.py` (legacy) |
| IC evaluation | `investment-qlib/src/qlib_project/ic_eval.py` |
| Utils (email/log/api/params) | `investment-qlib/src/qlib_project/utils/` |
| Daily automation scripts | `investment-qlib/src/qlib_project/scripts/` (`run_daily_predict.py`, `run_daily_data_update.sh`, `run_daily_pipeline.sh`) |
| Trading discipline doc | `investment-qlib/docs/trend_trading_discipline.md` |

Rules that govern live trading/backtests are **defined once** in `constants.TREND_RULES` and reused by
`backtest_trend.py`, `trend_strategy.predict_day`, and `position_manager` — when you change a rule, change it
there (and say so), never fork a copy.

## Core Domain Invariants (apply to every task)
1. **No lookahead.** Decisions use the previous trading day's close (`asof = T-1`); fill at `T` close. Never let
   features or labels see the future. A label must use `Ref(..., -k)` (future shift), never `+k`.
2. **Adjusted vs real price.** `$close` in qlib bin is *forward-adjusted*. Strategy signals/features/MA/IC use
   adjusted prices. Real-world price = `$close / $factor`. Real daily return =
   `($close/Ref($close,1))*(Ref($factor,1)/$factor)-1` (removes ex-dividend fake gaps).
3. **`D.features` returns MultiIndex `(instrument, datetime)`.** When indexing by stock, `reset_index()` first and
   rename feature columns **by original `fields` order** (`list(df.columns)[2:2+len(names)]`), never via
   `columns.difference()` (sorts → column misalignment → stop-loss silently breaks).
4. **Board limit thresholds.** Main board 0.098 / ChiNext(STAR 300/301/688) 0.198 / BSE 0.298. Filter out
   near-limit (`ret_real >= threshold - 0.002`) and one-word boards before any buy/signal.
5. **Data lag.** `D.calendar()[-1]` can be ahead of the feature data by 1 day (calendar to 09-05 but features to
   09-04). Probe the latest ~5 calendar days with a tiny `D.features([...top20 codes], ["$close"], ...)` call and
   use the most recent day that actually returns data.
6. **Actual-fill accounting.** In custom backtests qlib rejects buy-on-limit-up / sell-on-limit-down
   (`order.deal_amount == 0`). Book positions only from executed `deal_amount > 0`, never at order time.

---

## Task 1 — Inspect / Validate / Update Qlib Data
Use for: "check data", "is data up to date", "why is a stock missing", "update qlib data", "HS300/CSI500 coverage".

### Procedure
1. Confirm freshness against the calendar without assuming the last date has data (invariant 5):
   ```python
   import qlib
   from qlib.data import D
   qlib.init(provider_uri="investment-qlib/data/source/qlib_bin", region="cn")
   cal = D.calendar(start_time="2026-08-01", end_time="2026-09-30")
   last = cal[-1]
   # probe last 5 trade days; pick the latest day that truly has features
   probe = D.features(["SH600000"], ["$close"], start_time=cal[-5], end_time=last)
   print("calendar last:", last, "| features last:", probe.index.get_level_values(0).max())
   ```
2. Check universe/stock-pool files: `D.instruments("csi300")`/`"csi500"` for index constituents, or the project's
   custom pool `.txt` files referenced by `constants.BASE_POOL/TREND_POOL`. Reconcile counts and membership.
3. Verify a field/range you need: `D.features(codes, ["$close", "$factor", "$amount"], start_time, end_time)`.
   Check for empty returns (wrong date or empty pool), all-NaN columns, and price continuity.
4. Verify real-vs-adjusted on a known stock (e.g., `$close / $factor` ≈ published real price) when the user cares
   about live fill prices.
5. **Update data** through the repo's pipeline rather than inventing a new dump path:
   - `investment-qlib/src/qlib_project/scripts/run_daily_data_update.sh` (Docker container → writes `qlib_bin`).
   - Never re-predict for the same trade date: dedup state lives in `investment-qlib/logs/.daily_predict_last.txt`.
   - Run full loop only when the update actually produced a *new* trading day (weekends/holidays are no-ops).
6. Report exactly: last calendar day vs last feature day, #instruments, date range requested vs found, anything stale.

---

## Task 2 — Generate / Modify Model Training Code (LightGBM / XGBoost / GRU)
Use for: "train a LightGBM/XGBoost/GRU model", "add features", "change label horizon", "new rolling window".

### Canonical repo architecture (follow `trend_strategy.py`)
- **Handler**: a `DataHandlerLP` subclass (this repo no longer mixes in Alpha158's mean-reversion factors for trend).
  - Features as Qlib **Expression Engine strings**, raw fields prefixed `$` (e.g., `$close/Mean($close,5)-1`).
  - Operators from the standard set: `Ref`, `Mean`, `Std`, `Slope`, `Correlation`, `Rsquared`, `Max`, `Rank`, `EMA`.
  - This repo's momentum set is in `_TREND_MOMENTUM_FIELDS`; keep momentum/trend factors for trend strategies.
  - Processors: `DropnaLabel` then `CSZScoreNorm` (group `"label"`).
  - Label (repo convention, `hold_days`-ahead): `Ref($open, -{hold_days+1}) / Ref($open, -1) - 1` → `LABEL_TREND`.
- **Dataset**: `DatasetH(handler=..., segments={...})` with `train/valid/test` date ranges.
- **Model**: LightGBM via `qlib.contrib.model.gbdt.LGBModel`; params come from `get_trend_model_params()`.
- **Rolling**: `_run_week_cycle(...)` retrains weekly **anchored to Monday** and re-selects the pool; `roll_train`
  produces the signal file `TREND_SIGNAL_FILE` (used later by the backtest). No fixed random seed today (accept the
  small week-to-week LightGBM jitter; do not silently add one unless asked).

### Templates per model type
- **LightGBM / XGBoost**: keep `LGBModel`/`XGBModel` from `qlib.contrib.model.gbdt`; only tune
  `get_trend_model_params()`-style dicts. XGBoost is a drop-in swap of the model class.
- **GRU / other NN**: subclass `qlib.model.base.ModelFT` (or `Model`), implement `fit(self, dataset)` that calls
  `dataset.prepare("train"/"valid")` and `predict(self, dataset) -> pd.Series`. Keep feature/label pipeline identical
  so IC and backtest stay comparable.

### Procedure
1. Clarify target: same strategy but new model class? new feature block? different `hold_days`/label? weekly roll vs one-shot?
2. Present the change as a diff-style plan against the canonical file; preserve the invariant list above.
3. Keep the model's *output contract* unchanged (score indexed like the label), and keep the **buy gate in one place**
   (`apply_trend_buy_filter` in `trend_strategy.py`) so training output and backtest/live filters never diverge.
4. For any new factor set, verify no future leakage and give per-field one-line rationale (finance logic, not filler).
5. Tell the user exactly how to regenerate signals (Task 3 precondition) and where files are written.

---

## Task 3 — Run Backtest & Produce a Report
Use for: "backtest the strategy", "how did it perform over period X", "produce report/equity curve".

### Precondition (critical)
The signal file the backtest reads must **cover the backtest period**. Regenerate it first with
`--mode roll` and `--test_end >= backtest end`, otherwise the tail of the backtest consumes stale/expired signals
(signals are only valid ~`signal_max_age` = 10 days). Example:
```bash
cd investment-qlib
python src/qlib_project/strategy/trend_strategy.py --mode roll --test_start 2026-02-01 --test_end 2026-09-06
```
```bash
python src/qlib_project/backtest/backtest_trend.py \
  --start 2026-02-15 --end 2026-08-25 \
  --topk 2 --max-positions 3 --hold-days 5 \
  --stop-loss -0.08 --take-profit 0.10
```
Supported overrides (defaults all read from `TREND_RULES`): `--pred --start --end --topk --max-positions --hold-days
--stop-loss --take-profit --trend-exit --trend-exit-ma --trend-exit-buffer --trend-break-min-hold --market-filter
--market-exit`.

### What `backtest_trend.py` does (explain when reporting)
- **Timing (no lookahead)**: decide on `T-1` close, fill both buys and sells at `T` close (`deal_price="close"`).
  Buying at next-day open (追涨) usually performs worse — say so if the user proposes `open`.
- **Multi-exit priority**: market risk (HS300 < MA20, currently disabled → warn-only) > expiry(hold_days) > stop-loss >
  take-profit > trend-break (close < MA{trend_exit_ma} × (1-buffer), min-hold `trend_break_min_hold`). Stop-loss/top-up
  cooldown respected.
- **Buy gate shared with live**: `apply_trend_buy_filter` (bias_5d above cross-section 70th pct & ≥0.02, ret_real>0,
  vol_ratio<1.5, adj price>1.0, no limit-up/one-word).
- **Equal-weight positions** `1/max_positions`, actual-fill accounting (invariant 6).
- **Outputs**: summary JSON → `logs/trend_backtest_summary.json`, equity PNG →
  `backtest/trend_backtest_equity.png`. Print a compact summary: total/annualized return, max drawdown, win rate,
  #trades, avg hold days, vs benchmark.

---

## Task 4 — Analyze Results & Recommend Improvements
Use for: "why is PnL bad/good", "improve the strategy", "what does the IC tell me", "compare parameter sets".

### Procedure
1. **Isolate signal quality from execution.** Compute cross-sectional IC / RankIC per day with
   `qlib_project.ic_eval.calc_ic_rank_ic(pred, label_col=...)` (or `trend_strategy.signal_ic`). Healthy momentum IC
   here is roughly mean IC/rank-IC ≈ 0.05–0.17 with IC_IR > 0.3; if IC ≈ 0 or unstable, the problem is **alpha/features**,
   not the backtest.
2. **Diagnose the backtest number first.** If signal IC is fine but equity is bad, follow the bug checklist (below) —
   this repo's historical "-47% despite IC 0.17" was 100% an execution/accounting/index bug, not the model. Verify in
   order: lookahead timing, MultiIndex rename, phantom positions from rejected orders, stale/expired signals,
   whipsaw exits.
3. **Read the equity curve + summary JSON**: drawdown shape (when do losses cluster?), win rate and payoff asymmetry
   (stop-loss vs take-profit), number of round trips (too few → signal too rare; too many → over-trading/cooldown).
4. **Levers (in likely order of value)**, each mapped to a real knob:
   | Symptom | Lever | Where |
   |---|---|---|
   | IC weak / unstable | more/sharper momentum features, feature selection via `utils/model_params_adjust.py`, hold_days | handler/features, `--hold-days` |
   | Losses cluster in down markets | market filter (MA20) is currently *off* — re-enable `--market-filter 1 --market-exit 1` to compare | `TREND_RULES` / CLI |
   | Whipsaw exits | raise `--trend-exit-ma` (5→10) or widen `--trend-exit-buffer`; raise `trend_break_min_hold` | CLI |
   | Stop-loss too tight vs take-profit | adjust `--stop-loss`/`--take-profit` asymmetry | CLI |
   | Too-few names / over-trading | `--topk`, `--max-positions`, buy-gate quantile (0.70) in `apply_trend_buy_filter` | CLI / filter |
   | Single-position blowups | cap per-name risk, equal-weight already in place | `--max-positions` |
5. **A/B honestly**: only change one knob per run, keep `TREND_RULES` as the baseline, and report both runs side by
   side. Do not tune on the same period you report as the result.
6. End with concrete, ordered next steps (data → alpha → rules) rather than vague "improve features".

---

## Pitfall Quick Reference (check before trusting any result)
| Pitfall | Symptom | Fix |
|---|---|---|
| Same-bar decision+fill | lookahead-inflated returns | decide on T-1 close, fill T close |
| `columns.difference()` rename | stops/TP never trigger, garbage columns | rename by original `fields` order |
| Booking at order time | phantom positions / double sells | book only `deal_amount > 0` |
| Calendar ahead of features | empty `D.features` at `cal[-1]` | probe last 5 days (invariant 5) |
| Stale signal file | late-period backtest off from live | `--mode roll` with `test_end >= backtest end` |
| Using `$close` as real price | wrong fill/trigger prices | real = `$close/$factor`, real ret formula (invariant 2) |
| Index `(datetime, instrument)` | `loc[stock]` fails | `reset_index()` → set `instrument` index |

## Definition of Done
- Data task: stated last-calendar vs last-feature day, counts, and anything stale; no silent assumptions.
- Training task: generated code is repo-consistent, lookahead-free, rule params point at `TREND_RULES`, model output
  contract unchanged, exact regeneration command given.
- Backtest task: signals refreshed first, run completed, summary JSON + equity PNG produced, plain-language results.
- Analysis task: signal quality (IC) separated from execution quality, at most one lever changed per experiment,
  concrete ordered recommendations delivered.
