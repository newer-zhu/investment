"""
趋势策略独立回测 (仅验证 trend 策略, 带止盈/止损)

核心修复与设计 (基于对 qlib 执行机制的排查):
  1. 按「实际成交」记账 (execute_result): 只有真正成交的买卖才入账,
     杜绝 qlib 涨跌停拒单造成的幻影持仓与重复卖出。
  2. 止损/止盈/趋势破坏(跌破MA5)/持有到期 四重卖出规则。
  3. 买入前剔除当日涨停/一字板 (买不进的不买, 与选股池逻辑一致)。
  4. 持仓等权分仓 (1/max_positions), 避免单票过度集中。
  5. 正确处理 D.features 的 MultiIndex (instrument, datetime),
     避免止损止盈因索引错位而失效。

用法:
  # 默认持有 3 只股票
  python backtest_trend.py --start 2026-02-15 --end 2026-07-25
  # 持有股票数可配置(--max-positions), 每次买入数(--topk), 止盈止损等均可调
  python backtest_trend.py --max-positions 3 --topk 3 --stop-loss -0.06 --take-profit 0.12
"""
import sys
from pathlib import Path
import argparse
import pandas as pd
import numpy as np
import qlib
from qlib.data import D
from qlib.strategy.base import BaseStrategy
from qlib.backtest.decision import OrderDir, TradeDecisionWO, OrderHelper
from qlib.backtest import backtest
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
SRC_ROOT = PROJECT_ROOT.parent
for path in (str(BASE_DIR), str(PROJECT_ROOT), str(SRC_ROOT)):
    if path not in sys.path:
        sys.path.insert(0, path)
try:
    from constants import DATA_PATH, TREND_SIGNAL_FILE
except ImportError:
    from qlib_project.constants import DATA_PATH, TREND_SIGNAL_FILE


# ============================================================
# 工具函数
# ============================================================
def _limit_up_threshold(code: str) -> float:
    """按板块返回涨停涨幅阈值(留少量舍入余量)"""
    c = code.upper()
    if c.startswith(("SZ300", "SZ301", "SH688", "SH689")):   # 创业板/科创板 20%
        return 0.198
    if c.startswith(("BJ4", "BJ8", "BJ9")):                  # 北交所 30%
        return 0.298
    return 0.098                                             # 主板 10%


def _features_single_day(codes, fields, trade_date, names):
    """
    安全地取单日行情: 处理 D.features 的 MultiIndex (instrument, datetime),
    返回以 instrument 为索引、列名为 names 的 DataFrame。
    """
    if not codes:
        return pd.DataFrame(columns=names)
    df = D.features(codes, fields, start_time=trade_date, end_time=trade_date)
    if df is None or df.empty:
        return pd.DataFrame(columns=names)
    df = df.reset_index()
    df["instrument"] = df["instrument"].astype(str).str.upper()
    # 把字段表达式列重命名为 names (index 列可能含 instrument/datetime)
    field_cols = df.columns.difference(["instrument", "datetime"])
    df = df.rename(columns=dict(zip(field_cols, names)))
    out = df.set_index("instrument")[names]
    return out


# ============================================================
# 策略
# ============================================================
class TrendBacktestStrategy(BaseStrategy):
    """
    趋势跟踪回测策略
      - 买入: 最新信号(未过期)按 score 取 topk, 过滤涨停/低价/放量杀跌,
              等权分仓 (每仓 1/max_positions)
      - 卖出: 止盈 / 止损 / 趋势破坏(跌破MA5) / 持有到期, 四选一先到先卖
      - 记账: 基于 execute_result 实际成交, 防止涨跌停拒单造成幻影持仓
    """

    def __init__(self, signal, topk=2, max_positions=3, hold_days=5,
                 stop_loss=-0.08, take_profit=0.10, trend_exit=True,
                 min_price=1.0, signal_max_age=10, use_market_filter=False,
                 vol_ratio_max=1.5, **kwargs):
        self.signal = signal
        self.topk = topk
        self.max_positions = max_positions
        self.hold_days = hold_days
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        self.trend_exit = trend_exit
        self.min_price = min_price
        self.signal_max_age = signal_max_age
        self.use_market_filter = use_market_filter
        self.vol_ratio_max = vol_ratio_max

        # 持仓簿记 (只记录实际成交的持仓)
        self.holding_days: dict = {}     # stock -> 已持有天数
        self.entry_prices: dict = {}     # stock -> 实际成交价
        self.last_update_date = None
        self._market_ok = True

        # 交易日志 (用于胜率统计)
        self.trades = []                 # list[dict]
        self.trade_open: dict = {}       # stock -> 开仓记录(成本/日期)
        super().__init__(**kwargs)

    # ---------------- 持仓获取 ----------------
    def get_current_holdings(self):
        holdings = {}
        if hasattr(self, "trade_position"):
            for stock in self.trade_position.get_stock_list():
                pos = self.trade_position.get_stock_amount(stock)
                if pos and pos > 0:
                    holdings[stock] = pos
        return holdings

    # ---------------- 主决策 ----------------
    def generate_trade_decision(self, execute_result=None):
        trade_date, _ = self.trade_calendar.get_step_time()
        trade_date_str = pd.Timestamp(trade_date).strftime("%Y-%m-%d")
        order_list = []

        # 1️⃣ 按上一日实际成交更新簿记 (涨跌停拒单的 order.deal_amount=0, 不入账)
        if execute_result is not None:
            for item in execute_result:
                if not isinstance(item, (tuple, list)) or len(item) < 4:
                    continue
                order, _val, _cost, trade_price = item[:4]
                deal = getattr(order, "deal_amount", 0) or 0
                if deal <= 1e-5:
                    continue
                stock_id = order.stock_id
                if order.direction == OrderDir.BUY:
                    self.holding_days[stock_id] = 0
                    self.entry_prices[stock_id] = trade_price
                    if stock_id not in self.trade_open:
                        self.trade_open[stock_id] = {
                            "date": trade_date_str,
                            "price": trade_price,
                        }
                elif order.direction == OrderDir.SELL:
                    self.holding_days.pop(stock_id, None)
                    self.entry_prices.pop(stock_id, None)

        current_holdings = self.get_current_holdings()

        # 2️⃣ 每日递增持有天数 + 清理已离场记录
        if self.last_update_date != trade_date_str:
            for stock in list(self.holding_days.keys()):
                if stock not in current_holdings:
                    self.holding_days.pop(stock, None)
                    self.entry_prices.pop(stock, None)
            for stock in current_holdings.keys():
                if stock not in self.holding_days:
                    self.holding_days[stock] = 0
                else:
                    self.holding_days[stock] += 1
            self.last_update_date = trade_date_str

        # 3️⃣ 卖出决策: 止盈 / 止损 / 趋势破坏 / 到期
        prices_df = _features_single_day(
            list(current_holdings.keys()),
            ["$close", "$close / Mean($close, 5) - 1"],
            trade_date,
            ["price", "bias_5d"],
        )

        stocks_to_sell = set()
        for stock, amount in current_holdings.items():
            if stock not in prices_df.index:
                continue                      # 无行情(停牌等), 次日再试
            price = float(prices_df.loc[stock, "price"])
            if not np.isfinite(price) or price <= 0:
                continue
            days = self.holding_days.get(stock, 0)
            entry = self.entry_prices.get(stock)
            pnl = (price / entry - 1) if entry else 0.0

            reason = None
            if days >= self.hold_days:
                reason = f"T+{self.hold_days}到期"
            elif entry and pnl <= self.stop_loss:
                reason = "止损"
            elif entry and pnl >= self.take_profit:
                reason = "止盈"
            elif self.trend_exit and prices_df.loc[stock, "bias_5d"] < 0:
                reason = "趋势破坏"

            if reason:
                stocks_to_sell.add(stock)
                order_list.append(
                    OrderHelper.create(code=stock, amount=amount, direction=OrderDir.SELL)
                )
                # 记录平仓 (若卖出被跌停拒单, deal_amount=0, 下一日会重新尝试并再次记录;
                # 用 trade_open 存在与否避免重复记账)
                if stock in self.trade_open:
                    self.trades.append({
                        "code": stock,
                        "buy_date": self.trade_open[stock]["date"],
                        "sell_date": trade_date_str,
                        "buy_price": self.trade_open[stock]["price"],
                        "sell_price": price,
                        "pnl": pnl,
                        "reason": reason,
                    })
                    del self.trade_open[stock]
                print(f"[{trade_date_str}] 🔴 卖出({reason}): {stock} 盈亏={pnl:+.2%}")

        # 4️⃣ 买入决策
        if self.use_market_filter and self.last_update_date != trade_date_str:
            mkt = D.features(["SH000300"], ["$close", "Mean($close, 20)"],
                             start_time=trade_date, end_time=trade_date)
            if mkt is not None and not mkt.empty:
                row = mkt.iloc[0]
                self._market_ok = row["$close"] >= row["Mean($close, 20)"]

        if self.use_market_filter and not self._market_ok:
            print(f"[{trade_date_str}] ⚠️ 大盘在 MA20 下方，暂停买入")
            return TradeDecisionWO(order_list, self)

        active = len([s for s in current_holdings if s not in stocks_to_sell])
        slots_left = self.max_positions - active
        if slots_left <= 0:
            return TradeDecisionWO(order_list, self)

        # --- 信号时效 ---
        sig_dates = pd.to_datetime(self.signal.index.get_level_values("datetime"))
        sig_strs = sig_dates.strftime("%Y-%m-%d")
        valid_mask = sig_strs <= trade_date_str
        if not valid_mask.any():
            return TradeDecisionWO(order_list, self)
        latest_sig = sig_strs[valid_mask].max()
        age = (pd.Timestamp(trade_date_str) - pd.Timestamp(latest_sig)).days
        if age > self.signal_max_age:
            print(f"[{trade_date_str}] ⚠️ 信号过期({latest_sig}, {age}天前), 跳过买入")
            return TradeDecisionWO(order_list, self)

        cur_scores = self.signal[sig_strs == latest_sig]
        if isinstance(cur_scores, pd.DataFrame):
            cur_scores = cur_scores["score"] if "score" in cur_scores.columns else cur_scores.iloc[:, 0]
        if cur_scores.empty:
            return TradeDecisionWO(order_list, self)

        instruments = cur_scores.index.get_level_values("instrument").unique().tolist()

        # --- 候选行情: 5日乖离 / 收盘 / 量比 / 真实日涨幅(涨停检测) ---
        feats = _features_single_day(
            instruments,
            [
                "$close / Mean($close, 5) - 1",
                "$close",
                "$amount / Mean($amount, 5)",
                "($close / Ref($close, 1)) * (Ref($factor, 1) / $factor) - 1",
            ],
            trade_date,
            ["bias_5d", "close_price", "vol_ratio", "ret_real"],
        )
        if feats.empty:
            return TradeDecisionWO(order_list, self)

        sc = cur_scores.reset_index()
        sc["instrument"] = sc["instrument"].astype(str).str.upper()
        sc = sc.set_index("instrument")["score"]
        combined = pd.DataFrame({"score": sc}).join(feats, how="inner")

        # --- 质量门: 价格下限 / 量比 / 剔除当日涨停(买不进) ---
        lim = combined.index.map(_limit_up_threshold)
        lim = pd.Series(lim.values, index=combined.index)
        candidates = combined[
            (combined["close_price"] > self.min_price) &
            (combined["vol_ratio"] < self.vol_ratio_max) &
            (combined["ret_real"] < (lim - 0.002))
        ].copy()
        if candidates.empty:
            return TradeDecisionWO(order_list, self)

        # --- 趋势模式: 取 5 日乖离顶部分位中 score 最高的 ---
        top_q = candidates["bias_5d"].quantile(0.70)
        safe = candidates[candidates["bias_5d"] > max(top_q, 0.01)]
        ranked = safe["score"].dropna().sort_values(ascending=False)
        topk_list = ranked.head(self.topk).index.tolist()

        account = self.common_infra.get("trade_account")
        cash = account.get_cash() if hasattr(account, "get_cash") else getattr(account, "current_cash", 0)

        to_buy = [s for s in topk_list if (s not in current_holdings) and (s not in stocks_to_sell)][:slots_left]
        if to_buy and cash > 0:
            # 等权分仓: 每仓 = 现金 * 0.98 / max_positions (避免单票过度集中)
            cash_per = (cash * 0.98) / self.max_positions
            for stock in to_buy:
                price = float(safe.loc[stock, "close_price"])
                if not np.isfinite(price) or price <= 0:
                    continue
                shares = int(cash_per / price / 100) * 100
                if shares > 0:
                    order_list.append(
                        OrderHelper.create(code=stock, amount=shares, direction=OrderDir.BUY)
                    )
                    print(f"[{trade_date_str}] 🟢 买入: {stock} ({shares}股 @{price:.2f}, "
                          f"bias={safe.loc[stock,'bias_5d']:.3f}, score={safe.loc[stock,'score']:.2f})")
                    # 成本在下一日按实际成交价入账 (见步骤1)

        return TradeDecisionWO(order_list, self)


# ============================================================
# 主流程
# ============================================================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pred", type=str, default=None, help="信号文件, 默认 TREND_SIGNAL_FILE")
    parser.add_argument("--start", type=str, default="2026-02-10")
    parser.add_argument("--end", type=str, default="2026-08-10")
    parser.add_argument("--topk", type=int, default=2, help="每次买入的股票数(信号日)")
    parser.add_argument("--max-positions", type=int, default=3, help="持有股票数上限")
    parser.add_argument("--hold-days", type=int, default=5)
    parser.add_argument("--stop-loss", type=float, default=-0.08)
    parser.add_argument("--take-profit", type=float, default=0.10)
    parser.add_argument("--trend-exit", type=int, default=1, help="跌破MA5即退出(1开0关)")
    parser.add_argument("--market-filter", type=int, default=0, help="大盘MA20风控(1开0关)")
    args = parser.parse_args()

    qlib.init(provider_uri=str(DATA_PATH), region="cn")
    pred_path = Path(args.pred) if args.pred else TREND_SIGNAL_FILE
    if not Path(pred_path).exists():
        print(f"❌ 找不到信号文件: {pred_path}")
        return

    pred_df = pd.read_pickle(pred_path)
    if isinstance(pred_df.index, pd.MultiIndex):
        pred_df = pred_df.reset_index()
    pred_df["datetime"] = pd.to_datetime(pred_df["datetime"])
    pred_df["instrument"] = pred_df["instrument"].astype(str).str.upper()
    pred_df.set_index(["datetime", "instrument"], inplace=True)
    pred_df = pred_df.sort_index()

    print(f"信号范围: {pred_df.index.get_level_values('datetime').min()} ~ "
          f"{pred_df.index.get_level_values('datetime').max()}, 共 {len(pred_df)} 条")

    strategy_config = {
        "class": "TrendBacktestStrategy",
        "module_path": "__main__",
        "kwargs": {
            "signal": pred_df,
            "topk": args.topk,
            "max_positions": args.max_positions,
            "hold_days": args.hold_days,
            "stop_loss": args.stop_loss,
            "take_profit": args.take_profit,
            "trend_exit": bool(args.trend_exit),
            "use_market_filter": bool(args.market_filter),
            "min_price": 1.0,
            "signal_max_age": 10,
        },
    }
    executor_config = {
        "class": "SimulatorExecutor",
        "module_path": "qlib.backtest.executor",
        "kwargs": {"time_per_step": "day", "generate_portfolio_metrics": True},
    }

    print("🚀 开始回测...")
    report_dict, indicator_dict = backtest(
        start_time=args.start,
        end_time=args.end,
        strategy=strategy_config,
        executor=executor_config,
        benchmark="SH000300",
        account=1_000_000,
        exchange_kwargs={
            "freq": "day",
            "limit_threshold": 0.095,   # 涨跌停拒单(真实市场约束)
            "deal_price": "close",
            "open_cost": 0.0005,
            "close_cost": 0.001,
        },
    )

    # ---------- 统计 ----------
    rd = report_dict["1day"]
    report_df = rd[0] if isinstance(rd, tuple) else rd
    cum = (1 + report_df["return"]).cumprod()
    total = cum.iloc[-1]
    n = len(report_df)
    annual = total ** (252 / n) - 1
    vol = report_df["return"].std() * np.sqrt(252)
    sharpe = (report_df["return"].mean() / report_df["return"].std()) * np.sqrt(252) if report_df["return"].std() > 0 else 0
    mdd = (cum / cum.cummax() - 1).min()

    # 基准: 直接用沪深300计算(不依赖 qlib indicator 内部结构)
    bench_cum = pd.Series(dtype=float)
    _bm = D.features(["SH000300"], ["$close"], start_time=report_df.index[0], end_time=report_df.index[-1])
    if _bm is not None and not _bm.empty:
        _bm = _bm.reset_index().sort_values("datetime")
        _br = _bm["$close"].pct_change().dropna()
        bench_cum = (1 + _br).cumprod()

    print("\n" + "=" * 60)
    print(f"📊 趋势策略绩效 ({report_df.index[0]} ~ {report_df.index[-1]}, {n} 个交易日)")
    print("=" * 60)
    bm_total = bench_cum.iloc[-1] - 1 if len(bench_cum) > 1 else float("nan")
    print(f"  累计收益:   {total - 1:+.2%}   (基准 {bm_total:+.2%})")
    print(f"  年化收益:   {annual:+.2%}")
    print(f"  年化波动:   {vol:.2%}")
    print(f"  夏普比率:   {sharpe:.2f}")
    print(f"  最大回撤:   {mdd:.2%}")
    print(f"  日均换手:   {report_df['turnover'].mean():.4f}")
    cost = report_df["turnover"].mean() * (0.0005 + 0.001) * 252
    print(f"  年化成本:   ~{cost:.2%}")

    print(f"\n📈 配置: topk={args.topk} 持仓上限={args.max_positions} 持有={args.hold_days}日 "
          f"止损={args.stop_loss:+.0%} 止盈={args.take_profit:+.0%} 趋势破坏退出={'开' if args.trend_exit else '关'}")

    # 月度收益
    monthly = report_df["return"].resample("ME").apply(lambda x: (1 + x).prod() - 1)
    print("\n📅 月度收益:")
    for m, r in monthly.items():
        print(f"  {m.strftime('%Y-%m')}: {'🟢' if r > 0 else '🔴'} {r:+.2%}")

    # 极端日
    print("\n⚠️ 极端日:")
    print(f"  最佳: {report_df['return'].idxmax()} +{report_df['return'].max():.2%}")
    print(f"  最差: {report_df['return'].idxmin()} {report_df['return'].min():.2%}")

    # 画图
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(cum.index, cum.values, label="Trend Strategy", lw=1.5)
    if len(bench_cum) > 1:
        ax.plot(bench_cum.index, bench_cum.values, label="Benchmark (HS300)", lw=1.2, alpha=0.7)
    ax.set_title("Trend Strategy - Cumulative Return")
    ax.legend()
    ax.grid(True, alpha=0.3)
    out_png = BASE_DIR / "trend_backtest_equity.png"
    fig.tight_layout()
    fig.savefig(out_png, dpi=120)
    print(f"\n💾 净值曲线已保存: {out_png}")


if __name__ == "__main__":
    main()
