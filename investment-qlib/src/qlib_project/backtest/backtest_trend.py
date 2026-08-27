"""
趋势策略独立回测 (仅验证 trend 策略, 带止盈/止损)

执行时序 (无前视):
  T日收盘: 用 T 日收盘数据出信号/做止盈止损判定 (asof = 上一交易日)
  T日收盘: 按 T 日收盘价统一成交 (买卖都是收盘价, 含到期卖出)
  即 "T-1收盘决策 → T收盘执行"。决策只用上一交易日及以前的数据, 无前视。
  (对比: 若 deal_price="open" 则是"次日开盘执行", 追涨策略会追高开、卖低开, 通常更差)

核心修复与设计 (基于对 qlib 执行机制的排查):
  1. 按「实际成交」记账 (execute_result): 只有真正成交的买卖才入账,
     杜绝 qlib 涨跌停拒单造成的幻影持仓与重复卖出。
  2. 止盈/止损/趋势破坏(跌破趋势均线, 默认MA10+缓冲)/市场风控离场/持有到期 多重卖出规则。
  3. 买入前剔除涨停/一字板 (买不进的不买, 与选股池逻辑一致)。
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

# 与趋势实盘 (trend_strategy.predict_day) 共用同一买入过滤, 保证回测与实盘一致
from qlib_project.strategy.trend_strategy import apply_trend_buy_filter


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
    # 按 fields 传入顺序重命名 (不能按 columns.difference 的排序结果, 会错位)
    field_cols = list(df.columns)[2:len(names) + 2]
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
      - 卖出: 止盈 / 止损 / 趋势破坏(跌破MA5/MA10+缓冲) / 市场风控离场 / 持有到期, 先到先卖
      - 冷却: 止损/趋势破坏卖出后 STOP_COOLDOWN_DAYS 内禁止重买同一只票
              (止盈/到期属策略主动兑现, 不进入冷却)
      - 记账: 基于 execute_result 实际成交, 防止涨跌停拒单造成幻影持仓
    """

    # 止损/趋势破坏卖出后的冷却期(自然日, 约 2 个交易日)。
    # 防止同一只票止损卖出后隔天又被旧信号买回 → 反复止损(whipsaw)。
    STOP_COOLDOWN_DAYS = 3

    def __init__(self, signal, topk=2, max_positions=3, hold_days=5,
                 stop_loss=-0.08, take_profit=0.10, trend_exit=True,
                 min_price=1.0, signal_max_age=10, use_market_filter=False,
                 vol_ratio_max=1.5, trend_break_min_hold=2,
                 market_exit=True, trend_exit_ma=10, trend_exit_buffer=0.01, **kwargs):
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
        self.trend_break_min_hold = trend_break_min_hold  # 趋势破坏退出的最短持有天数
        self.market_exit = market_exit                    # 大盘<MA20 时全仓离场
        self.trend_exit_ma = trend_exit_ma                # 趋势破坏均线周期(5/10)
        self.trend_exit_buffer = trend_exit_buffer        # 趋势破坏缓冲带(收盘需跌破均线该比例才触发)

        # 持仓簿记 (只记录实际成交的持仓)
        self.holding_days: dict = {}     # stock -> 已持有天数
        self.entry_prices: dict = {}     # stock -> 实际成交价
        self.last_update_date = None
        self._market_ok = True

        # 交易日志 (用于胜率统计)
        self.trades = []                 # list[dict]
        self.trade_open: dict = {}       # stock -> 开仓记录(成本/日期)
        # 止损冷却: stock -> 最近一次"止损/趋势破坏"卖出的日期 (止盈/到期不记)
        self.stop_cooldown_until: dict = {}
        # 缓存交易日历, 用于计算"决策基准日"(上一交易日, 收盘后才出信号/决策)
        self.cal = D.calendar()
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

        # 决策基准日 asof = 上一交易日: T日收盘拿到T日数据出信号/决策, 次日(T+1)开盘执行,
        # 消除"当日收盘决策+当日收盘成交"的前视偏差。所有信号/止盈/止损/到期判断都用 asof 收盘数据。
        _td_ts = pd.Timestamp(trade_date)
        asof_idx = int(np.searchsorted(self.cal, _td_ts)) - 1
        if asof_idx < 0:                      # 回测起始日无上一交易日, 空仓等待
            return TradeDecisionWO(order_list, self)
        asof_date = self.cal[asof_idx]
        asof_str = pd.Timestamp(asof_date).strftime("%Y-%m-%d")

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

        # 3️⃣ 卖出决策: 市场风控离场 / 止盈 / 止损 / 趋势破坏 / 到期
        prices_df = _features_single_day(
            list(current_holdings.keys()),
            ["$close",
             "$close / Mean($close, 5) - 1",
             "$close / Mean($close, 10) - 1"],
            asof_date,        # 决策基准日收盘价(次日开盘执行, 无前视)
            ["price", "bias_5d", "bias_10d"],
        )

        # 大盘 MA20 风控 (每日一次): 供"暂停买入"与"市场风控离场"共用
        if (self.use_market_filter or self.market_exit) and self.last_update_date != trade_date_str:
            mkt = D.features(["SH000300"], ["$close", "Mean($close, 20)"],
                             start_time=asof_date, end_time=asof_date)
            if mkt is not None and not mkt.empty:
                row = mkt.iloc[0]
                self._market_ok = row["$close"] >= row["Mean($close, 20)"]

        # 市场风险离场: 大盘<MA20 时全仓卖出 (避免7月式 beta 回撤; 决策只用 asof 收盘, 无前视)
        if self.market_exit and not self._market_ok and current_holdings:
            for stock, amount in current_holdings.items():
                if stock not in prices_df.index:
                    continue                      # 无行情(停牌等), 次日再试
                price = float(prices_df.loc[stock, "price"])
                if not np.isfinite(price) or price <= 0:
                    continue
                entry = self.entry_prices.get(stock)
                pnl = (price / entry - 1) if entry else 0.0
                order_list.append(
                    OrderHelper.create(code=stock, amount=amount, direction=OrderDir.SELL)
                )
                if stock in self.trade_open:
                    self.trades.append({
                        "code": stock,
                        "buy_date": self.trade_open[stock]["date"],
                        "sell_date": trade_date_str,
                        "buy_price": self.trade_open[stock]["price"],
                        "sell_price": price,
                        "pnl": pnl,
                        "reason": "市场风控",
                    })
                    del self.trade_open[stock]
                print(f"[{trade_date_str}] 🔴 卖出(市场风控): {stock} 盈亏={pnl:+.2%}")
            return TradeDecisionWO(order_list, self)

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
            # 趋势破坏: 收盘跌破趋势均线(默认MA10)且低于缓冲带, 需持有>=trend_break_min_hold 天才触发
            # (MA5 太灵敏易被动量洗出, 用更长均线+缓冲带减少 whipsaw); 止损不受此限制, 随时生效
            elif self.trend_exit and days >= self.trend_break_min_hold \
                    and prices_df.loc[stock, f"bias_{self.trend_exit_ma}d"] < -self.trend_exit_buffer:
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
                # 止损/趋势破坏 → 进入冷却期, 防止次日被旧信号买回反复止损
                if reason in ("止损", "趋势破坏"):
                    self.stop_cooldown_until[stock] = trade_date_str
                print(f"[{trade_date_str}] 🔴 卖出({reason}): {stock} 盈亏={pnl:+.2%}")

        # 4️⃣ 买入决策 (大盘<MA20 暂停买入, 不清仓 —— 清仓由"市场风控离场"负责)
        if self.use_market_filter and not self._market_ok:
            print(f"[{trade_date_str}] ⚠️ 大盘在 MA20 下方，暂停买入")
            return TradeDecisionWO(order_list, self)

        active = len([s for s in current_holdings if s not in stocks_to_sell])
        slots_left = self.max_positions - active
        if slots_left <= 0:
            return TradeDecisionWO(order_list, self)

        # --- 信号时效 (只用到 asof=上一交易日收盘的信号, 无前视) ---
        sig_dates = pd.to_datetime(self.signal.index.get_level_values("datetime"))
        sig_strs = sig_dates.strftime("%Y-%m-%d")
        valid_mask = sig_strs <= asof_str
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

        # --- 候选过滤: 与趋势实盘 (trend_strategy.predict_day) 共用同一函数, 保证二者一致 ---
        # 用 asof(上一交易日)收盘数据做筛选, 次日开盘执行, 无前视
        sc = cur_scores.reset_index()
        sc["instrument"] = sc["instrument"].astype(str).str.upper()
        sc = sc.set_index("instrument")["score"]
        safe, _ = apply_trend_buy_filter(
            sc, asof_date=asof_date, topk=self.topk,
            min_price=self.min_price, vol_ratio_max=self.vol_ratio_max,
        )
        if safe.empty:
            return TradeDecisionWO(order_list, self)
        topk_list = safe.index.tolist()

        account = self.common_infra.get("trade_account")
        cash = account.get_cash() if hasattr(account, "get_cash") else getattr(account, "current_cash", 0)

        # 止损冷却: 计算冷却期内禁止重买的集合, 并顺带清理过期记录
        cooldown_reject = set()
        expired = []
        for s, d in list(self.stop_cooldown_until.items()):
            if (pd.Timestamp(trade_date_str) - pd.Timestamp(d)).days <= self.STOP_COOLDOWN_DAYS:
                cooldown_reject.add(s)
            else:
                expired.append(s)
        for s in expired:
            self.stop_cooldown_until.pop(s, None)

        to_buy = [
            s for s in topk_list
            if (s not in current_holdings) and (s not in stocks_to_sell)
            # 止损冷却: 止损/趋势破坏卖出后冷却期内禁止重买同一只票
            and (s not in cooldown_reject)
        ][:slots_left]
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
    parser.add_argument("--trend-exit", type=int, default=1, help="趋势破坏退出(1开0关)")
    parser.add_argument("--trend-exit-ma", type=int, default=10,
                        help="趋势破坏均线周期(5=MA5, 10=MA10, 默认10更钝化少被洗)")
    parser.add_argument("--trend-exit-buffer", type=float, default=0.01,
                        help="趋势破坏缓冲带: 收盘需跌破均线该比例才触发(0=关闭缓冲)")
    parser.add_argument("--trend-break-min-hold", type=int, default=2,
                        help="趋势破坏退出的最短持有天数(持有<该天数不触发, 止损仍生效)")
    parser.add_argument("--market-filter", type=int, default=1, help="大盘MA20风控(1开0关)")
    parser.add_argument("--market-exit", type=int, default=1,
                        help="大盘<MA20时全仓离场(1开0关, 叠加暂停买入降低beta回撤)")
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
            "trend_exit_ma": args.trend_exit_ma,
            "trend_exit_buffer": args.trend_exit_buffer,
            "trend_break_min_hold": args.trend_break_min_hold,
            "use_market_filter": bool(args.market_filter),
            "market_exit": bool(args.market_exit),
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
            # 无前视 + 收盘执行: 决策只用 asof(上一交易日)收盘数据, T日收盘成交。
            # 对比: "open"=次日开盘成交(追高开/卖低开, 对追涨不利), "close"=当日收盘成交。
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

    trend_exit_desc = (f"开(MA{args.trend_exit_ma}, 缓冲{args.trend_exit_buffer:.0%}, "
                       f"最短持有{args.trend_break_min_hold}日)" if args.trend_exit else "关")
    print(f"\n📈 配置: topk={args.topk} 持仓上限={args.max_positions} 持有={args.hold_days}日 "
          f"止损={args.stop_loss:+.0%} 止盈={args.take_profit:+.0%} "
          f"趋势破坏退出={trend_exit_desc} "
          f"MA20风控={'开' if args.market_filter else '关'}"
          f"{' + 全仓离场' if args.market_exit else ''}")

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
