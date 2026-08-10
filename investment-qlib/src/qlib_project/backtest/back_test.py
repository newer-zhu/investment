import sys
from pathlib import Path
import pandas as pd
import numpy as np
import qlib
from qlib.data import D
from qlib.strategy.base import BaseStrategy
from qlib.backtest.decision import OrderDir, TradeDecisionWO, OrderHelper
from qlib.backtest import backtest
import matplotlib.pyplot as plt

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
SRC_ROOT = PROJECT_ROOT.parent
for path in (str(BASE_DIR), str(PROJECT_ROOT), str(SRC_ROOT)):
    if path not in sys.path:
        sys.path.insert(0, path)
try:
    from constants import DATA_PATH
except ImportError:
    from qlib_project.constants import DATA_PATH

class ReboundHoldStrategy(BaseStrategy):
    """
    超跌反弹持仓策略（可配置持仓天数）
    
    买入逻辑（与 predict_daily 对齐）：
      1. 大盘 MA20 风控（市场在 MA20 下方时空仓）
      2. 取最新信号日期的预测分（需在有效期内）
      3. 量比 < 1.5（排除放量杀跌）
      4. bias_5d < 30% 分位数（自适应超跌，bias_limit 兜顶）
      5. 按 score 降序取 topk
      
    卖出逻辑：
      1. hold_days 到期 → 卖出
      2. 止损：持仓亏损 > stop_loss → 强制卖出
    """
    
    def __init__(self, signal, topk=5, bias_limit=0.05, stop_loss=-0.08,
                 max_positions=6, signal_max_age=5, hold_days=5,
                 use_market_filter=True, trend_mode=False, **kwargs):
        self.signal = signal
        self.topk = topk
        self.bias_limit = bias_limit
        self.stop_loss = stop_loss
        self.max_positions = max_positions
        self.signal_max_age = signal_max_age
        self.hold_days = hold_days
        self.use_market_filter = use_market_filter
        self.trend_mode = trend_mode            # True=趋势追涨, False=超跌反弹
        self.holding_days = {}
        self.entry_prices = {}
        self.last_update_date = None
        self._market_ok = True
        super().__init__(**kwargs)

    def get_current_holdings(self):
        holdings = {}
        if hasattr(self, 'trade_position'):
            for stock in self.trade_position.get_stock_list():
                pos = self.trade_position.get_stock_amount(stock)
                if pos and pos > 0:
                    holdings[stock] = pos
        return holdings

    def generate_trade_decision(self, execute_result=None):
        trade_date, _ = self.trade_calendar.get_step_time()
        trade_date_str = pd.Timestamp(trade_date).strftime('%Y-%m-%d')
        
        order_list = []
        current_holdings = self.get_current_holdings()
        
        # ==========================================
        # 🔒 日期锁：防止同一天被 Qlib 重复调用
        # ==========================================
        if self.last_update_date != trade_date_str:
            for stock in list(self.holding_days.keys()):
                if stock not in current_holdings:
                    del self.holding_days[stock]
                    self.entry_prices.pop(stock, None)  # 清理已清仓的成本记录
            
            for stock in current_holdings.keys():
                if stock not in self.holding_days:
                    self.holding_days[stock] = 0
                else:
                    self.holding_days[stock] += 1
            self.last_update_date = trade_date_str

        # ==========================================
        # 🛑 卖出逻辑 1：hold_days 到期卖出
        # ==========================================
        stocks_to_sell = set()
        for stock, days in list(self.holding_days.items()):
            if days >= self.hold_days:
                amount = current_holdings.get(stock, 0)
                if amount > 0:
                    order_list.append(OrderHelper.create(code=stock, amount=amount, direction=OrderDir.SELL))
                    stocks_to_sell.add(stock)
                    print(f"[{trade_date_str}] 🔴 T+{self.hold_days} 到期卖出: {stock} ({amount} 股)")

        # ==========================================
        # 🛑 卖出逻辑 2：止损（持仓亏损 > 阈值）
        # ==========================================
        all_held = [s for s in current_holdings if s not in stocks_to_sell]
        if all_held:
            prices_df = D.features(all_held, ["$close"], start_time=trade_date, end_time=trade_date)
            if not prices_df.empty:
                if isinstance(prices_df.index, pd.MultiIndex):
                    prices_df = prices_df.reset_index(level=0, drop=True)
                for stock in all_held:
                    entry = self.entry_prices.get(stock)
                    if entry is None or stock not in prices_df.index:
                        continue
                    current_price = prices_df.loc[stock].iloc[0] if hasattr(prices_df.loc[stock], 'iloc') else prices_df.loc[stock]
                    pnl = (current_price / entry - 1)
                    if pnl < self.stop_loss:
                        amount = current_holdings[stock]
                        order_list.append(OrderHelper.create(code=stock, amount=amount, direction=OrderDir.SELL))
                        stocks_to_sell.add(stock)
                        self.entry_prices.pop(stock, None)
                        print(f"[{trade_date_str}] 🛑 止损卖出: {stock} (亏损 {pnl:.2%}, 买入={entry:.2f}, 现价={current_price:.2f})")

        # ==========================================
        # 🟢 买入逻辑：大盘风控 + 信号时效 + 量比 + 自适应bias
        # ==========================================
        # --- 大盘 MA20 风控（同一天只查一次） ---
        if self.use_market_filter and self.last_update_date != trade_date_str:
            market_df = D.features(["SH000300"], ["$close", "Mean($close, 20)"],
                                   start_time=trade_date, end_time=trade_date)
            if not market_df.empty:
                row = market_df.iloc[0]
                self._market_ok = row["$close"] >= row["Mean($close, 20)"]

        if self.use_market_filter and not self._market_ok:
            print(f"[{trade_date_str}] ⚠️ 大盘在 MA20 下方，暂停买入")
            return TradeDecisionWO(order_list, self)

        # 检查当前持仓数，已达上限则跳过买入
        active_positions = len([s for s in current_holdings if s not in stocks_to_sell])
        if active_positions >= self.max_positions:
            print(f"[{trade_date_str}] ⏸️ 持仓已达上限 {self.max_positions}，跳过买入")
            return TradeDecisionWO(order_list, self)

        signal_dates = pd.to_datetime(self.signal.index.get_level_values('datetime'))
        signal_date_strs = signal_dates.strftime('%Y-%m-%d')
        
        valid_mask = signal_date_strs <= trade_date_str
        if not valid_mask.any():
            return TradeDecisionWO(order_list, self)
            
        latest_signal_date = signal_date_strs[valid_mask].max()
        
        # 🕐 信号新鲜度检查（日历日，应 >= 重训周期）
        # 周度重训 → signal_max_age=10 日历日覆盖周末+缓冲
        signal_age = (pd.Timestamp(trade_date_str) - pd.Timestamp(latest_signal_date)).days
        if signal_age > self.signal_max_age:
            print(f"[{trade_date_str}] ⚠️ 最新信号过期 ({latest_signal_date}, {signal_age}天前, 上限{self.signal_max_age}天)，跳过买入")
            return TradeDecisionWO(order_list, self)

        current_scores = self.signal[signal_date_strs == latest_signal_date]

        if current_scores.empty:
            return TradeDecisionWO(order_list, self)

        if isinstance(current_scores, pd.DataFrame):
            current_scores = current_scores['score'] if 'score' in current_scores.columns else current_scores.iloc[:, 0]

        instruments = current_scores.index.get_level_values('instrument').unique().tolist()
        
        # 获取行情数据（收盘价，与模型预测目标对齐）
        features_df = D.features(
            instruments,
            ["$close / Mean($close, 5) - 1", "$close", "$amount / Mean($amount, 5)"],
            start_time=trade_date, end_time=trade_date
        )
        
        if features_df.empty:
            return TradeDecisionWO(order_list, self)

        features_df.columns = ['bias_5d', 'close_price', 'vol_ratio']
        features_df = features_df.reset_index()
        features_df['instrument'] = features_df['instrument'].astype(str).str.upper()
        features_df = features_df.set_index('instrument')[['bias_5d', 'close_price', 'vol_ratio']]
        
        current_scores_single = current_scores.reset_index()
        current_scores_single['instrument'] = current_scores_single['instrument'].astype(str).str.upper()
        current_scores_single = current_scores_single.set_index('instrument')['score']
        
        combined = pd.DataFrame({'score': current_scores_single}).join(features_df, how='inner')

        # --- 过滤逻辑（趋势/反弹自适应） ---
        # 1. 质量门
        candidates = combined[(combined['close_price'] > 0) & (combined['vol_ratio'] < 1.5)].copy()
        if candidates.empty:
            return TradeDecisionWO(order_list, self)

        # 2. 自适应分位 + 方向切换
        quantile_threshold = candidates['bias_5d'].quantile(0.30)
        effective_limit = min(quantile_threshold, self.bias_limit)

        if self.trend_mode:
            # 趋势模式：买偏5d最高的（追涨），取顶部分位
            top_quantile = candidates['bias_5d'].quantile(0.70)
            safe_stocks = candidates[candidates['bias_5d'] > max(top_quantile, 0.01)]
        else:
            # 反弹模式：买偏5d最低的（超跌），取底部分位
            safe_stocks = candidates[candidates['bias_5d'] < effective_limit]

        current_scores = safe_stocks['score'].dropna().sort_values(ascending=False)
        close_prices = safe_stocks['close_price']

        topk_list = current_scores.head(self.topk).index.tolist()

        account = self.common_infra.get("trade_account")
        available_cash = account.get_cash() if hasattr(account, "get_cash") else getattr(account, "current_cash", 0)
        
        # 排除已持仓和当日已卖出
        stocks_to_buy = [s for s in topk_list if (s not in current_holdings) and (s not in stocks_to_sell)]
        
        # 控制买入后总持仓不超过 max_positions
        slots_left = self.max_positions - active_positions
        stocks_to_buy = stocks_to_buy[:slots_left]
        
        if len(stocks_to_buy) > 0 and available_cash > 0:
            cash_per_stock = (available_cash * 0.97) / len(stocks_to_buy) 
            
            for stock in stocks_to_buy:
                price = close_prices.get(stock)
                if pd.notna(price) and price > 0:
                    shares = int(cash_per_stock / price / 100) * 100
                    if shares > 0:
                        order_list.append(OrderHelper.create(code=stock, amount=shares, direction=OrderDir.BUY))
                        self.entry_prices[stock] = price  # 记录买入价用于止损
                        print(f"[{trade_date_str}] 🟢 买入: {stock} ({shares} 股, 收盘={price:.2f}, bias={safe_stocks.loc[stock,'bias_5d']:.3f})")

        return TradeDecisionWO(order_list, self)
        
# ==========================================
# 3. Main 运行入口 (保持你最熟悉的配置)
# ==========================================
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy", type=str, choices=["rebound", "trend"], default="rebound")
    parser.add_argument("--pred", type=str, default=None, help="预测信号文件路径，默认根据 strategy 自动选择")
    parser.add_argument("--start", type=str, default="2026-02-15", help="回测开始日期")
    parser.add_argument("--end", type=str, default="2026-07-25", help="回测结束日期")
    args = parser.parse_args()

    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    
    if args.pred:
        PRED_PATH = Path(args.pred)
    elif args.strategy == "trend":
        PRED_PATH = PROJECT_ROOT / "data" / "models" / "trend" / "lgb_trend_pred.pkl"
    else:
        PRED_PATH = PROJECT_ROOT / "data" / "models" / "rebound" / "lgb_rebound_pred.pkl"

    qlib.init(provider_uri=str(DATA_PATH), region="cn")

    if not Path(PRED_PATH).exists():
        print(f"❌ 找不到预测文件: {PRED_PATH}")
        print("   请先运行: python strategy/trend_strategy.py --mode=roll")
    else:
        pred_df = pd.read_pickle(PRED_PATH)
        
        if isinstance(pred_df.index, pd.MultiIndex):
            pred_df = pred_df.reset_index()
        
        pred_df['datetime'] = pd.to_datetime(pred_df['datetime'])
        pred_df['instrument'] = pred_df['instrument'].astype(str).str.upper()
        
        pred_df.set_index(['datetime', 'instrument'], inplace=True)
        pred_df = pred_df.sort_index()

        print("\n📊 ===== 预测信号数据诊断 =====")
        print(f"策略类型: {args.strategy}")
        print(f"信号总行数: {len(pred_df)}")
        print(f"日期范围: {pred_df.index.get_level_values('datetime').min()} ~ {pred_df.index.get_level_values('datetime').max()}")
        print("===============================\n")

        is_trend = (args.strategy == "trend")
        strategy_config = {
            "class": "ReboundHoldStrategy",
            "module_path": "__main__",
            "kwargs": {
                "signal": pred_df,
                "topk": 2,
                "bias_limit": 0.05,
                "stop_loss": -0.08,
                "max_positions": 6,
                "signal_max_age": 10,
                "hold_days": 5,
                "use_market_filter": not is_trend,  # 趋势策略不限制大盘方向
                "trend_mode": is_trend,
            }
        }

        executor_config = {
            "class": "SimulatorExecutor",
            "module_path": "qlib.backtest.executor",
            "kwargs": {
                "time_per_step": "day",
                "generate_portfolio_metrics": True,
            }
        }

        print("🚀 开始回测...")
        report_dict, indicator_dict = backtest(
            start_time=args.start,
            end_time=args.end,
            strategy=strategy_config,
            executor=executor_config,
            benchmark="SH000300",
            account=1000000,
            exchange_kwargs={
                "freq": "day",
                "limit_threshold": 0.095,
                "deal_price": "close",     # 收盘价成交，与模型标签对齐
                "open_cost": 0.0005,
                "close_cost": 0.001,       # 收盘卖出仅印花税+佣金，无隔夜滑点
            }
        )

        print("\n✅ 回测完成！")
        
        # ==========================================
        # 📊 全面诊断输出
        # ==========================================
        report_data = report_dict['1day']
        report_df = report_data[0] if isinstance(report_data, tuple) else report_data

        # --- 策略收益 ---
        strategy_return = (1 + report_df['return']).cumprod()
        strategy_total = strategy_return.iloc[-1]
        strategy_annual = strategy_total ** (252 / len(report_df)) - 1
        strategy_vol = report_df['return'].std() * np.sqrt(252)
        strategy_mdd = (strategy_return / strategy_return.cummax() - 1).min()
        strategy_sharpe = (report_df['return'].mean() / report_df['return'].std()) * np.sqrt(252) if report_df['return'].std() > 0 else 0

        print(f"\n{'='*60}")
        print(f"📊 策略绩效（{report_df.index[0]} ~ {report_df.index[-1]}，共 {len(report_df)} 个交易日）")
        print(f"{'='*60}")
        print(f"  累计收益:     {strategy_total:.4f} ({strategy_total-1:+.2%})")
        print(f"  年化收益:     {strategy_annual:+.2%}")
        print(f"  年化波动:     {strategy_vol:.2%}")
        print(f"  夏普比率:     {strategy_sharpe:.2f}")
        print(f"  最大回撤:     {strategy_mdd:.2%}")
        print(f"  日均换手:     {report_df['turnover'].mean():.4f}")
        print(f"  日均换手(买入): {report_df['turnover'].mean()/2:.4f}")
        
        # --- 年化交易成本估算 ---
        daily_cost = report_df['turnover'].mean() * (0.0005 + 0.0015)  # 买入0.05% + 卖出0.15%
        annual_cost = daily_cost * 252
        print(f"  年化交易成本:  ~{annual_cost:.2%}")
        print(f"  扣除成本后年化: {strategy_annual - annual_cost:+.2%}")

        # --- 基准对比 ---
        if indicator_dict and '1day' in indicator_dict:
            bench_raw = indicator_dict['1day']
            # Qlib 返回的可能是 tuple (DataFrame, ...) 或 DataFrame
            if isinstance(bench_raw, tuple):
                bench_df = bench_raw[0]
            else:
                bench_df = bench_raw
            if hasattr(bench_df, 'columns') and 'bench' in bench_df.columns:
                bench_return = bench_df['bench'].dropna()
            else:
                bench_return = pd.Series(dtype=float)
            if not bench_return.empty and len(bench_return) > 1:
                bench_cum = (1 + bench_return).cumprod()
                bench_total = bench_cum.iloc[-1]
                bench_annual = bench_total ** (252 / len(bench_return)) - 1
                excess = strategy_annual - bench_annual
                print(f"\n📈 基准 (沪深300) 对比:")
                print(f"  基准累计收益: {bench_total:.4f} ({bench_total-1:+.2%})")
                print(f"  基准年化收益: {bench_annual:+.2%}")
                print(f"  超额收益(α):  {excess:+.2%}")

                # 日胜率
                aligned = pd.concat([report_df['return'], bench_return], axis=1).dropna()
                if not aligned.empty:
                    win_days = (aligned.iloc[:, 0] > aligned.iloc[:, 1]).sum()
                    print(f"  跑赢基准天数: {win_days}/{len(aligned)} ({win_days/len(aligned):.1%})")

        # --- 月度收益明细 ---
        print(f"\n📅 月度收益明细:")
        monthly = report_df['return'].resample('ME').apply(lambda x: (1+x).prod()-1)
        for m, r in monthly.items():
            bar = '🟢' if r > 0 else '🔴'
            print(f"  {m.strftime('%Y-%m')}: {bar} {r:+.2%}")

        # --- 极端日 ---
        worst_day = report_df['return'].idxmin()
        best_day = report_df['return'].idxmax()
        print(f"\n⚠️ 极端交易日:")
        print(f"  最佳日: {best_day}  +{report_df.loc[best_day, 'return']:.2%}")
        print(f"  最差日: {worst_day}  {report_df.loc[worst_day, 'return']:.2%}")

        plt.figure(figsize=(10, 5))
        cumulative_return.plot(title=f"{args.strategy.upper()} Strategy - Cumulative Return")
        plt.grid(True)
        plt.tight_layout()
        plt.show()
