import sys
from pathlib import Path
import pandas as pd
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

class TwoDayHoldStrategy(BaseStrategy):
    def __init__(self, signal, topk=5, bias_limit=0.10, **kwargs):
        self.signal = signal 
        self.topk = topk
        self.bias_limit = bias_limit
        self.holding_days = {} 
        self.last_update_date = None  # 🔒 记取教训：仅引入这个无害的单日日期锁，不破坏你原本的代码结构
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
        # 核心逻辑 1：🔒 日期锁防护（完全保留你的逻辑，只加一道单日闸门）
        # ==========================================
        if self.last_update_date != trade_date_str:
            for stock in list(self.holding_days.keys()):
                if stock not in current_holdings:
                    del self.holding_days[stock]
            
            for stock in current_holdings.keys():
                if stock not in self.holding_days:
                    self.holding_days[stock] = 0
                else:
                    self.holding_days[stock] += 1
            self.last_update_date = trade_date_str # 彻底锁死，防止同一天被 Qlib 重复调用时无限叠加天数

        # ==========================================
        # 核心逻辑 2：检查卖出信号 (完全使用你原本的卖出逻辑)
        # ==========================================
        stocks_to_sell = set()  # 🛡️ 新增隔离盾：记录今天决定卖出的股票
        for stock, days in list(self.holding_days.items()):
            if days >= 2:
                amount = current_holdings[stock]
                order_list.append(OrderHelper.create(code=stock, amount=amount, direction=OrderDir.SELL))
                stocks_to_sell.add(stock)  # 打上卖出标记
                print(f"[{trade_date_str}] 🔴 持有满 2 天，触发卖出下单: {stock} ({amount} 股)")

        # ==========================================
        # 核心逻辑 3：检查买入信号 (完全搬运你原版能运行的 D.features 逻辑)
        # ==========================================
        signal_dates = pd.to_datetime(self.signal.index.get_level_values('datetime'))
        signal_date_strs = signal_dates.strftime('%Y-%m-%d')
        
        valid_mask = signal_date_strs <= trade_date_str
        if not valid_mask.any():
            return TradeDecisionWO(order_list, self)
            
        latest_signal_date = signal_date_strs[valid_mask].max()
        current_scores = self.signal[signal_date_strs == latest_signal_date]

        if current_scores.empty:
            return TradeDecisionWO(order_list, self)

        if isinstance(current_scores, pd.DataFrame):
            current_scores = current_scores['score'] if 'score' in current_scores.columns else current_scores.iloc[:, 0]

        instruments = current_scores.index.get_level_values('instrument').unique().tolist()
        
        # 🎯 这里完全沿用你原版的数据获取方式
        features_df = D.features(instruments, ["$close / Mean($close, 5) - 1", "$open"], 
                                 start_time=trade_date, end_time=trade_date)
        
        if not features_df.empty:
            features_df.columns = ['bias_5d', 'open_price']
            features_df = features_df.reset_index()
            features_df['instrument'] = features_df['instrument'].astype(str).str.upper()
            features_df = features_df.set_index('instrument')[['bias_5d', 'open_price']]
            
            current_scores_single = current_scores.reset_index()
            current_scores_single['instrument'] = current_scores_single['instrument'].astype(str).str.upper()
            current_scores_single = current_scores_single.set_index('instrument')['score']
            
            combined = pd.DataFrame({'score': current_scores_single}).join(features_df, how='inner')
            
            # 🎯 这里完全沿用你原本的过滤代码
            safe_stocks = combined[(combined['bias_5d'] < self.bias_limit) & (combined['open_price'] > 0)]
            current_scores = safe_stocks['score']
            open_prices = safe_stocks['open_price']
        else:
            return TradeDecisionWO(order_list, self)

        current_scores = current_scores.dropna().sort_values(ascending=False)
        topk_list = current_scores.head(self.topk).index.tolist()

        account = self.common_infra.get("trade_account")
        available_cash = account.get_cash() if hasattr(account, "get_cash") else getattr(account, "current_cash", 0)
        
        # ==========================================
        # 🛡️ 唯一优化：剔除今天刚好在卖出的股票，防止自己跟自己对倒
        # ==========================================
        stocks_to_buy = [s for s in topk_list if (s not in current_holdings) and (s not in stocks_to_sell)]
        
        if len(stocks_to_buy) > 0 and available_cash > 0:
            cash_per_stock = (available_cash * 0.97) / len(stocks_to_buy) 
            
            for stock in stocks_to_buy:
                price = open_prices.get(stock)
                if pd.notna(price) and price > 0:
                    shares = int(cash_per_stock / price / 100) * 100
                    if shares > 0:
                        order_list.append(OrderHelper.create(code=stock, amount=shares, direction=OrderDir.BUY))
                        print(f"[{trade_date_str}] 🟢 触发买入下单: {stock} (预估买入 {shares} 股, 开盘单价: {price:.2f})")

        return TradeDecisionWO(order_list, self)
        
# ==========================================
# 3. Main 运行入口 (保持你最熟悉的配置)
# ==========================================
if __name__ == "__main__":
    PROJECT_ROOT = Path(__file__).resolve().parents[1] 
    PRED_PATH = PROJECT_ROOT / "data" / "models" / "rebound" / "lgb_rebound_pred.pkl"

    qlib.init(provider_uri=str(DATA_PATH), region="cn")

    if not Path(PRED_PATH).exists():
        print(f"❌ 找不到预测文件: {PRED_PATH}")
    else:
        pred_df = pd.read_pickle(PRED_PATH)
        
        if isinstance(pred_df.index, pd.MultiIndex):
            pred_df = pred_df.reset_index()
        
        pred_df['datetime'] = pd.to_datetime(pred_df['datetime'])
        pred_df['instrument'] = pred_df['instrument'].astype(str).str.upper()
        
        pred_df.set_index(['datetime', 'instrument'], inplace=True)
        pred_df = pred_df.sort_index()

        print("\n📊 ===== 预测信号数据诊断 =====")
        print(f"信号总行数: {len(pred_df)}")
        print("===============================\n")

        strategy_config = {
            "class": "TwoDayHoldStrategy",
            "module_path": "__main__",
            "kwargs": {
                "signal": pred_df,
                "topk": 3,
                "bias_limit": 0.10, # 🎯 还原回你原本的参数
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
            start_time="2026-01-15",
            end_time="2026-05-15",
            strategy=strategy_config,
            executor=executor_config,
            benchmark="SH000300",
            account=1000000,
            exchange_kwargs={
                "freq": "day",
                "limit_threshold": 0.095,
                "deal_price": "open", 
                "open_cost": 0.0005,
                "close_cost": 0.0015,
            }
        )

        print("\n✅ 回测完成！")
        
        report_data = report_dict['1day']
        report_df = report_data[0] if isinstance(report_data, tuple) else report_data

        cumulative_return = (1 + report_df['return']).cumprod()
        print(f"最终累计收益: {cumulative_return.iloc[-1]:.4f}")

        avg_turnover = report_df['turnover'].mean()
        print(f"平均每日换手率: {avg_turnover:.4f}")

        plt.figure(figsize=(10, 5))
        cumulative_return.plot(title="Two-Day Holding Strategy - Cumulative Return")
        plt.grid(True)
        plt.tight_layout()
        plt.show()
