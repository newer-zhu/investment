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
        self.last_update_date = None  # 🔒 核心防御锁：防止单日时间静止死循环
        super().__init__(**kwargs)

    def get_current_holdings(self):
        """准确获取当前账户真实的持仓股票和数量"""
        holdings = {}
        if hasattr(self, 'trade_position'):
            for stock in self.trade_position.get_stock_list():
                pos = self.trade_position.get_stock_amount(stock)
                if pos and pos > 0:
                    holdings[stock] = pos
        return holdings

    def generate_trade_decision(self, execute_result=None):
        trade_date = self.trade_calendar.start_time
        trade_date_str = pd.Timestamp(trade_date).strftime('%Y-%m-%d')
        
        order_list = []
        current_holdings = self.get_current_holdings()
        
        # ==========================================
        # 核心逻辑 1：🔒 引入单日日期锁，确保天数每日仅自增一次
        # ==========================================
        if self.last_update_date != trade_date_str:
            # 清理历史已卖出的股票计数
            for stock in list(self.holding_days.keys()):
                if stock not in current_holdings:
                    del self.holding_days[stock]
            
            # 更新当前持仓股票的天数计数
            for stock in current_holdings.keys():
                if stock not in self.holding_days:
                    self.holding_days[stock] = 1   # 新买入成功，第 1 天持仓
                else:
                    self.holding_days[stock] += 1  # 严格限制：单日仅自增一次
            
            # 锁死当前交易日标签
            self.last_update_date = trade_date_str

        # ==========================================
        # 核心逻辑 2：同步获取全市场今日行情（用于风控及开盘价计算）
        # ==========================================
        # 集合今日需要判断的所有标的（当前持仓 + 预测信号覆盖的股票）
        all_relevant_stocks = list(set(current_holdings.keys()) | set(self.signal.index.get_level_values('instrument')))
        
        features_df = D.features(all_relevant_stocks, ["$open", "$close / Mean($close, 5) - 1"], 
                                 start_time=trade_date, end_time=trade_date)
        
        if not features_df.empty:
            features_df.columns = ['open_price', 'bias_5d']
            features_df = features_df.reset_index()
            features_df['instrument'] = features_df['instrument'].astype(str).str.upper()
            features_df = features_df.set_index('instrument')
        else:
            # 如果当天没有任何基础数据，安全返回空决策
            return TradeDecisionWO(order_list, self)

        # ==========================================
        # 核心逻辑 3：检查卖出信号 (持有满 2 天) & 预估释放资金
        # ==========================================
        estimated_released_cash = 0  # 盘前预估卖出可获得的现金流
        stocks_to_sell = set()       # 记录今日被强平的标的
        
        for stock, days in list(self.holding_days.items()):
            if days >= 2:
                amount = current_holdings[stock]
                open_price = features_df.loc[stock, 'open_price'] if stock in features_df.index else 0
                
                if open_price > 0:
                    order_list.append(OrderHelper.create(code=stock, amount=amount, direction=OrderDir.SELL))
                    # 扣除印花税及手续费后，预估释放现金
                    estimated_released_cash += amount * open_price * 0.998
                    stocks_to_sell.add(stock)
                    print(f"[{trade_date_str}] 🔴 持有满 {days} 天，触发卖出下单: {stock} ({amount} 股)")

        # ==========================================
        # 核心逻辑 4：资金动态合并，彻底消除持仓滚动断层
        # ==========================================
        account = self.common_infra.get("trade_account")
        current_cash = account.get_cash() if hasattr(account, "get_cash") else getattr(account, "current_cash", 0)
        
        # 盘前买入总额度 = 闲置现金 + 盘前强平即将释放的现金
        total_available_buy_cash = current_cash + estimated_released_cash
        
        # ==========================================
        # 核心逻辑 5：检查买入信号并根据真实额度建仓
        # ==========================================
        signal_dates = pd.to_datetime(self.signal.index.get_level_values('datetime'))
        signal_date_strs = signal_dates.strftime('%Y-%m-%d')
        valid_mask = signal_date_strs <= trade_date_str
        
        if not valid_mask.any():
            return TradeDecisionWO(order_list, self)
            
        latest_signal_date = signal_date_strs[valid_mask].max()
        current_scores = self.signal[signal_date_strs == latest_signal_date]

        if isinstance(current_scores, pd.DataFrame):
            current_scores = current_scores['score'] if 'score' in current_scores.columns else current_scores.iloc[:, 0]

        current_scores_single = current_scores.reset_index()
        current_scores_single['instrument'] = current_scores_single['instrument'].astype(str).str.upper()
        current_scores_single = current_scores_single.set_index('instrument')['score']
        
        # 合并今日得分与行情风控指标
        combined = pd.DataFrame({'score': current_scores_single}).join(features_df, how='inner')
        
        # 核心超跌风控：5日乖离率必须跌破阈值，且开盘有效
        safe_stocks = combined[(combined['bias_5d'] < self.bias_limit) & (combined['open_price'] > 0)]
        current_scores = safe_stocks['score'].dropna().sort_values(ascending=False)
        topk_list = current_scores.head(self.topk).index.tolist()
        
        # 🛡️ 核心风控：如果一只股票今天刚好触发“持有满2天卖出”，则今天绝对不重新买回，防止账目混乱
        stocks_to_buy = [s for s in topk_list if (s not in current_holdings) and (s not in stocks_to_sell)]
        
        if len(stocks_to_buy) > 0 and total_available_buy_cash > 10000:
            # 预留 3% 的摩擦垫片防止滑点或手续费导致超额报单失败
            cash_per_stock = (total_available_buy_cash * 0.97) / len(stocks_to_buy)
            
            for stock in stocks_to_buy:
                price = features_df.loc[stock, 'open_price']
                if pd.notna(price) and price > 0:
                    shares = int(cash_per_stock / price / 100) * 100
                    if shares > 0:
                        order_list.append(OrderHelper.create(code=stock, amount=shares, direction=OrderDir.BUY))
                        print(f"[{trade_date_str}] 🟢 触发买入下单: {stock} (预估买入 {shares} 股, 分配金额: {cash_per_stock:.2f}, 开盘价: {price:.2f})")

        return TradeDecisionWO(order_list, self)
        
# ================= 2. Main 运行入口 =================
if __name__ == "__main__":
    PROJECT_ROOT = Path(__file__).resolve().parents[1] 
    PRED_PATH = PROJECT_ROOT / "data" / "models" / "rebound" / "lgb_rebound_pred.pkl"

    qlib.init(provider_uri=str(DATA_PATH), region="cn")

    if not Path(PRED_PATH).exists():
        print(f"❌ 找不到预测文件: {PRED_PATH}，请确认上一步训练生成正常。")
    else:
        pred_df = pd.read_pickle(PRED_PATH)
        
        if isinstance(pred_df.index, pd.MultiIndex):
            pred_df = pred_df.reset_index()
        
        pred_df['datetime'] = pd.to_datetime(pred_df['datetime'])
        # 🛠️ 核心防御：强制全大写，匹配 Qlib CN 物理数据底层结构
        pred_df['instrument'] = pred_df['instrument'].astype(str).str.upper()
        
        pred_df.set_index(['datetime', 'instrument'], inplace=True)
        pred_df = pred_df.sort_index()

        print("\n📊 ===== 预测信号数据诊断 =====")
        print(f"信号总行数: {len(pred_df)}")
        if len(pred_df) > 0:
            print(f"信号日期范围: {pred_df.index.get_level_values('datetime').min().date()} 至 {pred_df.index.get_level_values('datetime').max().date()}")
            print(f"股票代码样例（必须为大写）: {list(pred_df.index.get_level_values('instrument')[:3])}")
        print("===============================\n")

        strategy_config = {
            "class": "TwoDayHoldStrategy",
            "module_path": "__main__",
            "kwargs": {
                "signal": pred_df,
                "topk": 3,
                "bias_limit": 0.10,
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
            start_time="2025-12-15",
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