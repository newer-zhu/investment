import sys
from pathlib import Path
import pandas as pd
import qlib
from qlib.data import D
from qlib.strategy.base import BaseStrategy
from qlib.backtest.decision import OrderDir, TradeDecisionWO, OrderHelper
from qlib.backtest import backtest, executor
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
        # 🛠️ 引入持仓天数计数器：{stock_code: held_days}
        self.holding_days = {} 
        super().__init__(**kwargs)

    def get_current_holdings(self):
        """获取当前实际持仓的股票列表及数量"""
        holdings = {}
        if hasattr(self, 'trade_position'):
            for stock in self.trade_position.get_stock_list():
                pos = self.trade_position.get_stock_position(stock)
                if pos > 0:
                    holdings[stock] = pos
        return holdings

    def generate_trade_decision(self, execute_result=None):
        trade_date = self.trade_calendar.start_time
        # 🛠️ 统一转换为标准格式字符串，彻底杜绝 Timestamp 匹配 Bug
        trade_date_str = pd.Timestamp(trade_date).strftime('%Y-%m-%d')
        
        order_list = []
        current_holdings = self.get_current_holdings()
        
        # ==========================================
        # 核心逻辑 1：更新与维护持仓天数计数器
        # ==========================================
        # 1.1 如果股票已经不在实际持仓中，移出计数器
        for stock in list(self.holding_days.keys()):
            if stock not in current_holdings:
                del self.holding_days[stock]
        
        # 1.2 如果股票在实际持仓中，但计数器没有，说明是今天刚成交的，记为 0 天
        #     如果是上一轮就在持仓中的，天数 + 1
        for stock in current_holdings.keys():
            if stock not in self.holding_days:
                self.holding_days[stock] = 0
            else:
                self.holding_days[stock] += 1

        # ==========================================
        # 核心逻辑 2：检查卖出信号 (持有满 2 天)
        # ==========================================
        for stock, days in list(self.holding_days.items()):
            if days >= 2:
                amount = current_holdings[stock]
                order_list.append(OrderHelper.create(code=stock, amount=amount, direction=OrderDir.SELL))
                print(f"[{trade_date_str}] 🔴 持有满 2 天，触发卖出下单: {stock} ({amount} 股)")

        # ==========================================
        # 核心逻辑 3：检查买入信号 (根据每日信号建仓)
        # ==========================================
        # 3.1 安全提取全量信号的日期并转为字符串
        signal_dates = pd.to_datetime(self.signal.index.get_level_values('datetime'))
        signal_date_strs = signal_dates.strftime('%Y-%m-%d')
        
        # 3.2 匹配当天或最近的历史有效信号
        valid_mask = signal_date_strs <= trade_date_str
        if valid_mask.any():
            latest_signal_date = signal_date_strs[valid_mask].max()
            current_scores = self.signal[signal_date_strs == latest_signal_date]
        else:
            current_scores = pd.DataFrame()

        if not current_scores.empty:
            if isinstance(current_scores, pd.DataFrame):
                current_scores = current_scores['score'] if 'score' in current_scores.columns else current_scores.iloc[:, 0]

            # 3.3 提取清晰的股票代码字符串列表
            instruments = current_scores.index.get_level_values('instrument').unique().tolist()
            
            # 3.4 乖离率风控过滤
            bias_df = D.features(instruments, ["$close / Mean($close, 5) - 1"], 
                                 start_time=trade_date, end_time=trade_date)
            
            if not bias_df.empty:
                bias_df.columns = ['bias_5d']
                bias_df = bias_df.reset_index().set_index('instrument')[['bias_5d']]
                current_scores_single = current_scores.reset_index().set_index('instrument')['score']
                combined = pd.DataFrame({'score': current_scores_single}).join(bias_df, how='inner')
                safe_stocks = combined[combined['bias_5d'] < self.bias_limit]
                current_scores = safe_stocks['score']
            else:
                current_scores = current_scores.reset_index().set_index('instrument')['score']

            # 3.5 排序获取 TopK
            current_scores = current_scores.dropna().sort_values(ascending=False)
            topk_list = current_scores.head(self.topk).index.tolist()

            # 3.6 计算可用现金并分仓买入
            account = self.common_infra.get("trade_account")
            available_cash = account.current_cash
            
            # 过滤掉已经在持仓中的股票，避免重复买入
            stocks_to_buy = [s for s in topk_list if s not in current_holdings]
            
            if len(stocks_to_buy) > 0 and available_cash > 0:
                # 预留 3% 缓冲资金防止因开盘价跳空导致现金透支而拒绝订单
                cash_per_stock = (available_cash * 0.97) / len(stocks_to_buy) 
                
                for stock in stocks_to_buy:
                    # 使用交易日开盘价估算购买数量
                    price = self.trade_exchange.get_quote_info(stock, trade_date, "open") 
                    if price and price > 0:
                        shares = int(cash_per_stock / price / 100) * 100
                        if shares > 0:
                            order_list.append(OrderHelper.create(code=stock, amount=shares, direction=OrderDir.BUY))
                            print(f"[{trade_date_str}] 🟢 满足选股条件，触发买入下单: {stock} (预估买入 {shares} 股)")

        return TradeDecisionWO(order_list, self)
        
# ================= 2. Main 运行入口 =================
if __name__ == "__main__":
    # 强制闭嘴 NumPy 的空切片警告，让控制台干净
    import warnings
    warnings.filterwarnings("ignore", category=RuntimeWarning, message="Mean of empty slice")

    PROJECT_ROOT = Path(__file__).resolve().parents[1] 
    PRED_PATH = PROJECT_ROOT / "data" / "models" / "rebound" / "lgb_rebound_pred.pkl"

    # 1. 初始化 Qlib
    qlib.init(provider_uri=str(DATA_PATH), region="cn")

    # 2. 准备预测信号数据
    if not Path(PRED_PATH).exists():
        print(f"❌ 找不到预测文件: {PRED_PATH}，请确认上一步训练生成正常。")
    else:
        pred_df = pd.read_pickle(PRED_PATH)
        
        # 统一索引格式
        if isinstance(pred_df.index, pd.MultiIndex):
            pred_df = pred_df.reset_index()
        
        pred_df['datetime'] = pd.to_datetime(pred_df['datetime'])
        
        # 🛠️【核心修复】Qlib CN 默认物理数据全为小写（例如 sh600000），必须转为小写！
        pred_df['instrument'] = pred_df['instrument'].astype(str).str.lower()
        
        pred_df.set_index(['datetime', 'instrument'], inplace=True)
        pred_df = pred_df.sort_index()

        # 🔍【新增数据诊断】帮你一眼看出信号文件是不是空的，日期对不对
        print("\n📊 ===== 预测信号数据诊断 =====")
        print(f"信号总行数: {len(pred_df)}")
        if len(pred_df) > 0:
            print(f"信号日期范围: {pred_df.index.get_level_values('datetime').min().date()} 至 {pred_df.index.get_level_values('datetime').max().date()}")
            print(f"股票代码样例（确认是否为小写）: {list(pred_df.index.get_level_values('instrument')[:3])}")
        print("===============================\n")

        # 3. 回测配置
        strategy_config = {
            "class": "TwoDayHoldStrategy", # 确保你上面的类名叫 TwoDayHoldStrategy
            "module_path": "__main__",
            "kwargs": {
                "signal": pred_df,
                "topk": 3,          # 每天最多买 3 只
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

        # 4. 执行回测
        # ⚠️ 请通过上面的【数据诊断】确认你的信号日期是否覆盖了 2025-12-08 到 2026-03-01！
        print("🚀 开始回测...")
        report_dict, indicator_dict = backtest(
            start_time="2025-12-08",
            end_time="2026-03-01",
            strategy=strategy_config,
            executor=executor_config,
            benchmark="SH000300",
            account=1000000,
            exchange_kwargs={
                "freq": "day",
                "limit_threshold": 0.095,
                "deal_price": "open",  # 开盘买入，开盘卖出
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

        # 5. 绘图
        plt.figure(figsize=(10, 5))
        cumulative_return.plot(title="Two-Day Holding Strategy - Cumulative Return")
        plt.grid(True)
        plt.tight_layout()
        plt.show()
    PROJECT_ROOT = Path(__file__).resolve().parents[1] 
    PRED_PATH = PROJECT_ROOT / "data" / "models" / "rebound" / "lgb_rebound_pred.pkl"

    qlib.init(provider_uri=str(DATA_PATH), region="cn")

    if not Path(PRED_PATH).exists():
        print(f"❌ 找不到预测文件: {PRED_PATH}，请确认上一步训练生成正常。")
    else:
        pred_df = pd.read_pickle(PRED_PATH)
        
        # 强制将索引级别转换为标准字符串，保证大小写兼容与格式对齐
        if isinstance(pred_df.index, pd.MultiIndex):
            pred_df = pred_df.reset_index()
        
        pred_df['datetime'] = pd.to_datetime(pred_df['datetime'])
        # 🛠️ 确保股票代码格式全部转换为大写（与 Qlib 底层物理数据对齐）
        pred_df['instrument'] = pred_df['instrument'].astype(str).str.upper()
        pred_df.set_index(['datetime', 'instrument'], inplace=True)
        pred_df = pred_df.sort_index()

        strategy_config = {
            "class": "TwoDayHoldStrategy",
            "module_path": "__main__",
            "kwargs": {
                "signal": pred_df,
                "topk": 3,          # 每天最多买 3 只
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
            start_time="2025-12-08",
            end_time="2026-04-01",
            strategy=strategy_config,
            executor=executor_config,
            benchmark="SH000300",
            account=1000000,
            exchange_kwargs={
                "freq": "day",
                "limit_threshold": 0.095,
                "deal_price": "open",  # 🛠️ 完美契合：开盘价（"open"）买入和卖出
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

        # 简单绘图
        plt.figure(figsize=(10, 5))
        cumulative_return.plot(title="Two-Day Holding Strategy - Cumulative Return")
        plt.grid(True)
        plt.tight_layout()
        plt.show()