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
from qlib.config import REG_CN

class WeeklySwingStrategy(BaseStrategy):
    def __init__(self, signal, topk=10, bias_limit=0.10, **kwargs):
        self.signal = signal 
        self.topk = topk
        self.bias_limit = bias_limit
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
        weekday = trade_date.weekday() # 0是周一, 4是周五
        
        # 提取策略运行所需的日期数据，安全过滤
        signal_datetimes = pd.to_datetime(self.signal.index.get_level_values('datetime'))
        trade_day = pd.Timestamp(trade_date).normalize()
        if trade_day not in signal_datetimes.normalize().unique():
            # 如果当天没有信号，允许使用最近一个有效历史信号
            prev_dates = signal_datetimes[signal_datetimes.normalize() <= trade_day]
            if prev_dates.empty:
                return TradeDecisionWO([], self)
            latest_date = prev_dates.max().normalize()
        else:
            latest_date = trade_day

        order_list = []
        current_holdings = self.get_current_holdings()
        
        # ==========================================
        # 动作 A: 周五清仓 (Sell on Friday)
        # ==========================================
        if weekday == 4:
            for stock, amount in current_holdings.items():
                # [修复点1] amount 必须是具体的持仓股数，而不是 0.0
                order_list.append(OrderHelper.create(code=stock, amount=amount, direction=OrderDir.SELL))
            
            if order_list:
                print(f"[{trade_date.date()}] 🔴 周五清仓: 下单卖出 {len(order_list)} 只股票")
            return TradeDecisionWO(order_list, self)

        # ==========================================
        # 动作 B: 周一建仓 (Buy on Monday)
        # ==========================================
        elif weekday == 0:
            # [修复点2] 直接用 pandas 布尔遮罩提取截面数据，兼容日期时间不完全一致的情况
            try:
                signal_dates = pd.to_datetime(self.signal.index.get_level_values('datetime'))
                mask = signal_dates.normalize() == latest_date
                current_scores = self.signal[mask]
                if current_scores.empty:
                    print(f"[{trade_date.date()}] ⚠️ 没有匹配的信号日期: {latest_date.date()}")
                    return TradeDecisionWO([], self)
            except Exception as e:
                print(f"[{trade_date.date()}] ⚠️ 获取信号失败: {e}")
                return TradeDecisionWO([], self)

            if isinstance(current_scores, pd.DataFrame):
                current_scores = current_scores['score'] if 'score' in current_scores.columns else current_scores.iloc[:, 0]

            # 风控过滤
            instruments = current_scores.index.tolist()
            bias_df = D.features(instruments, ["$close / Mean($close, 5) - 1"], 
                                 start_time=trade_date, end_time=trade_date)
            
            if not bias_df.empty:
                bias_df.columns = ['bias_5d']
                bias_df = bias_df.reset_index().set_index('instrument')[['bias_5d']]
                combined = pd.DataFrame({'score': current_scores}).join(bias_df, how='inner')
                safe_stocks = combined[combined['bias_5d'] < self.bias_limit]
                current_scores = safe_stocks['score']

            # 选取 TopK
            current_scores = current_scores.dropna().sort_values(ascending=False)
            topk_list = current_scores.head(self.topk).index.tolist()

            # [修复点3] 计算可买股数 (A股规则: 1手=100股)
            # 1. 获取当前账户可用现金
            account = self.common_infra.get("trade_account")
            available_cash = account.current_cash
            
            if len(topk_list) > 0 and available_cash > 0:
                # 均分资金给 topk 只股票 (预留 2% 做手续费和滑点缓冲)
                cash_per_stock = (available_cash * 0.98) / len(topk_list) 
                
                for stock in topk_list:
                    if stock not in current_holdings:
                        # 获取前一日收盘价或当天开盘价用于估算能买多少股
                        price = self.trade_exchange.get_quote_info(stock, trade_date, "close") 
                        if price and price > 0:
                            # 计算能买多少股，并向下取整到 100 的倍数
                            shares = int(cash_per_stock / price / 100) * 100
                            if shares > 0:
                                order_list.append(OrderHelper.create(code=stock, amount=shares, direction=OrderDir.BUY))

            if order_list:
                print(f"[{trade_date.date()}] 🟢 周一建仓: 下单买入 {len(order_list)} 只股票, 动用资金 {available_cash:.2f}")
                
            return TradeDecisionWO(order_list, self)

        # ==========================================
        # 动作 C: 其它时间持仓不动
        # ==========================================
        else:
            return TradeDecisionWO([], self)
        
# ================= 2. Main 运行入口 =================
if __name__ == "__main__":
    # --- 配置区 ---
    # 这里填写你选股脚本生成的 pkl 路径
    PROJECT_ROOT = Path(__file__).resolve().parents[3] 
    PRED_PATH = PROJECT_ROOT / "data" / "short_term_predictions" / "full_test_predictions.pkl"

    # 1. 初始化 Qlib
    qlib.init(provider_uri=str(DATA_PATH), region="cn")

    # 2. 准备预测信号数据 (对接你的选股结果)
    if not Path(PRED_PATH).exists():
        print(f"❌ 找不到预测文件: {PRED_PATH}，请先运行选股脚本。")
    else:
        pred_df = pd.read_pickle(PRED_PATH)
        # 统一索引格式
        if not isinstance(pred_df.index, pd.MultiIndex):
            pred_df['datetime'] = pd.to_datetime(pred_df['datetime'])
            pred_df.set_index(['datetime', 'instrument'], inplace=True)
        pred_df = pred_df.sort_index()

        # 3. 回测配置
        strategy_config = {
            "class": "WeeklySwingStrategy",
            "module_path": "__main__",  # 因为类就在当前文件
            "kwargs": {
                "signal": pred_df,
                "topk": 5,
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
        print("🚀 开始回测...")
        report_dict, indicator_dict = backtest(
            start_time="2025-10-08",
            end_time="2026-03-01",
            strategy=strategy_config,
            executor=executor_config,
            benchmark="SH000300",
            account=1000000,
            exchange_kwargs={
                "freq": "day",
                "limit_threshold": 0.095,
                "deal_price": "close",
                "open_cost": 0.0005,
                "close_cost": 0.0015,
            }
        )

        # 5. 简单结果打印
        print("\n✅ 回测完成！")
        
        # [核心修复] 安全提取 DataFrame：兼容 Qlib 返回元组的情况
        report_data = report_dict['1day']
        if isinstance(report_data, tuple):
            report_df = report_data[0]  # 第一个元素是账户资金流水 DataFrame
            positions_dict = report_data[1]  # 第二个元素是历史持仓明细
        else:
            report_df = report_data

        # 1. 查看累计收益率
        # 注意：有时候 Qlib 的列名可能会有些许差异，如果还报错找不到 'return'，可以打印 print(report_df.columns) 看一下
        cumulative_return = (1 + report_df['return']).cumprod()
        print(f"最终累计收益: {cumulative_return.iloc[-1]:.4f}")

        # 2. 查看换手率 (判断是否有交易)
        avg_turnover = report_df['turnover'].mean()
        print(f"平均每日换手率: {avg_turnover:.4f}")

        # 3. 简单绘图
        import matplotlib.pyplot as plt
        
        # 设置中文字体（可选，防止标题乱码）
        plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS', 'DejaVu Sans']
        plt.rcParams['font.family'] = 'sans-serif'
        plt.rcParams['axes.unicode_minus'] = False
        
        plt.figure(figsize=(10, 5))
        cumulative_return.plot(title="每周波段策略 - 累计收益率 (Cumulative Return)")
        plt.grid(True)
        plt.tight_layout()
        plt.show()