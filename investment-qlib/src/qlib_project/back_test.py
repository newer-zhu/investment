import sys
from pathlib import Path
import pandas as pd
import qlib
from qlib.data import D
from qlib.contrib.strategy import TopkDropoutStrategy
# [修复1] 正确的导入路径：backtest 在 qlib.backtest 下
from qlib.backtest import backtest, executor
# [修复2] 风险分析工具在 qlib.contrib.evaluate 下
from qlib.contrib.evaluate import risk_analysis
import matplotlib.pyplot as plt
from qlib.backtest.decision import OrderDir, TradeDecisionWO, OrderHelper
# 引用你的配置
BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
try:
    from constants import DATA_PATH
except ImportError:
    from qlib_project.constants import DATA_PATH
from qlib.config import REG_CN

# ================= 1. 自定义风控策略类 (保持不变) =================
class RiskControlTopkStrategy(TopkDropoutStrategy):
    def __init__(self, bias_limit=0.10, **kwargs):
        super().__init__(**kwargs)
        self.bias_limit = bias_limit

    def get_portfolio_instruments(self, *args, **kwargs):
        """Compatibility wrapper: delegate to parent if available,
        otherwise derive instrument list from `self.signal`.
        """
        # Try parent implementation first
        parent = getattr(super(), "get_portfolio_instruments", None)
        if callable(parent):
            try:
                return parent(*args, **kwargs)
            except TypeError:
                try:
                    return parent()
                except Exception:
                    pass

        # Fallback: extract instruments from self.signal
        try:
            sig = None
            if hasattr(self.signal, "get_signal"):
                sig = self.signal.get_signal()
            elif hasattr(self.signal, "to_dataframe"):
                sig = self.signal.to_dataframe()
            else:
                sig = self.signal

            if isinstance(sig, (pd.DataFrame, pd.Series)):
                idx = sig.index
                if isinstance(idx, pd.MultiIndex) and 'instrument' in idx.names:
                    return list(idx.get_level_values('instrument').unique())
                else:
                    return list(idx.unique())
        except Exception:
            pass

        return []

    def generate_trade_decision(self, execute_result=None):
        trade_date = self.trade_calendar.start_time
        
        # 1. 获取原始预测分 (使用你观察到的只有两列的情况)
        try:
            # 这里的 pred_score 可能是 Series 或 DataFrame
            pred_score = self.signal.get_signal(start_time=trade_date, end_time=trade_date)
        except Exception:
            return super().generate_trade_decision(execute_result)

        if pred_score is None or pred_score.empty:
            return super().generate_trade_decision(execute_result)

        # 2. 标准化 current_scores，确保索引是 instrument
        if isinstance(pred_score, pd.DataFrame):
            # 处理 MultiIndex 或普通列
            if 'score' in pred_score.columns:
                current_scores = pred_score.set_index('instrument')['score'] if 'instrument' in pred_score.columns else pred_score['score']
            else:
                current_scores = pred_score.iloc[:, 0] # 取第一列
        else:
            current_scores = pred_score

        # 移除可能存在的 datetime 层级
        if isinstance(current_scores.index, pd.MultiIndex):
            current_scores = current_scores.droplevel('datetime')

        # 3. 获取 Bias 特征并过滤
        instruments = current_scores.index.tolist()
        bias_df = D.features(instruments, ["$close / Mean($close, 5) - 1"], 
                            start_time=trade_date, end_time=trade_date)
        
        if not bias_df.empty:
            bias_df.columns = ['bias_5d']
            bias_df = bias_df.reset_index().set_index('instrument')[['bias_5d']]
            
            combined = pd.DataFrame({'score': current_scores}).join(bias_df, how='inner')
            # 执行风控剔除
            safe_stocks = combined[combined['bias_5d'] < self.bias_limit]
            
            # 使用过滤后的分数更新 current_scores
            current_scores = safe_stocks['score']
            print(f"[{trade_date.date()}] 风控后剩余股票: {len(current_scores)} 只")

        # 4. 【核心逻辑重写】手动执行 TopK 逻辑
        # 这一步代替了父类的内部逻辑
        current_scores = current_scores.dropna().sort_values(ascending=False)
        topk_list = current_scores.head(self.topk).index.tolist()

        # 5. 生成交易指令 (TradeDecision)
        # 这里构造 Order 列表并返回 TradeDecisionWO
        order_list = []

        # 获取当前持仓
        current_hold = self.get_portfolio_instruments()

        # 卖出逻辑：不在 TopK 且在持仓中的 -> 全卖（amount=0 表示全卖）
        for stock in current_hold:
            if stock not in topk_list:
                order_list.append(OrderHelper.create(code=stock, amount=0.0, direction=OrderDir.SELL))

        # 买入逻辑：在 TopK 但不在持仓中的 -> 下买单（amount=1 表示占位，executor 会处理实际分配）
        for stock in topk_list:
            if stock not in current_hold:
                order_list.append(OrderHelper.create(code=stock, amount=1.0, direction=OrderDir.BUY))

        return TradeDecisionWO(order_list, self)
    
# ================= 2. 执行回测主流程 =================
def run_backtest_analysis(
    pred_path, 
    start_date="2025-10-01", 
    end_date="2026-02-01",
    topk=10,
    bias_limit=0.12
):
    # 初始化 Qlib
    qlib.init(provider_uri=str(DATA_PATH), region=REG_CN)
    
    # 1. 读取 LGB 生成的预测分
    print(f"正在读取预测数据: {pred_path}")
    pred_df = pd.read_pickle(pred_path)
    
# 1. 强制检查并设置索引名字
    # 如果索引名是 None，或者层级名字不对，重新指定
    if isinstance(pred_df.index, pd.MultiIndex):
        pred_df.index.names = ['datetime', 'instrument']
    else:
        # 如果是 reset_index 过的 DataFrame，重新设回 MultiIndex
        if 'datetime' in pred_df.columns and 'instrument' in pred_df.columns:
            pred_df['datetime'] = pd.to_datetime(pred_df['datetime'])
            pred_df.set_index(['datetime', 'instrument'], inplace=True)
        else:
            raise ValueError("预测数据格式错误，缺少 datetime 或 instrument 列/索引")

    # 2. 确保日期格式标准化 (去除时分秒)
    # 这一步能解决 [2025-10-01 00:00:00] 信号为空的问题
    temp_df = pred_df.reset_index()
    temp_df['datetime'] = pd.to_datetime(temp_df['datetime']).dt.normalize()
    pred_df = temp_df.set_index(['datetime', 'instrument']).sort_index()

    # 3. 诊断打印（非常关键）
    all_dates = pred_df.index.get_level_values('datetime').unique()
    print(f"📊 索引层级名: {pred_df.index.names}")
    print(f"📊 数据起始日期: {all_dates.min()}, 结束日期: {all_dates.max()}")
        
    pred_df = pred_df.reset_index()
    
    # 数据清洗：确保索引是 [datetime, instrument] 且只有一列 score
    if "score" not in pred_df.columns:
        # 尝试找最后一列作为 score
        pred_df.rename(columns={pred_df.columns[-1]: "score"}, inplace=True)
    
    # 如果索引不是 MultiIndex，设置一下
    if not isinstance(pred_df.index, pd.MultiIndex):
        pred_df = pred_df.set_index(["datetime", "instrument"])
    
    # 确保只保留 score 列，并且按索引排序
    pred_df = pred_df[["score"]].sort_index()

    # 2. 配置回测参数
    # 资金 100万，基准沪深300
    STRATEGY_CONFIG = {
        "class": "RiskControlTopkStrategy",
        "module_path": "__main__",  # 因为类定义在当前文件
        "kwargs": {
            "signal": pred_df,
            "topk": topk,
            "n_drop": topk, # 激进模式：每天全换 (也可设为 2 或 5)
            "bias_limit": bias_limit, # 传入风控阈值
        },
    }

    EXECUTOR_CONFIG = {
        "class": "SimulatorExecutor",
        "module_path": "qlib.backtest.executor",
        "kwargs": {
            "time_per_step": "day",
            "generate_portfolio_metrics": True,
        }
    }

    # 3. [核心修复] 调用 backtest 函数
    print("🚀 开始回测 (RiskControlTopkStrategy)...")
    
    # backtest 函数签名通常为: (start_time, end_time, strategy, executor, benchmark, account, ...)
    portfolio_metric_dict, indicator_metric_dict = backtest(
        start_time=start_date,
        end_time=end_date,
        strategy=STRATEGY_CONFIG,
        executor=EXECUTOR_CONFIG,
        benchmark="SH000300",
        account=1000000,
        exchange_kwargs={
            "freq": "day",
            "limit_threshold": 0.095,
            "deal_price": "close",
            "open_cost": 0.0005,
            "close_cost": 0.0015,
            "min_cost": 5,
        }
    )

    # DEBUG: inspect returned dicts
    print('\n--- DEBUG: portfolio_metric_dict keys ---')
    if isinstance(portfolio_metric_dict, dict):
        print(list(portfolio_metric_dict.keys()))
        for k, v in portfolio_metric_dict.items():
            try:
                print(k, type(v), getattr(v, 'shape', None))
            except Exception:
                print(k, type(v))
                
            # if tuple, inspect contents
            if isinstance(v, (tuple, list)):
                for i, item in enumerate(v):
                    print(f"  - item[{i}] type: {type(item)}")
                    if isinstance(item, pd.DataFrame):
                        print(f"    columns: {list(item.columns)}; shape: {item.shape}")
                        print(item.head(2))
    else:
        print('portfolio_metric_dict is not a dict:', type(portfolio_metric_dict))

    print('\n--- DEBUG: indicator_metric_dict keys ---')
    if isinstance(indicator_metric_dict, dict):
        print(list(indicator_metric_dict.keys()))
    else:
        print('indicator_metric_dict is not a dict:', type(indicator_metric_dict))

    # ================= 3. 结果分析与绘图 =================
    print("\n===== 回测结束，正在生成报告 =====")
    
    # [修复] 使用 risk_analysis 进行分析
    # portfolio_metric_dict 可能使用键 '1day' 或 '1d'，并且值可能是 DataFrame 或包含 DataFrame 的 tuple
    analysis_df = None
    # candidate keys ordered by preference
    candidates = ['1d', '1day', '1day_metrics', '1day_metric', '1']
    for key in candidates:
        if key in portfolio_metric_dict:
            val = portfolio_metric_dict[key]
            # if tuple, find first DataFrame inside
            if isinstance(val, tuple) or isinstance(val, list):
                for item in val:
                    if isinstance(item, pd.DataFrame):
                        analysis_df = item
                        break
                if analysis_df is not None:
                    chosen_key = key
                    break
            elif isinstance(val, pd.DataFrame):
                analysis_df = val
                chosen_key = key
                break

    # fallback: try to locate any DataFrame in the dict
    if analysis_df is None:
        for k, v in portfolio_metric_dict.items():
            if isinstance(v, pd.DataFrame):
                analysis_df = v
                chosen_key = k
                break

    if analysis_df is None or (isinstance(analysis_df, pd.DataFrame) and analysis_df.empty):
        print("❌ 回测结果为空，请检查时间范围或预测数据是否覆盖该时间段。")
        return None
    else:
        print(f"ℹ️ 使用回测输出键: {chosen_key}")

    # 计算风险指标
        if 'excess_return_without_cost' in analysis_df.columns:
            risk_data = risk_analysis(analysis_df['excess_return_without_cost'], freq="day")
        elif 'return' in analysis_df.columns and 'bench' in analysis_df.columns:
            analysis_df['excess_return_without_cost'] = analysis_df['return'] - analysis_df['bench']
            risk_data = risk_analysis(analysis_df['excess_return_without_cost'], freq="day")
        else:
            print("❌ 无法计算风险指标，缺少必要的列。")
            return None
    
    print("\n----------- 核心指标 (Benchmark: SH000300) -----------")
    print('DEBUG risk_data type:', type(risk_data))
    try:
        print('DEBUG risk_data:', risk_data)
    except Exception:
        pass
    # Try to read common keys
    def _get_metric(rdata, key):
        try:
            if isinstance(rdata, dict):
                return rdata.get(key)
            if isinstance(rdata, pd.DataFrame):
                if 'risk' in rdata.columns:
                    return float(rdata.loc[key, 'risk'])
                row = rdata.loc[key]
                if isinstance(row, pd.Series):
                    return float(row.iloc[0])
                return float(row)
            if isinstance(rdata, pd.Series):
                return float(rdata[key])
        except Exception:
            return None

    ar = _get_metric(risk_data, 'annualized_return')
    mdd = _get_metric(risk_data, 'max_drawdown')
    ir = _get_metric(risk_data, 'information_ratio')
    if ar is not None:
        print(f"年化超额收益 (Alpha): {ar:.2%}")
    if mdd is not None:
        print(f"最大回撤 (MDD): {mdd:.2%}")
    if ir is not None:
        print(f"夏普比率 (Sharpe): {ir:.2f}")

    # 绘制资金曲线
    # value 是账户总权益
    report_df = analysis_df.copy()
    report_df["value_norm"] = report_df["value"] / report_df["value"].iloc[0]
    
    plt.figure(figsize=(12, 6))
    plt.plot(report_df.index, report_df["value_norm"], label=f"Strategy (Top{topk})", color='red')
    
    # 尝试画基准
    if 'bench' in report_df.columns:
         report_df["bench_norm"] = report_df["bench"] / report_df["bench"].iloc[0]
         plt.plot(report_df.index, report_df["bench_norm"], label="Benchmark (SH000300)", color='gray', linestyle='--')

    # 安全使用已提取的指标来设置标题（避免直接索引 risk_data 可能的 DataFrame 结构）
    sharpe_str = f"{ir:.2f}" if ir is not None else "N/A"
    mdd_str = f"{mdd:.2%}" if mdd is not None else "N/A"
    plt.title(f"Backtest: Bias<{bias_limit} | Sharpe: {sharpe_str} | MDD: {mdd_str}")
    plt.grid(True)
    plt.legend()
    plt.savefig("backtest_result.png")
    print("\n📊 资金曲线已保存至 backtest_result.png")

    return risk_data

if __name__ == "__main__":
    # 路径要和你 LGB 脚本里保存的一致
    PROJECT_ROOT = Path(__file__).resolve().parents[2] 
    PRED_FILE = PROJECT_ROOT / "data" / "pool_predictions_lgb" / "full_test_predictions.pkl"
    
    # 确保文件存在
    if not PRED_FILE.exists():
        print(f"❌ 错误: 找不到预测文件 {PRED_FILE}")
        print("请先运行 LGB 模型脚本生成预测数据。")
    else:
        run_backtest_analysis(
            pred_path=PRED_FILE,
            start_date="2025-10-30", 
            end_date="2026-04-01",   
            topk=10,
            bias_limit=0.10
        )
