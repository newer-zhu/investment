import qlib
from qlib.contrib.strategy import TopkDropoutStrategy
from qlib.contrib.report import analysis_model, analysis_position
from qlib.backtest import backtest, executor

# 1. 定义策略逻辑：Top 10 选股，持仓 2 天
strategy_config = {
    "class": "TopkDropoutStrategy",
    "module_path": "qlib.contrib.strategy",
    "kwargs": {
        "model": model_v2,          # 你刚训练好的 Huber 优化版模型
        "dataset": dataset_v2,      # 精炼特征后的数据集
        "topk": 10,                 # 每日持仓 10 只
        "n_drop": 5,                # 每次调仓换掉预测排名掉出前 5 的股票
        "signal_column": "score",   # 使用模型预测的分数列
    },
}

# 2. 定义执行器：控制手续费、滑点和持仓时间
executor_config = {
    "class": "SimulatorExecutor",
    "module_path": "qlib.backtest.executor",
    "kwargs": {
        "time_per_step": "day",
        "generate_report": True,
        "verbose": True,
        "indicator_config": {"show_indicator": True},
        # 模拟真实交易摩擦：手续费 (万三) + 印花税 (千一)
        "trade_exchange": {
            "limit_threshold": 0.095,      # 涨停买不进
            "deal_price": "close",         # 以收盘价成交
            "open_cost": 0.0003,
            "close_cost": 0.0013,
            "min_cost": 5,
        },
    },
}

# 3. 运行回测 (针对 test 段数据)
report_normal, positions_normal = backtest(
    server=None, 
    strategy=strategy_config, 
    executor=executor_config,
    start_time=test_start, 
    end_time=pool_date
)

# 4. 生成可视化报告
analysis_df, analysis_obj = analysis_model.analyze_model_all(
    report_normal, 
    positions_normal, 
    dataset_v2
)