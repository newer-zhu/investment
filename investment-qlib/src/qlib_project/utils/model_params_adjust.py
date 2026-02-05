def get_model_params(hold_days: int):
    """
    改造目标：
    - 强制抑制过拟合（解决 best iteration = 1）
    - 保留你「周期越长越稳」的设计思想
    """

    # 基础参数（收紧）
    params = {
        "objective": "regression",
        "metric": "l2",
        "num_leaves": 16,          # 🔴 关键：直接砍半
        "n_jobs": -1,
        "verbosity": -1,
        "early_stopping_rounds": 50,
    }

    # 周期配置（整体变“保守”）
    # 注意：短周期不再更激进
    configs = {
        1: {"n_estimators": 800,  "lr": 0.03,  "l1": 0.05, "l2": 0.05, "depth": 4},
        2: {
            "n_estimators": 1000, 
            "lr": 0.02,           # 从 0.01 提升到 0.02
            "l1": 0.5,            # 稍微降低正则化，让更多特征参与
            "l2": 0.5, 
            "depth": 3,           # 从 2 层提升到 3 层，允许模型理解简单的特征组合
        },
        3: {"n_estimators": 1200, "lr": 0.02,  "l1": 0.10, "l2": 0.10, "depth": 4},
        4: {"n_estimators": 1400, "lr": 0.015, "l1": 0.15, "l2": 0.15, "depth": 3},
        5: {"n_estimators": 1600, "lr": 0.01,  "l1": 0.20, "l2": 0.20, "depth": 3},
    }

    c = configs.get(hold_days, configs[3])

    # 动态部分（幅度拉大，真正起作用）
    feature_fraction = max(0.5, 0.75 - hold_days * 0.05)
    min_data_in_leaf = 80 + hold_days * 40   # 🔴 关键：数量级变化

    params.update({
        "n_estimators": c["n_estimators"],
        "learning_rate": c["lr"],
        "max_depth": c["depth"],
        "lambda_l1": c["l1"],
        "lambda_l2": c["l2"],
        "bagging_freq": 5,
        "num_leaves": 8,           # 限制叶子数，比 depth 更有效地控制复杂度
        "min_data_in_leaf": 30,     # 强制降低！CSI300 样本少，必须降到 5-10
        "feature_fraction": 0.6,   # 允许模型看所有特征
        "bagging_fraction": 0.7,
        "early_stopping_rounds": 100, # 即使验证集不动，也多给它 100 轮机会
    })

    return params
