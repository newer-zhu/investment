def get_model_params(hold_days: int):
    """
    核心调参逻辑：
    1. 预测周期越长（hold_days 越大），市场噪音增益越高，因此：
       - 降低学习率 (learning_rate)，防止模型过快拟合随机噪音。
       - 增强正则化 (lambda_l1/l2)，强制模型保持简洁。
       - 减小树深度 (max_depth)，由“深而精”转向“浅而稳”。
    2. 引入动态随机采样：
       - 随持仓天数增加，降低特征采样比例，增加特征间的竞争。
       - 增加叶子节点最少样本数，确保长线信号具有统计意义。
    """
    # 基础通用参数
    params = {
        "objective": "regression",
        "loss": "mse",
        "num_leaves": 31,
        "n_jobs": -1,
        "verbosity": -1,
    }
    
    # 周期性硬配置表
    # key: hold_days -> values: [n_estimators, lr, l1, l2, depth]
    configs = {
        1: {"n_estimators": 500, "lr": 0.010, "l1": 0.05, "l2": 0.05, "depth": 6},
        2: {"n_estimators": 550, "lr": 0.008, "l1": 0.10, "l2": 0.10, "depth": 6},
        3: {"n_estimators": 600, "lr": 0.005, "l1": 0.15, "l2": 0.15, "depth": 5},
        4: {"n_estimators": 650, "lr": 0.005, "l1": 0.20, "l2": 0.20, "depth": 5},
        5: {"n_estimators": 700, "lr": 0.003, "l1": 0.25, "l2": 0.25, "depth": 4},
    }
    
    # 获取对应配置，若超出 1-5 范围则默认取 3 日配置
    c = configs.get(hold_days, configs[3])
    
    # 动态公式计算部分
    # 核心：feature_fraction 随周期增加线性下降，min_data_in_leaf 随周期线性上升
    dynamic_feature_fraction = max(0.6, 0.8 - (hold_days * 0.03))
    dynamic_min_data = 30 + (hold_days * 15)
    
    params.update({
        "n_estimators": c["n_estimators"],
        "learning_rate": c["lr"],
        "max_depth": c["depth"],
        "lambda_l1": c["l1"],
        "lambda_l2": c["l2"],
        "feature_fraction": dynamic_feature_fraction,
        "bagging_fraction": dynamic_feature_fraction,
        "bagging_freq": 5,
        "min_data_in_leaf": dynamic_min_data,
    })
    
    return params