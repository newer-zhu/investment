# def get_model_params(hold_days: int):
#     """
#     【最终优化版】
#     目标：降低 RankIC 波动，实现真正的“动态适配”
#     修复：去除了 params.update 中硬编码对动态计算值的覆盖
#     """

#     # 1. 基础配置（通用）
#     # 针对 RankIC 波动大，我们开启 extra_trees (直方图优化)，增加鲁棒性
#     params = {
#         "objective": "regression",
#         "metric": "l2",
#         "n_jobs": -1,
#         "verbosity": -1,
#         "early_stopping_rounds": 100, # 保持耐心
#         "extra_trees": True,          # 🟢 新增：增加随机性，平滑 RankIC 波动
#     }

#     # 2. 周期差异化配置表
#     # 逻辑：周期越短，噪音越大，需要更强的 L1/L2 压制，但允许稍高的学习率
#     # 周期越长，信号越稳，可以降低正则，但降低学习率慢慢学
#     configs = {
#         # hold_days: [n_estimators, lr, l1, l2, depth]
#         1: {"est": 800,  "lr": 0.04, "l1": 0.2, "l2": 0.5, "depth": 4},
#         2: {"est": 1000, "lr": 0.03, "l1": 0.1, "l2": 0.5, "depth": 3}, # 你现在的周期
#         3: {"est": 1200, "lr": 0.02, "l1": 0.1, "l2": 0.1, "depth": 3},
#         4: {"est": 1400, "lr": 0.02, "l1": 0.05,"l2": 0.05,"depth": 3},
#         5: {"est": 1600, "lr": 0.01, "l1": 0.0, "l2": 0.0, "depth": 3},
#     }
    
#     # 获取当前周期的配置，默认为周期 3 的配置
#     c = configs.get(hold_days, configs[3])

#     # 3. 动态计算核心参数
    
#     # 🟢 动态叶子数：根据深度自动计算 (约等于 2^depth * 0.7)
#     # depth 3 -> 6 leaves, depth 4 -> 12 leaves
#     # 这样避免了 depth=3 但 leaves=16 这种无效设置
#     dynamic_num_leaves = int((2 ** c["depth"]) * 0.75)

#     # 🟢 动态最小样本数 (针对 CSI300 每日 300 只票的特性优化)
#     # 300只票，如果是 hold_days=1 (高噪)，我们需要每片叶子至少有 40 只票支撑 (更稳)
#     # hold_days=5 (低噪)，我们可以允许 20 只票就分裂 (更细)
#     # 注意：这个逻辑和之前的“越长越保守”是反直觉的，但在小样本下，短线反而需要大样本来抗噪
#     dynamic_min_data = int(50 - hold_days * 5) 
#     # 范围控制：最少 15，最多 60
#     dynamic_min_data = max(15, min(60, dynamic_min_data))

#     # 4. 更新参数
#     params.update({
#         "n_estimators": c["est"],
#         "learning_rate": c["lr"],
#         "max_depth": c["depth"],
#         "lambda_l1": c["l1"],
#         "lambda_l2": c["l2"],
        
#         # 动态值注入 (🔴 关键：这里不再写死数字)
#         "num_leaves": dynamic_num_leaves,
#         "min_data_in_leaf": dynamic_min_data,
        
#         # 采样配置
#         "feature_fraction": 0.7,  # 稍微提高一点，让你好不容易加的 IC 特征能被选到
#         "bagging_fraction": 0.7,
#         "bagging_freq": 1,        # 每一轮都重采样，增加抗波动能力
#     })

#     print(f"🔧 模型参数已生成 (Hold: {hold_days}d): Depth={c['depth']}, Leaves={dynamic_num_leaves}, MinData={dynamic_min_data}, LR={c['lr']}")
    
#     return params

def get_model_params(hold_days: int):
    """
    【最终排序优化版】
    目标：提升 RankIC_IR，增强模型在极端波动环境下的稳健性
    核心改动：切换至 Huber 损失函数，并引入更严格的复杂度控制
    """

    # 1. 基础配置：引入 huber 目标函数
    params = {
        "objective": "huber",          # 🟢 核心改动：抗离群值，保护排序稳定性
        "alpha": 0.9,                  # Huber 的阈值，控制对异常值的容忍度
        "metric": "huber",             # 监控 huber 损失
        "n_jobs": -1,
        "verbosity": -1,
        "early_stopping_rounds": 100,
        "extra_trees": True,           # 保持随机性，抑制震荡
    }

    # 2. 周期配置表 (针对 Huber 调整)
    configs = {
        # 针对 2 天持仓 (hold_days=2)，我们进一步收紧学习率
        1: {"est": 800,  "lr": 0.03, "l1": 0.2, "l2": 0.5, "depth": 4},
        2: {"est": 1200, "lr": 0.02, "l1": 0.5, "l2": 1.0, "depth": 3}, 
        3: {"est": 1400, "lr": 0.015,"l1": 0.5, "l2": 0.5, "depth": 3},
        4: {"est": 1600, "lr": 0.01, "l1": 0.3, "l2": 0.3, "depth": 3},
        5: {"est": 1800, "lr": 0.01, "l1": 0.1, "l2": 0.1, "depth": 3},
    }
    
    c = configs.get(hold_days, configs[3])

    # 3. 动态计算（基于你的 CSI300 样本量）
    dynamic_num_leaves = int((2 ** c["depth"]) * 0.75)
    
    # 调大最小样本数，防止模型钻牛角尖
    dynamic_min_data = int(60 - hold_days * 5) 
    dynamic_min_data = max(30, min(80, dynamic_min_data))

    # 4. 参数注入
    params.update({
        "n_estimators": c["est"],
        "learning_rate": c["lr"],
        "max_depth": c["depth"],
        "lambda_l1": c["l1"],
        "lambda_l2": c["l2"],
        "num_leaves": dynamic_num_leaves,
        "min_data_in_leaf": dynamic_min_data,
        "feature_fraction": 0.65,      # 略微降低，强制让不同决策树看不同的黄金特征
        "bagging_fraction": 0.75,
        "bagging_freq": 1,
    })

    print(f"🎯 排序优化版启动 (Hold: {hold_days}d): Obj=Huber, Depth={c['depth']}, MinData={dynamic_min_data}")
    
    return params