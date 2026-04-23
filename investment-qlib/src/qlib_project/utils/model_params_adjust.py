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
    【科技短线攻击版】
    目标：捕捉高波动科技股的短期爆发信号
    改动：增加模型表达能力，针对科技股特有的“非线性”逻辑进行调优
    """

    params = {
        "objective": "huber",          # 保持 Huber，防止个别妖股把模型带偏
        "alpha": 0.85,                 # 略微调低，对异常波动稍微敏感一点点
        "metric": "huber",
        "n_jobs": -1,
        "verbosity": -1,
        "early_stopping_rounds": 80,   # 稍微收紧，防止在噪音过大的科技股里过拟合
        "extra_trees": True,
    }

    # 2. 周期配置表 (针对科技股短线重写)
    configs = {
        # 1-2天持仓：科技股弹性最大，需要更深一点的树和更灵敏的速率
        1: {"est": 1000, "lr": 0.05,  "l1": 0.5, "l2": 1.0, "depth": 5},
        2: {"est": 1200, "lr": 0.04,  "l1": 0.5, "l2": 1.5, "depth": 4}, 
        3: {"est": 1400, "lr": 0.03,  "l1": 0.3, "l2": 1.0, "depth": 4},
        4: {"est": 1500, "lr": 0.02,  "l1": 0.2, "l2": 0.5, "depth": 4},
        5: {"est": 1600, "lr": 0.015, "l1": 0.1, "l2": 0.2, "depth": 4},
    }
    
    c = configs.get(hold_days, configs[3])

    # 3. 动态计算
    # 科技股池子通常比全市场小，num_leaves 不宜过大
    dynamic_num_leaves = int((2 ** c["depth"]) * 0.8)
    
    # 科技股样本相对少，且波动大，min_data 调低一点，允许模型捕捉更细分的局部机会
    dynamic_min_data = int(45 - hold_days * 5) 
    dynamic_min_data = max(20, min(60, dynamic_min_data))

    # 4. 参数注入
    params.update({
        "n_estimators": c["est"],
        "learning_rate": c["lr"],
        "max_depth": c["depth"],
        "lambda_l1": c["l1"],
        "lambda_l2": c["l2"],
        "num_leaves": dynamic_num_leaves,
        "min_data_in_leaf": dynamic_min_data,
        "feature_fraction": 0.7,      # 提高特征采样，因为科技股依赖多个因子的共振
        "bagging_fraction": 0.75,
        "bagging_freq": 1,
    })

    print(f"🚀 科技短线版启动 (Hold: {hold_days}d): LR={c['lr']}, Depth={c['depth']}, MinData={dynamic_min_data}")
    
    return params

def get_model_params_for_trash(hold_days: int):
    """
    【A股妖股/散户票暴力博弈版】
    目标：抛弃平稳性，去捕捉那些稀缺的、极端的涨停/连板收益。
    改动：移除异常值压制，加深树结构，拥抱高波动！
    """

    params = {
        "objective": "regression",     # 放弃 Huber！改回标准的 regression(MSE)，让模型去追逐那些产生极端暴利的样本
        "metric": "rmse",
        "n_jobs": -1,
        "verbosity": -1,
        "early_stopping_rounds": 50,   # 妖股噪音大，缩短早停轮数，见好就收防止过度拟合白马股
        "extra_trees": True,
    }

    # 2. 周期配置表 (针对妖股极端短线重写)
    configs = {
        # 1-2天持仓：妖股博弈往往就是今买明卖/后卖，需要更深的树挖掘极其苛刻的量价条件
        1: {"est": 800,  "lr": 0.08,  "l1": 0.1, "l2": 0.5, "depth": 6}, # depth提高到6，刻画更复杂的逻辑
        2: {"est": 1000, "lr": 0.06,  "l1": 0.2, "l2": 1.0, "depth": 6}, 
        3: {"est": 1200, "lr": 0.05,  "l1": 0.3, "l2": 1.0, "depth": 5},
        4: {"est": 1400, "lr": 0.04,  "l1": 0.4, "l2": 1.5, "depth": 5},
        5: {"est": 1500, "lr": 0.03,  "l1": 0.5, "l2": 2.0, "depth": 4},
    }
    
    c = configs.get(hold_days, configs[2])

    # 3. 动态计算
    dynamic_num_leaves = int((2 ** c["depth"]) * 0.85) # 稍微放开一点叶子数
    
    # 【核心】妖股样本极度稀少（比如“连续两日放量+烂板”这种形态），min_data必须调低！
    dynamic_min_data = int(25 - hold_days * 3) 
    dynamic_min_data = max(10, min(40, dynamic_min_data)) # 最低降到 10，允许极小众策略分支存活

    # 4. 参数注入
    params.update({
        "n_estimators": c["est"],
        "learning_rate": c["lr"],
        "max_depth": c["depth"],
        "lambda_l1": c["l1"],
        "lambda_l2": c["l2"],
        "num_leaves": dynamic_num_leaves,
        "min_data_in_leaf": dynamic_min_data,
        "feature_fraction": 0.85,     # 提高到0.85，妖股博弈需要同时看到价格、成交量、振幅的多维共振
        "bagging_fraction": 0.80,
        "bagging_freq": 1,
    })

    print(f"🚀 妖股暴力博弈版启动 (Hold: {hold_days}d): 拥抱MSE, Depth={c['depth']}, MinData={dynamic_min_data}")
    
    return params

def get_model_params_csi300(hold_days: int):
    """
    【沪深300稳健版】
    目标：捕捉大盘蓝筹股的趋势性机会
    改动：增强模型泛化能力，针对低波动、高容量样本进行结构化调优
    """

    params = {
        "objective": "huber",          # 依然推荐 Huber，处理指数成份股偶尔的极端异动
        "alpha": 0.9,                  # 提高 alpha，对异常值更不敏感，追求大趋势的平均回归
        "metric": "huber",
        "n_jobs": -1,
        "verbosity": -1,
        "early_stopping_rounds": 120,  # 沪深300噪音相对小，早停轮次放宽，允许模型更充分学习趋势
        "extra_trees": False,          # 大票逻辑较清晰，不一定需要极端的随机性
    }

    # 2. 周期配置表 (针对沪深300重载)
    # 大票持仓通常略长，学习率调低，树深度增加以捕捉多因素博弈
    configs = {
        # 沪深300短线波动小，需要更低的学习率和更浅的深度防止捕捉到纯随机噪音
        1: {"est": 800,  "lr": 0.03,  "l1": 1.5, "l2": 2.0, "depth": 4},
        2: {"est": 1000, "lr": 0.02,  "l1": 1.2, "l2": 1.5, "depth": 4}, 
        3: {"est": 1200, "lr": 0.015, "l1": 1.0, "l2": 1.5, "depth": 5},
        4: {"est": 1300, "lr": 0.01,  "l1": 0.8, "l2": 1.2, "depth": 5},
        5: {"est": 1500, "lr": 0.01,  "l1": 0.5, "l2": 1.0, "depth": 6},
    }
    
    c = configs.get(hold_days, configs[3])

    # 3. 动态计算
    # 沪深300样本量虽然固定，但单标的厚度大，可以适当增加叶子数
    dynamic_num_leaves = int((2 ** c["depth"]) * 0.7)
    
    # 大票池子样本量极多，必须大幅提高 min_data_in_leaf，防止模型学到某几只个股的特异性
    # 沪深300过滤掉个股噪音至少需要 100-200 个样本支撑一个叶子
    dynamic_min_data = int(100 + hold_days * 20) 
    dynamic_min_data = max(80, min(250, dynamic_min_data))

    # 4. 参数注入
    params.update({
        "n_estimators": c["est"],
        "learning_rate": c["lr"],
        "max_depth": c["depth"],
        "lambda_l1": c["l1"],
        "lambda_l2": c["l2"],
        "num_leaves": dynamic_num_leaves,
        "min_data_in_leaf": dynamic_min_data,
        "feature_fraction": 0.6,       # 降低特征采样，大票特征相关性高，减少冗余
        "bagging_fraction": 0.85,      # 提高数据采样，利用沪深300样本充足的优势增加稳定性
        "bagging_freq": 5,             # 降低采样频率，进一步平滑模型
    })

    print(f"🏛️ 沪深300稳健版启动 (Hold: {hold_days}d): LR={c['lr']}, Depth={c['depth']}, MinData={dynamic_min_data}")
    
    return params