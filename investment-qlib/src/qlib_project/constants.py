from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]   # investment-qlib/
REPO_ROOT = PROJECT_ROOT.parent                       # investment/ (repo root)

# ================== 配置文件 ==================
CONFIG_PATH = REPO_ROOT / "config.ini"

# ================== 数据根路径 ==================
SOURCE_PATH = PROJECT_ROOT / "data"
# 量价数据路径
DATA_PATH = PROJECT_ROOT / "data" / "source" / "qlib_bin"
FINANCE_PATH = PROJECT_ROOT / "data" / "source" / "finance"

# ================== 股票池文件 ==================
FILTERED_POOL = DATA_PATH / "instruments" / "my_filtered_pool.txt"
TRASH_POOL = DATA_PATH / "instruments" / "my_trash_pool.txt"
# 基础池: 仅主板+行业过滤（不含动态量价过滤），用于滚动训练的每周刷新
BASE_POOL = DATA_PATH / "instruments" / "my_base_pool.txt"
TREND_POOL = DATA_PATH / "instruments" / "my_trend_pool.txt"

# ================== 模型存储 ==================
MODELS_DIR = PROJECT_ROOT / "data" / "models"
TREND_MODEL_DIR = MODELS_DIR / "trend"
REBOUND_MODEL_DIR = MODELS_DIR / "rebound"

# 趋势策略
TREND_MODEL_FILE = TREND_MODEL_DIR / "lgb_trend_model.pkl"
TREND_FEATURE_FILE = TREND_MODEL_DIR / "trend_refined_features.json"
TREND_SIGNAL_FILE = TREND_MODEL_DIR / "lgb_trend_pred.pkl"

# 反弹策略
REBOUND_MODEL_FILE = REBOUND_MODEL_DIR / "lgb_rebound_model.pkl"
REBOUND_FEATURE_FILE = REBOUND_MODEL_DIR / "rebound_refined_features.json"
REBOUND_SIGNAL_FILE = REBOUND_MODEL_DIR / "lgb_rebound_pred.pkl"

# ================== 预测结果输出 ==================
TREND_PREDICTIONS_DIR = PROJECT_ROOT / "data" / "trend_predictions"
REBOUND_PREDICTIONS_DIR = PROJECT_ROOT / "data" / "rebound_predictions"
# 回测绩效摘要 (backtest_trend.py 生成, 存档用)
TREND_BACKTEST_SUMMARY = PROJECT_ROOT / "logs" / "trend_backtest_summary.json"

# ================== 趋势交易规则 (回测与仓位管理共用, 修改需保持一致) ==================
TREND_RULES = {
    "topk": 2,
    "max_positions": 3,
    "hold_days": 5,
    "stop_loss": -0.08,
    "take_profit": 0.10,
    "trend_exit": True,
    "trend_exit_ma": 10,
    "trend_exit_buffer": 0.01,
    "trend_break_min_hold": 2,
    "use_market_filter": True,
    "market_exit": True,
    "signal_max_age": 10,
    "min_price": 1.0,
    "vol_ratio_max": 1.5,
}

# ================== 持仓状态 (position_manager 记录实盘持仓) ==================
TREND_POSITIONS_FILE = PROJECT_ROOT / "data" / "positions" / "trend_positions.json"

# ================== API Token ==================
TUSHARE_TOKEN = "2f80f707c09dc4ce6c59eb215739349f7a203485b19b4401a683bb6549ec"

TECH_KEYWORDS = [
    # 1. 硬科技（半导体、先进制造与材料）
    "半导体", "芯片", "集成电路", "电子", "元器件",
    "光电", "光学", "材料", "传感器", "光刻机", "光刻胶", 
    "先进封装", "Chiplet", "HBM", "半导体设备", "先进制造",
    "工业母机", "增材制造", "3D打印", "数控系统", "靶材",

    # 2. 软件 / AI / 数字经济 / 信创
    "软件", "计算机", "信息", "信息化", "系统",
    "人工智能", "AI", "大模型", "算法", "IT", 
    "云", "云计算", "SaaS", "数据", "数据要素",
    "数字", "数字经济", "信创", "网络安全", "边缘计算",

    # 3. 通信 / 算力基础设施
    "通信", "5G", "6G", "卫星", "互联网", "航天",
    "物联网", "IDC", "算力", "服务器", "CPO", "光模块", 
    "液冷", "光通信", "散热", "空管",

    # 4. 新能源科技
    "新能源", "储能", "锂电", "电池",
    "光伏", "风电", "逆变器",
    "氢能", "充电桩", "生物",

    # 5. 汽车科技
    "智能驾驶", "自动驾驶", "车联网",
    "激光雷达", "雷达",
    "汽车电子", "智能座舱",

    # 6. 前沿概念 / 新质生产力
    "机器人", "人形机器人", "低空", "无人机",
    "AR", "VR", "MR", "元宇宙", "区块链", "Web3",
    "量子", "量子计算", "脑机",  
    "碳纤维", "合成生物", "基因", "创新"
]
