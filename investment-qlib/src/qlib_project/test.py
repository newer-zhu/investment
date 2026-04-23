import qlib
from qlib.data import D
from pathlib import Path
import pandas as pd
# 配置你的路径
PROJECT_ROOT = Path(__file__).resolve().parents[2] # 确保指向你的数据根目录

# 路径配置（请修改为你的实际绝对路径）
path_price = PROJECT_ROOT / "data" / "source" / "qlib_bin"

# ==========================================
# 第一步：读取量价数据 (qlib_bin)
# ==========================================
qlib.init(provider_uri=path_price)
# 选一只股票，查公告日之后的一周数据
fields = [ "$amount"]
df = D.features(["sz302132"], fields, start_time="2025-10-28", end_time="2025-11-05")

print(df)

fields = [ "$roe"]
df = D.features(["sz302132-fi"], fields, start_time="2025-03-27", end_time="2025-11-05")

print(df)