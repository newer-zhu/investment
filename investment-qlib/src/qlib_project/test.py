import qlib
from qlib.data import D
from pathlib import Path
import pandas as pd
# 配置你的路径
PROJECT_ROOT = Path(__file__).resolve().parents[2] # 确保指向你的数据根目录

# 路径配置（请修改为你的实际绝对路径）
path_price = PROJECT_ROOT / "data" / "source" / "qlib_bin"
path_fin = PROJECT_ROOT / "data" / "source" / "finance"

# ==========================================
# 第一步：读取量价数据 (qlib_bin)
# ==========================================
qlib.init(provider_uri=path_price)
print("正在读取量价数据...")
df_price = D.features(
    instruments=['SH600519'], 
    fields=['$close', '$volume'], 
    start_time='2026-01-01', 
    end_time='2026-02-04'
)

# ==========================================
# 第二步：读取财务数据 (finance)
# ==========================================
# 注意：Qlib 初始化后通常是单例，但在这种脚本模式下，
# 我们可以重新 init 来切换数据源指针
qlib.init(provider_uri=path_fin)
print("正在读取财务数据...")
# 注意：这里取出的财务数据还是原始的（稀疏的）
df_fin = D.features(
    instruments=['SH600519'],
    fields=["$roe"],
    start_time='2025-01-01',
    end_time='2026-06-01'
)

print(df_fin.dropna().tail(10))

# ==========================================
# 第三步：合并数据 (Join)
# ==========================================
df_fin_aligned = df_fin.reindex(df_price.index)

# 4. 再 ffill（这一步才“金融语义正确”）
df_fin_aligned['roe_filled'] = df_fin_aligned['roe'].ffill(limit=120)

# 5. 合并
df_total = pd.concat([df_price, df_fin_aligned], axis=1)

print(df_total)