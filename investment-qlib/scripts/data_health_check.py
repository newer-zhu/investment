import qlib
from qlib.data import D
import pandas as pd
import numpy as np

# 1. 初始化（请替换为你的实际 data 路径）
qlib.init(provider_uri='investment-qlib/data/source')

# 2. 获取所有标的
instruments = D.list_instruments(D.instruments('all'))
fields = ['$open', '$close', '$high', '$low', '$volume', '$factor']

print(f"开始检查 {len(instruments)} 个标的的数据健康状况...")

# 3. 批量读取并校验
# 我们抽取最近 1000 天的数据作为样本，或者去掉 start_time 检查全量
df = D.features(instruments, fields, start_time='2020-01-01')

# --- 执行健康指标检查 ---
print("\n[ 1. 缺失值检查 ]")
nan_report = df.isnull().sum()
print(nan_report[nan_report > 0] if not nan_report.empty else "无缺失值")

print("\n[ 2. 价格逻辑检查 (High < Low) ]")
error_price = df[df['$high'] < df['$low']]
print(f"逻辑错误行数: {len(error_price)}")

print("\n[ 3. 复权因子检查 ]")
# 检查 factor 是否存在 <= 0 的情况（会导致计算复权价报错）
invalid_factor = df[df['$factor'] <= 0]
print(f"非法复权因子行数: {len(invalid_factor)}")

print("\n[ 4. 零成交量检查 ]")
zero_vol = (df['$volume'] == 0).sum()
print(f"成交量为 0 的天数: {zero_vol} (这在停牌时是正常的，但在交易日可能存疑)")