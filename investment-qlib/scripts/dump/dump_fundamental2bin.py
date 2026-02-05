import pandas as pd
from pathlib import Path

import pandas as pd
import numpy as np
import struct
from pathlib import Path
from tqdm import tqdm

# ================= 配置区 =================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
# 1. 刚才生成的日频 CSV 文件夹
CSV_DIR = PROJECT_ROOT / "data"  / "fundamental" / "temp_daily_csv"
# 2. 你的 QLib 数据源目录 (目标目录)
QLIB_DATA_DIR = PROJECT_ROOT / "data" / "source"
# 3. 需要打包的字段 (必须与 CSV header 一致)
FIELDS = ['net_profit', 'revenue', 'net_profit_yoy', 'revenue_yoy', 'roe', 'gross_margin', 'debt_ratio']

def debug_and_dump():
    print(f"🔍 正在检查 QLib 根目录: {QLIB_DATA_DIR}")
    if not (QLIB_DATA_DIR / "features").exists():
        print(f"❌ 错误：在 {QLIB_DATA_DIR} 下没找到 'features' 文件夹！")
        # 尝试列出当前目录下所有文件夹协助排查
        print(f"当前目录下包含: {[x.name for x in QLIB_DATA_DIR.iterdir() if x.is_dir()]}")
        return

    csv_files = list(CSV_DIR.glob("*.csv"))
    print(f"📊 找到待处理 CSV 数量: {len(csv_files)}")

    for csv_file in csv_files[:5]: # 先拿前5个测试
        # 统一转为小写，这是 QLib 的目录规范
        symbol = csv_file.stem.lower()
        
        # 自动识别是否带 SH/SZ 前缀
        # 如果你的 CSV 叫 SH600021.csv，这里就是 sh600021
        target_dir = QLIB_DATA_DIR / "features" / symbol
        
        print(f"📍 正在尝试写入股票 {symbol} 到目录: {target_dir}")
        
        if not target_dir.exists():
            print(f"⚠️  警告：目录 {target_dir} 不存在，将尝试创建。")
            target_dir.mkdir(parents=True, exist_ok=True)

        # 执行写入
        df = pd.read_csv(csv_file)
        for col in df.columns:
            if col in ['date', 'datetime', 'symbol', 'instrument']: continue
            
            data = df[col].values.astype(np.float32)
            bin_file = target_dir / f"{col.lower()}.bin"
            
            with open(bin_file, 'wb') as f:
                data.tofile(f)
            print(f"   ✅ 已生成: {bin_file.name}")

if __name__ == "__main__":
    debug_and_dump()