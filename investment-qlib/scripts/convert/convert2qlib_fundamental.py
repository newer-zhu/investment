import pandas as pd
from pathlib import Path

# --- 配置区 ---
INPUT_DIR = Path("/mnt/f/Code/investment/investment-qlib/data/fundamental/fina_indicator")   # 原始 CSV 文件夹
OUTPUT_DIR = Path("/mnt/f/Code/investment/investment-qlib/data/fundamental/features") # 处理后的文件夹
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def transform_and_deduplicate():
    files = list(INPUT_DIR.glob("*.csv"))
    
    for file in files:
        df = pd.read_csv(file)
        
        # --- 新增检查逻辑 ---
        # 定义必须存在的原始列名
        required_cols = ['ann_date', 'end_date', 'ts_code']
        # 检查是否所有必须列都在这个 CSV 里
        if not all(col in df.columns for col in required_cols):
            missing = [c for c in required_cols if c not in df.columns]
            print(f"⚠️ 跳过文件 {file.name}: 缺少列 {missing}")
            continue
        # ------------------

        # 1. 核心修复：剔除空值
        df = df.dropna(subset=['ann_date', 'end_date'])
        
        # 2. 日期转换并过滤失败行
        df['date_dt'] = pd.to_datetime(df['ann_date'], format='%Y%m%d', errors='coerce')
        df['end_date_dt'] = pd.to_datetime(df['end_date'], format='%Y%m%d', errors='coerce')
        
        df = df.dropna(subset=['date_dt', 'end_date_dt'])
        
        if df.empty:
            continue

        # 3. 格式化日期
        df['date'] = df['date_dt'].dt.strftime('%Y-%m-%d')
        df['end_date'] = df['end_date_dt'].dt.strftime('%Y-%m-%d')
        
        # 4. 去重
        df = df.sort_values(by=['date_dt', 'end_date_dt'], ascending=[True, True])
        df = df.drop_duplicates(subset=['date'], keep='last')
        
        # 5. 代码转换
        def format_code(c):
            parts = str(c).split('.')
            return f"{parts[1].upper()}{parts[0]}" if len(parts) == 2 else c
        
        df['symbol'] = df['ts_code'].apply(format_code)
        
        # 6. 整理字段
        target_cols = ['date', 'symbol', 'roe','end_date', 'grossprofit_margin', 'or_yoy', 'debt_to_assets', 'current_ratio','netprofit_margin']
        
        # 只取存在的列，防止因为缺少某个财务指标列又报错
        existing_cols = [c for c in target_cols if c in df.columns]
        df = df[existing_cols]

        # 7. 保存
        new_filename = format_code(file.stem.replace('.csv', '')) + ".csv"
        df.to_csv(OUTPUT_DIR / new_filename, index=False)
if __name__ == "__main__":
    transform_and_deduplicate()
    files = list(OUTPUT_DIR.glob("*.csv"))

    print(f"正在检查 {len(files)} 个文件...")

    for f in files:
        try:
            # 只读日期列，速度快
            df = pd.read_csv(f, usecols=['date'])
            
            # 检查是否存在空值
            if df['date'].isnull().any():
                print(f"❌ 发现空日期: {f.name}")
                
            # 尝试转换日期，看是否会产生 NaT
            test_date = pd.to_datetime(df['date'], errors='coerce')
            if test_date.isnull().any():
                bad_indices = df.index[test_date.isnull()].tolist()
                print(f"⚠️ 文件 {f.name} 第 {bad_indices} 行日期无法解析: {df.loc[bad_indices, 'date'].values}")
                
        except Exception as e:
            print(f"🚨 文件 {f.name} 读取失败: {e}")

    print("检查结束。")