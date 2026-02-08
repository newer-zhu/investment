from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
# 数据根路径
SOURCE_PATH = PROJECT_ROOT / "data" 
# 量价数据路径
DATA_PATH = PROJECT_ROOT / "data" / "source" / "qlib_bin"
FINANCE_PATH = PROJECT_ROOT / "data" / "source"  / "finance"

FILTERED_POOL = DATA_PATH / "instruments" / "my_filtered_pool.txt"
TUSHARE_TOKEN= "2f80f707c09dc4ce6c59eb215739349f7a203485b19b4401a683bb6549ec"