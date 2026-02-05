from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PATH = PROJECT_ROOT / "data" / "source" / "qlib_bin"
FINANCE_PATH = PROJECT_ROOT / "data" / "source"  / "finance"

FILTERED_POOL = DATA_PATH / "instruments" / "my_filtered_pool.txt"
