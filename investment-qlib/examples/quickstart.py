"""Minimal quickstart example for this project.

Edit `provider_uri` to point to your local qlib dataset (folder) if you have one.
See README for setup steps.
"""

import sys
from pathlib import Path

# Ensure `src/` is on sys.path so imports work when running the script directly
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from qlib_project.qlib_utils import init_qlib, check_qlib_initialized


def main():
    # 如果你已经准备好本地数据，可以把路径填到下面
    provider_uri = None  # e.g., "~/qlib_data/cn_data"
    try:
        init_qlib(provider_uri=provider_uri, region="cn")
        ok = check_qlib_initialized()
        print("qlib 初始化成功:", ok)
    except Exception as e:
        print("初始化 qlib 失败:", e)


if __name__ == "__main__":
    main()
