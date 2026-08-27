"""每日自动趋势预测入口 (由 cron 在交易日 20:00 调用)。

流程:
  1. 取 <= 今天的最近一个"实际有行情数据"的交易日 (用几只流动性好的标的探测, 防数据滞后)。
  2. 与上次已处理日期 (state 文件) 比较, 相同则跳过 → 周末/节假日/数据未更新时不会重复发邮件。
  3. 有新交易日则调 trend_strategy.predict_day, 生成推荐并保存/推后端/发邮件。

依赖: 数据更新任务需在 20:00 前把当日行情写入 qlib bin, 否则会因"今日无数据"而跳过。
用法:
  python run_daily_predict.py            # 正常执行
  python run_daily_predict.py --dry-run  # 只打印将执行的动作, 不真正跑 predict
"""
import sys
from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent            # scripts/
PROJECT_ROOT = BASE_DIR.parent                        # qlib_project/
SRC_ROOT = PROJECT_ROOT.parent                        # src/
for p in (str(BASE_DIR), str(PROJECT_ROOT), str(SRC_ROOT)):
    if p not in sys.path:
        sys.path.insert(0, p)

import qlib
from qlib.data import D
from qlib_project.constants import DATA_PATH, BASE_POOL
from qlib_project.strategy.trend_strategy import predict_day, _ensure_qlib_init

LOG_DIR = PROJECT_ROOT.parent.parent / "logs"         # investment-qlib/logs
LOG_DIR.mkdir(parents=True, exist_ok=True)
STATE_FILE = LOG_DIR / ".daily_predict_last.txt"

# 用于探测"当日行情是否已入库"的流动性标的 (任一有收盘价即视为当日有数据)
_PROBE_CODES = ["SH600000", "SZ000001", "SH601318", "SZ000858", "SH600036"]


def latest_day_with_data(cal, target, max_back=5):
    """<= target 的最近一个实际有行情数据的交易日, 没有则返回 None。"""
    recent = [d for d in cal if d <= target][-max_back:]
    for d in reversed(recent):
        probe = D.features(_PROBE_CODES, ["$close"], start_time=d, end_time=d)
        if probe is not None and not probe.empty:
            return d
    return None


def main():
    import argparse
    parser = argparse.ArgumentParser(description="每日自动趋势预测 (交易日 20:00 由 cron 调用)")
    parser.add_argument("--dry-run", action="store_true", help="只打印将执行的动作, 不真正跑 predict")
    args = parser.parse_args()

    today = pd.Timestamp.now().normalize()
    print(f"[{today.date()}] 每日趋势预测任务启动", flush=True)

    _ensure_qlib_init()
    cal = D.calendar()

    latest = latest_day_with_data(cal, today)
    if latest is None:
        print("  ⚠️ 未找到有行情数据的交易日, 跳过 (请检查数据更新)", flush=True)
        return

    last = STATE_FILE.read_text().strip() if STATE_FILE.exists() else ""
    if last == latest.strftime('%Y-%m-%d'):
        print(f"  {latest.date()} 已处理过, 跳过 (避免重复推荐)", flush=True)
        return

    print(f"  ✅ 新交易日: {latest.date()}, 开始 predict", flush=True)
    if args.dry_run:
        print("  [dry-run] 将调用: predict_day("
              f"{latest.strftime('%Y-%m-%d')}, BASE_POOL, DATA_PATH, hold_days=5, topk=6)", flush=True)
        return

    result = predict_day(
        latest.strftime('%Y-%m-%d'), BASE_POOL, DATA_PATH,
        hold_days=5, topk=6, use_market_filter=True,
    )
    STATE_FILE.write_text(latest.strftime('%Y-%m-%d'))
    if result is None or result.empty:
        print("  本次无推荐 (大盘风控暂停买入 或 过滤后无候选)", flush=True)
    else:
        print(f"  推荐 {len(result)} 只", flush=True)
    print(f"[{today.date()}] 任务结束", flush=True)


if __name__ == "__main__":
    main()
