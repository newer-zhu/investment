#!/bin/zsh
# 每日 20:00 自动趋势预测 (由 cron 调用)
# 依赖: 主机需在 19:55 由 pmset 定时唤醒 (sudo pmset repeat wakeorpoweron MTWRF 19:55:00), 且插电。
# cron 环境没有登录 shell 的 PATH, 全部用绝对路径
export PATH="/Users/zhuhongduo/miniconda3/bin:/usr/local/bin:/usr/bin:/bin:$PATH"
PYTHON="/Users/zhuhongduo/miniconda3/envs/qlib_env/bin/python"
SCRIPT="/Users/zhuhongduo/Code/investment/investment-qlib/src/qlib_project/scripts/run_daily_predict.py"
LOG_DIR="/Users/zhuhongduo/Code/investment/investment-qlib/logs"
mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/daily_predict_$(date +%Y%m%d).log"
# 任务执行期间禁止系统睡眠/空闲睡眠 (-i -s), 避免唤醒后跑到一半又睡回去; 结束后自动恢复空闲睡眠
/usr/bin/caffeinate -i -s "$PYTHON" "$SCRIPT" >> "$LOG" 2>&1
echo "--- done $(date '+%Y-%m-%d %H:%M:%S') ---" >> "$LOG"
