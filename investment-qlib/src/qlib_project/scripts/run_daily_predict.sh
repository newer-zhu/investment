#!/bin/zsh
# 每日自动趋势预测 (launchd 流水线调用, 也可手动跑)
# launchd 启动时 cwd=/ , 先 cd 到项目根, 避免相对路径日志报只读。
export PATH="/Users/zhuhongduo/miniconda3/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:$PATH"
PROJECT_ROOT="/Users/zhuhongduo/Code/investment/investment-qlib"
cd "$PROJECT_ROOT" || exit 1
PYTHON="/Users/zhuhongduo/miniconda3/envs/qlib_env/bin/python"
SCRIPT="$PROJECT_ROOT/src/qlib_project/scripts/run_daily_predict.py"
LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/daily_predict_$(date +%Y%m%d).log"
# 任务执行期间禁止系统睡眠/空闲睡眠 (-i -s), 避免唤醒后跑到一半又睡回去; 结束后自动恢复空闲睡眠
/usr/bin/caffeinate -i -s "$PYTHON" "$SCRIPT" >> "$LOG" 2>&1
echo "--- done $(date '+%Y-%m-%d %H:%M:%S') ---" >> "$LOG"
