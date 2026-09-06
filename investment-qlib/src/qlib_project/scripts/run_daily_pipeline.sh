#!/bin/zsh
# 每日 23:30 自动流水线 (launchd 调度):
#   1) 数据更新 (Docker 容器 dump_qlib_bin, 输出到 data/source/qlib_bin)
#   2) 趋势 predict (读更新后的数据 → 推荐/邮件/后端), 更新完成立即执行
# 整段用 caffeinate -i -s 保活, 防止 Mac 在两步之间或过程中睡眠。
# 注意: launchd 启动时 cwd=/ , 先 cd 到项目根, 避免相对路径日志报只读。
export PATH="/Users/zhuhongduo/miniconda3/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:$PATH"
PROJECT_ROOT="/Users/zhuhongduo/Code/investment/investment-qlib"
cd "$PROJECT_ROOT" || exit 1
SCRIPTS="$PROJECT_ROOT/src/qlib_project/scripts"
LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"

echo "[$(date '+%F %T')] pipeline 启动 (数据更新 → predict)" >> "$LOG_DIR/daily_pipeline_$(date +%Y%m%d).log"
if /usr/bin/caffeinate -i -s /bin/zsh "$SCRIPTS/run_daily_data_update.sh"; then
  echo "[$(date '+%F %T')] 数据更新成功, 开始 predict" >> "$LOG_DIR/daily_pipeline_$(date +%Y%m%d).log"
  /usr/bin/caffeinate -i -s /bin/zsh "$SCRIPTS/run_daily_predict.sh"
else
  echo "[$(date '+%F %T')] ❌ 数据更新失败, 本次跳过 predict" >> "$LOG_DIR/daily_pipeline_$(date +%Y%m%d).log"
fi
echo "[$(date '+%F %T')] pipeline 结束" >> "$LOG_DIR/daily_pipeline_$(date +%Y%m%d).log"
