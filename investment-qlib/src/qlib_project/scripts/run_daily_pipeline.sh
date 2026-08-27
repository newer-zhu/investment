#!/bin/zsh
# 每日 21:00 自动流水线 (由 cron 调用):
#   1) 数据更新 (Docker 容器 dump_qlib_bin, 输出到 data/source/qlib_bin)
#   2) 趋势 predict (读更新后的数据 → 推荐/邮件/后端)
# 整段用 caffeinate -i -s 保活, 防止 Mac 在两步之间或过程中睡眠。
# 依赖: 主机 20:55 由 pmset 定时唤醒 (sudo pmset repeat wakeorpoweron MTWRF 20:55:00), 且插电。
export PATH="/Users/zhuhongduo/miniconda3/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:$PATH"
SCRIPTS="/Users/zhuhongduo/Code/investment/investment-qlib/src/qlib_project/scripts"
LOG_DIR="/Users/zhuhongduo/Code/investment/investment-qlib/logs"
mkdir -p "$LOG_DIR"

echo "[$(date '+%F %T')] pipeline 启动 (数据更新 → predict)" >> "$LOG_DIR/daily_pipeline_$(date +%Y%m%d).log"
/usr/bin/caffeinate -i -s /bin/zsh "$SCRIPTS/run_daily_data_update.sh"
/usr/bin/caffeinate -i -s /bin/zsh "$SCRIPTS/run_daily_predict.sh"
echo "[$(date '+%F %T')] pipeline 结束" >> "$LOG_DIR/daily_pipeline_$(date +%Y%m%d).log"
