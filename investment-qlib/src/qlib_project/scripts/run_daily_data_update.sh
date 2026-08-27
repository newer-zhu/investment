#!/bin/zsh
# 每日 21:00 数据更新: 在 Docker 容器 quirky_joliot 内跑 dump_qlib_bin.sh /
# 输出写进挂载的 investment-qlib/data/source (predict 读取的 qlib_bin)
# 注意: 由 run_daily_pipeline.sh 调用, 单独跑也可以。
export PATH="/Users/zhuhongduo/miniconda3/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:$PATH"
CONTAINER="quirky_joliot"
LOG_DIR="/Users/zhuhongduo/Code/investment/investment-qlib/logs"
mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/daily_data_update_$(date +%Y%m%d).log"

echo "[$(date '+%F %T')] 数据更新开始 (容器 $CONTAINER)" >> "$LOG"
# Docker Desktop 唤醒后可能未就绪: 重试最多 3 次, 每次间隔 30s
ok=0
for i in 1 2 3; do
  if docker exec "$CONTAINER" bash /investment_data/dump_qlib_bin.sh / >> "$LOG" 2>&1; then
    ok=1
    break
  fi
  echo "[$(date '+%F %T')] 第 ${i} 次失败, 30s 后重试" >> "$LOG"
  sleep 30
done
if [ "$ok" -eq 1 ]; then
  echo "[$(date '+%F %T')] 数据更新完成" >> "$LOG"
else
  echo "[$(date '+%F %T')] ❌ 数据更新失败 (已重试3次), 继续跑 predict 用现有数据" >> "$LOG"
fi
exit 0
