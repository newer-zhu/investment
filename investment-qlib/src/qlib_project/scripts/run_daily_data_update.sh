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

# ---- 跑前清理 (2026-09-07 修复) ----
# dump_qlib_bin.sh 用 `dolt sql-server &` 后台起服务且从不清理, 上次运行残留的 server
# 会占 3306 / /tmp/mysql.sock, 并让下次新起的 sql-server 读到旧库 → "table not found"
# (表现: 手动跑能成、定时 09:18 跑必失败)。这里先杀掉残留 dolt 进程 + 删陈旧 socket。
echo "[$(date '+%F %T')] 清理上次残留的 dolt sql-server / 陈旧 socket" >> "$LOG"
docker exec "$CONTAINER" bash -c 'pkill -f "dolt sql-server" 2>/dev/null; pkill -x dolt 2>/dev/null; sleep 2; rm -f /tmp/mysql.sock 2>/dev/null; true' >> "$LOG" 2>&1

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
  exit 0
else
  echo "[$(date '+%F %T')] ❌ 数据更新失败 (已重试3次), 返回非0让 pipeline 跳过本次 predict" >> "$LOG"
  exit 1
fi
