#!/bin/bash
# 放服务器上,cron 定时跑。只在检测到远端 origin/main 有新提交时才 pull + 重新部署。
# 建议配合 flock 防止上一次构建没结束又开一次:
#   * * * * * flock -n /tmp/dota-deploy.lock /opt/dota-stat/deploy/deploy.sh >> /var/log/dota-deploy.log 2>&1
set -euo pipefail
cd "$(dirname "$0")/.."

git fetch --quiet origin main

LOCAL=$(git rev-parse HEAD)
REMOTE=$(git rev-parse origin/main)

if [ "$LOCAL" != "$REMOTE" ]; then
    echo "$(date '+%F %T') 检测到更新($LOCAL -> $REMOTE),开始部署"
    git pull --ff-only
    docker compose up -d --build
    docker image prune -f >/dev/null 2>&1 || true   # 清理旧镜像,避免磁盘越占越多
    echo "$(date '+%F %T') 部署完成"
else
    echo "$(date '+%F %T') 无更新,跳过"
fi
