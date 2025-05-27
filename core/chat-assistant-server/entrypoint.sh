#!/usr/bin/env sh
#
# Simple loop: start uvicorn, when it stops wait 10 min and restart
#

while true; do
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting uvicorn…" >&2
  uvicorn main:app \
    --host 0.0.0.0 \
    --port 8001 \
    --reload

  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Uvicorn exited; restarting in 10 minutes…" >&2
  sleep 600
done