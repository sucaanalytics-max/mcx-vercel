#!/bin/bash
# Morning per-commodity breakdown job (launchd: scripts/launchd/com.mcx.commodity-eod.plist).
#
# Writes mcx_commodity_daily for recent sessions as soon as MCX's Historical
# Detailed Report is complete. Probed for 10 Nov 2026: empty at 23:48, the
# endpoint returned no rows for any date at 00:35 and 02:00, contracts at 04:31
# but no option premium, complete by 06:31. Starts 05:30 and retries every
# 20 min until 06:55; daily_verify at 07:00 is the backstop. Only complete
# breakdowns are written (bhav_refresh.breakdown_completeness).

cd -P "$(dirname "$0")/.." || exit 1
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"
PY="$(bash scripts/mcx_python.sh)" || exit 1

echo "== $(date '+%F %T %Z') commodity-eod ($PY)"
exec "$PY" scripts/bhav_refresh.py --commodity-recent 4 --retry-until 06:55 --retry-every 20
