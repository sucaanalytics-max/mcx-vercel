#!/bin/bash
# MCX Relay launcher for launchd
# Activates the correct Python environment and runs the relay loop.
# Wraps in `caffeinate -is` to keep the Mac awake (no idle sleep, no system
# sleep) during the entire 14.5h trading session. `-i` alone blocks idle sleep
# only — adding `-s` also blocks system sleep so lid-close mid-session can't
# silently kill the loop (this happened Jun 10).

# Repo root = parent of this script's directory (any user, paths with spaces ok)
src="$0"
while [ -L "$src" ]; do  # follow symlinks (portable; no readlink -f on old macOS)
  d="$(cd -P "$(dirname "$src")" && pwd)"
  src="$(readlink "$src")"
  [ "${src#/}" = "$src" ] && src="$d/$src"
done
cd -P "$(dirname "$src")/.." || exit 1

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"

# Pinned to an interpreter that has the packages (see scripts/mcx_python.sh).
if ! PY="$(bash scripts/mcx_python.sh)"; then
  # Back off before exiting non-zero so KeepAlive doesn't relaunch every 10s.
  echo "$(date '+%F %T %Z') start_relay.sh: no usable Python — retrying in 5 min" >&2
  sleep 300
  exit 1
fi
echo "$(date '+%F %T %Z') start_relay.sh: using $PY"

exec /usr/bin/caffeinate -is "$PY" scripts/mcx_relay.py --loop
