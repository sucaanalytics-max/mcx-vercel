#!/bin/bash
# Install (or re-install) the MCX launchd jobs from this folder on the relay Mac.
#
#   scripts/launchd/install.sh                 # the production set (below)
#   scripts/launchd/install.sh freshness ...   # specific jobs, by name
#
# Production set: relay, daily-verify, commodity-eod, backtest. Optional:
# freshness-alert (read-only scan + macOS notification, no Claude),
# freshness (headless Claude agent that also repairs data), options-paper
# (Dhan paper trading).
# Logs go to ~/Library/Logs/mcx/ (survives reboots, outside Dropbox). The
# plists carry this Mac's absolute paths (/Users/pranayagarwal/...).
#
# Each job is booted out and bootstrapped again, so a running relay restarts:
# run this outside market hours.
set -euo pipefail
here="$(cd -P "$(dirname "$0")" && pwd)"
agents="$HOME/Library/LaunchAgents"
mkdir -p "$HOME/Library/Logs/mcx" "$agents"
jobs=("$@")
[ ${#jobs[@]} -gt 0 ] || jobs=(relay daily-verify commodity-eod backtest)
for j in "${jobs[@]}"; do
  src="$here/com.mcx.$j.plist"
  [ -f "$src" ] || { echo "no such job: $j ($src)" >&2; exit 1; }
  plutil -lint "$src" >/dev/null
  dst="$agents/com.mcx.$j.plist"
  launchctl bootout "gui/$(id -u)/com.mcx.$j" 2>/dev/null || true
  cp "$src" "$dst"
  launchctl bootstrap "gui/$(id -u)" "$dst"
  echo "installed com.mcx.$j"
done
