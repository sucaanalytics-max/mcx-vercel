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
# run this outside market hours. bootout returns before launchd has removed
# the job; bootstrapping too soon fails with "Bootstrap failed: 5:
# Input/output error" and leaves the job unloaded (18 Nov 2026: the relay was
# down until loaded by hand). So wait for the old job to go, retry the
# bootstrap, and fail loudly unless the job is loaded at the end.
#
# MCX_LAUNCHCTL / MCX_LAUNCH_AGENTS / MCX_WAIT_STEP override launchctl, the
# LaunchAgents folder and the poll interval (tests only).
set -euo pipefail
here="$(cd -P "$(dirname "$0")" && pwd)"
agents="${MCX_LAUNCH_AGENTS:-$HOME/Library/LaunchAgents}"
lc="${MCX_LAUNCHCTL:-launchctl}"
step="${MCX_WAIT_STEP:-0.5}"
domain="gui/$(id -u)"

loaded() { "$lc" print "$domain/$1" >/dev/null 2>&1; }

wait_gone() {            # up to ~20 s for launchd to finish the bootout
  local i
  for i in $(seq 1 40); do
    loaded "$1" || return 0
    sleep "$step"
  done
  return 1
}
mkdir -p "$HOME/Library/Logs/mcx" "$agents"
jobs=("$@")
[ ${#jobs[@]} -gt 0 ] || jobs=(relay daily-verify commodity-eod backtest)
for j in "${jobs[@]}"; do
  src="$here/com.mcx.$j.plist"
  [ -f "$src" ] || { echo "no such job: $j ($src)" >&2; exit 1; }
  plutil -lint "$src" >/dev/null
  dst="$agents/com.mcx.$j.plist"
  label="com.mcx.$j"
  "$lc" bootout "$domain/$label" 2>/dev/null || true
  wait_gone "$label" || echo "warning: $label still loaded after bootout; trying anyway" >&2
  cp "$src" "$dst"
  ok=""
  for attempt in 1 2 3 4 5; do
    if "$lc" bootstrap "$domain" "$dst" 2>/dev/null; then ok=1; break; fi
    sleep "$step"; sleep "$step"
  done
  if [ -z "$ok" ] || ! loaded "$label"; then
    echo "FAILED: $label is not loaded. Load it by hand: launchctl bootstrap $domain $dst" >&2
    exit 1
  fi
  echo "installed $label"
done
