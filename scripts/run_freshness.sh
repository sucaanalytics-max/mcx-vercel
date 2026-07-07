#!/bin/bash
# MCX daily freshness orchestrator launcher for launchd.
# Runs the headless `claude -p /freshness-check` cycle: scans all 10 tab data
# domains, dispatches the mapped fixer subagent for any stale domain, re-verifies,
# escalates on repeated failure, and writes .claude/state/freshness-status.json.
#
# Scheduled weekdays + Saturday 09:00 IST (after the 07:00 daily_verify and all
# overnight Vercel crons; Saturday catches Friday's late margins cron).
#
# Prereq (one-time): activate the scoped permissions by copying
#   .claude/settings.recommended.json  ->  .claude/settings.local.json
# so the run is non-interactive for exactly the freshness scripts.

cd "/Users/pranayagarwal/Dropbox/My Mac (Pranay's MacBook Air)/Documents/MCX/mcx-vercel" || exit 1
export PATH="/Users/pranayagarwal/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"

LOG="logs/freshness-$(date +%Y-%m-%d).log"
mkdir -p logs

# --permission-mode acceptEdits lets quarterly-updater stage its api/quarterly.py
# edit non-interactively; all other actions are gated by .claude/settings.local.json.
exec claude -p "/freshness-check" \
  --permission-mode acceptEdits \
  --output-format json >> "$LOG" 2>&1
