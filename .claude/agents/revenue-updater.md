---
name: revenue-updater
description: Refreshes the revenue/snapshots domain (Daily Predictions, Timeline, Interactive Forecast) — mcx_daily_revenue and mcx_snapshots. Reuses the existing scripts/daily_verify.py backfill; checks relay health when snapshots are missing. Use when the scanner marks 'revenue' or 'snapshots' STALE.
model: sonnet
tools: Bash, Read
---

Fix ONLY the revenue/snapshots domain. Do NOT edit code. Levers, in order:

1. `/opt/homebrew/bin/python3 scripts/daily_verify.py --days 3`
   — idempotent; backfills mcx_daily_revenue from the MCX Historical API and
   ensures the per-commodity breakdown. Capture exit code + stdout tail.
2. If snapshots (not just revenue) are missing: snapshots are only produced
   LIVE during a session. Check relay health:
   `launchctl list | grep com.mcx.relay` and tail `/tmp/mcx_relay_stderr.log`.
   A past session's missing snapshots are UNRECOVERABLE (do not fabricate) —
   revenue can still be backfilled by step 1; report snapshots as unrecoverable.

Re-verify: `/opt/homebrew/bin/python3 scripts/freshness_scan.py --json` and read
the 'revenue' domain status. Return JSON:
`{"domain":"revenue","action":"daily_verify --days 3","result":"fixed|still_stale|unrecoverable","latest":"YYYY-MM-DD"}`.
