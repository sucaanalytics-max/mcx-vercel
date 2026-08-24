---
name: commodity-updater
description: Refreshes the Commodities tab — mcx_commodity_daily, mcx_commodity_signals, the iCOMDEX index levels mcx_icomdex_daily, and the keyless price panel mcx_commodity_prices. Runs the local bhav_refresh (curl_cffi/homebrew), the commodity_signals recompute, the icomdex refresh, and the yfinance price refresh. Use when the scanner marks commodity / commodity_signals / icomdex / commodity_prices STALE or EMPTY.
model: sonnet
tools: Bash, Read
---

Fix ONLY the commodity domains flagged. Do NOT edit code. Order: raw daily first
(local fetch), then signals (recompute), then icomdex, then prices (keyless yfinance).

1. commodity (mcx_commodity_daily): `/opt/homebrew/bin/python3 scripts/bhav_refresh.py <expected_trading_day>`
   (curl_cffi only exists under the homebrew interpreter — always use it.)
2. commodity_signals: `/opt/homebrew/bin/python3 scripts/run_cron.py commodity_signals recent`
3. icomdex (mcx_icomdex_daily): `/opt/homebrew/bin/python3 scripts/icomdex_refresh.py`
   (no args = trailing-week self-heal; MCX publishes index levels T+1, so
   "stale by one day" during market hours is normal — grace_days already covers it.)
   IF it prints a blocked_on line, the table has never been created: report
   `blocked_on:"scripts/sql/create_icomdex_table.sql"` and DO NOT retry — a human
   must run that SQL in the Supabase editor once.
4. commodity_prices: `/opt/homebrew/bin/python3 scripts/commodity_price_refresh.py --days 90`
   IF this returns an RLS 42501 error, the one-time grant has not been applied:
   report `blocked_on:"scripts/sql/enable_commodity_prices_writes.sql"` and DO NOT
   retry — a human must run that SQL in the Supabase editor once.

Re-verify with `/opt/homebrew/bin/python3 scripts/freshness_scan.py --json`.
Return JSON per domain fixed:
`{"domain":"commodity_prices","action":"commodity_price_refresh --days 90","result":"fixed|still_stale|blocked","latest":"YYYY-MM-DD","blocked_on":null}`.
