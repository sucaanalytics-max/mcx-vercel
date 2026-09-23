# Add Bulldex / iCOMDEX indices to dashboard (approved 2026-08-24)

Design: Option A (iCOMDEX index-level panel on Commodities tab) + Option B
(unhide BULLDEX margins). Verified endpoints (see memory / analysis):
- History: GET https://www.mcxindia.com/market-data/mcx-icomdex-indices/GetMCXIComdexIndicesHistoryFilter?instrument_Identifier=0&fromDate=dd/mm/yyyy&toDate=dd/mm/yyyy
- Warm page + chrome142 impersonation + XHR headers (same as bhav_refresh).
- History available from 2015-12-31. 12 published indices; codes like MCXBULLDEX.

## Tasks

- [x] 1. Supabase DDL written → scripts/sql/create_icomdex_table.sql.
      ⚠ BLOCKED on human: the MCX Supabase project (avqwpebveqetwwzkmtux) is not
      reachable from the connected Supabase account, no service key / CLI / psql
      on this machine — paste the SQL once in the Supabase SQL editor.
- [x] 2. scripts/icomdex_refresh.py written (curl_cffi, chunked backfill,
      --dry-run, blocked_on message on 404/42501).
- [x] 3. Dry-run tested: 60 rows / 5 days / 12 indices current week; 2016 window
      parses (143 rows, 13 indices; pre-2020 O/H/L=0 stored as NULL).
- [ ] 4. One-time backfill 2015-12-31 → today. AFTER the SQL is run:
      /opt/homebrew/bin/python3 scripts/icomdex_refresh.py --backfill 2015-12-31 today
- [x] 5. daily_verify.py runs icomdex_refresh (trailing week) at the end of main();
      refactored run_bhav_refresh into shared _run_script helper.
- [x] 6. api/commodities.py ?view=icomdex (range-aware series + latest table;
      graceful error while table missing — verified).
- [x] 7. index.html iCOMDEX card (rebased chart, BULLDEX highlighted, 12-index
      table, 'icomdex' range toggle, tabCommodity dispatch). node --check OK.
- [x] 8. lib/margin_dashboard.py FUTCOM+FUTIDX (both filter edits). VERIFIED
      live: MCXBULLDEX 8.68% / MCXMETLDEX 5.0% now in payload.
- [x] 9. Freshness manifest 'icomdex' domain (grace 2, T+1 publication) +
      commodity-updater.md step 3.
- [x] 10. tasks/ added to .vercelignore (todo must not deploy).
- [x] 11. Committed (19e8c85), pushed, Vercel deploy READY, verified live.

## Review (2026-08-24)

- DEPLOYED + VERIFIED in production (browser walk, zero console errors):
  - Margins (Option B) live now: MCXBULLDEX 8.68% / MCXMETLDEX 5.0% in
    /api/commodity_dashboard?view=margins.
  - iCOMDEX card renders on Commodities tab with graceful "No iCOMDEX data yet"
    note; existing lineup/signals unaffected (9 commodities render).
- REMAINING (single human step): paste scripts/sql/create_icomdex_table.sql in
  the Supabase SQL editor (project avqwpebveqetwwzkmtux — not reachable from
  the connected Supabase MCP account; no service key/CLI/psql on this machine).
  Then backfill: /opt/homebrew/bin/python3 scripts/icomdex_refresh.py --backfill 2015-12-31 today
  After that the panel self-maintains via daily_verify 07:00 (T+1 publication)
  and the freshness agent's 'icomdex' domain.
- Existing views regression-checked: ?view=signals still success:true.
- Not bundled (pre-existing dirty state): .gitignore housekeeping edit,
  untracked CLAUDE.md, untracked trading/.

---

# Full dashboard verification — 2026-09-23

Scope: "verify and update all tabs and all data" (all 10 tabs, 16 endpoints,
12 freshness domains).

## Done
- [x] Swept all 16 API endpoints; all HTTP 200, no regressions.
- [x] **Signals cron-phase bug (root cause + fix).** models/momentum/
      commodity_signals Vercel crons fire 14:10-14:20 UTC but mcx_valuation
      lands ~20:27 UTC, so those runs can only ever see the PREVIOUS session —
      Forecast/Momentum/Analytics/Commodities-signals read one day stale from
      ~02:00 to ~19:45 IST daily. Recomputed to 09-22; daily_verify's 07:00 run
      now does it every morning. → f4b1f55
- [x] Margins backfilled: real hole at 09-21, plus 09-23 (330 rows).
- [x] **Q1 FY27 actuals added** (702 / 208 / 413 consolidated, 6 sources,
      cross-checked against our own tape at 92.0% txn-fee ratio vs the
      90.0-91.2% band of prior quarters). → ca0a36a
- [x] **Partial-FY labelling bug** surfaced by the above: card called a
      2-of-4-quarter subtotal a "Full Year Projection" (FY27 EPS 33.54 next to
      FY26's 52.30, reading as a collapse). Now "FY27 — 2 of 4 Quarters
      (Partial)" with NQ-scoped tiles. No computation changed. → 2c07e7d
- [x] freshness_scan: 12/12 OK (was 11/12).

## Not issues (verified, do not "fix")
- /api/backtest 404s by design; loadBacktest() guards on content-type.
- /api/mcxprice has no date fields (live price snapshot) — not staleness.
- OI at 09-21 is MCX's own publication lag; stored history has no holes.

## Known / deferred
- Deployed Vercel cron config has DRIFTED from committed vercel.json
  (something hits valuation ~20:27 UTC that isn't in the file). Structural fix
  would be to move the signal crons after the valuation write; not done because
  the deployed state is not the repo state. daily_verify now covers the gap.
- Q2 FY27 reportable mid-to-late Oct 2026.

## Cron reconciliation — 2026-09-23 (follow-up)
- [x] Checked deployed vs committed vercel.json via Vercel API: **no drift**
      (the earlier drift hypothesis was wrong; deployed == committed).
- [x] **Margins cron was dead and is removed.** Prod logs: `GET /api/cron 500
      Vercel Runtime Timeout Error: Task timed out after 60 seconds` at 20:30
      UTC. Sharekhan's XLS never completes from datacenter IPs (0.4s locally).
      Zero rows ever created in the 20:xx UTC window across all history.
      → daily_verify now runs margin_refresh.py --backfill 3. Commit 9842644.
- [x] Signal cron times left as-is: daily_verify's 07:00 recompute (f4b1f55)
      already makes tabs current before the IST workday; moving the 14:xx
      Vercel crons would only gain freshness at ~02:15 IST and would depend on
      the unidentified 20:27 valuation writer being reliable. Not worth it.
- [ ] OPEN: who writes mcx_valuation at 20:27:10 UTC daily? Not Vercel (no
      /api/cron hit at that minute), not launchd/crontab/system daemon, no
      GitHub Actions, no duplicate Vercel project. Reliable ~16/18 days and the
      14:00 UTC cron catches up when it misses. Candidates: Supabase pg_cron,
      a Supabase Edge Function, or another machine. Harmless but unexplained.
