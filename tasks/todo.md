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
