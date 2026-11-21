# CLAUDE.md

Guidance for Claude Code in this repository. Rewritten in Phase 3 (Nov 2026);
keep it true when you change the layout.

## What this is

MCX (Multi Commodity Exchange of India) revenue analytics. A local relay on the
production Mac scrapes MCX every 15 minutes and writes Supabase; Vercel Python
functions read Supabase; a static single-page frontend (`index.html` +
`assets/js/*.js`, hand-built SVG charts, no build step) renders 14 pages:

| Section | Pages (route) |
|---|---|
| Today | `/today` |
| Revenue | `/revenue/trends`, `/revenue/seasonality`, `/revenue/commodities` |
| Earnings & value | `/value/quarter`, `/value/fair-value`, `/value/scenarios` |
| Signals | `/signals/momentum`, `/signals/ensemble`, `/signals/options`, `/signals/bands` |
| Lab | `/lab/diagnostics`, `/lab/positioning`, `/lab/margins` |

Routes live in `assets/js/shell.js`. MCX's Akamai front end blocks Vercel and
other cloud IPs, so **every MCX fetch runs locally** (relay / scripts). Vercel
reads Supabase (plus yfinance for the share price); its only writer is
`/api/cron`. `/api/refresh` is GET-only: the header's refresh button re-reads
the relay's latest snapshot.

## Layout

```
api/            Vercel functions (10 of the Hobby plan's 12). Each defines
                `class handler(JSONHandler)`; maxDuration in vercel.json.
lib/            Shared code, imported by api/ and scripts/
  mcx_config.py   fee schedule, intraday curve, projection, day types,
                  valuation constants; re-exports lib.supabase + lib.calendar
  supabase.py     the only Supabase REST client (keys, reads, paginated reads,
                  upserts, upsert_batches, fetch_all; raw get_rows / post_rows /
                  patch_rows for callers that branch on the HTTP status)
  calendar.py     session hours, holidays, part-day / special sessions,
                  calendar_status, now_ist, FY / quarter labels
  curl_session.py curl_cffi session with Chrome impersonation (local only)
  api_handler.py  JSONHandler: JSON + CORS + OPTIONS + no-cache-on-error
  cron_*.py       signal / valuation / margin / OI jobs (Vercel cron + local)
  home_view.py, intraday_curves.py, hourly_analysis.py, margin_dashboard.py,
  oi_participants_dashboard.py, house_model.py, strategy_bands.py (ADR band tracker), options_costs.py, dhan_client.py,
  mcx_bhavcopy.py (futures OI from MCX's daily Bhavcopy; local only),
  contract_eod.py (per-contract EOD CSVs from the Bhavcopy for a separate local project)
scripts/        Local jobs, never deployed (.vercelignore)
  mcx_relay.py      15-min snapshots, EOD revenue, margins, OI participants
  daily_verify.py   07:00 reconcile + backfills + signal recompute (see below)
  bhav_refresh.py   per-commodity breakdown from MCX's historical report
  launchd/          plists + install.sh (logs: ~/Library/Logs/mcx/)
  run_tests.sh, mcx_uv.sh, mcx_python.sh, freshness_scan.py, project_sweep.py
  test_*.py / test_*_js.js   self-checking tests (run_tests.sh runs them all)
research/       one-off studies (not scheduled, not deployed)
docs/           write-ups (not deployed); tasks/  todo + reports (not deployed)
.claude/        agents, commands (freshness-check, project-steward), manifest
```

## Commands

```bash
scripts/run_tests.sh -q                       # all tests; must be green before a push (hook)
scripts/mcx_uv.sh python scripts/<job>.py     # any script with the locked deps
scripts/mcx_uv.sh --extra research python -I research/<study>.py
python3 scripts/freshness_scan.py             # data freshness per domain (read-only)
scripts/run_cron.py valuation|models|momentum|commodity_signals|margins|oi [recent|latest|backfill]
vercel deploy                                 # preview; previews are behind Vercel auth:
vercel curl /api/quarterly --deployment <preview-url>
scripts/launchd/install.sh [job ...]          # (re)install launchd jobs; restarts them
```

Dependencies: `pyproject.toml` + `uv.lock` is the only manifest. `dependencies`
is what Vercel installs (yfinance); the `local` extra is for the Mac jobs and
tests (curl_cffi, pandas, xlrd, openpyxl, dhanhq<2.1); `research` for studies.
`scripts/mcx_uv.sh` keeps the venv in `~/.cache/mcx-vercel/venv`, outside
Dropbox. The launchd relay / daily_verify use `scripts/mcx_python.sh` (first
interpreter that can import curl_cffi + pandas).

## Data flow and schedule (IST)

| When | Job | Writes |
|---|---|---|
| 08:55–close, every 15 min | `com.mcx.relay` → `mcx_relay.py --loop` | `mcx_snapshots`, `relay_heartbeat`; at the close `mcx_daily_revenue` (EOD), `mcx_margin_daily`, `mcx_oi_participants` |
| 05:30–06:55 | `com.mcx.commodity-eod` → `bhav_refresh.py --commodity-recent 4` | `mcx_commodity_daily` (only complete reports) |
| 07:00 | `com.mcx.daily-verify` → `daily_verify.py --days 3` | reconciles revenue vs MCX's report (>1% → replace), commodity breakdown, iCOMDEX, commodity prices, margins (MCXCCL), OI participants, futures OI from MCX's Bhavcopy (`backfill_commodity_oi.py --nightly`), per-contract futures EOD CSVs from the same Bhavcopy (`backfill_contract_eod.py --recent 10`; files in `$MCX_CONTRACT_EOD_DIR`, not Supabase), then valuation → models → momentum → commodity_signals |
| 19:30 / 19:45 / 19:50 | Vercel crons: valuation, models, commodity_signals | `mcx_valuation`, `mcx_model_signals`, `mcx_commodity_signals` |
| 08:00 Tue–Sat | Vercel cron: momentum | `mcx_momentum_signals` |
| 09:00 Mon–Sat | `com.mcx.freshness-alert` → `freshness_alert.py` (read-only scan) | nothing; a macOS notification when any domain is not OK |
| 09:00 (optional) | `com.mcx.freshness` → headless `/freshness-check` (also repairs data) | `.claude/state/freshness-status.json` |

The session closes 23:30, or 23:55 while the US is on standard time
(`lib.calendar.session_end`). An unidentified writer also rewrites recent
`mcx_valuation` rows at 01:57 IST with pre-29-Sep-2026 constants; the 07:00
recompute puts them back, and `/api/valuation` rebuilds history on current
constants. Find and stop it if you can.

Other tables: `mcx_share_price` (price_refresh / valuation cron),
`mcx_icomdex_daily`, `mcx_commodity_prices` (written by daily_verify's
commodity price step; no endpoint reads it any more), `mcx_options_paper`,
`mcx_options_feed` (options_paper). Quarterly actuals are **code**, not a table:
`QUARTERLY_ACTUALS` in `api/quarterly.py` (process: `.claude/agents/quarterly-updater.md`).

## Rules

- **Fee schedule and valuation constants** only in `lib/mcx_config.py`; so are
  the range chips (`RANGE_DAYS`, `BOUNDED_RANGE_DAYS` for endpoints without
  Max, `DEFAULT_RANGE`; keep them in step with `RANGE_TRADING_DAYS` in
  `assets/js/core.js`), `FINAL_ROW_SOURCES` and `safe_float`.
- **Calendar** only in `lib/calendar.py`. When MCX publishes a year's holiday
  circular (mid-December), add its full-day, morning-closed and evening-closed
  dates **and** the year to `MCX_CALENDAR_YEARS`; the freshness `calendar`
  domain, `daily_verify` and `/api/refresh?health=1` go red until you do.
  `options_paper.py` keeps its own NSE calendar (the stock trades on NSE).
- **Supabase** only through `lib/supabase.py`. PostgREST returns at most 1,000
  rows per request whatever `limit=` says: page with `supabase_read_all` (it
  adds each table's unique key via `PAGINATION_KEYS`, reads the row count on
  the first page and fetches the rest `PAGE_WORKERS` at a time, in offset
  order); a capped ascending read of a growing table needs `newest_first=True`.
- **No MCX fetches from Vercel.** No new `api/*.py` without retiring one (Hobby
  plan: 12 functions; 10 used).
- **api handlers** subclass `lib.api_handler.JSONHandler`; pass `cache=` (or
  set `default_cache`) for a CDN-cacheable 200, one of `CACHE_SNAPSHOT` /
  `CACHE_INTRADAY` / `CACHE_DAILY` by how often the data changes (`s-maxage`
  is the CDN's, `max-age` the browser's). Errors are never cached. A page that reads a
  fraction of a big payload asks for `?fields=page` (OI participants,
  analytics); the full payload stays the default.
- **Frontend fetches**: `fetchRanged` caches per URL; the 2-minute refresh
  drops only `exchange_dashboard` views (and anything over 30 min old).
  `view=home` loads on Today (off Today only before the open, for the header);
  `/api/mcxprice` only on Fair value and Scenarios.
- The **production checkout is the Dropbox folder** that launchd runs. Never
  switch branches there; work in a worktree. One PR per change; the user merges.
- **Do not write production data** (backfills, deletes, SQL) without asking.
  Reads are fine. Every change to a displayed number goes in the PR as
  before → after with the reason.

## Environment

`.env.local` on the relay Mac (gitignored) and Vercel env:

| Variable | Purpose |
|---|---|
| `SUPABASE_PUBLISHABLE_KEY` / `SUPABASE_SECRET_KEY` | new Supabase keys (reads / writes); legacy `SUPABASE_ANON_KEY` / `SUPABASE_SERVICE_KEY` still work |
| `SUPABASE_URL` | project URL (has a default) |
| `CRON_SECRET` | auth for `/api/cron` |
| `ALLOWED_ORIGINS` | CORS override |
| `MCX_TRADING_DAYS`, `MCX_PAT_MARGIN`, `MCX_NON_FO_REV`, `MCX_DILUTED_SHARES`, `MCX_PE_MEAN`, `MCX_PE_SD`, `MCX_NONTX_DAILY` | valuation / fee overrides |
| `MCX_IMPERSONATE` | curl_cffi target (default chrome142) |
| `MCX_PYTHON` | interpreter override for the launchd jobs |
| `MCX_CONTRACT_EOD_DIR` | folder for the per-contract EOD CSVs (default `~/tradingone/data/mcx_contract_eod`) |

Dhan (options paper trading only; futures OI comes from MCX's Bhavcopy,
`lib/mcx_bhavcopy.py`, no login): `~/.dhan/credentials.json`, else
`DHAN_CLIENT_ID` / `DHAN_ACCESS_TOKEN` in `.env`.
