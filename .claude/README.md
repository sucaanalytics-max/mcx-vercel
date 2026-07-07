# MCX Daily Freshness Agent

A multi-model Claude Code agent system that checks every dashboard tab's data
daily and dispatches the right fixer for anything stale.

## How it works

```
launchd (com.mcx.freshness, weekdays+Sat 09:00 IST)
  └─ scripts/run_freshness.sh → claude -p "/freshness-check"   (headless)
       ├─ PHASE 1  freshness-scanner   [haiku]  runs scripts/freshness_scan.py
       │           → JSON: per-domain OK/STALE vs the expected latest trading day
       │           (all OK → write status, stop. Zero fixer cost on a good day.)
       ├─ PHASE 2  fixers (only for stale domains, in dependency order) [sonnet]
       │           revenue-updater · signals-updater · commodity-updater
       │           · margins-updater · oi-updater · price-updater
       ├─ PHASE 3  re-verify; 2nd failure → investigator [opus]
       │           quarterly stale → quarterly-updater [opus] (human-reviewed edit)
       └─ PHASE 4  .claude/state/freshness-status.json + fix_streak
                   + macOS notification when DEGRADED/FAILED or streak ≥ 3
```

Model tiering: **Haiku scans (cheap, every day) → Sonnet fixes (only when
stale) → Opus investigates / edits code (rare)**. Cost scales with actual
staleness.

## Files

| Path | Role | Git |
|---|---|---|
| `state/freshness-manifest.json` | Freshness rules: domain → table → grace → fixer → depends_on | committed |
| `state/freshness-status.json` | Last run's results + fix_streak counters | ignored |
| `agents/*.md` | The 9 subagents (model + tools in frontmatter) | committed |
| `commands/freshness-check.md` | `/freshness-check` orchestrator | committed |
| `commands/freshness-report.md` | `/freshness-report` last-run summary | committed |
| `settings.recommended.json` | Scoped permission template (review → activate) | committed |
| `../scripts/freshness_scan.py` | Deterministic scan core (read-only) | committed |
| `../scripts/run_cron.py` | Local dispatcher for the 6 cron jobs (no HTTP/secret) | committed |
| `../scripts/run_freshness.sh` | launchd wrapper | committed |
| `../scripts/launchd/com.mcx.freshness.plist` | Schedule (Mon–Sat 09:00 IST) | committed |

## One-time activation (human steps)

1. **Permissions** — review, then activate the scoped allowlist:
   ```bash
   cp .claude/settings.recommended.json .claude/settings.local.json
   ```
2. **Commodity prices grant** — run `scripts/sql/enable_commodity_prices_writes.sql`
   once in the Supabase SQL editor, then populate:
   ```bash
   python3 scripts/commodity_price_refresh.py --days 120
   ```
3. **Schedule** — install + verify the launchd job:
   ```bash
   cp scripts/launchd/com.mcx.freshness.plist ~/Library/LaunchAgents/
   plutil -lint ~/Library/LaunchAgents/com.mcx.freshness.plist
   launchctl load ~/Library/LaunchAgents/com.mcx.freshness.plist
   launchctl kickstart -k gui/$(id -u)/com.mcx.freshness   # one manual fire
   tail -f logs/freshness_launchd.log
   ```

## Manual use

- `/freshness-check` — full cycle now (add `--dry-run` for scan-only)
- `/freshness-report` — summary of the last run
- `python3 scripts/freshness_scan.py` — raw scan, no agents involved

## Design guarantees

- **Reuses, never reimplements**: revenue checks delegate to `daily_verify.py`;
  signal fixes call the same `lib/cron_*.py` the Vercel crons run.
- **No unattended shipping**: fixers refresh *data* only. The one code-editing
  agent (`quarterly-updater`) touches only `api/quarterly.py`, must corroborate
  two sources, and stages its diff for human review — deploy/push are denied.
- **Grace windows, not alarms**: OI publishes T+1 evening, SPAN margins are
  sporadic, NSE ≠ MCX calendar — each domain's `grace_days` encodes that, so a
  legitimate lag isn't a daily false positive.
- **Systemic-issue detection**: `fix_streak ≥ 3` (fixed daily but breaks again)
  triggers an alert — a cron that silently died stays visible even though each
  day's run "succeeds".
