---
description: Daily MCX data-freshness orchestrator — scan all 10 tabs, dispatch the right fixer for each stale domain (in dependency order), re-verify, escalate on repeated failure, and write a status report. Runs headless via the com.mcx.freshness launchd job each morning.
argument-hint: "[--dry-run]"
allowed-tools: Task, Bash, Read, Write, Edit
---

Run the daily MCX freshness cycle. Be economical: only spawn a fixer for a domain
the scanner marks not-OK. Multi-model by design — Haiku scans, Sonnet fixes, Opus
investigates/edits. If `$ARGUMENTS` contains `--dry-run`, do PHASE 1 only (scan +
report) and skip all fixers.

**PHASE 1 — scan.** Dispatch the `freshness-scanner` subagent (haiku). Parse its
JSON. If `overall == "OK"`, write the status file (PHASE 4) and stop — no fixers.

**PHASE 2 — fix (skip if --dry-run).** For each domain in `stale_domains`,
dispatch its mapped fixer subagent (from the manifest's `fixer` field; all
Sonnet except `quarterly-updater`=Opus). Dispatch in DEPENDENCY ORDER using each
domain's `depends_on`: fix upstream domains (revenue, share_price, commodity)
before the signal domains that read them. Pass each fixer the
`expected_trading_day` from the scan. Collect each fixer's JSON result.
- `commodity_prices` blocked on the SQL grant → record it, do not retry.
- `margins` result `source_stale` → not a failure; record and move on.

**PHASE 3 — re-verify + escalate.** Re-run `python3 scripts/freshness_scan.py --json`.
For any domain still not-OK after its fixer ran, retry that fixer ONCE. On a
second failure (manifest `max_fix_attempts`=2), dispatch the `investigator` (opus)
for THAT domain only. For `quarterly` STALE, dispatch `quarterly-updater` (opus),
which proposes a HUMAN-reviewed code edit — never auto-deploy.

**PHASE 4 — report.** Write `.claude/state/freshness-status.json` (schema below).
Update per-domain `fix_streak` (increment when a domain needed fixing, reset to 0
when it was OK). Print a one-screen table: domain | before | after | status.
If `overall != "OK"` after fixes, OR any `fix_streak >= 3` (systemic), fire a
macOS notification:
`osascript -e 'display notification "<summary>" with title "MCX freshness"'`.

Read `$CRON_SECRET`/keys from the environment only if ever needed — the local
cron functions write with the anon key, so fixers should NOT need HTTP or secrets.

freshness-status.json schema:
```json
{"last_run":"<iso>","expected_trading_day":"YYYY-MM-DD","overall":"OK|DEGRADED|FAILED",
 "domains":{"<domain>":{"before":"YYYY-MM-DD|EMPTY","after":"YYYY-MM-DD|EMPTY",
   "status":"ok|fixed|still_stale|source_stale|blocked|escalated","attempts":N,
   "note":"..."}},
 "fix_streak":{"<domain>":N},
 "escalations":[{"domain":"...","diagnosis":"..."}]}
```
