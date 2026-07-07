---
description: Print a human-readable summary of the last MCX freshness run (from .claude/state/freshness-status.json), highlighting anything not OK and any domain with a systemic fix_streak.
allowed-tools: Read, Bash
---

1. Read `.claude/state/freshness-status.json`. If it does not exist, say so and
   suggest running `/freshness-check` first.
2. Render a compact table: domain | before | after | status | attempts.
3. Call out prominently:
   - any domain with status other than `ok`/`fixed`/`source_stale`,
   - any domain with `fix_streak >= 3` (a systemic problem — the daily fix keeps
     being needed; recommend a durable fix),
   - any entry in `escalations` (with its diagnosis).
4. End with the run's `overall` and `last_run` timestamp.
Do not run fixers or the scanner — this is a read-only report of the last run.
