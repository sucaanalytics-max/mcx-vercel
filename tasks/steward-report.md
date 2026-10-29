# Project steward report

_2026-10-29T14:01:28+00:00 · dry run (sweep + triage only, nothing changed)_

**Status: 11 agent items open · 2 need you · 18 decisions for you.** Tests 13/13 green · freshness 12/12 OK · 16/16 live endpoints healthy.

## Steward PRs awaiting your merge

None yet (dry run).

## Needs you

- **After merging PR #8: re-run scripts/sql/guard_mcx_daily_revenue.sql in the Supabase SQL editor if the guard is already l** — PR #8 is merged: paste scripts/sql/guard_mcx_daily_revenue.sql in the Supabase SQL editor (create-or-replace, no permission change) if the guard is live.
- **launchd job not loaded on this Mac: com.mcx.commodity-oi** — cp 'scripts/launchd/com.mcx.commodity-oi.plist' ~/Library/LaunchAgents/ && launchctl load ~/Library/LaunchAgents/com.mcx.commodity-oi.plist

## Decisions for you

- Branch not in main: options-agent-research (2 commits ahead, PR #6 MERGED) — PR #6 merged; 2 later commits never landed. Open a PR for them or delete the branch.
- Branch not in main: wip/options-checkout-2026-10-29 (3 commits ahead, no PR) — 3 WIP commits, no PR. Keep, PR, or delete.
- Repo plist runs daily_verify under /usr/bin/python3 (no curl_cffi), where the historical fetch can't load. It now report — Same decision as the daily-verify plist (which Mac). Note: on this Mac /usr/bin/python3 HAS curl_cffi; homebrew does not.
- Side finding: 46 excel_calibrated rows (2025-12-22 → 2026-02-25) have the right total_rev_cr but opt_prem_cr off MCX by  — Financial data correction — needs your call before any rewrite of historical rows.
- Side finding: recompute backtest (no drift) under-projects ~15–18% over the last 5 days on HEAD too — pre-existing, unre — Model-accuracy question — your call whether to investigate.
- Untracked test not in the gate: scripts/test_bhav_refresh.py — Calls bhav_refresh._field, which committed code lacks — goes with the uncommitted bhav_refresh edits (stash@{1} / other Mac). Commit both or delete the test.
- Stash: stash@{0}: autostash — Apply or drop (pre-session WIP touches api/refresh, mcx_config, mcx_relay).
- Stash: stash@{1}: On main: pre-session WIP (api/refresh, mcx_config, mcx_relay) — Apply or drop (pre-session WIP touches api/refresh, mcx_config, mcx_relay).
- Untracked: .claude/launch.json — Commit, gitignore, or delete.
- Untracked: 13032026_MCX_Momentum_Entry and Exit Methodology.docx — Commit, gitignore, or delete.
- Untracked: Docs/MCX_CEO_Meeting_Questions.md — Commit, gitignore, or delete.
- Untracked: Docs/MCX_vs_CME_ICE_Competitive_Benchmark.md — Commit, gitignore, or delete.
- Untracked: data/ — Commit, gitignore, or delete.
- Untracked: scripts/backfill_commodity_gaps.py — Commit, gitignore, or delete.
- Untracked: scripts/sql/fix_commodity_daily_rls.sql — Commit, gitignore, or delete.
- scripts/com.mcx.backtest.plist is written for another Mac (/Users/pranayagarwal) — Which Mac runs this job? If this one, the plist needs /Users/tusk-jvb paths; if Pranay's, leave it.
- scripts/com.mcx.daily-verify.plist is written for another Mac (/Users/pranayagarwal) — Which Mac runs this job? If this one, the plist needs /Users/tusk-jvb paths; if Pranay's, leave it.
- scripts/launchd/com.mcx.freshness.plist is written for another Mac (/Users/pranayagarwal) — Which Mac runs this job? If this one, the plist needs /Users/tusk-jvb paths; if Pranay's, leave it.

## Closed this run

None (dry run).

## Still open (agent) — the next real run takes these

- [high] **/api/cron?job=oi_participants times out at 60s (16:00 UTC cron)** — investigate: Read-only: time the job locally (scripts/run_cron.py), compare with OI freshness; likely fix = move to daily_verify like margins (9842644).
- [high] **Before 2 Nov: check MCX's 2026-27 circular matches (add an override if not).** — investigate: Find MCX's 2026-27 winter-timing circular; if dates differ from 2 Nov 2026 → 12 Mar 2027, add a SESSION_END_OVERRIDES entry (fix PR).
- [medium] **4. One-time backfill 2015-12-31 → today. AFTER the SQL is run: /opt/homebrew/bin/python3 scripts/icomdex_refresh.py --ba** — investigate: icomdex domain is OK to 2026-10-28, so the table exists; check min(trade_date) to see if the 2015 backfill ran.
- [medium] **OPEN: who writes mcx_valuation at 20:27:10 UTC daily? Not Vercel (no /api/cron hit at that minute), not launchd/crontab/** — investigate: Read-only: Supabase pg_cron / edge functions; else stays decision.
- [medium] **scripts/run_freshness.sh hardcodes another user's home (/Users/pranayagarwal)** — fix: Resolve repo from $0 and tools from $HOME.
- [medium] **scripts/start_relay.sh hardcodes another user's home (/Users/pranayagarwal)** — fix: Resolve repo from $0 and tools from $HOME.
- [low] **Local branch already in main: claude/interesting-jones-316915** — local_op: git branch -d (merged into main).
- [low] **Local branch already in main: claude/quizzical-mccarthy-945fdf** — local_op: git branch -d (merged into main).
- [low] **Memory says PR #6 (options research) is not merged — merged 28 Oct** — local_op: Correct the note.
- [low] **Memory says PR #8 (winter close) is unmerged — merged 28 Oct** — local_op: Correct the note.
- [low] **Worktree already merged into main: .claude/worktrees/quizzical-mccarthy-945fdf** — local_op: git worktree remove (clean, HEAD in main).
