# Project steward report

_2026-10-29T14:15:17+00:00 · run 1 (2 sweep passes)_

**Status: CLEAR for the agent — 0 agent items open. 3 fix PRs await your merge · 3 need you · 20 decisions.** Tests green · freshness 12/12 OK · 16/16 endpoints healthy.

## Steward PRs awaiting your merge

- [#12](https://github.com/sucaanalytics-max/mcx-vercel/pull/12) the steward itself (merge first: it brings `scripts/run_tests.sh` and the test gate)
- [#13](https://github.com/sucaanalytics-max/mcx-vercel/pull/13) scripts/run_freshness.sh hardcodes another user's home (/Users/pranayagarwal) — Reviewer approved round 1.
- [#14](https://github.com/sucaanalytics-max/mcx-vercel/pull/14) scripts/start_relay.sh hardcodes another user's home (/Users/pranayagarwal) — Rejected round 1 (symlinks), approved round 2.
- [#15](https://github.com/sucaanalytics-max/mcx-vercel/pull/15) /api/cron?job=oi_participants requests a not-yet-published file and can exceed 60s — Root cause: fires 21:30 IST for a T+1 file; retry budget ~96s > 60s. Round 1 rejected (my unverified 403 claim), round 2 approved.

## Needs you

- **After merging PR #8: re-run scripts/sql/guard_mcx_daily_revenue.sql in the Supabase SQL editor if the guard is already l** — PR #8 is merged: paste scripts/sql/guard_mcx_daily_revenue.sql in the Supabase SQL editor (create-or-replace, no permission change) if the guard is live.
- **OPEN: who writes mcx_valuation at 20:27:10 UTC daily? Not Vercel (no /api/cron hit at that minute), not launchd/crontab/** — Nothing in the repo writes mcx_valuation at 20:27 UTC (writers: Vercel cron 14:00 UTC, unscheduled valuation_refresh.py). Check outside the repo: on Pranay's Mac `crontab -l; launchctl list | grep -i mcx`, and Supabase dashboard → Database → Cron Jobs / Edge Functions.
- **launchd job not loaded on this Mac: com.mcx.commodity-oi** — cp 'scripts/launchd/com.mcx.commodity-oi.plist' ~/Library/LaunchAgents/ && launchctl load ~/Library/LaunchAgents/com.mcx.commodity-oi.plist

## Decisions for you

- [high] Muhurat trading Sun 8 Nov 2026 is outside the trading calendar — is_trading_day(2026-11-08)=False, so the relay/daily row will miss the session (2025's weekday Muhurat, 21 Oct, booked ₹1.26 Cr). Timings not yet notified by MCX. Your call: add a special session to the calendar (changes Q3 FY27 revenue) or accept the gap.
- [medium] Branch not in main: options-agent-research (2 commits ahead, PR #6 MERGED) — PR #6 merged; 2 later commits never landed. Open a PR for them or delete the branch.
- [medium] Branch not in main: wip/options-checkout-2026-10-29 (3 commits ahead, no PR) — 3 WIP commits, no PR. Keep, PR, or delete.
- [medium] Repo plist runs daily_verify under /usr/bin/python3 (no curl_cffi), where the historical fetch can't load. It now report — Same decision as the daily-verify plist (which Mac). Note: on this Mac /usr/bin/python3 HAS curl_cffi; homebrew does not.
- [medium] Side finding: 46 excel_calibrated rows (2025-12-22 → 2026-02-25) have the right total_rev_cr but opt_prem_cr off MCX by  — Financial data correction — needs your call before any rewrite of historical rows.
- [medium] Side finding: recompute backtest (no drift) under-projects ~15–18% over the last 5 days on HEAD too — pre-existing, unre — Model-accuracy question — your call whether to investigate.
- [medium] Untracked test not in the gate: scripts/test_bhav_refresh.py — Calls bhav_refresh._field, which committed code lacks — goes with the uncommitted bhav_refresh edits (stash@{1} / other Mac). Commit both or delete the test.
- [low] Stash: stash@{0}: autostash — Apply or drop (pre-session WIP touches api/refresh, mcx_config, mcx_relay).
- [low] Stash: stash@{1}: On main: pre-session WIP (api/refresh, mcx_config, mcx_relay) — Apply or drop (pre-session WIP touches api/refresh, mcx_config, mcx_relay).
- [low] Untracked: .claude/launch.json — Commit, gitignore, or delete.
- [low] Untracked: 13032026_MCX_Momentum_Entry and Exit Methodology.docx — Commit, gitignore, or delete.
- [low] Untracked: Docs/MCX_CEO_Meeting_Questions.md — Commit, gitignore, or delete.
- [low] Untracked: Docs/MCX_vs_CME_ICE_Competitive_Benchmark.md — Commit, gitignore, or delete.
- [low] Untracked: data/ — Commit, gitignore, or delete.
- [low] Untracked: scripts/backfill_commodity_gaps.py — Commit, gitignore, or delete.
- [low] Untracked: scripts/sql/fix_commodity_daily_rls.sql — Commit, gitignore, or delete.
- [low] mcx_daily_revenue 2024-11-01 (Muhurat 2024) has total_rev_cr = null — Row exists (source mcx_historical_backfill) with null revenue. Historical-data fix needs your call before any write.
- [low] scripts/com.mcx.backtest.plist is written for another Mac (/Users/pranayagarwal) — Which Mac runs this job? If this one, the plist needs /Users/tusk-jvb paths; if Pranay's, leave it.
- [low] scripts/com.mcx.daily-verify.plist is written for another Mac (/Users/pranayagarwal) — Which Mac runs this job? If this one, the plist needs /Users/tusk-jvb paths; if Pranay's, leave it.
- [low] scripts/launchd/com.mcx.freshness.plist is written for another Mac (/Users/pranayagarwal) — Which Mac runs this job? If this one, the plist needs /Users/tusk-jvb paths; if Pranay's, leave it.

## Closed this run (verified)

- 4. One-time backfill 2015-12-31 → today. AFTER the SQL is run: /opt/homebrew/bin/python3 scripts/icomdex_refresh.py --ba — mcx_icomdex_daily spans 2015-12-31 → 2026-10-28: backfill ran.
- Before 2 Nov: check MCX's 2026-27 circular matches (add an override if not). — MCX notice (via 5paisa repro, primary circular not retrieved): 23:55 from Mon 2 Nov 2026 to Fri 12 Mar 2027 inclusive. session_end() gives 23:55 on 2 Nov & 12 Mar, 23:30 on 30 Oct & 15 Mar. No override needed.
- Local branch already in main: claude/interesting-jones-316915 — git branch -d (merged into main). Done; re-sweep no longer finds it.
- Local branch already in main: claude/quizzical-mccarthy-945fdf — git branch -d (merged into main). Done; re-sweep no longer finds it.
- Memory said PR #8 (winter close) unmerged — it merged 29 Oct 13:34 UTC (edffcd0) — Corrected in mcx-winter-close, mcx-backend-defects-sep-2026, MEMORY.md. (Dry run wrongly said 28 Oct.)
- Memory says PR #6 (options research) is not merged — merged 28 Oct — Corrected in mcx-options-paper-trading (PR #6 merged 28 Oct, 695040c).
- Worktree already merged into main: .claude/worktrees/quizzical-mccarthy-945fdf — git worktree remove (clean, HEAD in main). Done; re-sweep no longer finds it.

## Still open (agent)

None.

## Run notes

- Fix → independent review worked as designed: 2 of 3 fixes were rejected once (a symlink regression; an unverified root-cause claim of mine) and approved on round 2.
- Steward fix to itself: the API sweep now re-probes a failing endpoint once (a cold `?view=margins` call timed out once, then answered in 0.25s).
- Fix PRs #13–#15 are based on `main`, which lacks `scripts/run_tests.sh` until #12 merges, so the push gate had nothing to run; reviewers ran the suite manually.
