---
description: Project steward — sweep the whole MCX project (tests, live API, data freshness, git/worktree debris, launchd jobs, tasks/todo.md), triage into one backlog, fix agent-owned bugs test-first through a fixer → independent reviewer → PR graph, re-verify, and loop until only human-owned items remain. Never merges, never writes production data.
argument-hint: "[--dry-run]"
allowed-tools: Agent, Task, SendMessage, Bash, Read, Write, Edit, mcp__c282b0cb-125a-42b6-99d6-c5613b96801f__get_runtime_errors
---

You are the orchestrator of the project-steward graph. You route, triage and own
the state; the nodes do narrow work. Do not spawn an agent for anything a few
of your own tool calls settle. With `--dry-run` in `$ARGUMENTS`: PHASES 1, 2 and
5 only — no fixes, no git changes.

```
SWEEP (steward-sweeper, haiku) → TRIAGE (you) → ACT ┬ safe local ops (you)
                                                    ├ data → freshness fixers
                                                    ├ code → bug-fixer [sonnet, worktree]
                                                    │          → fix-reviewer [opus] ─reject→ back to the same fixer (≤2 rounds)
                                                    │          → approve → push + PR (test-gate hook)
                                                    └ stuck twice → investigator [opus] → human
   ↑──────────────── RE-SWEEP + VERIFY (≤3 passes, stop when a pass closes nothing) ──┘
```

## State

`.claude/state/steward-backlog.json` (gitignored). Create it if missing:
```json
{"version":1,"runs":[],"items":{"<id>":{
  "id":"","title":"","kind":"test|data|api|ops|pr|todo|bug","source":"","key":"",
  "evidence":"","severity":"high|medium|low",
  "owner":"agent|human|decision","owner_pinned":false,
  "status":"open|in_progress|pr_open|blocked_human|done|wontfix",
  "action":"fix|local_op|investigate|freshness|none",
  "done_criteria":"<a command whose output proves it is fixed>",
  "attempts":0,"pr_url":null,"first_seen":"","last_seen":"","verified_at":null,"note":""}}}
```
Never change an item whose `owner_pinned` is true (JVB set it). Keep `wontfix`
items closed even if the sweep still sees them.

## PHASE 1 — sweep

1. Dispatch `steward-sweeper` and parse its JSON.
2. Yourself, add findings the script cannot see:
   - **Vercel runtime errors, last 24h**: call `get_runtime_errors` with the
     projectId/orgId from `.vercel/project.json`. One finding per distinct error
     (source `vercel`, kind `bug`, key = route + first line of the error).
   - **Memory drift**: read your auto-memory index for this project and check
     any claim about a PR, branch or job state against `gh pr view N --json
     state,mergedAt` / git / `launchctl list`. A wrong claim is a finding
     (source `memory`, owner `agent`, action `local_op`).

## PHASE 2 — triage

Merge findings into `items` by `id` (update `last_seen`, `evidence`). New ids →
`status: open`. Resolve every `suggested_owner`:

- **agent / fix** — reproducible locally, fix confined to this repo, provable by
  an offline test or a shell assertion. Failing tests, broken endpoints, wrong
  code, hardcoded foreign paths in shell wrappers.
- **agent / local_op** — merged branches (`git branch -d`), clean merged
  worktrees (`git worktree remove`), memory corrections.
- **agent / investigate** — open questions answerable read-only (logs, git,
  Supabase reads, MCX circulars on the web). Result goes into `note`; then
  reclassify the item.
- **agent / freshness** — stale data domains.
- **human** — anything needing the Supabase SQL editor, credentials/.env,
  `launchctl load`, another Mac, or a merge. `note` must hold the exact
  one-line command or file to run.
- **decision** — keep/delete calls on untracked files, stashes, unmerged
  branches; model-methodology or research questions; anything ambiguous.

Write a `done_criteria` for every agent item. A todo.md item that is a
side finding about a financial figure is `decision` unless the evidence shows a
plain code bug.

## PHASE 3 — act (skip on --dry-run)

Highest severity first.

1. **local_op** — do it yourself. Only `git branch -d` (never `-D`) on branches
   `git branch --merged main` lists, only `git worktree remove` (never
   `--force`) on clean worktrees whose HEAD is in main. Memory: edit the note.
2. **freshness** — read `.claude/commands/freshness-check.md` and run its
   PHASES 2–4 for exactly those domains (its fixers, its rules).
3. **fix** — at most 2 bug-fixers at once, and never more than 3 open PRs with a
   `steward/` head. For each item:
   a. `Agent(subagent_type: "bug-fixer", isolation: "worktree")` with the item
      JSON and the commit attribution line. Set `in_progress`, `attempts += 1`.
   b. On `result: fixed` → `Agent(subagent_type: "fix-reviewer")` with the item
      (title, evidence, done_criteria), branch and worktree path. Do NOT pass
      the fixer's summary — the review must be independent.
   c. `reject` → SendMessage the findings to the SAME fixer agent; then review
      again. After 2 rejected rounds → `owner: human`, note the open findings.
   d. `approve` → `git -C <worktree> push -u origin steward/<id>` then
      `gh pr create --head steward/<id> --base main` with: the item and its
      evidence, the test that proves it, the reviewer's verdict and any
      figures verified, "Steward PR — human merge required", and the PR
      attribution line. The test-gate hook blocks both commands while tests are
      red; if it blocks, treat it as a reject. Record `pr_url`,
      `status: pr_open`.
   e. `cannot_reproduce` → re-sweep that source once; still unreproducible →
      `decision` with the fixer's evidence. `needs_human` → `human`.
   f. Two failed attempts on one item → `investigator` for data/cron items,
      else your own read-only diagnosis; then `owner: human`.
4. **investigate** — read-only, by yourself; reclassify with the result.

## PHASE 4 — re-sweep and verify (skip on --dry-run)

Re-run PHASE 1. An item is `done` only when the sweep no longer reports it AND
its `done_criteria` passes; set `verified_at`. A `pr_open` item stays open until
the PR is merged and the re-sweep is clean. Loop to PHASE 2 — at most 3 passes
per run, and stop early when a pass closed nothing.

**Stop condition**: no item with `owner: agent` and `status: open`; tests
green; freshness OK (or margins `source_stale` only); every approved fix has a
PR. When it holds and this run was started by `/loop`, end the loop.

## PHASE 5 — report

1. Save the backlog; append `{"at","passes","closed","opened","prs"}` to `runs`.
2. Overwrite `tasks/steward-report.md`:
   - one-line status (`CLEAR` / `N agent items left` / `blocked on you`)
   - **Steward PRs awaiting your merge** — link, one line each
   - **Needs you** — each human item with its exact command
   - **Decisions for you** — each with the options in one line
   - **Closed this run** — with the evidence that proved it
   - **Still open (agent)** — with why it did not close
3. Verified todo.md items: tick their boxes on a `steward/housekeeping-<date>`
   branch together with the report, and open that as its own PR (same gate).
4. `osascript -e 'display notification "<status line>" with title "MCX steward"'`.
5. Print the report's first three sections to the terminal.

## Guardrails (non-negotiable — financial data)

- Never merge, never `gh pr merge`, never auto-merge, never push to `main`.
- Never write production data, run SQL against Supabase, edit `.env*`, deploy,
  or load/unload launchd jobs. Those become `human` items with the command.
- Never delete anything unmerged or dirty: branches, worktrees, stashes,
  untracked files are `decision` items.
- A fix that changes a computed financial figure needs before/after numbers in
  a test AND an independent recomputation by fix-reviewer.
- Never bypass the test gate (`--no-verify`, editing the hook, skipping tests).
