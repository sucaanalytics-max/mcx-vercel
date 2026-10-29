#!/usr/bin/env python3
"""Offline tests for scripts/project_sweep.py: the todo parser, stable finding ids, the
foreign-home detector, the run_tests.sh / worktree parsers, and the git sweep's
merged-vs-unmerged classification on a throwaway repo. No network.
Run:  python3 scripts/test_project_sweep.py"""
import importlib.util
import os
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
spec = importlib.util.spec_from_file_location("project_sweep", os.path.join(ROOT, "scripts", "project_sweep.py"))
ps = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ps)

failed = 0


def check(name, got, want):
    global failed
    ok = got == want
    print(("PASS " if ok else "FAIL ") + name + ("" if ok else f": got {got!r}, want {want!r}"))
    if not ok:
        failed += 1


# ── todo parser ──
todo = """# Feature A
## Tasks
- [x] 1. done thing
- [ ] 2. open thing
      continued on the next line
- [ ] 3. paste scripts/sql/x.sql in the Supabase SQL editor

## Review
- [X] closed upper-case
  - [ ] nested open item
Not a continuation.
"""
items = ps.parse_todo(todo)
check("only unchecked items", [i["text"][:2] for i in items], ["2.", "3.", "ne"])
check("continuation lines are joined", items[0]["text"], "2. open thing continued on the next line")
check("nearest heading is kept", [i["heading"] for i in items], ["Tasks", "Tasks", "Review"])
f = ps.sweep_todo.__globals__["finding"]("todo", "todo", "k", "t")
check("findings carry a 10-char id", len(f["id"]), 10)
check("ids are stable across runs", ps.fid("todo", "abc"), ps.fid("todo", "abc"))
check("ids differ by source", ps.fid("git", "abc") != ps.fid("todo", "abc"), True)
check("SQL-editor items suggest a human", bool(ps.HUMAN_HINTS.search(items[1]["text"])), True)
check("plain items go to triage", bool(ps.HUMAN_HINTS.search(items[0]["text"])), False)

# ── foreign home paths ──
text = 'cd "/Users/pranayagarwal/Dropbox/x"\nexport PATH="/Users/me/.local/bin:/Users/Shared/bin"'
check("other users' homes are flagged", ps.foreign_paths(text, "me"), ["pranayagarwal"])
check("own home and /Users/Shared are not", ps.foreign_paths("/Users/me/a /Users/Shared/b", "me"), [])

# ── run_tests.sh output ──
out = """FAIL scripts/test_a.py (exit 1)
    Traceback
    AttributeError: x
ok   scripts/test_b.py
FAIL scripts/test_c_js.js (exit 2)
    boom
1 of 3 test files FAILED"""
fails = ps.parse_test_output(out)
check("each FAIL line is one failure", [(f["file"], f["rc"]) for f in fails],
      [("scripts/test_a.py", 1), ("scripts/test_c_js.js", 2)])
check("indented tail is attached", fails[0]["tail"], ["Traceback", "AttributeError: x"])

# ── git worktree porcelain ──
por = "worktree /r\nHEAD aaa\nbranch refs/heads/main\n\nworktree /r/wt\nHEAD bbb\ndetached\n"
wts = ps.parse_worktrees(por)
check("worktrees parsed", [(w["worktree"], w["HEAD"]) for w in wts], [("/r", "aaa"), ("/r/wt", "bbb")])
check("detached worktree has no branch", "branch" in wts[1], False)

# ── git sweep on a throwaway repo ──
with tempfile.TemporaryDirectory() as tmp:
    def git(*a):
        subprocess.run(["git", *a], cwd=tmp, check=True, capture_output=True)
    git("init", "-q", "-b", "main")
    git("config", "user.email", "t@t"); git("config", "user.name", "t")
    open(os.path.join(tmp, "a"), "w").write("1")
    git("add", "a"); git("commit", "-qm", "base")
    git("branch", "merged-one")
    git("switch", "-qc", "ahead-one")
    open(os.path.join(tmp, "b"), "w").write("2")
    git("add", "b"); git("commit", "-qm", "ahead")
    git("switch", "-q", "main")
    git("branch", "checked-out-elsewhere")
    git("worktree", "add", "-q", os.path.join(tmp, "wt"), "checked-out-elsewhere")
    open(os.path.join(tmp, ".gitignore"), "w").write("wt/\n")
    git("add", ".gitignore"); git("commit", "-qm", "ignore worktrees")
    open(os.path.join(tmp, "stray.txt"), "w").write("x")

    ps.ROOT = tmp
    ps.pr_for_branch = lambda b: None  # no GitHub in a temp repo
    found = {f["key"]: f for f in ps.sweep_git()}
    ps.ROOT = ROOT

check("merged branch is safe for the agent", found.get("merged-branch:merged-one", {}).get("suggested_owner"), "agent")
check("unmerged branch needs a decision", found.get("unmerged-branch:ahead-one", {}).get("suggested_owner"), "decision")
check("unmerged branch reports commits ahead", "1 commits ahead" in found["unmerged-branch:ahead-one"]["title"], True)
check("branch checked out in a worktree is not offered for deletion",
      "merged-branch:checked-out-elsewhere" in found, False)
check("clean merged worktree is safe to remove", found.get("worktree:wt", {}).get("suggested_owner"), "agent")
check("untracked file needs a decision", found.get("untracked:stray.txt", {}).get("suggested_owner"), "decision")

print(f"\n{failed} FAILED" if failed else "\nAll project sweep tests passed.")
sys.exit(1 if failed else 0)
