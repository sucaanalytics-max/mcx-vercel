#!/usr/bin/env python3
"""
PreToolUse hook (Bash): block `gh pr create` and `git push` while any tracked
test is red. Wired in .claude/settings.json; the project steward relies on it as
the one handoff edge the model cannot skip.

Tests run in the checkout being pushed: a `git -C <dir>` / `cd <dir> &&` target,
else the worktree that has the pushed branch checked out, else the session cwd.
Remote branch deletions (`--delete`) are not gated. Exit 2 = block (stderr is
shown to Claude); exit 0 = allow.
"""
import json
import os
import re
import shlex
import subprocess
import sys

GATED = re.compile(r"(^|[;&|]\s*|\s)(gh\s+pr\s+create|git\s+(-C\s+\S+\s+)?push)\b")


def git(args, cwd):
    p = subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True)
    return p.stdout.strip() if p.returncode == 0 else ""


def target_dir(cmd, cwd):
    m = re.search(r"git\s+-C\s+(\"[^\"]+\"|'[^']+'|\S+)", cmd) or \
        re.search(r"(?:^|&&|;)\s*cd\s+(\"[^\"]+\"|'[^']+'|\S+)\s*&&", cmd)
    if m:
        return os.path.expanduser(shlex.split(m.group(1))[0])
    try:
        tokens = set(shlex.split(cmd))
    except ValueError:
        tokens = set(cmd.split())
    for block in git(["worktree", "list", "--porcelain"], cwd).split("\n\n"):
        path = re.search(r"^worktree (.+)$", block, re.M)
        branch = re.search(r"^branch refs/heads/(.+)$", block, re.M)
        if path and branch and branch.group(1) in tokens | {t.split(":")[0] for t in tokens}:
            return path.group(1)
    return cwd


def main():
    try:
        data = json.load(sys.stdin)
    except ValueError:
        return 0
    cmd = (data.get("tool_input") or {}).get("command", "")
    if not GATED.search(cmd) or "--delete" in cmd:
        return 0
    cwd = data.get("cwd") or os.environ.get("CLAUDE_PROJECT_DIR") or os.getcwd()
    top = git(["rev-parse", "--show-toplevel"], target_dir(cmd, cwd))
    runner = os.path.join(top, "scripts", "run_tests.sh") if top else ""
    if not os.path.exists(runner):
        return 0  # a branch from before the runner existed: nothing to gate on
    p = subprocess.run(["bash", runner, "-q"], capture_output=True, text=True, timeout=900)
    if p.returncode == 0:
        return 0
    sys.stderr.write(f"Test gate blocked this command: tests are red in {top}\n"
                     + (p.stdout + p.stderr)[-3000:]
                     + "\nFix the failures (scripts/run_tests.sh), then retry.\n")
    return 2


if __name__ == "__main__":
    sys.exit(main())
