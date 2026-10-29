#!/usr/bin/env python3
"""
project_sweep.py — deterministic, read-only sweep of everything the project
steward must drive to zero: failing tests, stale data, broken live endpoints,
git/worktree debris, launchd jobs, and the open [ ] items in tasks/todo.md.

Emits one JSON object. Writes/mutates nothing (no git changes, no data writes).
Used by the `steward-sweeper` subagent and the /project-steward orchestrator.

Each finding: {id, source, kind, key, title, evidence, severity, suggested_owner}
  id               stable sha1(source:key)[:10] — the backlog dedupes on it
  kind             test | data | api | ops | pr | todo
  severity         high | medium | low
  suggested_owner  agent (safe for the steward) | human (needs JVB's hands)
                   | decision (needs JVB's call) | freshness (route to
                   /freshness-check) | triage (orchestrator decides)

Usage:
  python3 scripts/project_sweep.py --json
  python3 scripts/project_sweep.py --json --skip api,freshness   # offline
  python3 scripts/project_sweep.py --json --only git,todo
"""
import argparse
import getpass
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SOURCES = ("tests", "freshness", "api", "git", "launchd", "todo")
PROD_URL = os.environ.get("MCX_PROD_URL", "https://mcx-vercel.vercel.app")

# GET-safe production endpoints. /api/cron is excluded (auth + writes).
# Known by-design behaviour, verified 23 Sep 2026 (tasks/todo.md "Not issues"):
# /api/backtest 404s; /api/mcxlive's mcxpy is blocked from cloud IPs.
ENDPOINTS = [
    "/api/quarterly", "/api/valuation", "/api/analytics", "/api/history",
    "/api/mcxprice", "/api/refresh", "/api/exchange_dashboard",
    "/api/models", "/api/models?view=momentum", "/api/models?view=options",
    "/api/commodities", "/api/commodities?view=signals", "/api/commodities?view=icomdex",
    "/api/commodity_dashboard", "/api/commodity_dashboard?view=margins",
    "/api/commodity_dashboard?view=oi_participants",
]
SOFT_ENDPOINTS = {"/api/mcxlive"}  # not probed: fails from cloud IPs by design

HUMAN_HINTS = re.compile(
    r"SQL editor|paste|BLOCKED on human|human step|user must|launchctl load|"
    r"\.env|SUPABASE_SERVICE_KEY|install", re.I)


def fid(source, key):
    return hashlib.sha1(f"{source}:{key}".encode()).hexdigest()[:10]


def finding(source, kind, key, title, evidence="", severity="medium", owner="triage"):
    return {"id": fid(source, key), "source": source, "kind": kind, "key": key,
            "title": title, "evidence": evidence[:1500], "severity": severity,
            "suggested_owner": owner}


def sh(cmd, timeout=600, cwd=None):
    """Run a command; return (rc, combined output). Never raises."""
    try:
        p = subprocess.run(cmd, cwd=cwd or ROOT, capture_output=True, text=True, timeout=timeout,
                           shell=isinstance(cmd, str))
        return p.returncode, (p.stdout + p.stderr).strip()
    except Exception as e:  # missing binary, timeout
        return 127, f"{type(e).__name__}: {e}"


def python_cmd():
    """Same interpreter choice as scripts/run_tests.sh."""
    uv = shutil.which("uv") or os.path.expanduser("~/.local/bin/uv")
    if os.path.exists(uv):
        return [uv, "run", "--no-project", "--quiet", "--with-requirements",
                "requirements.txt", "python"]
    return [sys.executable]


# ── 1. tests ────────────────────────────────────────────────────────────────
def parse_test_output(out):
    """run_tests.sh prints 'FAIL <file> (exit N)' then an indented tail."""
    fails, cur = [], None
    for line in out.splitlines():
        m = re.match(r"FAIL (\S+) \(exit (\d+)\)", line)
        if m:
            cur = {"file": m.group(1), "rc": int(m.group(2)), "tail": []}
            fails.append(cur)
        elif cur and line.startswith("    "):
            cur["tail"].append(line[4:])
        else:
            cur = None
    return fails


def sweep_tests():
    out = []
    rc, text = sh(["bash", "scripts/run_tests.sh", "-q"], timeout=900)
    for f in parse_test_output(text):
        out.append(finding("tests", "test", f["file"], f"Test fails: {f['file']}",
                           "\n".join(f["tail"]), "high", "agent"))
    if rc != 0 and not out:
        out.append(finding("tests", "test", "runner", "Test runner itself failed",
                           text[-1500:], "high", "agent"))
    _, untracked = sh(["git", "ls-files", "--others", "--exclude-standard",
                       "scripts/test_*"])
    for path in filter(None, untracked.splitlines()):
        out.append(finding("tests", "test", f"untracked:{path}",
                           f"Untracked test not in the gate: {path}",
                           "Commit it (and whatever it tests) or delete it.",
                           "medium", "decision"))
    return out


# ── 2. freshness ────────────────────────────────────────────────────────────
def sweep_freshness():
    rc, text = sh(python_cmd() + ["scripts/freshness_scan.py", "--json"], timeout=300)
    try:
        scan = json.loads(text.splitlines()[-1])
    except (ValueError, IndexError):
        return [finding("freshness", "data", "scan", "freshness_scan.py failed",
                        text[-1500:], "high", "agent")]
    out = []
    for d in scan.get("domains", []):
        st = d.get("status")
        if d["domain"] in scan.get("stale_domains", []):
            out.append(finding("freshness", "data", d["domain"],
                               f"Data {st}: {d['domain']} ({d.get('table', 'code')})",
                               json.dumps(d), "medium", "freshness"))
    return out


# ── 3. live API ─────────────────────────────────────────────────────────────
def probe(path, timeout=40):
    req = urllib.request.Request(PROD_URL + path, headers={"User-Agent": "mcx-steward/1"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            code, body = r.status, r.read()
    except urllib.error.HTTPError as e:
        code, body = e.code, e.read(2000)
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"
    try:
        js = json.loads(body)
    except ValueError:
        return code, "non-JSON body: " + body[:200].decode("utf-8", "replace")
    if isinstance(js, dict) and (js.get("success") is False or js.get("error")):
        return code, "success=false: " + json.dumps(js)[:400]
    return code, ""


def sweep_api():
    out = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        results = list(ex.map(probe, ENDPOINTS))
    for path, (code, problem) in zip(ENDPOINTS, results):
        if code != 200 or problem:
            # One re-probe: a cold, uncached function can exceed the timeout
            # once (seen 29 Oct on ?view=margins: >40s, then 0.25s x3).
            code, problem = probe(path)
        if code != 200 or problem:
            out.append(finding("api", "api", path, f"Endpoint unhealthy: {path}",
                               f"HTTP {code}; {problem}", "high", "triage"))
    return out


# ── 4. git ──────────────────────────────────────────────────────────────────
def is_ancestor(rev, base="main"):
    rc, _ = sh(["git", "merge-base", "--is-ancestor", rev, base])
    return rc == 0


def pr_for_branch(branch):
    rc, text = sh(["gh", "pr", "list", "--head", branch, "--state", "all",
                   "--json", "number,state", "--limit", "1"], timeout=60)
    try:
        prs = json.loads(text) if rc == 0 else []
    except ValueError:
        prs = []
    return prs[0] if prs else None


def parse_worktrees(porcelain):
    wts, cur = [], {}
    for line in porcelain.splitlines() + [""]:
        if not line:
            if cur:
                wts.append(cur)
            cur = {}
            continue
        k, _, v = line.partition(" ")
        cur[k] = v or True
    return wts


def sweep_git():
    out = []
    _, text = sh(["git", "status", "--porcelain", "--untracked-files=normal"])
    for line in text.splitlines():
        if not line.startswith("?? "):
            continue
        path = line[3:].strip('"')
        if path.endswith(".DS_Store") or path.startswith("scripts/test_"):
            continue
        out.append(finding("git", "ops", f"untracked:{path}", f"Untracked: {path}",
                           "Commit, gitignore, or delete.", "low", "decision"))

    # Branches checked out anywhere (this checkout or a worktree) can't be deleted.
    _, porcelain = sh(["git", "worktree", "list", "--porcelain"])
    worktrees = parse_worktrees(porcelain)
    checked_out = {w["branch"].removeprefix("refs/heads/") for w in worktrees if isinstance(w.get("branch"), str)}
    _, merged = sh(["git", "branch", "--merged", "main", "--format=%(refname:short)"])
    for b in filter(None, merged.splitlines()):
        if b != "main" and b not in checked_out:
            out.append(finding("git", "ops", f"merged-branch:{b}",
                               f"Local branch already in main: {b}",
                               f"Safe: git branch -d {b}", "low", "agent"))

    _, unmerged = sh(["git", "branch", "--no-merged", "main", "--format=%(refname:short)"])
    for b in filter(None, unmerged.splitlines()):
        _, ahead = sh(["git", "rev-list", "--count", f"main..{b}"])
        pr = pr_for_branch(b)
        pr_txt = f"PR #{pr['number']} {pr['state']}" if pr else "no PR"
        out.append(finding("git", "ops", f"unmerged-branch:{b}",
                           f"Branch not in main: {b} ({ahead} commits ahead, {pr_txt})",
                           "Merge, open a PR, or delete — never auto-deleted.",
                           "medium", "decision"))

    for wt in worktrees[1:]:  # [0] is the main checkout
        path, head = wt.get("worktree"), wt.get("HEAD", "")
        _, dirty = sh(["git", "status", "--porcelain"], cwd=path) if os.path.isdir(path or "") else (0, "missing")
        merged_in = bool(head) and is_ancestor(head)
        rel = os.path.relpath(os.path.realpath(path), os.path.realpath(ROOT)) if path else "?"
        if merged_in and not dirty:
            out.append(finding("git", "ops", f"worktree:{rel}",
                               f"Worktree already merged into main: {rel}",
                               f"HEAD {head[:7]} is in main, clean. Safe: git worktree remove '{path}'",
                               "low", "agent"))
        else:
            out.append(finding("git", "ops", f"worktree:{rel}", f"Worktree with unmerged or dirty work: {rel}",
                               f"HEAD {head[:7]} in main={merged_in}; dirty={bool(dirty)}",
                               "medium", "decision"))

    _, stash = sh(["git", "stash", "list"])
    for line in filter(None, stash.splitlines()):
        out.append(finding("git", "ops", f"stash:{line.split(':')[0]}", f"Stash: {line[:100]}",
                           "Apply or drop.", "low", "decision"))

    rc, text = sh(["gh", "pr", "list", "--state", "open", "--json", "number,title,headRefName,url"], timeout=60)
    for pr in (json.loads(text) if rc == 0 and text.startswith("[") else []):
        owner = "human" if pr["headRefName"].startswith("steward/") else "decision"
        out.append(finding("git", "pr", f"pr:{pr['number']}",
                           f"Open PR #{pr['number']}: {pr['title']}", pr["url"], "medium", owner))
    rc, text = sh(["gh", "issue", "list", "--state", "open", "--json", "number,title,url"], timeout=60)
    for iss in (json.loads(text) if rc == 0 and text.startswith("[") else []):
        out.append(finding("git", "bug", f"issue:{iss['number']}",
                           f"Open issue #{iss['number']}: {iss['title']}", iss["url"], "medium", "triage"))
    return out


# ── 5. launchd ──────────────────────────────────────────────────────────────
FOREIGN_USER = re.compile(r"/Users/([A-Za-z0-9._-]+)/")


def foreign_paths(text, me):
    return sorted({u for u in FOREIGN_USER.findall(text) if u not in (me, "Shared")})


def plist_label(path):
    _, label = sh(["plutil", "-extract", "Label", "raw", path])
    return label.strip()


def sweep_launchd():
    out, me = [], getpass.getuser()
    plists = sorted(
        os.path.join(d, f)
        for d in (os.path.join(ROOT, "scripts", "launchd"), os.path.join(ROOT, "scripts"))
        for f in os.listdir(d) if f.startswith("com.mcx.") and f.endswith(".plist"))
    _, loaded = sh(["launchctl", "list"])
    loaded_labels = {ln.split("\t")[-1]: ln.split("\t") for ln in loaded.splitlines()[1:]}

    for p in plists:
        rel = os.path.relpath(p, ROOT)
        label = plist_label(p)
        others = foreign_paths(open(p).read(), me)
        if others:
            out.append(finding("launchd", "ops", f"plist-foreign:{rel}",
                               f"{rel} is written for another Mac (/Users/{', '.join(others)})",
                               "launchd needs absolute paths, so this plist cannot load here as-is. "
                               "Decide which Mac runs this job.", "low", "decision"))
            continue
        if label not in loaded_labels:
            out.append(finding("launchd", "ops", f"not-loaded:{label}",
                               f"launchd job not loaded on this Mac: {label}",
                               f"cp '{rel}' ~/Library/LaunchAgents/ && launchctl load ~/Library/LaunchAgents/{os.path.basename(p)}",
                               "medium", "human"))
            continue
        status = loaded_labels[label][1]
        if status not in ("0", "-"):
            out.append(finding("launchd", "ops", f"exit:{label}",
                               f"launchd job {label} last exited {status}",
                               f"launchctl print gui/$(id -u)/{label}", "high", "triage"))

    for d in ("scripts",):
        for f in sorted(os.listdir(os.path.join(ROOT, d))):
            if not f.endswith(".sh"):
                continue
            rel = os.path.join(d, f)
            others = foreign_paths(open(os.path.join(ROOT, rel)).read(), me)
            if others:
                out.append(finding("launchd", "bug", f"sh-foreign:{rel}",
                                   f"{rel} hardcodes another user's home (/Users/{', '.join(others)})",
                                   "Shell wrappers can resolve the repo from $0 and tools from $HOME.",
                                   "medium", "agent"))
    return out


# ── 6. todo ─────────────────────────────────────────────────────────────────
def parse_todo(text):
    """Unchecked '- [ ]' items with their continuation lines and nearest heading."""
    items, heading, cur = [], "", None
    for line in text.splitlines():
        if line.startswith("#"):
            heading, cur = line.lstrip("#").strip(), None
            continue
        m = re.match(r"\s*- \[( |x|X)\]\s*(.*)", line)
        if m:
            cur = {"heading": heading, "text": m.group(2).strip()} if m.group(1) == " " else None
            if cur:
                items.append(cur)
            continue
        if cur and line.startswith((" ", "\t")) and line.strip():
            cur["text"] += " " + line.strip()
        else:
            cur = None
    return items


def sweep_todo():
    path = os.path.join(ROOT, "tasks", "todo.md")
    if not os.path.exists(path):
        return []
    out = []
    for it in parse_todo(open(path).read()):
        key = re.sub(r"\s+", " ", it["text"])[:80]
        owner = "human" if HUMAN_HINTS.search(it["text"]) else "triage"
        out.append(finding("todo", "todo", key, it["text"][:120],
                           f"[{it['heading']}] {it['text']}", "medium", owner))
    return out


SWEEPS = {"tests": sweep_tests, "freshness": sweep_freshness, "api": sweep_api,
          "git": sweep_git, "launchd": sweep_launchd, "todo": sweep_todo}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true", help="JSON only")
    ap.add_argument("--only", default="", help="comma list of sources")
    ap.add_argument("--skip", default="", help="comma list of sources")
    a = ap.parse_args()
    only = [s for s in a.only.split(",") if s] or list(SOURCES)
    skip = {s for s in a.skip.split(",") if s}
    findings, errors = [], []
    for src in only:
        if src in skip:
            continue
        try:
            findings += SWEEPS[src]()
        except Exception as e:  # one broken source must not hide the others
            errors.append({"source": src, "error": f"{type(e).__name__}: {e}"})
    sev = {"high": 0, "medium": 1, "low": 2}
    findings.sort(key=lambda f: (sev[f["severity"]], f["source"], f["key"]))
    by_owner = {}
    for f in findings:
        by_owner[f["suggested_owner"]] = by_owner.get(f["suggested_owner"], 0) + 1
    result = {"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
              "sources": [s for s in only if s not in skip], "findings": findings,
              "summary": {"total": len(findings), "by_owner": by_owner}, "errors": errors}
    if not a.json:
        for f in findings:
            print(f"  [{f['severity']:6s}] {f['suggested_owner']:9s} {f['id']}  {f['title']}")
        print(f"\n{len(findings)} findings  {by_owner}  errors={len(errors)}\n")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
