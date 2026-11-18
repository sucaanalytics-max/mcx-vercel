#!/usr/bin/env python3
"""
scripts/freshness_alert.py: the morning alert notifies only when something is
not OK, names each problem, survives a failing scan, and never notifies on a
dry run. Pure: the scan and the notifier are stubbed.
"""
import os, sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "scripts"))
import freshness_alert as fa   # noqa: E402

FAILED = []


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}" + (f"  ({detail})" if detail and not cond else ""))
    if not cond:
        FAILED.append(name)


OK = {"overall": "OK", "expected_trading_day": "2026-11-17",
      "domains": [{"domain": "revenue", "status": "OK"}, {"domain": "margins", "status": "OK"}]}
BAD = {"overall": "DEGRADED", "expected_trading_day": "2026-11-17", "domains": [
    {"domain": "revenue", "status": "OK"},
    {"domain": "relay_coverage", "status": "STALE", "message": "2026-11-17: last snapshot 19:08, close 23:55"},
    {"domain": "margins", "status": "STALE", "latest": "2026-11-12", "expected": "2026-11-17"},
    {"domain": "watchlist", "status": "INFO_STALE"},
]}


def run(result=None, argv=(), boom=None):
    sent = []

    def scan():
        if boom:
            raise boom
        return result
    rc = fa.main(list(argv), scan=scan, notify_fn=lambda s, b: sent.append((s, b)))
    return rc, sent


print("[1] all OK")
rc, sent = run(OK)
check("exit 0", rc == 0)
check("no notification", sent == [], str(sent))
rc, sent = run(OK, ["--always"])
check("--always notifies even when OK", len(sent) == 1 and sent[0][0].startswith("All OK"), str(sent))

print("[2] something stale")
rc, sent = run(BAD)
check("exit 1", rc == 1)
check("one notification", len(sent) == 1, str(sent))
sub, body = sent[0]
check("subtitle counts the issues", sub == "2 issues for 2026-11-17", sub)
check("names relay coverage with its message", "relay_coverage STALE: 2026-11-17: last snapshot 19:08" in body, body)
check("names margins with latest vs expected", "margins STALE: latest 2026-11-12, expected 2026-11-17" in body, body)
check("unmonitored INFO_ domain left out", "watchlist" not in body, body)
rc, sent = run(BAD, ["--dry-run"])
check("--dry-run never notifies", rc == 1 and sent == [], str(sent))

print("[3] the scan itself fails")
rc, sent = run(boom=RuntimeError("Supabase 503"))
check("exit 2", rc == 2)
check("still notifies", len(sent) == 1 and sent[0][0] == "Freshness scan failed" and "Supabase 503" in sent[0][1], str(sent))

print("[4] notification text")
long = {**BAD, "domains": [{"domain": f"d{i}", "status": "STALE", "message": "x" * 60} for i in range(10)]}
_, body = fa.notification(long)
check("body capped for macOS", len(body) <= fa.MAX_BODY and body.endswith("…"), str(len(body)))
calls = []
fa.notify('He said "hi"', 'path \\ quote "x"', run=lambda args, **k: calls.append(args))
check("quotes and backslashes escaped for AppleScript",
      calls and '\\"hi\\"' in calls[0][2] and '\\\\' in calls[0][2], str(calls))

print()
if FAILED:
    print(f"{len(FAILED)} FAILED: {', '.join(FAILED)}")
    sys.exit(1)
print("All tests passed.")
