#!/usr/bin/env python3
"""Offline tests for the evaluation helpers in api/analytics.py: signals are traded from the
next day's close, and rolling windows only use returns already known on their date. No network.
Run:  python3 scripts/test_analytics.py"""
import importlib.util
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
spec = importlib.util.spec_from_file_location("analytics", os.path.join(ROOT, "api", "analytics.py"))
an = importlib.util.module_from_spec(spec)
spec.loader.exec_module(an)

failed = 0


def check(name, got, want):
    global failed
    ok = got == want
    print(("PASS " if ok else "FAIL ") + name + ("" if ok else f": got {got!r}, want {want!r}"))
    if not ok:
        failed += 1


dates = [f"2026-08-{d:02d}" for d in range(1, 13)]
closes = [100, 110, 121, 100, 100, 100, 100, 100, 100, 100, 100, 100]
pmap = dict(zip(dates, closes))
pidx = {d: i for i, d in enumerate(dates)}

check("entry lag is one day", an.ENTRY_LAG, 1)
r, exit_idx = an.forward_return(dates, pmap, pidx, dates[0], 1)
check("a signal on day 0 is traded from day 1's close, not day 0's", (round(r, 4), exit_idx), (0.1, 2))
r0, _ = an.forward_return(dates, pmap, pidx, dates[0], 1, lag=0)
check("the old same-day entry would have caught day 0 → 1 as well", round(r0, 4), 0.1)
check("no return when the exit is past the data", an.forward_return(dates, pmap, pidx, dates[-2], 1), None)
check("no return for a date without a price", an.forward_return(dates, pmap, pidx, "2026-07-01", 1), None)

signals = [{"trading_date": d} for d in dates]
fwd = {d: an.forward_return(dates, pmap, pidx, d, 1) for d in dates}
fwd = {d: f for d, f in fwd.items() if f}
win = an.known_window(signals, 5, fwd, pidx, 60)
check("on day 5 only signals whose exit (signal day + 2) is on or before day 5 count",
      [s["trading_date"] for s in win], dates[:4])
check("the window keeps the most recent N", [s["trading_date"] for s in an.known_window(signals, 9, fwd, pidx, 3)], dates[5:8])

print(f"\n{failed} FAILED" if failed else "\nAll analytics tests passed.")
sys.exit(1 if failed else 0)
