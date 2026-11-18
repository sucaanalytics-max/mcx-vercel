#!/usr/bin/env python3
"""
scripts/backtest_projection.py: a transient Supabase read failure is retried
(18 Nov 2026: one timeout killed the 07:30 job), a persistent one still
raises, and the reads pass a longer timeout. No network: reads are stubbed.
"""
import os, sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "scripts"))
import backtest_projection as bp   # noqa: E402

FAILED = []


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}" + (f"  ({detail})" if detail and not cond else ""))
    if not cond:
        FAILED.append(name)


def flaky(fails, result):
    calls = []

    def fn(*a, **k):
        calls.append((a, k))
        if len(calls) <= fails:
            raise TimeoutError("[Errno 60] Operation timed out")
        return result
    return fn, calls


slept = []
fn, calls = flaky(1, [{"x": 1}])
check("one timeout, then success: returns the rows",
      bp._with_retry(fn, "t", sleep=slept.append) == [{"x": 1}])
check("…after two calls and one 2s wait", (len(calls), slept) == (2, [2]), (len(calls), slept))

slept = []
fn, calls = flaky(2, [])
check("two timeouts, then success", bp._with_retry(fn, sleep=slept.append) == [] and slept == [2, 5])

slept = []
fn, calls = flaky(99, None)
try:
    bp._with_retry(fn, sleep=slept.append)
    check("a persistent failure raises", False)
except TimeoutError:
    check("a persistent failure raises after 3 attempts", len(calls) == 3 and slept == [2, 5])

# fetch_eod / fetch_snaps go through the retry with the longer timeout
orig_r, orig_ra, orig_sleep = bp.supabase_read, bp.supabase_read_all, bp.time.sleep
try:
    bp.time.sleep = lambda s: None
    fn, calls = flaky(1, [{"trading_date": "2026-11-17", "total_rev_cr": 17.1}])
    bp.supabase_read = fn
    row = bp.fetch_eod("2026-11-17")
    check("fetch_eod survives a timeout and returns the row", row and row["total_rev_cr"] == 17.1)
    check("fetch_eod passes timeout=READ_TIMEOUT", calls[-1][1].get("timeout") == bp.READ_TIMEOUT)
    fn, calls = flaky(0, [{"elapsed_min": 10}])
    bp.supabase_read_all = fn
    check("fetch_snaps passes timeout=READ_TIMEOUT",
          bp.fetch_snaps("2026-11-17") == [{"elapsed_min": 10}] and calls[0][1].get("timeout") == bp.READ_TIMEOUT)
finally:
    bp.supabase_read, bp.supabase_read_all, bp.time.sleep = orig_r, orig_ra, orig_sleep

print(f"\n{'ALL PASS' if not FAILED else f'{len(FAILED)} FAILED'}")
sys.exit(1 if FAILED else 0)
