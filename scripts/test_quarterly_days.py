#!/usr/bin/env python3
"""
Regression tests for the quarterly projection's day counting (api/quarterly.py).

The projection is actual revenue + daily_proj x remaining sessions. Until
29 Sep 2026 it counted elapsed = rows dated up to today and remaining = days
from tomorrow, so today was in neither until its end-of-day row landed
(~23:30-23:45 IST): on 29 Sep at 09:10 it showed 64 + 1 of 66 days and
Rs 733.9 Cr instead of Rs 747.7 Cr. On a quarter's last day it dropped the
whole session, on a first day it fell back to a hard-coded Rs 12 Cr/day, and a
missing past row vanished from the projection.

Offline: supabase_read is replaced with an in-memory table.
Run:  python3 scripts/test_quarterly_days.py
"""
import sys, os
from datetime import date, datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import api.quarterly as q

FAILED = []

# Q2 FY27 daily revenue (Rs Cr) as published by /api/quarterly on 29 Sep 2026.
Q2_REV = [
    ("07-01", 8.44), ("07-02", 7.11), ("07-03", 5.15), ("07-06", 6.34), ("07-07", 7.78),
    ("07-08", 12.89), ("07-09", 9.14), ("07-10", 9.47), ("07-13", 12.68), ("07-14", 15.05),
    ("07-15", 12.42), ("07-16", 12.3), ("07-17", 7.48), ("07-20", 7.96), ("07-21", 7.49),
    ("07-22", 9.06), ("07-23", 10.0), ("07-24", 10.55), ("07-27", 9.36), ("07-28", 11.3),
    ("07-29", 11.25), ("07-30", 7.79), ("07-31", 8.3), ("08-03", 8.36), ("08-04", 11.05),
    ("08-05", 10.74), ("08-06", 9.59), ("08-07", 10.56), ("08-10", 9.73), ("08-11", 11.64),
    ("08-12", 10.22), ("08-13", 10.71), ("08-14", 10.16), ("08-17", 12.93), ("08-18", 7.52),
    ("08-19", 10.98), ("08-20", 11.87), ("08-21", 12.55), ("08-24", 14.58), ("08-25", 10.06),
    ("08-26", 10.83), ("08-27", 12.48), ("08-28", 17.54), ("08-31", 14.17), ("09-01", 10.69),
    ("09-02", 10.73), ("09-03", 11.9), ("09-04", 12.29), ("09-07", 7.87), ("09-08", 10.23),
    ("09-09", 11.58), ("09-10", 15.06), ("09-11", 15.35), ("09-14", 11.54), ("09-15", 15.13),
    ("09-16", 15.54), ("09-17", 18.56), ("09-18", 10.98), ("09-21", 10.6), ("09-22", 12.53),
    ("09-23", 13.94), ("09-24", 16.45), ("09-25", 16.93), ("09-28", 14.53),
]
Q2_ROWS = [{"trading_date": f"2026-{md}", "total_rev_cr": v, "source": "mcx_relay_eod"} for md, v in Q2_REV]


def run(now, rows):
    """generate_quarterly at IST clock time 'now' against an in-memory mcx_daily_revenue."""
    def fake_read(table, params, **kw):
        if "trading_date=lt." in params:              # prior-quarter fallback
            before = params.split("trading_date=lt.")[1].split("&")[0]
            prior = sorted((r for r in rows if r["trading_date"] < before),
                           key=lambda r: r["trading_date"], reverse=True)
            return prior[:10]
        lo = params.split("trading_date=gte.")[1].split("&")[0]
        hi = params.split("trading_date=lte.")[1].split("&")[0]
        return sorted((r for r in rows if lo <= r["trading_date"] <= hi), key=lambda r: r["trading_date"])
    q.supabase_read = fake_read
    return q.generate_quarterly(now=now)


def check(name, result, **expected):
    cq = result["current_quarter"]
    got = {k: cq[k] for k in expected}
    ok = all(abs(got[k] - v) <= 0.05 if isinstance(v, float) else got[k] == v for k, v in expected.items())
    ok = ok and cq["trading_days_elapsed"] + cq["trading_days_remaining"] == cq["trading_days_total"]
    print(("PASS " if ok else "FAIL ") + name, got)
    if not ok:
        FAILED.append(name)


def upto(iso):
    return [r for r in Q2_ROWS if r["trading_date"] <= iso]


R29 = {"trading_date": "2026-09-29", "total_rev_cr": 13.87, "source": "mcx_relay_eod"}
BASE = upto("2026-09-28")

# ── The reported case and the times of day around the end-of-day row ─────────
check("29 Sep 09:10 live: today counted as remaining", run(datetime(2026, 9, 29, 9, 10), BASE),
      trading_days_elapsed=64, trading_days_remaining=2, trading_days_total=66, today_status="live",
      today_in_remaining=True, revenue_projected_cr=747.74, pat_projected_cr=456.7)
check("28 Sep 14:14 live (rows to 25 Sep)", run(datetime(2026, 9, 28, 14, 14), upto("2026-09-25")),
      trading_days_elapsed=63, trading_days_remaining=3, revenue_projected_cr=746.33, pat_projected_cr=455.6)
check("before the open", run(datetime(2026, 9, 29, 8, 0), BASE),
      trading_days_elapsed=64, trading_days_remaining=2, today_status="pre_open")
check("closed, end-of-day row not written yet", run(datetime(2026, 9, 29, 23, 35), BASE),
      trading_days_elapsed=64, trading_days_remaining=2, today_status="closed_awaiting_eod")
check("end-of-day row landed", run(datetime(2026, 9, 29, 23, 44), BASE + [R29]),
      trading_days_elapsed=65, trading_days_remaining=1, today_status="final", today_in_remaining=False,
      rows_through="2026-09-29")
check("partial historical row mid-session is not a finished day",
      run(datetime(2026, 9, 29, 15, 0), BASE + [dict(R29, source="mcx_historical", total_rev_cr=4.1)]),
      trading_days_elapsed=64, trading_days_remaining=2, revenue_actual_cr=720.02, rows_through="2026-09-28")
check("row from a non-final source after the close is not a finished day",
      run(datetime(2026, 9, 29, 23, 50), BASE + [dict(R29, source="bhav_mcxpy")]),
      trading_days_elapsed=64, trading_days_remaining=2)

# ── Quarter boundaries and gaps ───────────────────────────────────────────────
check("last day of the quarter keeps today", run(datetime(2026, 9, 30, 14, 0), BASE + [R29]),
      trading_days_elapsed=65, trading_days_remaining=1, trading_days_total=66, today_in_remaining=True)
no_24 = [r for r in BASE if r["trading_date"] != "2026-09-24"]
check("missing past row is estimated, not dropped", run(datetime(2026, 9, 29, 10, 0), no_24),
      trading_days_elapsed=63, trading_days_remaining=3, trading_days_total=66, missing_dates=["2026-09-24"])
r = run(datetime(2026, 10, 1, 10, 0), BASE + [R29])
prior10 = sorted(BASE + [R29], key=lambda x: x["trading_date"])[-10:]
check("first day of Q3 uses the last 10 sessions of Q2", r,
      trading_days_elapsed=0, trading_days_remaining=64, trading_days_total=64,
      revenue_ma10_cr=round(sum(x["total_rev_cr"] for x in prior10) / 10, 2))
if any("placeholder" in e for e in r["errors"]):
    print("FAIL first day of Q3 fell back to the placeholder"); FAILED.append("q3 placeholder")
r = run(datetime(2026, 10, 1, 10, 0), [])
if not any("placeholder" in e for e in r["errors"]) or r["current_quarter"]["revenue_projected_cr"] != 12.0 * 64:
    print("FAIL no history at all should flag the placeholder"); FAILED.append("no history")
else:
    print("PASS no history at all flags the placeholder", r["errors"])
O1 = {"trading_date": "2026-10-01", "total_rev_cr": 14.0, "source": "mcx_relay_eod"}
check("full holiday (2 Oct)", run(datetime(2026, 10, 2, 12, 0), [O1]),
      trading_days_elapsed=1, trading_days_remaining=63, today_status="no_session", today_in_remaining=False)
check("weekend (3 Oct)", run(datetime(2026, 10, 3, 12, 0), [O1]),
      trading_days_elapsed=1, trading_days_remaining=63, today_status="no_session")
BUDGET_SUN = {"trading_date": "2026-02-01", "total_rev_cr": 9.5, "source": "mcx_historical"}
r = run(datetime(2026, 2, 2, 10, 0), [BUDGET_SUN])   # Q4 FY26 has 63 calendar sessions
check("Sunday budget session counts as a finished day on top of the calendar", r,
      trading_days_elapsed=1, trading_days_remaining=63, trading_days_total=64)
if len(r["current_quarter"]["missing_dates"]) != 21:   # January's sessions have no rows here
    print("FAIL January sessions should be listed as missing"); FAILED.append("jan missing")

# ── The close time comes from lib.mcx_config (23:55 in US winter once seasonal) ─
day = date(2026, 11, 16)
close = q.session_end(day)
row = {"trading_date": "2026-11-16", "total_rev_cr": 15.0, "source": "mcx_relay_eod"}
hm = lambda m: datetime(2026, 11, 16, m // 60, m % 60)
check(f"row before the close ({close // 60:02d}:{close % 60:02d}) is not final", run(hm(close - 5), [row]),
      today_status="live", today_in_remaining=True)
check("row after the close is final", run(hm(close + 1), [row]),
      today_status="final", today_in_remaining=False)

print()
if FAILED:
    print(f"{len(FAILED)} FAILED: {FAILED}")
    sys.exit(1)
print("All quarterly day-count tests passed.")
