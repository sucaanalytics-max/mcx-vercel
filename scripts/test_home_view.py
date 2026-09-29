#!/usr/bin/env python3
"""Offline tests for lib/home_view.py (Today page figures). No network.
Run:  python3 scripts/test_home_view.py"""
import os
import sys
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lib import home_view as hv
from lib.mcx_config import is_trading_day

failed = 0


def check(name, got, want):
    global failed
    ok = got == want
    print(("PASS " if ok else "FAIL ") + name + ("" if ok else f": got {got!r}, want {want!r}"))
    if not ok:
        failed += 1


def close(a, b, tol=1e-6):
    return a is not None and b is not None and abs(a - b) < tol


def rows_for(dates, total=10.0, source="mcx_relay_eod"):
    """One row per date with futures 20% and options 80% of `total` (a number or a function of the date)."""
    out = []
    for d in dates:
        t = total(d) if callable(total) else total
        out.append({"trading_date": d.isoformat(), "fut_rev_cr": 0.2 * t, "opt_rev_cr": 0.8 * t,
                    "total_rev_cr": t, "source": source})
    return out


def sessions(start, end):
    d, out = start, []
    while d <= end:
        if is_trading_day(d):
            out.append(d)
        d += timedelta(days=1)
    return out


# ── Calendar helpers ────────────────────────────────────────────────────────
check("FY label, September", hv.fy_label(date(2026, 9, 29)), "FY27")
check("FY label, March", hv.fy_label(date(2027, 3, 31)), "FY27")
check("FY label, April", hv.fy_label(date(2027, 4, 1)), "FY28")
check("quarter labels", [hv.quarter_label(date(2026, m, 15)) for m in (4, 7, 10, 1)], ["Q1 FY27", "Q2 FY27", "Q3 FY27", "Q4 FY26"])
check("quarter start in Q4", hv.quarter_start(date(2027, 2, 10)), date(2027, 1, 1))
check("clock labels", [hv.minutes_to_clock(m) for m in (30, 342, 840)], ["09:30", "14:42", "23:00"])

# ── Completion rule ─────────────────────────────────────────────────────────
TODAY = date(2026, 9, 29)
past = rows_for([date(2026, 9, 25), date(2026, 9, 28)])
today_final = rows_for([TODAY], total=12.0)
today_other = rows_for([TODAY], total=12.0, source="excel_daily_data")
future = rows_for([date(2026, 9, 30)])
zero = [{"trading_date": "2026-09-24", "fut_rev_cr": 0, "opt_rev_cr": 0, "source": "mcx_relay_eod"}]
dates = lambda rs: [r["date"].isoformat() for r in rs]
mid = datetime(2026, 9, 29, 14, 42)
late = datetime(2026, 9, 29, 23, 50)
check("mid-session: today's row is not a completed day", dates(hv.completed_days(past + today_final + future + zero, mid)), ["2026-09-25", "2026-09-28"])
check("after the close: today's final row counts", dates(hv.completed_days(past + today_final, late))[-1], "2026-09-29")
check("after the close: a non-final source for today does not count", dates(hv.completed_days(past + today_other, late))[-1], "2026-09-28")
check("rows out of order are sorted", dates(hv.completed_days(list(reversed(past)), mid)), ["2026-09-25", "2026-09-28"])

# ── Averages ────────────────────────────────────────────────────────────────
# FY26 at 8/day, FY27 Q1 at 10/day, FY27 Q2 at 12/day, with the last 10 days at 20 and 30.
fy26 = sessions(date(2025, 4, 1), date(2026, 3, 31))
q1 = sessions(date(2026, 4, 1), date(2026, 6, 30))
q2 = sessions(date(2026, 7, 1), date(2026, 9, 28))
last10 = q2[-10:]
val = lambda d: 30.0 if d in last10[5:] else (20.0 if d in last10[:5] else (12.0 if d >= date(2026, 7, 1) else (10.0 if d >= date(2026, 4, 1) else 8.0)))
rows = rows_for(fy26 + q1 + q2, total=val)
days = hv.completed_days(rows, mid)
avg = {a["key"]: a for a in hv.averages(days, TODAY)}
check("5-day average and the 5 days before", (avg["d5"]["value"], avg["d5"]["prev"]["value"], round(avg["d5"]["chg_pct"], 6)), (30.0, 20.0, 50.0))
check("5-day window dates", (avg["d5"]["first"], avg["d5"]["last"]), (last10[5].isoformat(), "2026-09-28"))
check("10-day average", avg["d10"]["value"], 25.0)
check("quarter so far vs the whole previous quarter",
      (avg["qtd"]["label"], avg["qtd"]["n"], avg["qtd"]["prev"]["label"], avg["qtd"]["prev"]["value"], avg["qtd"]["prev"]["n"]),
      ("Q2 FY27 so far", len(q2), "Q1 FY27", 10.0, len(q1)))
check("quarter value (4 dp)", close(avg["qtd"]["value"], (12.0 * (len(q2) - 10) + 20 * 5 + 30 * 5) / len(q2), 1e-4), True)
check("year so far vs the whole previous year", (avg["fytd"]["label"], avg["fytd"]["prev"]["label"], avg["fytd"]["prev"]["value"], avg["fytd"]["prev"]["n"]),
      ("FY27 so far", "FY26", 8.0, len(fy26)))
same = avg["fytd"]["same_stretch"]
check("same stretch of the previous year ends on the same day and month", (same["first"] <= "2025-04-01", same["last"] <= "2025-09-28", same["value"]), (True, True, 8.0))

# First day of a quarter, before the close: the quarter so far is empty, not zero
oct1 = datetime(2026, 10, 1, 11, 0)
avg_oct = {a["key"]: a for a in hv.averages(hv.completed_days(rows, oct1), oct1.date())}
check("empty quarter so far on its first day", (avg_oct["qtd"]["label"], avg_oct["qtd"]["value"], avg_oct["qtd"]["n"], avg_oct["qtd"]["chg_pct"]),
      ("Q3 FY27 so far", None, 0, None))
check("the finished quarter becomes the comparison", (avg_oct["qtd"]["prev"]["label"], avg_oct["qtd"]["prev"]["n"]), ("Q2 FY27", len(q2)))

# Too little history: windows are empty rather than short
few = hv.completed_days(rows_for(q2[-7:]), mid)
avg_few = {a["key"]: a for a in hv.averages(few, TODAY)}
check("a 10-day window with 7 days is empty", (avg_few["d10"]["value"], avg_few["d10"]["n"]), (None, 0))
check("a 5-day window with no earlier 5 has no comparison", (avg_few["d5"]["prev"], avg_few["d5"]["chg_pct"]), (None, None))

# ── Missing sessions ───────────────────────────────────────────────────────
gap_rows = rows_for([d for d in q2 if d != date(2026, 9, 23)])
check("a weekday without a row is reported", hv.missing_sessions(hv.completed_days(gap_rows, mid), TODAY), ["2026-09-23"])
check("holidays and weekends are not missing", hv.missing_sessions(hv.completed_days(rows_for(q2), mid), TODAY), [])

# ── Projection accuracy ────────────────────────────────────────────────────
# 30 normal days (no part-day sessions): at 14:00 (m=300) the projection is final × (1 + e), e = −0.14 … +0.15.
acc_days = [d for d in q2 if d.isoformat() not in hv.THIN_SESSIONS][-31:-1]
finals = hv.completed_days(rows_for(acc_days, total=10.0), mid)
errs = [(-14 + i) / 100 for i in range(30)]
snaps = []
for d, e in zip(acc_days, errs):
    snaps.append({"trading_date": d.isoformat(), "elapsed_min": 285, "proj_total_rev": 99.0})           # older snapshot: ignored
    snaps.append({"trading_date": d.isoformat(), "elapsed_min": 295, "proj_total_rev": 10.0 * (1 + e)})
thin_day = sorted(hv.THIN_SESSIONS)[0]
snaps.append({"trading_date": thin_day, "elapsed_min": 295, "proj_total_rev": 50.0})
acc = hv.projection_accuracy(snaps, finals, TODAY)
at = {g["min"]: g for g in acc["grid"]}
abs_errs = sorted(abs(e) for e in errs)
want_p90 = round(hv._quantile(abs_errs, 0.9) * 100, 2)
check("p90 of |error| uses the last snapshot in the window", (at[300]["n"], at[300]["p90_abs_pct"]), (30, want_p90))
check("median signed error", at[300]["median_pct"], round(hv._quantile(errs, 0.5) * 100, 2))
check("9 in 10 past days fall inside [P/(1+p), P/(1−p)]",
      sum(1 for e in errs if abs(e) <= want_p90 / 100 + 1e-9) / len(errs) >= 0.9, True)
check("a time with too few days has no figure", (at[330]["n"], at[330]["p90_abs_pct"]), (0, None))
check("sessions counted and window dates", (acc["sessions"], acc["first"], acc["last"]), (30, acc_days[0].isoformat(), acc_days[-1].isoformat()))
check("part-day sessions are not measured", all(g["n"] <= 30 for g in acc["grid"]), True)

# ── Whole payload from fixtures ────────────────────────────────────────────
home = hv.generate_home(now=mid, rows=rows, snaps=snaps)
check("payload basics", (home["success"], home["today"], home["today_final"], home["sessions_through"], len(home["averages"])),
      (True, "2026-09-29", False, "2026-09-28", 6))
check("45-day average is the d45 window", home["ma45"], [a for a in home["averages"] if a["key"] == "d45"][0]["value"])
check("daily strip keeps about a year", (len(home["daily"]), home["daily"][-1]["date"]), (hv.DAILY_KEEP, "2026-09-28"))

print(f"\n{failed} FAILED" if failed else "\nAll home view tests passed.")
sys.exit(1 if failed else 0)
