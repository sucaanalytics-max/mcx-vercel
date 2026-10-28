#!/usr/bin/env python3
"""Offline tests for the quarterly revenue basis: F&O sums per quarter, the non-F&O share
guard, and the walk-forward backtest (api/quarterly.py). No network.
Run:  python3 scripts/test_quarterly_basis.py"""
import importlib.util
import os
import sys
from datetime import date, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
spec = importlib.util.spec_from_file_location("quarterly", os.path.join(ROOT, "api", "quarterly.py"))
q = importlib.util.module_from_spec(spec)
spec.loader.exec_module(q)

failed = 0


def check(name, got, want):
    global failed
    ok = got == want
    print(("PASS " if ok else "FAIL ") + name + ("" if ok else f": got {got!r}, want {want!r}"))
    if not ok:
        failed += 1


def daily(start, end, per_day, skip=0):
    """Daily rows on trading days between start and end, dropping the last `skip` sessions."""
    d, rows = date.fromisoformat(start), []
    while d <= date.fromisoformat(end):
        if q._is_trading_day(d):
            rows.append({"trading_date": d.isoformat(), "total_rev_cr": per_day})
        d += timedelta(days=1)
    return rows[:len(rows) - skip] if skip else rows


# Six synthetic quarters in 2026 (holiday calendar known), F&O ₹10/session, reported revenue +10%
QS = []
for i, (s, e) in enumerate([("2025-04-01", "2025-06-30"), ("2025-07-01", "2025-09-30"), ("2025-10-01", "2025-12-31"),
                            ("2026-01-01", "2026-03-31"), ("2026-04-01", "2026-06-30"), ("2026-07-01", "2026-09-30")]):
    n = len(daily(s, e, 10))
    fo = 10 * n
    QS.append({"quarter": f"Q{i}", "label": "", "fy": "FY", "q_num": (i % 4) + 1, "start": s, "end": e,
               "revenue_cr": round(fo * 1.10), "expenses_cr": round(40 + 0.2 * fo * 1.10), "pat_cr": round((fo * 1.10 - (40 + 0.2 * fo * 1.10)) * 0.8)})
rows = [r for a in QS for r in daily(a["start"], a["end"], 10)]
q.QUARTERLY_ACTUALS = QS

fo = q._fo_by_quarter(rows)
check("F&O summed per quarter", fo["Q5"]["fo_cr"], 10.0 * len(daily("2026-07-01", "2026-09-30", 10)))
check("non-F&O share is (reported − F&O) / F&O", round(fo["Q5"]["share"], 2), 0.10)

# Too few sessions in the daily table: no share
short = [r for a in QS for r in daily(a["start"], a["end"], 10, skip=20 if a["quarter"] == "Q4" else 0)]
check("a quarter missing a third of its sessions has no share", q._fo_by_quarter(short)["Q4"]["share"], None)

# Share above 20% (bad data) or negative (F&O above reported): no share
QS2 = [dict(a) for a in QS]
QS2[3]["revenue_cr"] = round(QS2[3]["revenue_cr"] * 1.3)
QS2[2]["revenue_cr"] = round(QS2[2]["revenue_cr"] * 0.8)
q.QUARTERLY_ACTUALS = QS2
fo2 = q._fo_by_quarter(rows)
check("share above 20% is rejected", fo2["Q3"]["share"], None)
check("negative share is rejected", fo2["Q2"]["share"], None)

# Walk-forward backtest: each quarter uses only earlier quarters
q.QUARTERLY_ACTUALS = QS
bt = q._backtest(q._fo_by_quarter(rows))
check("backtest starts once four earlier quarters exist", [r["quarter"] for r in bt["rows"]], ["Q4", "Q5"])
check("share used is the previous quarter's", round(bt["rows"][0]["share_used"], 2), 0.10)
check("with a stable business, including non-F&O lands close to reported PAT",
      all(abs(r["miss_all_cr"]) < abs(r["miss_fo_cr"]) for r in bt["rows"]), True)
check("F&O only comes in below reported PAT", all(r["miss_fo_cr"] < 0 for r in bt["rows"]), True)
check("summary averages", (bt["miss_fo_avg_cr"] is not None, bt["abs_miss_all_avg_cr"] is not None), (True, True))

# A quarter whose F&O sum exceeds reported revenue is excluded from the backtest with a reason
QS3 = [dict(a) for a in QS]
QS3[5]["revenue_cr"] = round(QS3[5]["revenue_cr"] * 0.85)
q.QUARTERLY_ACTUALS = QS3
bt3 = q._backtest(q._fo_by_quarter(rows))
check("bad-data quarter excluded, with its reason", ([r["quarter"] for r in bt3["rows"]], bt3["excluded"][0]["quarter"]), (["Q4"], "Q5"))

# The PAT model applies the expense floor and the Q4 seasonal addition
check("expense floor of ₹80 Cr", q._pat_model(100, 10, 0.1, 0.2, 1)[0], 80)
check("Q4 adds the seasonal expense", q._pat_model(1000, 50, 0.2, 0.2, 4)[0], 50 + 200 + q.Q4_EXPENSE_ADJ_CR)

print(f"\n{failed} FAILED" if failed else "\nAll quarterly basis tests passed.")
sys.exit(1 if failed else 0)
