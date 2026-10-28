#!/usr/bin/env python3
"""Walk-forward accuracy of the quarterly PAT projection on both revenue bases (api/quarterly.py).

For each reported quarter, the expense model, tax rate and non-F&O share come only from the
quarters before it, and the quarter's full F&O daily sum is projected to PAT (= EPS error, since
the share count is fixed). Reads mcx_daily_revenue from Supabase.
Run:  python3 scripts/quarterly_walkforward.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import api.quarterly as q
from lib.mcx_config import supabase_read_all, DILUTED_SHARES_CR

A = q.QUARTERLY_ACTUALS
rows = supabase_read_all(
    "mcx_daily_revenue",
    f"?select=trading_date,total_rev_cr&trading_date=gte.{A[0]['start']}"
    f"&trading_date=lte.{A[-1]['end']}&total_rev_cr=gt.0&order=trading_date.asc", max_rows=3000)
q.BACKTEST_QUARTERS = len(A)
bt = q._backtest(q._fo_by_quarter(rows))

print(f"{'Quarter':8} {'F&O':>6} {'Reptd':>6} {'Share':>6} {'PAT':>5} {'EPS':>6} | {'F&O-only EPS':>13} {'err':>7} | {'Incl. non-F&O':>13} {'err':>7}")
err_fo, err_all = [], []
for r in bt["rows"]:
    eps = lambda p: p / DILUTED_SHARES_CR
    e_fo, e_all = r["pat_fo_cr"] / r["reported_pat_cr"] - 1, r["pat_all_cr"] / r["reported_pat_cr"] - 1
    err_fo.append(e_fo); err_all.append(e_all)
    print(f"{r['quarter']:8} {r['fo_revenue_cr']:6.0f} {r['reported_revenue_cr']:6.0f} {r['share_used']:6.3f} {r['reported_pat_cr']:5.0f} "
          f"{eps(r['reported_pat_cr']):6.2f} | {eps(r['pat_fo_cr']):13.2f} {e_fo:+7.1%} | {eps(r['pat_all_cr']):13.2f} {e_all:+7.1%}")
n = len(err_fo)
print(f"\nn={n}  F&O only: mean abs error {sum(map(abs, err_fo)) / n:.1%}, bias {sum(err_fo) / n:+.1%}"
      f"  |  incl. non-F&O: mean abs error {sum(map(abs, err_all)) / n:.1%}, bias {sum(err_all) / n:+.1%}")
for e in bt["excluded"]:
    print(f"left out {e['quarter']}: {e['reason']}")
