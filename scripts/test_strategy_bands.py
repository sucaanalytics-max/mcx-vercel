#!/usr/bin/env python3
"""Offline tests for the ADR band tracker: lib/strategy_bands.py and /api/models?view=bands.
The study behind it lives in the private tusk-strategy-research repo. No network.
Run:  python3 scripts/test_strategy_bands.py"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np
import pandas as pd
from lib import strategy_bands as sb
from lib import supabase as sbase

failed = 0


def check(name, ok, detail=""):
    global failed
    print(("PASS " if ok else "FAIL ") + name + ("" if ok else f": {detail}"))
    if not ok:
        failed += 1


# 1. The line on day t is a plain OLS on the days before t (reference: numpy polyfit, row by row)
rng = np.random.default_rng(7)
ridx = pd.bdate_range("2022-01-03", periods=400)
rev = pd.Series(np.exp(np.cumsum(rng.normal(0.002, 0.15, 400))) * 3, index=ridx)
a45 = rev.rolling(45).mean()
eps = np.zeros(400)
for i in range(1, 400):
    eps[i] = 0.95 * eps[i - 1] + rng.normal(0, 20)
close = pd.Series(200 + 90 * a45.bfill().values + eps, index=ridx)
pidx = ridx.delete([50, 51, 200])                                   # price missing on 3 revenue days (ffill)
px_rows = [(d.strftime("%Y-%m-%d"), float(close[d])) for d in pidx]
rows = sb.compute([(d.strftime("%Y-%m-%d"), float(v)) for d, v in rev.items()], px_rows)
ref_close = close.reindex(pidx).reindex(ridx, method="ffill")
pairs = pd.DataFrame({"x": a45, "y": ref_close}).dropna()
ref_z = {}
for i in range(sb.MIN_OBS, len(pairs)):
    prior = pairs.iloc[:i]
    b, a = np.polyfit(prior.x, prior.y, 1)
    s = np.sqrt(((prior.y - (a + b * prior.x)) ** 2).sum() / (len(prior) - 2))
    ref_z[pairs.index[i].strftime("%Y-%m-%d")] = (pairs.y.iloc[i] - (a + b * pairs.x.iloc[i])) / s
check("rows cover every fitted day", len(rows) == len(ref_z), (len(rows), len(ref_z)))
check("z equals the reference OLS on prior days", all(abs(r["z"] - ref_z[r["date"]]) < 1e-3 for r in rows))
ratio_ref = rev.rolling(10).mean() / a45
check("10/45 ratio equals the reference", all(abs(r["ratio"] - ratio_ref[pd.Timestamp(r["date"])]) < 1e-4 for r in rows))
check("bands are line ± 1/2 SD", all(abs(r["hi2"] - r["line"] - 2 * r["sd"]) < 0.2 and abs(r["line"] - r["lo1"] - r["sd"]) < 0.2 for r in rows))

# 2. Look-ahead guard: changing later prices never changes earlier rows
px2 = [(d, p * (3 if d >= "2023-01-01" else 1)) for d, p in px_rows]
rows2 = sb.compute([(d.strftime("%Y-%m-%d"), float(v)) for d, v in rev.items()], px2)
early = [(r, r2) for r, r2 in zip(rows, rows2) if r["date"] < "2023-01-01"]
check("rows before a price change are unchanged", early and all(abs(r["z"] - r2["z"]) < 1e-12 for r, r2 in early))

# 3. States, as-of closes and flags
check("states", [sb.state(-1.2, 1.05), sb.state(1.3, 0.97), sb.state(-2.4, 0.9), sb.state(2.1, 1.1), sb.state(0.3, 1.0)]
      == ["TURNING UP", "TURNING DOWN", "AT LOWER", "AT UPPER", "IN BAND"])
check("closes are as-of (last price on or before the day)", sb.align_close(["2022-03-15", "2022-01-01"], [("2022-03-14", 5.0)]) == [5.0, None])
seq = [("IN BAND", 0), ("TURNING UP", -1.2), ("TURNING UP", -1.5), ("IN BAND", -0.5), ("TURNING UP", -1.1), ("AT LOWER", -2.2), ("IN BAND", 0)]
fl = sb.flags([{"date": f"d{i}", "state": st, "z": z} for i, (st, z) in enumerate(seq)])
check("flags: latest episode start, sessions ago, count",
      (fl["turning_up"]["date"], fl["turning_up"]["sessions_ago"], fl["turning_up"]["episodes"],
       fl["lower_band"]["sessions_ago"], fl["upper_band"]) == ("d4", 2, 2, 1, None), fl)

# 4. The BSE / Angel One key in .env.local is actually loaded (the loader takes only allow-listed names)
check(".env.local loader takes the NSE-BSE URL and key",
      {"NSEBSE_SUPABASE_URL", "NSEBSE_SUPABASE_KEY"} <= set(sbase._PROJECT_ENV_KEYS))

# 5. Angel One on the tracker: four segments summed per day, days missing one dropped, latest run used
import api.models as am
calls = []


def fake_read(table, params="", **kw):
    calls.append((table, params))
    if table == "sb_runs":
        return [{"run_id": "RUN1"}]
    if table == "sb_daily_revenue":
        seg = params.split("segment=eq.")[1].split("&")[0]
        out = [{"trade_date": "2026-01-02", "segment": seg, "revenue_cr": 1.0},
               {"trade_date": "2026-01-05", "segment": seg, "revenue_cr": 2.0}]
        return out if seg != "mtf_net" else out[:1]                 # mtf missing on the 5th
    return [{"trade_date": "2026-01-02", "close": 300.0}]


orig = (sbase.nsebse_read_all, sbase.NSEBSE_SUPABASE_KEY)
sbase.nsebse_read_all, sbase.NSEBSE_SUPABASE_KEY = fake_read, "test-key"
try:
    rev_rows, price_rows, why = am._angel_band_inputs()
finally:
    sbase.nsebse_read_all, sbase.NSEBSE_SUPABASE_KEY = orig
check("Angel One: four segments summed, incomplete day dropped", (rev_rows, why) == ([("2026-01-02", 4.0)], None), rev_rows)
check("Angel One: reads the latest run only", all("run_id=eq.RUN1" in p for t, p in calls if t == "sb_daily_revenue"))
check("Angel One: price read starts at the first revenue day", any("trade_date=gte.2026-01-02" in p for t, p in calls if t == "sb_share_prices"))

print(f"\n{'ALL PASS' if not failed else str(failed) + ' FAILED'}")
sys.exit(1 if failed else 0)
