#!/usr/bin/env python3
"""Offline tests for research/strategy/bands.py (ADR band strategy). No network.
Run:  python3 scripts/test_strategy_bands.py"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np
import pandas as pd
from lib import house_model as hm
from research.strategy import bands

failed = 0


def check(name, ok, detail=""):
    global failed
    print(("PASS " if ok else "FAIL ") + name + ("" if ok else f": {detail}"))
    if not ok:
        failed += 1


rng = np.random.default_rng(7)
idx = pd.bdate_range("2023-01-02", periods=600)
x = pd.Series(10 + np.cumsum(rng.normal(0, 0.2, 600)), index=idx)          # random-walk ADR
e = np.zeros(600)
for i in range(1, 600):                                                  # AR(1) residual, phi 0.9
    e[i] = 0.9 * e[i - 1] + rng.normal(0, 30)
y = pd.Series(500 + 120 * x.values + e, index=idx)

# 1. Synthetic cointegrated pair: recovers slope, finds cointegration, sensible half-life
fit = bands.expanding_fit(x, y, min_obs=120)
b_last = fit["b"].iloc[-1]
check("expanding fit recovers slope ~120", abs(b_last - 120) < 6, b_last)
co = bands.cointegration(x, y)
check("cointegration rejected-null at 5% or better", co["reject_at"] in (0.01, 0.05), co)
check("half-life near ln2/ln(1/0.9)=6.6 days", 3 < co["half_life_days"] < 13, co["half_life_days"])

# 2. Look-ahead guard: changing future rows never changes past signals
y2 = y.copy()
y2.iloc[400:] *= 3
x2 = x.copy()
x2.iloc[400:] += 50
fit2 = bands.expanding_fit(x2, y2, min_obs=120)
same = np.allclose(fit["z"].iloc[:400].dropna(), fit2["z"].iloc[:400].dropna())
check("expanding fit: rows < 400 unchanged when rows >= 400 change", same)
r1 = bands.expanding_fit(x, y, min_obs=120, window=250)
r2 = bands.expanding_fit(x2, y2, min_obs=120, window=250)
check("rolling fit: rows < 400 unchanged", np.allclose(r1["z"].iloc[:400].dropna(), r2["z"].iloc[:400].dropna()))
check("row t's fit excludes t itself", fit["n"].iloc[200] == 200, fit["n"].iloc[200])
check("no z before min_obs", fit["z"].iloc[:120].isna().all())

# 3. Matches lib/house_model.fit_regression on the same pairs
pairs = list(zip(x.values[:300], y.values[:300]))
hmf = hm.fit_regression(pairs)
f300 = fit.iloc[300]                       # fit at row 300 uses rows 0..299
check("a matches house_model", abs(f300["a"] - hmf["a"]) < 1e-6, (f300["a"], hmf["a"]))
check("b matches house_model", abs(f300["b"] - hmf["b"]) < 1e-6)
check("s matches house_model", abs(f300["s"] - hmf["s"]) < 1e-6)

# 4. Log fit returns price-space fit
xp = x - x.min() + 5                       # strictly positive for logs
yp = pd.Series(np.exp(2 + 1.5 * np.log(xp.values) + e / 300), index=idx)
lf = bands.expanding_fit(xp, yp, min_obs=120, log=True)
check("log fit recovers elasticity 1.5", abs(lf["b"].iloc[-1] - 1.5) < 0.15, lf["b"].iloc[-1])
check("log fit is in price space", abs(lf["fit"].iloc[-1] / yp.iloc[-1] - 1) < 0.5, lf["fit"].iloc[-1])

# 5. A random walk residual is not cointegrated
yrw = pd.Series(500 + 120 * x.values + np.cumsum(rng.normal(0, 30, 600)), index=idx)
check("random-walk residual not rejected", bands.cointegration(x, yrw)["reject_at"] is None)

# 6. Episode starts and labels
m = pd.Series([False, True, True, False, True, False, True, True])
check("episode starts", bands.episode_starts(m).tolist() == [False, True, False, False, True, False, True, False])
z = pd.Series([-1.5, -1.5, 1.2, 0.0, 1.2])
ratio = pd.Series([1.1, 0.9, 0.9, 1.0, 1.1])
check("turning state", bands.turning_state(z, ratio).tolist() == [1, 0, -1, 0, 0])
check("labels", [bands.band_state(-1.2, 1.05), bands.band_state(1.3, 0.97), bands.band_state(-2.4, 0.9),
                 bands.band_state(2.1, 1.1), bands.band_state(0.3, 1.0)]
      == ["TURNING UP", "TURNING DOWN", "AT LOWER", "AT UPPER", "IN BAND"])

# 7. Rules: time stop does not re-enter the same episode; exit at z crossing 0
from research.strategy import backtest as bt
zz = pd.Series([0, -2.1, -2.5, -2.2, -2.3, -1.0, -2.2, 0.1, 2.4], dtype=float)
r1 = bt.rule_band(zz, 2.0, max_hold=2).tolist()
check("R1 time stop, re-arm after leaving band", r1 == [0, 1, 1, 0, 0, 0, 1, 0, -1], r1)
st = pd.Series([0, 1, 1, 1, 1, 0, -1, -1, 1], dtype=float)
r2 = bt.rule_turning(st, max_hold=2).tolist()
check("R2 time stop, no re-fire, flip on opposite", r2 == [0, 1, 1, 0, 0, 0, -1, -1, 1], r2)

# 8. Simulation timing: signal on day d is held from the next open
pidx = pd.bdate_range("2024-01-01", periods=5)
px = pd.DataFrame({"Open": [100, 110, 120, 130, 140], "Close": [105, 115, 125, 135, 145]}, index=pidx, dtype=float)
tgt = pd.Series([1, 1, 0, 0, 0], index=pidx, dtype=float)
sim = bt.simulate(tgt, px)
check("position starts the session after the signal", sim.pos.tolist() == [0, 1, 1, 0, 0], sim.pos.tolist())
check("entry-day P&L is open-to-close", abs(sim.gross.iloc[1] - (115 / 110 - 1)) < 1e-12)
check("carry-day P&L includes the gap", abs(sim.gross.iloc[2] - (120 / 115 - 1 + 125 / 120 - 1)) < 1e-12)
tr = bt.trades(sim, px)
check("trade return = open to exit open less costs", len(tr) == 1 and abs(tr.ret.iloc[0] - (130 / 110 - 1 - 2 * bt.COST_SIDE)) < 1e-12, tr)

# 9. lib/strategy_bands.py (pure Python, served by /api/models?view=bands) matches the research maths
from lib import strategy_bands as sb
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
ref = bands.expanding_fit(a45, ref_close, min_obs=120)
ref_z = {d.strftime("%Y-%m-%d"): z for d, z in ref["z"].dropna().items()}
check("lib rows cover every fitted research day", len(rows) == len(ref_z), (len(rows), len(ref_z)))
check("lib z equals research z", all(abs(r["z"] - ref_z[r["date"]]) < 1e-3 for r in rows))
ratio_ref = (rev.rolling(10).mean() / a45)
check("lib 10/45 ratio equals research", all(abs(r["ratio"] - ratio_ref[pd.Timestamp(r["date"])]) < 1e-4 for r in rows))
check("lib state labels match research", all(r["state"] == bands.band_state(ref_z[r["date"]], ratio_ref[pd.Timestamp(r["date"])]) for r in rows))
check("bands are line ± 1/2 SD", all(abs(r["hi2"] - r["line"] - 2 * r["sd"]) < 0.2 and abs(r["line"] - r["lo1"] - r["sd"]) < 0.2 for r in rows))
check("closes are as-of (last price on or before the day)", sb.align_close(["2022-03-15", "2022-01-01"], [("2022-03-14", 5.0)]) == [5.0, None])
seq = [("IN BAND", 0), ("TURNING UP", -1.2), ("TURNING UP", -1.5), ("IN BAND", -0.5), ("TURNING UP", -1.1), ("AT LOWER", -2.2), ("IN BAND", 0)]
fl = sb.flags([{"date": f"d{i}", "state": st, "z": z} for i, (st, z) in enumerate(seq)])
check("flags: latest episode start, sessions ago, count",
      (fl["turning_up"]["date"], fl["turning_up"]["sessions_ago"], fl["turning_up"]["episodes"],
       fl["lower_band"]["sessions_ago"], fl["upper_band"]) == ("d4", 2, 2, 1, None), fl)

# 10. The BSE key in .env.local is actually loaded (the loader takes only allow-listed names)
from lib import supabase as sbase
check(".env.local loader takes the NSE-BSE URL and key",
      {"NSEBSE_SUPABASE_URL", "NSEBSE_SUPABASE_KEY"} <= set(sbase._PROJECT_ENV_KEYS))

# 11. Angel One on the tracker: four segments summed per day, days missing one dropped, latest run used
import api.models as am
calls = []
def fake_read(table, params="", **kw):
    calls.append((table, params))
    if table == "sb_runs":
        return [{"run_id": "RUN1"}]
    if table == "sb_daily_revenue":
        seg = params.split("segment=eq.")[1].split("&")[0]
        rows = [{"trade_date": "2026-01-02", "segment": seg, "revenue_cr": 1.0},
                {"trade_date": "2026-01-05", "segment": seg, "revenue_cr": 2.0}]
        return rows if seg != "mtf_net" else rows[:1]          # mtf missing on the 5th
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
