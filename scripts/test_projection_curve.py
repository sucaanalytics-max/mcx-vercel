#!/usr/bin/env python3
"""
Projection model (Nov 2026 fix): the multiplier is 1 / the curve's cumulative
share (no blend toward linear extrapolation, no drift scaling); a season with
no fitted curve borrows the other season's, moved to its hours; the drift
check compares like with like. No network: the curve fitter is stubbed.
"""
import os, sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from lib import mcx_config as m   # noqa: E402

FAILED = []


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}" + (f"  ({detail})" if detail and not cond else ""))
    if not cond:
        FAILED.append(name)


def close(a, b, tol=1e-9):
    return abs(a - b) <= tol


print("[1] multiplier is the curve only")
check("LOW day: 1 / cumulative share", close(m._multiplier_from_curve(0.5, 0.25, "LOW"), 4.0))
check("no linear pull: same share, any time_pct",
      close(m._multiplier_from_curve(0.2, 0.25, "LOW"), m._multiplier_from_curve(0.8, 0.25, "LOW")))
check("HIGH prior fades with time",
      close(m._multiplier_from_curve(0.5, 0.25, "HIGH"), 4.0 * (1 + 0.15 * 0.5)))
check("zero share falls back to linear", close(m._multiplier_from_curve(0.5, 0.0, "LOW"), 2.0))
check("time 0 → 1.0", m._multiplier_from_curve(0.0, 0.3, "LOW") == 1.0)

print("\n[2] season_transfer")
summer = [0.05, 0.08, 0.06, 0.09, 0.17, 0.38, 0.17]
w = m.season_transfer(summer, m.SESSION_END_US_DST, m.SESSION_END_US_STD)
ext = m.WINTER_EXTENSION_SHARE
check("summer → winter sums to 1", close(sum(w), 1.0))
check("first six scaled by 1 - ext", all(close(a, b * (1 - ext)) for a, b in zip(w[:6], summer[:6])))
check("late bucket gains the extension", close(w[6], summer[6] * (1 - ext) + ext))
back = m.season_transfer(w, m.SESSION_END_US_STD, m.SESSION_END_US_DST)
check("winter → summer round-trips", all(close(a, b) for a, b in zip(back, summer)))
check("same season is unchanged", m.season_transfer(summer, 1410, 1410) is summer)
check("an override close gets nothing", m.season_transfer(summer, 1410, 1000) is None)
check("None stays None", m.season_transfer(None, 1410, 1435) is None)

print("\n[3] a season without a fitted curve borrows the other one")
orig = m._compute_adaptive_weights
calls = []


def fake(vol_fn=None, close_min=None):
    calls.append(close_min)
    return list(summer) if close_min == m.SESSION_END_US_DST else None


try:
    m._compute_adaptive_weights = fake
    for c in (m._adaptive_cache, m._adaptive_split_cache):
        c.clear()
    split = m.get_adaptive_bucket_weights_split("2026-11-18")
    comb = m.get_adaptive_bucket_weights("2026-11-18")
    want = m.season_transfer(summer, m.SESSION_END_US_DST, m.SESSION_END_US_STD)
    check("winter split curves = summer curves moved to winter hours",
          split and all(close(a, b) for a, b in zip(split[0], want)) and all(close(a, b) for a, b in zip(split[1], want)))
    check("winter combined curve too", comb and all(close(a, b) for a, b in zip(comb, want)))
    for c in (m._adaptive_cache, m._adaptive_split_cache):
        c.clear()
    calls.clear()
    s2 = m.get_adaptive_bucket_weights_split("2026-10-15")
    check("summer uses its own fit, no transfer", s2 and s2[0] == summer and m.SESSION_END_US_STD not in calls)

    # project_full_day on a winter day follows the transferred curve exactly
    pf, po, _ = m.project_full_day(400.0, 100.0, 448, "LOW", trading_date="2026-11-18")
    share = m.get_intraday_weight_dynamic(448, want, "2026-11-18")
    check("winter projection = realized / transferred share", close(pf, 400.0 / share, 1e-6) and close(po, 100.0 / share, 1e-6))

    print("\n[4] drift check compares like with like")
    # A day that follows the curve exactly: no alerts (the old full-day
    # comparison flagged every completed bucket as 'higher').
    for c in (m._adaptive_cache, m._adaptive_split_cache):
        c.clear()
    d = "2026-10-15"
    snaps = [{"elapsed_min": e, "fut_notl_cr": 1000 * m.get_intraday_weight_dynamic(e, summer, d), "opt_prem_cr": 0}
             for e in range(15, 600, 15)]
    check("on-curve day raises no drift alert", m.check_regime_drift(snaps, trading_date=d) == [])
finally:
    m._compute_adaptive_weights = orig
    for c in (m._adaptive_cache, m._adaptive_split_cache):
        c.clear()

import inspect   # noqa: E402
check("project_full_day takes no today_snapshots",
      "today_snapshots" not in inspect.signature(m.project_full_day).parameters)

print(f"\n{'ALL PASS' if not FAILED else f'{len(FAILED)} FAILED'}")
sys.exit(1 if FAILED else 0)
