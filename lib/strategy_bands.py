"""ADR band tracker: price against the line fitted on the 45-day average daily revenue.

The production copy of the band maths in the private tusk-strategy-research repo
(strategy/bands.py), in pure Python (no numpy/pandas)
so /api/models?view=bands can compute it on request. Same maths, same timing:

- ADR45 / ADR10: rolling means of daily revenue (Muhurat sessions excluded upstream).
- Line: OLS close = a + b * ADR45 fitted on every day BEFORE t (expanding, at least
  MIN_OBS pairs). The fit never sees t's own price or anything later.
- z = (close - line) / residual SD; bands at +-1 and +-2 SD.
- State (the CIO's rules; verdict in tusk-strategy-research docs/2026-11-ADR-band-verdict.md):
  TURNING UP   ADR10 > ADR45 and z <= -1   (the live signal, long only)
  TURNING DOWN ADR10 < ADR45 and z >= +1   (shown; the verdict found it does not work as a short)
  AT LOWER     z <= -2                     (buy-watch)
  AT UPPER     z >= +2                     (below-average returns follow, not falls)
  IN BAND      otherwise
scripts/test_strategy_bands.py checks this module against a plain OLS on prior days.
"""
from __future__ import annotations
import math
from bisect import bisect_right

WINDOW = 45
FAST = 10
MIN_OBS = 120

# Headline results of the pre-registered test, 20 Nov 2026 (out-of-sample, after costs).
# Update with the memo; the page quotes these, it does not recompute them.
VERDICT = {
    # Rerun on point-in-time MCX revenue (21 Nov 2026): the stored Jan 2022 - Aug 2024 MCX series
    # was scaled to each quarter's reported revenue, which is only known later.
    "as_of": "2026-11-21",
    "doc": "tusk-strategy-research (private): docs/2026-11-ADR-band-verdict.md",
    "rules": [
        {"rule": "Line as fair value", "verdict": "Descriptive for exchanges",
         "detail": "R² 0.88 (MCX), 0.88 (BSE), 0.24 (Angel One); MCX price does not revert to the line (half-life about 86 trading days)"},
        {"rule": "Trade at ±2 SD", "verdict": "Not proven",
         "detail": "About 1 trade a year per stock; 8 test trades, chance the edge is real 0.17–0.31"},
        {"rule": "Turning up", "verdict": "Most promising, not for MCX",
         "detail": "52 episodes on 3 stocks, +4.9% above the stock's average 20-day return (t 2.4), carried by BSE and Angel One; "
                   "MCX alone +1.5% (t 0.9). Pooled t is overstated: the three stocks move together"},
        {"rule": "Turning down / top of the band", "verdict": "Exit flag, not a short",
         "detail": "Shorts lost in the rising market; returns afterwards were 4–5% below average (t 2.6–3.8)"},
        {"rule": "Revenue trend (10d > 45d), long only", "verdict": "Passes on MCX",
         "detail": "Test period from Apr 2025, after the revenue fix: Sharpe 2.3, drawdown −20%, chance the edge is real 0.84"},
    ],
}


def rolling_mean(xs, w):
    out, s = [], 0.0
    for i, x in enumerate(xs):
        s += x
        if i >= w:
            s -= xs[i - w]
        out.append(s / w if i >= w - 1 else None)
    return out


def expanding_fit(xs, ys, min_obs=MIN_OBS):
    """[(a, b, s, n) or None] per index, each fitted on the pairs before it (None x/y skipped)."""
    n = sx = sy = sxx = sxy = syy = 0.0
    out = []
    for x, y in zip(xs, ys):
        fit = None
        if n >= min_obs:
            cxx = sxx - sx * sx / n
            if cxx > 0:
                cxy = sxy - sx * sy / n
                cyy = syy - sy * sy / n
                b = cxy / cxx
                a = (sy - b * sx) / n
                ssr = max(cyy - b * cxy, 0.0)
                s = math.sqrt(ssr / (n - 2))
                fit = (a, b, s, int(n))
        out.append(fit)
        if x is not None and y is not None:
            n += 1; sx += x; sy += y; sxx += x * x; sxy += x * y; syy += y * y
    return out


def state(z, ratio):
    if z is None or ratio is None:
        return "NO DATA"
    if ratio > 1 and z <= -1:
        return "TURNING UP"
    if ratio < 1 and z >= 1:
        return "TURNING DOWN"
    if z <= -2:
        return "AT LOWER"
    if z >= 2:
        return "AT UPPER"
    return "IN BAND"


def _asof(rev_dates, price_rows):
    """Index into price_rows of the last close on or before each revenue date (-1: none)."""
    pd_ = [d for d, _ in price_rows]
    return [bisect_right(pd_, d) - 1 for d in rev_dates]


def align_close(rev_dates, price_rows):
    """Last close on or before each revenue date (MCX trades some NSE holidays).
    price_rows: [(iso_date, close)] sorted ascending."""
    return [price_rows[i][1] if i >= 0 else None for i in _asof(rev_dates, price_rows)]


def align_close_dates(rev_dates, price_rows):
    """The date of the close align_close uses for each revenue date (None: no close yet)."""
    return [price_rows[i][0] if i >= 0 else None for i in _asof(rev_dates, price_rows)]


def compute(rev_rows, price_rows, window=WINDOW, fast=FAST, min_obs=MIN_OBS):
    """rev_rows: [(iso_date, revenue_cr)] ascending, positive; price_rows: [(iso_date, close)].
    Returns one dict per revenue date from the first fitted day."""
    dates = [d for d, _ in rev_rows]
    rev = [float(r) for _, r in rev_rows]
    close = align_close(dates, price_rows)
    close_dates = align_close_dates(dates, price_rows)
    slow, quick = rolling_mean(rev, window), rolling_mean(rev, fast)
    fits = expanding_fit(slow, close, min_obs)
    rows = []
    for i, d in enumerate(dates):
        f, x, y = fits[i], slow[i], close[i]
        if f is None or x is None or y is None or f[2] <= 0:
            continue
        a, b, s, n = f
        line = a + b * x
        z = (y - line) / s
        ratio = quick[i] / x if quick[i] is not None and x > 0 else None
        rows.append({"date": d, "close": round(y, 2), "close_date": close_dates[i], "rev_cr": round(rev[i], 3), "adr45": round(x, 3),
                     "adr10": round(quick[i], 3) if quick[i] is not None else None,
                     "ratio": round(ratio, 4) if ratio is not None else None,
                     "line": round(line, 1), "sd": round(s, 1), "z": round(z, 3),
                     "lo2": round(line - 2 * s, 1), "lo1": round(line - s, 1),
                     "hi1": round(line + s, 1), "hi2": round(line + 2 * s, 1),
                     "a": round(a, 3), "b": round(b, 3), "n": n, "state": state(z, ratio)})
    return rows


FLAG_TESTS = (("turning_up", lambda r: r["state"] == "TURNING UP"),
              ("lower_band", lambda r: r["z"] <= -2),
              ("upper_band", lambda r: r["z"] >= 2),
              ("turning_down", lambda r: r["state"] == "TURNING DOWN"))


def episodes(rows, n=5, h=20):
    """The last n flag episode starts (any flag), newest first, each with the change in the
    close h rows later (close to close on revenue days; None until h rows have passed)."""
    out = []
    for key, test in FLAG_TESTS:
        prev = False
        for i, r in enumerate(rows):
            on = test(r)
            if on and not prev:
                j = i + h
                ret = round(rows[j]["close"] / r["close"] - 1, 4) if j < len(rows) and r["close"] else None
                out.append({"flag": key, "date": r["date"], "z": r["z"], "close": r["close"], "ret": ret, "h": h})
            prev = on
    out.sort(key=lambda e: e["date"], reverse=True)
    return out[:n]


def flags(rows):
    """The latest start of each flag (non-overlapping episodes) and how long ago it was."""
    out = {}
    for key, test in FLAG_TESTS:
        last, prev, count = None, False, 0
        for i, r in enumerate(rows):
            on = test(r)
            if on and not prev:
                last, count = i, count + 1
            prev = on
        out[key] = None if last is None else {"date": rows[last]["date"], "sessions_ago": len(rows) - 1 - last,
                                              "active": test(rows[-1]), "episodes": count}
    return out


def summary(rows):
    """Today's reading, with the CIO's weekly (5-session) averages."""
    if not rows:
        return None
    t, wk = rows[-1], rows[-5:]
    avg = lambda k: round(sum(r[k] for r in wk if r[k] is not None) / max(1, sum(r[k] is not None for r in wk)), 3)
    return {**t, "z_week": avg("z"), "ratio_week": avg("ratio"),
            "close_week": avg("close"), "line_week": avg("line")}


def line_at(row, adr):
    """Price the latest line implies at a given 45-day ADR (the input-output view)."""
    return round(row["a"] + row["b"] * adr, 1)
