"""
Figures for the Today page (/api/exchange_dashboard?view=home).

- Completed trading days only. A day counts if it is before today and has a
  row, or it is today and its row is final (FINAL_ROW_SOURCES) after the close.
  So the live day is never in an average.
- Six averages of revenue per trading day: the last 5, 10, 20 and 45 days, the
  quarter so far and the financial year so far, each with what it is compared
  against.
- How far past live projections landed from the final figure, by time of day,
  measured from the projections users actually saw (mcx_snapshots.proj_total_rev).
  error = projection / final - 1. With q10 and q90 the 10th and 90th percentiles
  of the signed error at that time, the final lay in [P / (1 + q90), P / (1 + q10)]
  on 8 past days in 10. Signed percentiles matter: at midday the projection has
  mostly run high, so mirroring the size of those misses onto the upside (as a
  |error| percentile does) overstates how far above the projection the final goes.
"""
from datetime import date, datetime, timedelta

from lib.mcx_config import (
    supabase_read_all, now_ist, is_trading_day,
    SESSION_START, MCX_MORNING_CLOSE, MCX_EVENING_CLOSE,
)
try:
    from lib.mcx_config import session_end      # seasonal close: 23:55 in US winter
except ImportError:                             # older lib: fixed 23:30 close
    from lib.mcx_config import SESSION_END

    def session_end(d=None):
        return SESSION_END

FINAL_ROW_SOURCES = {"mcx_relay_eod", "mcx_historical"}
THIN_SESSIONS = MCX_MORNING_CLOSE | MCX_EVENING_CLOSE    # part-day sessions: not typical days
# US market holidays (NYSE). MCX's evening session is thin when US markets are shut, so the
# projection runs high on these days; they are known in advance, so they are left out of the
# error measurement and flagged on the day. Add each year's dates when NYSE publishes them.
US_MARKET_HOLIDAYS = {
    "2025-01-01", "2025-01-09", "2025-01-20", "2025-02-17", "2025-04-18", "2025-05-26", "2025-06-19",
    "2025-07-04", "2025-09-01", "2025-11-27", "2025-12-25",
    "2026-01-01", "2026-01-19", "2026-02-16", "2026-04-03", "2026-05-25", "2026-06-19", "2026-07-03",
    "2026-09-07", "2026-11-26", "2026-12-25",
    "2027-01-01", "2027-01-18", "2027-02-15", "2027-03-26", "2027-05-31", "2027-06-18", "2027-07-05",
    "2027-09-06", "2027-11-25", "2027-12-24",
}
DAILY_KEEP = 300            # completed days returned: a year of bars plus a 45-day average's lead-in
ACCURACY_SESSIONS = 90      # past normal sessions used to measure projection error
ACCURACY_GRID = list(range(30, 841, 30))    # minutes after 09:00: 09:30 to 23:00
ACCURACY_WINDOW = 20        # use the last snapshot in the 20 minutes up to each grid time
ACCURACY_MIN_N = 20         # fewer days than this at a time: no figure


# ─── Calendar helpers ──────────────────────────────────────────────────────

def fy_label(d):
    """MCX's year runs April to March: 29 Sep 2026 is in FY27."""
    return f"FY{(d.year + 1 if d.month >= 4 else d.year) % 100:02d}"


def fy_start(d):
    return date(d.year if d.month >= 4 else d.year - 1, 4, 1)


def quarter_start(d):
    start = fy_start(d)
    q = ((d.month - 4) % 12) // 3
    m = start.month - 1 + 3 * q
    return date(start.year + m // 12, m % 12 + 1, 1)


def quarter_label(d):
    return f"Q{((d.month - 4) % 12) // 3 + 1} {fy_label(d)}"


def add_months(d, k):
    m = d.month - 1 + k
    return date(d.year + m // 12, m % 12 + 1, 1)


def minutes_to_clock(m):
    t = SESSION_START + m
    return f"{t // 60:02d}:{t % 60:02d}"


# ─── Rows ──────────────────────────────────────────────────────────────────

def completed_days(rows, now):
    """Rows for completed trading days, oldest first: {date, fut, opt, total}."""
    today = now.date()
    now_min = now.hour * 60 + now.minute
    out = []
    for r in rows:
        try:
            d = date.fromisoformat(r["trading_date"])
        except (KeyError, TypeError, ValueError):
            continue
        fut = r.get("fut_rev_cr") or 0
        opt = r.get("opt_rev_cr") or 0
        if fut + opt <= 0 or d > today:
            continue
        if d == today and not (r.get("source") in FINAL_ROW_SOURCES and now_min >= session_end(today)):
            continue
        out.append({"date": d, "fut": fut, "opt": opt, "total": fut + opt})
    out.sort(key=lambda x: x["date"])
    return out


def _mean(rows):
    return sum(r["total"] for r in rows) / len(rows) if rows else None


def _pct(cur, prev):
    return (cur / prev - 1) * 100 if cur is not None and prev else None


def _window(rows):
    if not rows:
        return {"value": None, "n": 0, "first": None, "last": None}
    return {"value": round(_mean(rows), 4), "n": len(rows),
            "first": rows[0]["date"].isoformat(), "last": rows[-1]["date"].isoformat()}


def averages(days, today):
    """Six windows, shortest first. Each compares with the window just before it
    (N days), the whole previous quarter, or the whole previous year."""
    out = []
    for n in (5, 10, 20, 45):
        cur, prev = days[-n:], days[-2 * n:-n]
        w = {"key": f"d{n}", "label": f"{n}-day average", **(_window(cur) if len(cur) == n else _window([]))}
        w["prev"] = _window(prev) if len(prev) == n else None
        w["chg_pct"] = _pct(w["value"], w["prev"]["value"]) if w["prev"] else None
        out.append(w)

    qs = quarter_start(today)
    pqs = add_months(qs, -3)
    cq = [r for r in days if r["date"] >= qs]
    pq = [r for r in days if pqs <= r["date"] < qs]
    w = {"key": "qtd", "label": f"{quarter_label(today)} so far", "period": quarter_label(today), **_window(cq)}
    w["prev"] = {**_window(pq), "label": quarter_label(pqs)} if pq else None
    w["chg_pct"] = _pct(w["value"], w["prev"]["value"]) if w["prev"] else None
    out.append(w)

    ys = fy_start(today)
    pys = ys.replace(year=ys.year - 1)
    cy = [r for r in days if r["date"] >= ys]
    py = [r for r in days if pys <= r["date"] < ys]
    w = {"key": "fytd", "label": f"{fy_label(today)} so far", "period": fy_label(today), **_window(cy)}
    w["prev"] = {**_window(py), "label": fy_label(pys)} if py else None
    w["chg_pct"] = _pct(w["value"], w["prev"]["value"]) if w["prev"] else None
    # The same stretch of the previous year: from its 1 April to the same day and month
    if cy and py:
        last = cy[-1]["date"]
        try:
            same_end = last.replace(year=last.year - 1)
        except ValueError:                      # 29 Feb
            same_end = last.replace(year=last.year - 1, day=28)
        same = [r for r in py if r["date"] <= same_end]
        w["same_stretch"] = {**_window(same), "label": fy_label(pys)} if same else None
        w["same_stretch_chg_pct"] = _pct(w["value"], w["same_stretch"]["value"]) if same else None
    out.append(w)
    return out


def missing_sessions(days, today, lookback=60):
    """Scheduled trading days in the last `lookback` completed days with no row."""
    if not days:
        return []
    have = {r["date"] for r in days}
    start = days[-lookback:][0]["date"]
    out, d = [], start
    while d < today:
        if is_trading_day(d) and d not in have:
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


# ─── Projection accuracy ───────────────────────────────────────────────────

def _quantile(xs, p):
    xs = sorted(xs)
    k = (len(xs) - 1) * p
    f = int(k)
    c = min(f + 1, len(xs) - 1)
    return xs[f] + (xs[c] - xs[f]) * (k - f)


def projection_accuracy(snaps, days, today):
    """Error of past live projections at each time of day, from stored snapshots.

    snaps: rows with trading_date, elapsed_min, proj_total_rev.
    days: completed days (the finals). Part-day sessions are left out.
    """
    finals = {r["date"].isoformat(): r["total"] for r in days}
    by_day = {}
    skip = THIN_SESSIONS | US_MARKET_HOLIDAYS
    for s in snaps:
        p, m, d = s.get("proj_total_rev"), s.get("elapsed_min"), s.get("trading_date")
        if not p or m is None or d not in finals or d >= today.isoformat():
            continue
        by_day.setdefault(d, []).append((m, p))
    all_days = sorted(by_day)
    use = [d for d in all_days if d not in skip][-ACCURACY_SESSIONS:]
    thin = [d for d in all_days if d in THIN_SESSIONS and use and d >= use[0]]
    us = [d for d in all_days if d in US_MARKET_HOLIDAYS and d not in THIN_SESSIONS and use and d >= use[0]]
    for d in use:
        by_day[d].sort()

    grid = []
    for m in ACCURACY_GRID:
        errs = []
        for d in use:
            inside = [p for (em, p) in by_day[d] if m - ACCURACY_WINDOW <= em <= m]
            if inside:
                errs.append(inside[-1] / finals[d] - 1)
        row = {"min": m, "time": minutes_to_clock(m), "n": len(errs),
               "q10_pct": None, "median_pct": None, "q90_pct": None, "p90_abs_pct": None}
        if len(errs) >= ACCURACY_MIN_N:
            row["q10_pct"] = round(_quantile(errs, 0.1) * 100, 2)
            row["median_pct"] = round(_quantile(errs, 0.5) * 100, 2)
            row["q90_pct"] = round(_quantile(errs, 0.9) * 100, 2)
            row["p90_abs_pct"] = round(_quantile([abs(e) for e in errs], 0.9) * 100, 2)
        grid.append(row)
    return {
        "sessions": len(use),
        "first": use[0] if use else None,
        "last": use[-1] if use else None,
        "excluded_part_day": len(thin),
        "excluded_us_holiday": len(us),
        "window_min": ACCURACY_WINDOW,
        "grid": grid,
    }


# ─── Payload ───────────────────────────────────────────────────────────────

def generate_home(now=None, rows=None, snaps=None):
    now = now or now_ist()
    today = now.date()
    if rows is None:
        rows = supabase_read_all(
            "mcx_daily_revenue",
            "?select=trading_date,fut_rev_cr,opt_rev_cr,total_rev_cr,source&order=trading_date.asc",
            max_rows=5000)
    if snaps is None:
        since = (today - timedelta(days=150)).isoformat()
        snaps = supabase_read_all(
            "mcx_snapshots",
            f"?select=trading_date,elapsed_min,proj_total_rev&proj_total_rev=not.is.null"
            f"&trading_date=gte.{since}&trading_date=lt.{today.isoformat()}"
            f"&order=trading_date.asc,elapsed_min.asc",
            max_rows=15000)
    days = completed_days(rows, now)
    if not days:
        return {"success": False, "error": "No completed trading days"}
    last45 = days[-45:]
    return {
        "success": True,
        "as_of": now.strftime("%Y-%m-%d %H:%M IST"),
        "today": today.isoformat(),
        "today_is_session": is_trading_day(today),
        "today_us_holiday": today.isoformat() in US_MARKET_HOLIDAYS,
        "today_final": days[-1]["date"] == today,
        "sessions_through": days[-1]["date"].isoformat(),
        "ma45": round(_mean(last45), 4),
        "ma45_window": _window(last45),
        "daily": [{"date": r["date"].isoformat(), "fut": round(r["fut"], 4), "opt": round(r["opt"], 4),
                   "total": round(r["total"], 4)} for r in days[-DAILY_KEEP:]],
        "averages": averages(days, today),
        "missing_sessions": missing_sessions(days, today),
        "accuracy": projection_accuracy(snaps, days, today),
    }
