"""
/api/quarterly — Quarterly PAT Predictor

Predicts current quarter's PAT by aggregating actual daily revenue,
projecting remaining days, and applying an expense regression model
derived from 8 quarters of MCX actuals.

Returns:
  - Historical quarterly P&L (8 quarters of actuals)
  - Current quarter projection (revenue, expenses, PAT with confidence bands)
  - FY26 full-year projection (3 actual quarters + 1 projected)
  - Expense regression model parameters
"""
from http.server import BaseHTTPRequestHandler
import json
from datetime import date, datetime, timedelta

from lib.mcx_config import (
    SUPABASE_URL, SUPABASE_ANON_KEY, DILUTED_SHARES_CR, SESSION_START,
    MCX_HOLIDAYS_2026, supabase_read, supabase_read_all, now_ist, make_cors_headers,
)
try:
    from lib.mcx_config import session_end      # seasonal close: 23:55 in US winter
except ImportError:                             # older lib: fixed 23:30 close
    from lib.mcx_config import SESSION_END

    def session_end(d=None):
        return SESSION_END

# Sources whose row for a date is the final full-day figure. A row for today
# only counts as a completed session if it comes from one of these and the
# session has closed; anything else could be a partial intraday figure.
FINAL_ROW_SOURCES = {"mcx_relay_eod", "mcx_historical"}

# ─── Quarterly Actuals (from Screener.in, validated) ─────────────────────────
QUARTERLY_ACTUALS = [
    {"quarter": "Q4 FY24", "label": "Mar 2024", "fy": "FY24", "q_num": 4,
     "start": "2024-01-01", "end": "2024-03-31",
     "revenue_cr": 181, "expenses_cr": 79, "pat_cr": 88},
    {"quarter": "Q1 FY25", "label": "Jun 2024", "fy": "FY25", "q_num": 1,
     "start": "2024-04-01", "end": "2024-06-30",
     "revenue_cr": 234, "expenses_cr": 102, "pat_cr": 111},
    {"quarter": "Q2 FY25", "label": "Sep 2024", "fy": "FY25", "q_num": 2,
     "start": "2024-07-01", "end": "2024-09-30",
     "revenue_cr": 286, "expenses_cr": 106, "pat_cr": 154},
    {"quarter": "Q3 FY25", "label": "Dec 2024", "fy": "FY25", "q_num": 3,
     "start": "2024-10-01", "end": "2024-12-31",
     "revenue_cr": 301, "expenses_cr": 108, "pat_cr": 160},
    {"quarter": "Q4 FY25", "label": "Mar 2025", "fy": "FY25", "q_num": 4,
     "start": "2025-01-01", "end": "2025-03-31",
     "revenue_cr": 291, "expenses_cr": 131, "pat_cr": 135},
    {"quarter": "Q1 FY26", "label": "Jun 2025", "fy": "FY26", "q_num": 1,
     "start": "2025-04-01", "end": "2025-06-30",
     "revenue_cr": 373, "expenses_cr": 132, "pat_cr": 203},
    {"quarter": "Q2 FY26", "label": "Sep 2025", "fy": "FY26", "q_num": 2,
     "start": "2025-07-01", "end": "2025-09-30",
     "revenue_cr": 374, "expenses_cr": 132, "pat_cr": 197},
    {"quarter": "Q3 FY26", "label": "Dec 2025", "fy": "FY26", "q_num": 3,
     "start": "2025-10-01", "end": "2025-12-31",
     "revenue_cr": 666, "expenses_cr": 172, "pat_cr": 401},
    # Q4 FY26 reported May 2026 (Screener.in consolidated; triple-corroborated
    # with Business Standard + MarketsMojo). Revenue +205% YoY, PAT +291% YoY.
    {"quarter": "Q4 FY26", "label": "Mar 2026", "fy": "FY26", "q_num": 4,
     "start": "2026-01-01", "end": "2026-03-31",
     "revenue_cr": 889, "expenses_cr": 224, "pat_cr": 530},
    # Q1 FY27 reported 04 Aug 2026 (Screener.in consolidated; corroborated with
    # Business Standard/Capital Market, Trendlyne, MarketsMojo and the filing via
    # ScanX — all six agree exactly). Revenue +88% YoY, PAT +103% YoY.
    # revenue_cr is revenue from OPERATIONS (702), not total income (751.79).
    # expenses_cr uses the same Screener basis as the rows above, i.e. EXCLUDING
    # depreciation: filed total expenses 228.88 = 208.02 + 20.86 depreciation.
    # pat_cr is CONSOLIDATED (413.44); standalone was 327.32 — the ~86 Cr gap is
    # the clearing-corporation subsidiary. No exceptional/extraordinary items.
    {"quarter": "Q1 FY27", "label": "Jun 2026", "fy": "FY27", "q_num": 1,
     "start": "2026-04-01", "end": "2026-06-30",
     "revenue_cr": 702, "expenses_cr": 208, "pat_cr": 413},
]

Q4_EXPENSE_ADJ_CR = 15

# Non-F&O revenue (the gap between reported revenue and the F&O daily sum) is estimated
# from the previous quarter's share of F&O revenue. A share outside this range, or a
# quarter with under 90% of its sessions in the daily table, points to bad data.
NON_FO_SHARE_MAX = 0.20
MIN_SESSION_COVERAGE = 0.9
BACKTEST_QUARTERS = 5


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _is_trading_day(d):
    return d.weekday() < 5 and d.strftime("%Y-%m-%d") not in MCX_HOLIDAYS_2026


def _classify_days(q_start, q_end, now, rows):
    """Split the quarter's sessions into completed rows and remaining days.

    A session is completed if it is before today and has a row, or it is today
    and has a final row (FINAL_ROW_SOURCES, after the close). Every other
    scheduled session counts as remaining: today until its final row lands,
    future days, and past days with no row (returned as missing). A row on a
    day the holiday calendar doesn't list as a session (e.g. a Sunday budget
    session) counts as completed. So elapsed + remaining always equals the total.

    Returns (completed_rows, remaining, missing_dates, today_status).
    """
    today = now.date()
    now_min = now.hour * 60 + now.minute
    close = session_end(today)
    by_date = {r["trading_date"]: r for r in rows}
    today_row = by_date.get(today.strftime("%Y-%m-%d"))
    today_final = (bool(today_row) and today_row.get("source") in FINAL_ROW_SOURCES
                   and now_min >= close)

    completed, missing, remaining = [], [], 0
    d = q_start
    while d <= q_end:
        iso = d.strftime("%Y-%m-%d")
        row = by_date.get(iso)
        done = bool(row) and (d < today or (d == today and today_final))
        if done:
            completed.append(row)
        elif _is_trading_day(d) and d >= today:
            remaining += 1
        elif _is_trading_day(d):
            missing.append(iso)
            remaining += 1
        d += timedelta(days=1)

    if today_final:
        today_status = "final"
    elif not _is_trading_day(today):
        today_status = "no_session"
    elif now_min < SESSION_START:
        today_status = "pre_open"
    elif now_min < close:
        today_status = "live"
    else:
        today_status = "closed_awaiting_eod"
    return completed, remaining, missing, today_status


def _get_quarter_bounds(d):
    """Return (q_label, q_num, fy, start_date, end_date) for MCX fiscal quarter.
    FY runs Apr-Mar: Q1=Apr-Jun, Q2=Jul-Sep, Q3=Oct-Dec, Q4=Jan-Mar."""
    m, y = d.month, d.year
    if m <= 3:
        return f"Q4 FY{str(y)[-2:]}", 4, f"FY{str(y)[-2:]}", date(y, 1, 1), date(y, 3, 31)
    elif m <= 6:
        return f"Q1 FY{str(y+1)[-2:]}", 1, f"FY{str(y+1)[-2:]}", date(y, 4, 1), date(y, 6, 30)
    elif m <= 9:
        return f"Q2 FY{str(y+1)[-2:]}", 2, f"FY{str(y+1)[-2:]}", date(y, 7, 1), date(y, 9, 30)
    else:
        return f"Q3 FY{str(y+1)[-2:]}", 3, f"FY{str(y+1)[-2:]}", date(y, 10, 1), date(y, 12, 31)


def _expected_sessions(a):
    d, end, n = date.fromisoformat(a["start"]), date.fromisoformat(a["end"]), 0
    while d <= end:
        n += _is_trading_day(d)
        d += timedelta(days=1)
    return n


def _fo_by_quarter(rows):
    """F&O revenue summed per reported quarter, with its non-F&O share when the data is sound."""
    out = {}
    for a in QUARTERLY_ACTUALS:
        rs = [r for r in rows if a["start"] <= r["trading_date"] <= a["end"]]
        fo = sum(r["total_rev_cr"] for r in rs)
        complete = len(rs) >= MIN_SESSION_COVERAGE * _expected_sessions(a)
        share = (a["revenue_cr"] - fo) / fo if fo > 0 else None
        ok = complete and share is not None and 0 <= share <= NON_FO_SHARE_MAX
        out[a["quarter"]] = {"fo_cr": round(fo, 2), "sessions": len(rs), "complete": complete,
                             "share": round(share, 4) if ok else None}
    return out


def _pat_model(rev, alpha, beta, tax_dep, q_num):
    exp = max(alpha + beta * rev + (Q4_EXPENSE_ADJ_CR if q_num == 4 else 0), 80)
    return exp, (rev - exp) * (1 - tax_dep)


def _backtest(fo):
    """Walk-forward test of both revenue bases on the last reported quarters: for each quarter,
    the expense model, tax rate and non-F&O share come only from the quarters before it."""
    rows, excluded = [], []
    for k in range(4, len(QUARTERLY_ACTUALS)):
        a, prev = QUARTERLY_ACTUALS[k], QUARTERLY_ACTUALS[k - 1]
        f, fp = fo[a["quarter"]], fo[prev["quarter"]]
        if not f["complete"] or f["fo_cr"] > a["revenue_cr"]:
            excluded.append({"quarter": a["quarter"], "reason": "F&O daily sum incomplete or above reported revenue"})
            continue
        if fp["share"] is None:
            excluded.append({"quarter": a["quarter"], "reason": f"no sound non-F&O share for {prev['quarter']}"})
            continue
        alpha, beta, _ = _fit_expense_model(QUARTERLY_ACTUALS[:k])
        tax_dep = _compute_tax_dep_rate(QUARTERLY_ACTUALS[:k])
        _, pat_fo = _pat_model(f["fo_cr"], alpha, beta, tax_dep, a["q_num"])
        _, pat_all = _pat_model(f["fo_cr"] * (1 + fp["share"]), alpha, beta, tax_dep, a["q_num"])
        rows.append({"quarter": a["quarter"], "fo_revenue_cr": f["fo_cr"], "reported_revenue_cr": a["revenue_cr"],
                     "non_fo_cr": round(a["revenue_cr"] - f["fo_cr"], 1), "share_used": fp["share"],
                     "reported_pat_cr": a["pat_cr"], "pat_fo_cr": round(pat_fo, 1), "pat_all_cr": round(pat_all, 1),
                     "miss_fo_cr": round(pat_fo - a["pat_cr"], 1), "miss_all_cr": round(pat_all - a["pat_cr"], 1)})
    rows = rows[-BACKTEST_QUARTERS:]
    mean = lambda xs: round(sum(xs) / len(xs), 1) if xs else None
    return {
        "method": "walk-forward: expense model, tax rate and non-F&O share from earlier quarters only",
        "rows": rows,
        "miss_fo_avg_cr": mean([r["miss_fo_cr"] for r in rows]),
        "miss_all_avg_cr": mean([r["miss_all_cr"] for r in rows]),
        "abs_miss_fo_avg_cr": mean([abs(r["miss_fo_cr"]) for r in rows]),
        "abs_miss_all_avg_cr": mean([abs(r["miss_all_cr"]) for r in rows]),
        "excluded": [e for e in excluded if e["quarter"] not in {r["quarter"] for r in rows}],
    }


def _fit_expense_model(actuals):
    """OLS: expenses = alpha + beta * revenue. Returns (alpha, beta, r_squared)."""
    n = len(actuals)
    x = [a["revenue_cr"] for a in actuals]
    y = [a["expenses_cr"] for a in actuals]
    x_mean = sum(x) / n
    y_mean = sum(y) / n
    ss_xy = sum((xi - x_mean) * (yi - y_mean) for xi, yi in zip(x, y))
    ss_xx = sum((xi - x_mean) ** 2 for xi in x)
    beta = ss_xy / ss_xx if ss_xx > 0 else 0
    alpha = y_mean - beta * x_mean
    y_pred = [alpha + beta * xi for xi in x]
    ss_res = sum((yi - yp) ** 2 for yi, yp in zip(y, y_pred))
    ss_tot = sum((yi - y_mean) ** 2 for yi in y)
    r_sq = 1 - ss_res / ss_tot if ss_tot > 0 else 0
    return round(alpha, 1), round(beta, 4), round(r_sq, 3)


def _compute_tax_dep_rate(actuals):
    """Effective (tax + depreciation) rate = median of (1 - PAT / (Rev - Exp))."""
    rates = []
    for a in actuals:
        pbt = a["revenue_cr"] - a["expenses_cr"]
        if pbt > 0 and a["pat_cr"] > 0:
            rates.append(1 - a["pat_cr"] / pbt)
    rates.sort()
    if not rates:
        return 0.20
    return round(rates[len(rates) // 2], 4)


# ─── Main computation ────────────────────────────────────────────────────────

def generate_quarterly(today=None, now=None):
    """`now` is the IST clock time; `today` alone means midday on that date."""
    if now is None:
        now = datetime.combine(today, datetime.min.time()).replace(hour=12) if today else now_ist()
    today = now.date()

    errors = []
    q_label, q_num, fy, q_start, q_end = _get_quarter_bounds(today)
    alpha, beta, r_sq = _fit_expense_model(QUARTERLY_ACTUALS)
    tax_dep_rate = _compute_tax_dep_rate(QUARTERLY_ACTUALS)

    # Format actuals
    actuals_resp = []
    for a in QUARTERLY_ACTUALS:
        margin = round(a["pat_cr"] / a["revenue_cr"] * 100, 1) if a["revenue_cr"] > 0 else 0
        actuals_resp.append({
            "quarter": a["quarter"], "label": a["label"],
            "q_num": a["q_num"], "fy": a["fy"],
            "revenue_cr": a["revenue_cr"], "expenses_cr": a["expenses_cr"],
            "pat_cr": a["pat_cr"], "pat_margin_pct": margin,
            "is_actual": True,
        })

    # Fetch daily revenue for current quarter
    q_start_iso = q_start.strftime("%Y-%m-%d")
    today_iso = today.strftime("%Y-%m-%d")
    fetched = []
    try:
        rows = supabase_read(
            "mcx_daily_revenue",
            f"?select=trading_date,total_rev_cr,source"
            f"&trading_date=gte.{q_start_iso}&trading_date=lte.{today_iso}"
            f"&order=trading_date.asc&limit=100"
        )
        fetched = [r for r in rows if r.get("total_rev_cr") and r["total_rev_cr"] > 0]
    except Exception as e:
        errors.append(f"supabase fetch: {e}")

    # Revenue projection
    daily_rows, remaining_trading, missing_dates, today_status = _classify_days(
        q_start, q_end, now, fetched)
    elapsed_trading = len(daily_rows)
    total_trading = elapsed_trading + remaining_trading

    actual_rev = round(sum(r["total_rev_cr"] for r in daily_rows), 2)
    daily_avg = round(actual_rev / elapsed_trading, 2) if elapsed_trading > 0 else 0

    last_10 = daily_rows[-10:] if len(daily_rows) >= 10 else daily_rows
    if not last_10:
        # No completed session yet this quarter: run the projection off the
        # last 10 sessions before the quarter started.
        try:
            prior = supabase_read(
                "mcx_daily_revenue",
                f"?select=trading_date,total_rev_cr"
                f"&trading_date=lt.{q_start_iso}&total_rev_cr=gt.0"
                f"&order=trading_date.desc&limit=10"
            )
            last_10 = [r for r in prior if r.get("total_rev_cr") and r["total_rev_cr"] > 0]
        except Exception as e:
            errors.append(f"supabase fetch (prior quarter): {e}")
    ma10 = round(sum(r["total_rev_cr"] for r in last_10) / len(last_10), 2) if last_10 else daily_avg

    if elapsed_trading > 0 and total_trading > 0:
        blend_w = min(elapsed_trading / total_trading, 0.8)
        daily_proj = blend_w * ma10 + (1 - blend_w) * daily_avg
    elif ma10 > 0:
        daily_proj = ma10
    else:
        errors.append("no revenue history: daily projection uses a 12 Cr placeholder")
        daily_proj = 12.0

    remaining_rev = round(daily_proj * remaining_trading, 2)
    total_rev = round(actual_rev + remaining_rev, 2)

    # Uncertainty bands
    completion = elapsed_trading / total_trading if total_trading > 0 else 0
    uncertainty = 0.02 + 0.18 * (1 - completion)
    rev_low = round(total_rev * (1 - uncertainty), 1)
    rev_high = round(total_rev * (1 + uncertainty), 1)

    # Expense projection
    seasonal_adj = Q4_EXPENSE_ADJ_CR if q_num == 4 else 0
    expenses_proj = round(alpha + beta * total_rev + seasonal_adj, 1)
    expenses_proj = max(expenses_proj, 80)

    # PAT projection
    pbt_proj = total_rev - expenses_proj
    pat_proj = round(pbt_proj * (1 - tax_dep_rate), 1)
    pat_margin = round(pat_proj / total_rev * 100, 1) if total_rev > 0 else 0

    pbt_low = rev_low - expenses_proj * 1.05
    pbt_high = rev_high - expenses_proj * 0.95
    pat_low = round(pbt_low * (1 - tax_dep_rate), 1)
    pat_high = round(pbt_high * (1 - tax_dep_rate), 1)

    # ── Revenue basis: F&O only (above) and including estimated non-F&O revenue ──
    fo, non_fo, backtest = {}, None, None
    try:
        hist = supabase_read_all(
            "mcx_daily_revenue",
            f"?select=trading_date,total_rev_cr&trading_date=gte.{QUARTERLY_ACTUALS[0]['start']}"
            f"&trading_date=lte.{QUARTERLY_ACTUALS[-1]['end']}&total_rev_cr=gt.0&order=trading_date.asc",
            max_rows=3000)
        fo = _fo_by_quarter(hist)
        backtest = _backtest(fo)
        last = QUARTERLY_ACTUALS[-1]
        share, share_q, fallback = fo[last["quarter"]]["share"], last["quarter"], False
        if share is None:           # previous quarter unsound: median of the last four sound shares
            sound = [fo[a["quarter"]]["share"] for a in QUARTERLY_ACTUALS[-5:-1] if fo[a["quarter"]]["share"] is not None]
            share = sorted(sound)[len(sound) // 2] if sound else None
            share_q, fallback = "median of recent quarters", True
        if share is not None:
            rev_b = total_rev * (1 + share)
            exp_b, pat_b = _pat_model(rev_b, alpha, beta, tax_dep_rate, q_num)
            lo_b, hi_b = rev_low * (1 + share), rev_high * (1 + share)
            non_fo = {
                "share": round(share, 4), "share_from": share_q, "fallback": fallback,
                "estimate_cr": round(total_rev * share, 1),
                "revenue_projected_cr": round(rev_b, 1),
                "revenue_low_cr": round(lo_b, 1), "revenue_high_cr": round(hi_b, 1),
                "expenses_projected_cr": round(exp_b, 1),
                "pat_projected_cr": round(pat_b, 1),
                "pat_low_cr": round((lo_b - exp_b * 1.05) * (1 - tax_dep_rate), 1),
                "pat_high_cr": round((hi_b - exp_b * 0.95) * (1 - tax_dep_rate), 1),
                "pat_margin_pct": round(pat_b / rev_b * 100, 1),
            }
    except Exception as e:
        errors.append(f"revenue basis: {e}")
    for a in actuals_resp:
        f = fo.get(a["quarter"])
        if f:
            a["fo_revenue_cr"] = f["fo_cr"] if f["complete"] else None
            a["non_fo_share"] = f["share"]

    # Daily series for chart
    cumul = 0
    daily_series = []
    for r in daily_rows:
        cumul += r["total_rev_cr"]
        daily_series.append({
            "date": r["trading_date"],
            "rev_cr": round(r["total_rev_cr"], 2),
            "cumul_cr": round(cumul, 2),
        })

    q_month = ["", "Jun", "Sep", "Dec", "Mar"][q_num]
    current_quarter = {
        "quarter": q_label, "label": f"{q_month} {today.year}",
        "q_num": q_num, "fy": fy,
        "trading_days_elapsed": elapsed_trading,
        "trading_days_total": total_trading,
        "trading_days_remaining": remaining_trading,
        "today_status": today_status,
        "today_in_remaining": today_status in ("pre_open", "live", "closed_awaiting_eod"),
        "missing_dates": missing_dates,
        "rows_through": daily_rows[-1]["trading_date"] if daily_rows else None,
        "revenue_actual_cr": actual_rev,
        "revenue_daily_avg_cr": daily_avg,
        "revenue_ma10_cr": ma10,
        "revenue_projected_cr": total_rev,
        "revenue_low_cr": rev_low, "revenue_high_cr": rev_high,
        "expenses_projected_cr": expenses_proj,
        "pat_projected_cr": pat_proj,
        "pat_low_cr": pat_low, "pat_high_cr": pat_high,
        "pat_margin_pct": pat_margin,
        "completion_pct": round(completion * 100, 1),
        "uncertainty_pct": round(uncertainty * 100, 1),
        "daily_series": daily_series,
    }

    # ── Full-year projection (fiscal-year generic) ──────────────────────────
    # Target the current FY once it has any reported quarter; until then show the
    # most recent COMPLETE fiscal year as final actuals. Previously this hardcoded
    # "FY26" and always added the current quarter's projection — which, once the
    # calendar rolled into FY27, silently folded an FY27 quarter into the FY26
    # total (and left FY26 missing its real Q4). This generic version fixes both.
    last_actual_fy = QUARTERLY_ACTUALS[-1]["fy"]
    target_fy = fy if any(a["fy"] == fy for a in QUARTERLY_ACTUALS) else last_actual_fy
    fy_actuals = [a for a in QUARTERLY_ACTUALS if a["fy"] == target_fy]
    fy_actual_rev = sum(a["revenue_cr"] for a in fy_actuals)
    fy_actual_pat = sum(a["pat_cr"] for a in fy_actuals)

    # Only fold the current-quarter projection into the FY total when that quarter
    # belongs to target_fy and has not yet been reported as an actual.
    reported_labels = {a["quarter"] for a in fy_actuals}
    project_current = (fy == target_fy) and (q_label not in reported_labels)
    if project_current:
        fy_total_rev = round(fy_actual_rev + total_rev, 1)
        fy_total_pat = round(fy_actual_pat + pat_proj, 1)
        projected_q = {"quarter": q_label, "pat_cr": pat_proj, "revenue_cr": total_rev}
    else:
        fy_total_rev = round(float(fy_actual_rev), 1)
        fy_total_pat = round(float(fy_actual_pat), 1)
        projected_q = None
    fy_eps = round(fy_total_pat / DILUTED_SHARES_CR, 2)
    fy_is_complete = (len(fy_actuals) == 4) and not project_current

    fy_projection = {
        "fy": target_fy,
        "is_complete": fy_is_complete,
        "quarters_actual": [{"quarter": a["quarter"], "pat_cr": a["pat_cr"],
                             "revenue_cr": a["revenue_cr"]} for a in fy_actuals],
        "projected_quarter": projected_q,   # None when the FY is complete
        "q4_projected": projected_q,         # back-compat alias (may be None)
        "fy_revenue_cr": fy_total_rev,
        "fy_pat_cr": fy_total_pat,
        "fy_eps": fy_eps,
        "diluted_shares_cr": DILUTED_SHARES_CR,
    }

    return {
        "success": True,
        "as_of": now_ist().strftime("%Y-%m-%d %H:%M IST"),
        "actuals": actuals_resp,
        "current_quarter": current_quarter,
        "expense_model": {
            "method": "ols_regression_seasonal",
            "fixed_cr": alpha,
            "variable_pct": round(beta * 100, 1),
            "seasonal_adj_cr": seasonal_adj,
            "tax_dep_rate_pct": round(tax_dep_rate * 100, 1),
            "r_squared": r_sq,
            "data_points": len(QUARTERLY_ACTUALS),
        },
        "fy_projection": fy_projection,
        "non_fo": non_fo,
        "backtest": backtest,
        "errors": errors,
    }


# ─── Vercel handler ──────────────────────────────────────────────────────────

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        origin = self.headers.get("Origin", "")
        cors = make_cors_headers(origin)
        try:
            result = generate_quarterly()
            self.send_response(200)
            for k, v in cors.items():
                self.send_header(k, v)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "public, max-age=60, s-maxage=60")
            self.end_headers()
            self.wfile.write(json.dumps(result).encode())
        except Exception as e:
            self.send_response(500)
            for k, v in cors.items():
                self.send_header(k, v)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"success": False, "error": str(e)}).encode())

    def do_OPTIONS(self):
        origin = self.headers.get("Origin", "")
        cors = make_cors_headers(origin)
        self.send_response(204)
        for k, v in cors.items():
            self.send_header(k, v)
        self.end_headers()

    def log_message(self, format, *args):
        pass
