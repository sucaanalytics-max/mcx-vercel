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
from datetime import date, timedelta

from lib.mcx_config import (
    SUPABASE_URL, SUPABASE_ANON_KEY, DILUTED_SHARES_CR,
    MCX_HOLIDAYS_2026, supabase_read, now_ist, make_cors_headers,
)

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
]

Q4_EXPENSE_ADJ_CR = 15


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _is_trading_day(d):
    return d.weekday() < 5 and d.strftime("%Y-%m-%d") not in MCX_HOLIDAYS_2026


def _count_trading_days(start, end):
    count = 0
    cur = start
    while cur <= end:
        if _is_trading_day(cur):
            count += 1
        cur += timedelta(days=1)
    return count


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

def generate_quarterly(today=None):
    if today is None:
        today = now_ist().date()

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
    daily_rows = []
    try:
        rows = supabase_read(
            "mcx_daily_revenue",
            f"?select=trading_date,total_rev_cr"
            f"&trading_date=gte.{q_start_iso}&trading_date=lte.{today_iso}"
            f"&order=trading_date.asc&limit=100"
        )
        daily_rows = [r for r in rows if r.get("total_rev_cr") and r["total_rev_cr"] > 0]
    except Exception as e:
        errors.append(f"supabase fetch: {e}")

    # Revenue projection
    elapsed_trading = len(daily_rows)
    total_trading = _count_trading_days(q_start, q_end)
    remaining_trading = _count_trading_days(today + timedelta(days=1), q_end)

    actual_rev = round(sum(r["total_rev_cr"] for r in daily_rows), 2)
    daily_avg = round(actual_rev / elapsed_trading, 2) if elapsed_trading > 0 else 0

    last_10 = daily_rows[-10:] if len(daily_rows) >= 10 else daily_rows
    ma10 = round(sum(r["total_rev_cr"] for r in last_10) / len(last_10), 2) if last_10 else daily_avg

    if elapsed_trading > 0 and total_trading > 0:
        blend_w = min(elapsed_trading / total_trading, 0.8)
        daily_proj = blend_w * ma10 + (1 - blend_w) * daily_avg
    else:
        daily_proj = ma10 if ma10 > 0 else 12.0

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

    # FY26 projection
    fy26_actuals = [a for a in QUARTERLY_ACTUALS if a["fy"] == "FY26"]
    fy26_actual_pat = sum(a["pat_cr"] for a in fy26_actuals)
    fy26_actual_rev = sum(a["revenue_cr"] for a in fy26_actuals)
    fy26_total_pat = round(fy26_actual_pat + pat_proj, 1)
    fy26_total_rev = round(fy26_actual_rev + total_rev, 1)
    fy26_eps = round(fy26_total_pat / DILUTED_SHARES_CR, 2)

    fy_projection = {
        "fy": "FY26",
        "quarters_actual": [{"quarter": a["quarter"], "pat_cr": a["pat_cr"],
                             "revenue_cr": a["revenue_cr"]} for a in fy26_actuals],
        "q4_projected": {"pat_cr": pat_proj, "revenue_cr": total_rev},
        "fy_revenue_cr": fy26_total_rev,
        "fy_pat_cr": fy26_total_pat,
        "fy_eps": fy26_eps,
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
