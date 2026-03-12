"""
/api/commodity_dashboard — Commodity-Level Revenue Breakdown

Computes per-commodity daily revenue from mcx_commodity_daily table,
aggregates across FY, quarterly, monthly, weekly, and day-of-week dimensions.
Top 5 commodities + "OTHERS" bucket.
"""
from http.server import BaseHTTPRequestHandler
import json
from datetime import date, timedelta
from collections import defaultdict

from lib.mcx_config import (
    SUPABASE_URL, SUPABASE_ANON_KEY,
    supabase_read_all, now_ist, make_cors_headers,
)

# Fee rates (SEBI Oct 2024)
FUTURES_RATE = 210.0      # ₹ per crore notional (both sides)
OPTIONS_RATE = 4180.0     # ₹ per crore premium (both sides)
TOP_N = 5                 # Number of top commodities to show individually

# ── Commodity symbol normalization ────────────────────────────────────────
# Group mini/micro variants under their parent commodity
COMMODITY_MAP = {
    "CRUDEOILM": "CRUDEOIL", "NATGASMINI": "NATURALGAS",
    "GOLDM": "GOLD", "GOLDGUINEA": "GOLD", "GOLDPETAL": "GOLD", "GOLDTEN": "GOLD",
    "SILVERM": "SILVER", "SILVERMIC": "SILVER",
    "LEADMINI": "LEAD", "ZINCMINI": "ZINC", "ALUMINI": "ALUMINIUM",
    "ELECDMBL": "NATURALGAS",  # Electric daily bilateral — group under energy
}


# ─── Date helpers (same as exchange_dashboard) ────────────────────────────

def _fy_label(d):
    if d.month >= 4:
        return f"FY{str(d.year + 1)[-2:]}"
    return f"FY{str(d.year)[-2:]}"

def _quarter_key(d):
    m, y = d.month, d.year
    if m <= 3:
        return f"Q4 FY{str(y)[-2:]}"
    elif m <= 6:
        return f"Q1 FY{str(y + 1)[-2:]}"
    elif m <= 9:
        return f"Q2 FY{str(y + 1)[-2:]}"
    else:
        return f"Q3 FY{str(y + 1)[-2:]}"

def _month_key(d):
    fy = _fy_label(d)
    return f"{fy} {d.strftime('%B')}"

def _prev_quarter(q):
    q_num = int(q[1])
    fy = q[3:]
    if q_num == 1:
        yy = int(fy[2:])
        return f"Q4 FY{str(yy - 1).zfill(2)}"
    return f"Q{q_num - 1} {fy}"

def _yoy_quarter(q):
    q_num = q[:2]
    yy = int(q[5:])
    return f"{q_num} FY{str(yy - 1).zfill(2)}"

def _avg(vals):
    return sum(vals) / len(vals) if vals else 0

def _pct(cur, prev):
    if not prev or prev == 0:
        return None
    return round((cur - prev) / abs(prev) * 100, 0)


# ─── Main computation ────────────────────────────────────────────────────

def generate_commodity_dashboard():
    """Fetch commodity data, compute per-commodity revenue, aggregate."""
    ist = now_ist()

    # Fetch last 2 FYs of commodity data (for YoY)
    prev_fy_start = date(ist.year - 2, 4, 1) if ist.month >= 4 else date(ist.year - 3, 4, 1)
    # Actually: just fetch from 2 FYs ago start
    fy_label = _fy_label(ist.date() if hasattr(ist, 'date') else ist)
    yy = int(fy_label[2:])
    start_date = date(2000 + yy - 2, 4, 1)  # 2 FYs back

    rows = supabase_read_all(
        "mcx_commodity_daily",
        f"?select=trading_date,commodity,instrument_type,turnover_cr,premium_turnover_cr"
        f"&trading_date=gte.{start_date.isoformat()}"
        f"&order=trading_date.asc",
        max_rows=25000,
    )

    if not rows:
        return {"success": False, "error": "no commodity data"}

    # ── Step 1: Compute per-row revenue and aggregate by (date, commodity) ──
    # daily_rev[date_str][normalized_commodity] = total_rev_cr
    daily_rev = defaultdict(lambda: defaultdict(float))

    for r in rows:
        dt = r["trading_date"]
        raw_sym = r["commodity"]
        sym = COMMODITY_MAP.get(raw_sym, raw_sym)
        inst = r["instrument_type"]
        turnover = r.get("turnover_cr") or 0
        premium = r.get("premium_turnover_cr") or 0

        if inst in ("FUTCOM", "FUTIDX"):
            rev = 2 * turnover * FUTURES_RATE / 1e7
        elif inst in ("OPTFUT", "OPTIDX"):
            rev = 2 * premium * OPTIONS_RATE / 1e7
        else:
            continue

        daily_rev[dt][sym] += rev

    # ── Step 2: Identify top N commodities by current FY total revenue ──
    current_fy = _fy_label(ist.date() if hasattr(ist, 'date') else ist)
    fy_totals = defaultdict(float)
    for dt_str, commodities in daily_rev.items():
        d = date.fromisoformat(dt_str)
        if _fy_label(d) == current_fy:
            for sym, rev in commodities.items():
                fy_totals[sym] += rev

    sorted_commodities = sorted(fy_totals.items(), key=lambda x: -x[1])
    top_commodities = [sym for sym, _ in sorted_commodities[:TOP_N]]
    all_commodities = top_commodities + ["OTHERS"]

    def bucket(sym):
        return sym if sym in top_commodities else "OTHERS"

    # ── Step 3: Build bucketed daily time series ──
    # daily_data: list of {date, commodity_revs: {sym: rev}} sorted by date
    daily_data = []
    for dt_str in sorted(daily_rev.keys()):
        d = date.fromisoformat(dt_str)
        bucketed = defaultdict(float)
        total = 0.0
        for sym, rev in daily_rev[dt_str].items():
            bucketed[bucket(sym)] += rev
            total += rev
        daily_data.append({
            "date": d,
            "revs": dict(bucketed),
            "total": total,
        })

    if not daily_data:
        return {"success": False, "error": "no processed data"}

    # ── Step 4: Summary matrix ──
    latest = daily_data[-1]
    last_5 = daily_data[-5:] if len(daily_data) >= 5 else daily_data
    last_45 = daily_data[-45:] if len(daily_data) >= 45 else daily_data

    # Current month data
    cur_month = ist.month if hasattr(ist, 'month') else ist.date().month
    cur_year = ist.year if hasattr(ist, 'year') else ist.date().year
    month_data = [d for d in daily_data if d["date"].month == cur_month and d["date"].year == cur_year]

    # Current quarter
    cur_q = _quarter_key(ist.date() if hasattr(ist, 'date') else ist)
    q_data = [d for d in daily_data if _quarter_key(d["date"]) == cur_q]

    # Current FY
    fy_data = [d for d in daily_data if _fy_label(d["date"]) == current_fy]

    # Previous FY for YoY
    prev_fy = f"FY{str(int(current_fy[2:]) - 1).zfill(2)}"
    prev_fy_data = [d for d in daily_data if _fy_label(d["date"]) == prev_fy]

    def avg_rev(data_slice, sym):
        vals = [d["revs"].get(sym, 0) for d in data_slice]
        return round(_avg(vals), 4) if vals else 0

    summary_matrix = []
    for sym in all_commodities:
        fy_avg = avg_rev(fy_data, sym)
        prev_fy_avg = avg_rev(prev_fy_data, sym)
        fy_total_avg = avg_rev(fy_data, "___total___")  # placeholder
        # Share = this commodity's FY total / exchange FY total
        sym_fy_total = sum(d["revs"].get(sym, 0) for d in fy_data)
        exchange_fy_total = sum(d["total"] for d in fy_data)
        share = round(sym_fy_total / exchange_fy_total * 100, 1) if exchange_fy_total > 0 else 0

        summary_matrix.append({
            "commodity": sym,
            "last_day": round(latest["revs"].get(sym, 0), 4),
            "avg_5d": avg_rev(last_5, sym),
            "avg_45d": avg_rev(last_45, sym),
            "avg_month": avg_rev(month_data, sym) if month_data else 0,
            "avg_quarter": avg_rev(q_data, sym) if q_data else 0,
            "avg_fy": fy_avg,
            "yoy_pct": _pct(fy_avg, prev_fy_avg),
            "share_pct": share,
        })

    # Add TOTAL row
    summary_matrix.append({
        "commodity": "TOTAL",
        "last_day": round(latest["total"], 4),
        "avg_5d": round(_avg([d["total"] for d in last_5]), 4),
        "avg_45d": round(_avg([d["total"] for d in last_45]), 4),
        "avg_month": round(_avg([d["total"] for d in month_data]), 4) if month_data else 0,
        "avg_quarter": round(_avg([d["total"] for d in q_data]), 4) if q_data else 0,
        "avg_fy": round(_avg([d["total"] for d in fy_data]), 4),
        "yoy_pct": _pct(
            _avg([d["total"] for d in fy_data]),
            _avg([d["total"] for d in prev_fy_data]) if prev_fy_data else 0
        ),
        "share_pct": 100.0,
    })

    # ── Step 5: Quarterly breakdown ──
    q_groups = defaultdict(list)  # quarter -> list of daily_data entries
    for d in daily_data:
        q_groups[_quarter_key(d["date"])].append(d)

    # Sort quarters chronologically and take last 6
    sorted_quarters = sorted(q_groups.keys(), key=lambda q: (int(q[5:]), int(q[1])))
    last_6_quarters = sorted_quarters[-6:]

    quarterly = []
    for q_label in last_6_quarters:
        q_entries = q_groups[q_label]
        prev_q = _prev_quarter(q_label)
        yoy_q = _yoy_quarter(q_label)
        commodities = {}
        total_rev = sum(d["total"] for d in q_entries)
        for sym in all_commodities:
            avg_r = avg_rev(q_entries, sym)
            sym_total = sum(d["revs"].get(sym, 0) for d in q_entries)
            share = round(sym_total / total_rev * 100, 1) if total_rev > 0 else 0
            # QoQ comparison
            prev_avg = avg_rev(q_groups.get(prev_q, []), sym)
            yoy_avg = avg_rev(q_groups.get(yoy_q, []), sym)
            commodities[sym] = {
                "avg_rev": avg_r,
                "share_pct": share,
                "qoq_pct": _pct(avg_r, prev_avg),
                "yoy_pct": _pct(avg_r, yoy_avg),
            }
        quarterly.append({
            "quarter": q_label,
            "trading_days": len(q_entries),
            "commodities": commodities,
            "total": round(_avg([d["total"] for d in q_entries]), 4),
        })

    # ── Step 6: Monthly breakdown (last 3 months) ──
    m_groups = defaultdict(list)
    for d in daily_data:
        m_groups[_month_key(d["date"])].append(d)

    sorted_months = sorted(m_groups.keys(),
                           key=lambda m: m_groups[m][0]["date"])
    last_3_months = sorted_months[-3:]

    monthly = []
    for m_label in last_3_months:
        m_entries = m_groups[m_label]
        commodities = {}
        total_rev = sum(d["total"] for d in m_entries)
        for sym in all_commodities:
            avg_r = avg_rev(m_entries, sym)
            sym_total = sum(d["revs"].get(sym, 0) for d in m_entries)
            share = round(sym_total / total_rev * 100, 1) if total_rev > 0 else 0
            commodities[sym] = {"avg_rev": avg_r, "share_pct": share}
        monthly.append({
            "month": m_label,
            "trading_days": len(m_entries),
            "commodities": commodities,
            "total": round(_avg([d["total"] for d in m_entries]), 4),
        })

    # ── Step 7: Weekly (last 5d vs previous 5d vs 45d) ──
    weekly = []
    for label, slc in [("Last 5 Days", last_5), ("Last 45 Days", last_45)]:
        commodities = {}
        total_rev = sum(d["total"] for d in slc)
        for sym in all_commodities:
            avg_r = avg_rev(slc, sym)
            sym_total = sum(d["revs"].get(sym, 0) for d in slc)
            share = round(sym_total / total_rev * 100, 1) if total_rev > 0 else 0
            commodities[sym] = {"avg_rev": avg_r, "share_pct": share}
        weekly.append({
            "label": label,
            "trading_days": len(slc),
            "commodities": commodities,
            "total": round(_avg([d["total"] for d in slc]), 4),
        })

    # ── Step 8: Daily trend (last 60 days) for stacked chart ──
    trend_data = daily_data[-60:]
    daily_trend = []
    for d in trend_data:
        entry = {"date": d["date"].isoformat()}
        for sym in all_commodities:
            entry[sym] = round(d["revs"].get(sym, 0), 4)
        entry["total"] = round(d["total"], 4)
        daily_trend.append(entry)

    return {
        "success": True,
        "as_of": ist.strftime("%Y-%m-%d %H:%M IST"),
        "current_fy": current_fy,
        "commodities": all_commodities,
        "summary_matrix": summary_matrix,
        "quarterly": quarterly,
        "monthly": monthly,
        "weekly": weekly,
        "daily_trend": daily_trend,
    }


# ─── HTTP handler ────────────────────────────────────────────────────────

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        headers = make_cors_headers()
        try:
            result = generate_commodity_dashboard()
            self.send_response(200)
            for k, v in headers.items():
                self.send_header(k, v)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "public, max-age=120, s-maxage=120")
            self.end_headers()
            self.wfile.write(json.dumps(result, default=str).encode())
        except Exception as e:
            self.send_response(500)
            for k, v in headers.items():
                self.send_header(k, v)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"success": False, "error": str(e)}).encode())

    def do_OPTIONS(self):
        headers = make_cors_headers()
        self.send_response(204)
        for k, v in headers.items():
            self.send_header(k, v)
        self.end_headers()
