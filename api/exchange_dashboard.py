"""
/api/exchange_dashboard — Exchange Breakdown Analysis

Computes daily average revenue breakdown (Futures, Options, Total, O/F Ratio)
across FY, quarterly, monthly, weekly, and day-of-week dimensions.
Mirrors the user's Excel "Exchanges Dashboard" with YoY/QoQ/MoM/WoW metrics.
"""
from http.server import BaseHTTPRequestHandler
import json
from datetime import date, timedelta
from collections import defaultdict

from lib.mcx_config import (
    SUPABASE_URL, SUPABASE_ANON_KEY,
    supabase_read, now_ist, make_cors_headers,
)


# ─── Date helpers ───────────────────────────────────────────────────────────

def _fy_label(d):
    """Return FY label for a date. Apr-Mar fiscal year."""
    if d.month >= 4:
        return f"FY{str(d.year + 1)[-2:]}"
    return f"FY{str(d.year)[-2:]}"


def _fy_start(fy_label):
    """Return start date for a FY label like 'FY26'."""
    yy = int(fy_label[2:])
    return date(2000 + yy - 1, 4, 1)


def _quarter_key(d):
    """Return (quarter_label, fy_label) for a date."""
    m, y = d.month, d.year
    if m <= 3:
        return f"Q4 FY{str(y)[-2:]}", f"FY{str(y)[-2:]}"
    elif m <= 6:
        return f"Q1 FY{str(y + 1)[-2:]}", f"FY{str(y + 1)[-2:]}"
    elif m <= 9:
        return f"Q2 FY{str(y + 1)[-2:]}", f"FY{str(y + 1)[-2:]}"
    else:
        return f"Q3 FY{str(y + 1)[-2:]}", f"FY{str(y + 1)[-2:]}"


def _quarter_num(label):
    """Extract quarter number from label like 'Q4 FY26' -> 4."""
    return int(label[1])


def _month_key(d):
    """Return month key like 'FY 2026 March'."""
    fy = _fy_label(d)
    return f"{fy} {d.strftime('%B')}"


def _prev_quarter(q_label):
    """Return the previous quarter label. Q1 FY26 -> Q4 FY25."""
    q_num = int(q_label[1])
    fy = q_label[3:]  # e.g., "FY26"
    if q_num == 1:
        yy = int(fy[2:])
        return f"Q4 FY{str(yy - 1).zfill(2)}"
    return f"Q{q_num - 1} {fy}"


def _yoy_quarter(q_label):
    """Return same quarter, prior year. Q4 FY26 -> Q4 FY25."""
    q_num = q_label[:2]
    fy = q_label[3:]
    yy = int(fy[2:])
    return f"{q_num} FY{str(yy - 1).zfill(2)}"


# ─── Aggregation helpers ────────────────────────────────────────────────────

def _avg(values):
    """Safe average."""
    if not values:
        return 0
    return sum(values) / len(values)


def _pct_change(current, previous):
    """Percentage change, None if previous is 0."""
    if not previous or previous == 0:
        return None
    return round((current - previous) / abs(previous) * 100, 0)


def _group_stats(rows):
    """Compute avg fut/opt/total and O/F from a list of row dicts."""
    if not rows:
        return None
    fut = [r["fut"] for r in rows if r["fut"] is not None]
    opt = [r["opt"] for r in rows if r["opt"] is not None]
    total = [r["total"] for r in rows if r["total"] is not None]
    avg_f = round(_avg(fut), 2) if fut else 0
    avg_o = round(_avg(opt), 2) if opt else 0
    avg_t = round(_avg(total), 2) if total else 0
    of_ratio = round(avg_o / avg_f, 2) if avg_f > 0 else None
    return {
        "avg_fut": avg_f, "avg_opt": avg_o, "avg_total": avg_t,
        "of_ratio": of_ratio, "trading_days": len(rows),
    }


# ─── Main computation ──────────────────────────────────────────────────────

def generate_exchange_dashboard(today=None):
    if today is None:
        today = now_ist().date()

    # Fetch all daily revenue rows
    rows = supabase_read(
        "mcx_daily_revenue",
        "?select=trading_date,fut_rev_cr,opt_rev_cr,total_rev_cr"
        "&trading_date=gt.1901-01-01"
        "&order=trading_date.asc&limit=2000"
    )

    # Parse and normalize
    data = []
    for r in rows:
        try:
            d = date.fromisoformat(r["trading_date"])
        except (ValueError, TypeError):
            continue
        fut = r.get("fut_rev_cr") or 0
        opt = r.get("opt_rev_cr") or 0
        total = fut + opt  # Exclude nontx for FUTURE+OPTION=TOTAL
        if total <= 0:
            continue
        data.append({
            "date": d, "dow": d.weekday(),  # 0=Mon, 4=Fri
            "fut": round(fut, 4), "opt": round(opt, 4), "total": round(total, 4),
        })

    if not data:
        return {"success": False, "error": "No data available"}

    # ── 1. FY Summary ─────────────────────────────────────────────────────
    fy_groups = defaultdict(list)
    for r in data:
        fy_groups[_fy_label(r["date"])].append(r)

    current_fy = _fy_label(today)
    fy_order = sorted(fy_groups.keys(), key=lambda f: int(f[2:]), reverse=True)

    fy_stats = {}
    for fy in fy_order:
        fy_stats[fy] = _group_stats(fy_groups[fy])

    fy_summary = []
    for fy in fy_order:
        s = fy_stats[fy]
        if not s:
            continue
        yy = int(fy[2:])
        prev_fy = f"FY{str(yy - 1).zfill(2)}"
        prev = fy_stats.get(prev_fy)
        entry = {**s, "fy": fy}
        if prev:
            entry["yoy_fut"] = _pct_change(s["avg_fut"], prev["avg_fut"])
            entry["yoy_opt"] = _pct_change(s["avg_opt"], prev["avg_opt"])
            entry["yoy_total"] = _pct_change(s["avg_total"], prev["avg_total"])
            entry["yoy_of"] = _pct_change(s["of_ratio"], prev["of_ratio"]) if s["of_ratio"] and prev.get("of_ratio") else None
        fy_summary.append(entry)

    # ── 2. Quarterly ──────────────────────────────────────────────────────
    q_groups = defaultdict(list)
    for r in data:
        ql, _ = _quarter_key(r["date"])
        q_groups[ql].append(r)

    # Sort quarters by start date (most recent first)
    def _q_sort_key(ql):
        qn = _quarter_num(ql)
        yy = int(ql.split("FY")[1])
        return yy * 10 + qn

    q_order = sorted(q_groups.keys(), key=_q_sort_key, reverse=True)

    q_stats = {}
    for q in q_order:
        q_stats[q] = _group_stats(q_groups[q])

    quarterly = []
    for q in q_order[:6]:  # Last 6 quarters
        s = q_stats[q]
        if not s:
            continue
        entry = {**s, "quarter": q}
        # QoQ
        pq = _prev_quarter(q)
        prev_q = q_stats.get(pq)
        if prev_q:
            entry["qoq_fut"] = _pct_change(s["avg_fut"], prev_q["avg_fut"])
            entry["qoq_opt"] = _pct_change(s["avg_opt"], prev_q["avg_opt"])
            entry["qoq_total"] = _pct_change(s["avg_total"], prev_q["avg_total"])
            entry["qoq_of"] = _pct_change(s["of_ratio"], prev_q["of_ratio"]) if s["of_ratio"] and prev_q.get("of_ratio") else None
        # YoY
        yq = _yoy_quarter(q)
        yoy_q = q_stats.get(yq)
        if yoy_q:
            entry["yoy_fut"] = _pct_change(s["avg_fut"], yoy_q["avg_fut"])
            entry["yoy_opt"] = _pct_change(s["avg_opt"], yoy_q["avg_opt"])
            entry["yoy_total"] = _pct_change(s["avg_total"], yoy_q["avg_total"])
            entry["yoy_of"] = _pct_change(s["of_ratio"], yoy_q["of_ratio"]) if s["of_ratio"] and yoy_q.get("of_ratio") else None
        quarterly.append(entry)

    # ── 3. Monthly ────────────────────────────────────────────────────────
    m_groups = defaultdict(list)
    for r in data:
        mk = (r["date"].year, r["date"].month)
        m_groups[mk].append(r)

    m_order = sorted(m_groups.keys(), reverse=True)

    m_stats = {}
    for mk in m_order:
        m_stats[mk] = _group_stats(m_groups[mk])

    # 6-month rolling average
    recent_6m = m_order[:6]
    all_6m_rows = []
    for mk in recent_6m:
        all_6m_rows.extend(m_groups[mk])
    avg_6m = _group_stats(all_6m_rows)

    monthly = []
    for i, mk in enumerate(m_order[:3]):  # Last 3 months
        s = m_stats[mk]
        if not s:
            continue
        d = date(mk[0], mk[1], 1)
        fy = _fy_label(d)
        label = f"{fy} {d.strftime('%B')}"
        entry = {**s, "label": label, "year": mk[0], "month": mk[1]}
        # MoM
        if i + 1 < len(m_order):
            prev_mk = m_order[i + 1]
            prev_s = m_stats.get(prev_mk)
            if prev_s:
                entry["mom_fut"] = _pct_change(s["avg_fut"], prev_s["avg_fut"])
                entry["mom_opt"] = _pct_change(s["avg_opt"], prev_s["avg_opt"])
                entry["mom_total"] = _pct_change(s["avg_total"], prev_s["avg_total"])
                entry["mom_of"] = _pct_change(s["of_ratio"], prev_s["of_ratio"]) if s["of_ratio"] and prev_s.get("of_ratio") else None
        # Mo6M
        if avg_6m:
            entry["mo6m_fut"] = _pct_change(s["avg_fut"], avg_6m["avg_fut"])
            entry["mo6m_opt"] = _pct_change(s["avg_opt"], avg_6m["avg_opt"])
            entry["mo6m_total"] = _pct_change(s["avg_total"], avg_6m["avg_total"])
            entry["mo6m_of"] = _pct_change(s["of_ratio"], avg_6m["of_ratio"]) if s["of_ratio"] and avg_6m.get("of_ratio") else None
        monthly.append(entry)

    # Add 6M average row
    if avg_6m:
        monthly.append({**avg_6m, "label": "Avg Of Last 6 Months", "is_average": True})

    # ── 4. Weekly ─────────────────────────────────────────────────────────
    last_50 = data[-50:] if len(data) >= 50 else data
    last_10 = data[-10:] if len(data) >= 10 else data
    prev_5 = data[-10:-5] if len(data) >= 10 else []
    last_5 = data[-5:] if len(data) >= 5 else data

    # 10-week average (last 50 days in blocks of 5)
    week_blocks = []
    for i in range(0, min(50, len(data)), 5):
        block = data[-(50 - i):-(50 - i - 5)] if 50 - i - 5 > 0 else data[-(50 - i):]
        if len(block) == 5:
            week_blocks.append(_group_stats(block))

    avg_10w = None
    if week_blocks:
        avg_10w = {
            "avg_fut": round(_avg([w["avg_fut"] for w in week_blocks]), 2),
            "avg_opt": round(_avg([w["avg_opt"] for w in week_blocks]), 2),
            "avg_total": round(_avg([w["avg_total"] for w in week_blocks]), 2),
            "of_ratio": round(_avg([w["of_ratio"] for w in week_blocks if w["of_ratio"]]), 2),
        }

    s_last5 = _group_stats(last_5)
    s_prev5 = _group_stats(prev_5)
    s_last50 = _group_stats(last_50)

    weekly = []
    if s_last5:
        entry = {**s_last5, "label": "Last 5 Trading Days"}
        if s_prev5:
            entry["wow_fut"] = _pct_change(s_last5["avg_fut"], s_prev5["avg_fut"])
            entry["wow_opt"] = _pct_change(s_last5["avg_opt"], s_prev5["avg_opt"])
            entry["wow_total"] = _pct_change(s_last5["avg_total"], s_prev5["avg_total"])
            entry["wow_of"] = _pct_change(s_last5["of_ratio"], s_prev5["of_ratio"]) if s_last5["of_ratio"] and s_prev5.get("of_ratio") else None
        if avg_10w:
            entry["wo10w_fut"] = _pct_change(s_last5["avg_fut"], avg_10w["avg_fut"])
            entry["wo10w_opt"] = _pct_change(s_last5["avg_opt"], avg_10w["avg_opt"])
            entry["wo10w_total"] = _pct_change(s_last5["avg_total"], avg_10w["avg_total"])
            entry["wo10w_of"] = _pct_change(s_last5["of_ratio"], avg_10w["of_ratio"]) if s_last5["of_ratio"] and avg_10w.get("of_ratio") else None
        weekly.append(entry)
    if s_prev5:
        weekly.append({**s_prev5, "label": "Previous 5 Trading Days"})
    if s_last50:
        weekly.append({**s_last50, "label": "Last 50 Trading Days"})

    # ── 5. Day-of-Week ────────────────────────────────────────────────────
    dow_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
    dow_groups = defaultdict(list)
    for r in data:
        if r["dow"] <= 4:
            dow_groups[r["dow"]].append(r)

    day_of_week = []
    for dow in range(5):
        rows_for_dow = dow_groups[dow]
        if not rows_for_dow:
            continue
        latest = rows_for_dow[-1]
        avg_3 = _group_stats(rows_for_dow[-3:]) if len(rows_for_dow) >= 3 else None
        avg_10 = _group_stats(rows_for_dow[-10:]) if len(rows_for_dow) >= 10 else None

        entry = {
            "day": dow_names[dow],
            "latest_fut": latest["fut"], "latest_opt": latest["opt"], "latest_total": latest["total"],
            "latest_of": round(latest["opt"] / latest["fut"], 2) if latest["fut"] > 0 else None,
            "latest_date": latest["date"].isoformat(),
        }
        if avg_3:
            entry["avg3_fut"] = avg_3["avg_fut"]
            entry["avg3_opt"] = avg_3["avg_opt"]
            entry["avg3_total"] = avg_3["avg_total"]
            entry["avg3_of"] = avg_3["of_ratio"]
            entry["var3_total"] = _pct_change(latest["total"], avg_3["avg_total"])
        if avg_10:
            entry["avg10_fut"] = avg_10["avg_fut"]
            entry["avg10_opt"] = avg_10["avg_opt"]
            entry["avg10_total"] = avg_10["avg_total"]
            entry["avg10_of"] = avg_10["of_ratio"]
            entry["var10_total"] = _pct_change(latest["total"], avg_10["avg_total"])
        day_of_week.append(entry)

    # ── 6. Quarter × Day-of-Week ──────────────────────────────────────────
    q_dow_groups = defaultdict(lambda: defaultdict(list))
    for r in data:
        if r["dow"] <= 4:
            ql, _ = _quarter_key(r["date"])
            q_dow_groups[ql][r["dow"]].append(r)

    current_q = _quarter_key(today)[0]
    prev_q_label = _prev_quarter(current_q)
    yoy_q_label = _yoy_quarter(current_q)

    quarter_dow = []
    for dow in range(5):
        entry = {"day": dow_names[dow]}
        for q_label, key_prefix in [(current_q, "cur"), (prev_q_label, "prev"), (yoy_q_label, "yoy")]:
            rows_q = q_dow_groups.get(q_label, {}).get(dow, [])
            stats = _group_stats(rows_q)
            if stats:
                entry[f"{key_prefix}_q"] = q_label
                entry[f"{key_prefix}_fut"] = stats["avg_fut"]
                entry[f"{key_prefix}_opt"] = stats["avg_opt"]
                entry[f"{key_prefix}_total"] = stats["avg_total"]
                entry[f"{key_prefix}_of"] = stats["of_ratio"]
        # QoQ and YoY for total
        if entry.get("cur_total") and entry.get("prev_total"):
            entry["qoq_total"] = _pct_change(entry["cur_total"], entry["prev_total"])
        if entry.get("cur_total") and entry.get("yoy_total"):
            entry["yoy_total_pct"] = _pct_change(entry["cur_total"], entry["yoy_total"])
        quarter_dow.append(entry)

    # ── 7. Daily Trend (last 60 days for chart) ──────────────────────────
    daily_trend = []
    for r in data[-60:]:
        daily_trend.append({
            "date": r["date"].isoformat(),
            "fut": r["fut"], "opt": r["opt"], "total": r["total"],
        })

    return {
        "success": True,
        "as_of": now_ist().strftime("%Y-%m-%d %H:%M IST"),
        "current_fy": current_fy,
        "current_quarter": current_q,
        "fy_summary": fy_summary,
        "quarterly": quarterly,
        "monthly": monthly,
        "weekly": weekly,
        "day_of_week": day_of_week,
        "quarter_dow": quarter_dow,
        "daily_trend": daily_trend,
    }


# ─── Vercel handler ────────────────────────────────────────────────────────

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        origin = self.headers.get("Origin", "")
        cors = make_cors_headers(origin)
        try:
            result = generate_exchange_dashboard()
            self.send_response(200)
            for k, v in cors.items():
                self.send_header(k, v)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "public, max-age=120, s-maxage=120")
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
