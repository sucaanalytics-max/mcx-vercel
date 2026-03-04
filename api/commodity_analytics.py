"""
/api/commodity_analytics — Commodity-level breakdown, sector rotation, momentum

Returns:
  - today: per-commodity signal, turnover, weight, z-scores
  - sector_rotation: 60-day history of sector (commodity_head) weight shifts
  - commodity_momentum: 60-day rolling win rate & momentum per commodity

Data: reads mcx_commodity_signals (pre-computed by cron_commodity_signals).
"""
from http.server import BaseHTTPRequestHandler
import json, math

try:
    from api.mcx_config import (
        SUPABASE_URL, SUPABASE_ANON_KEY, supabase_read,
        now_ist, make_cors_headers,
    )
except ImportError:
    from mcx_config import (
        SUPABASE_URL, SUPABASE_ANON_KEY, supabase_read,
        now_ist, make_cors_headers,
    )


def _f(v):
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _fetch_all(table, select, limit=5000):
    all_rows, offset = [], 0
    while True:
        rows = supabase_read(
            table,
            f"?select={select}&order=trading_date.asc&limit=1000&offset={offset}"
        )
        all_rows.extend(rows)
        if len(rows) < 1000 or len(all_rows) >= limit:
            break
        offset += 1000
    return all_rows


def generate_commodity_analytics():
    ist_now = now_ist()

    # Fetch commodity signals (last ~90 days for rolling calculations)
    signals = _fetch_all(
        "mcx_commodity_signals",
        "trading_date,commodity,commodity_head,"
        "total_turnover_cr,total_oi_value_cr,total_volume_lots,"
        "turnover_zscore,oi_zscore,volume_zscore,"
        "composite_z,commodity_signal,weight_of_turnover",
        limit=5000
    )

    if not signals:
        return {"success": False, "error": "No commodity signals available. Run cron_commodity_signals first."}

    # ── 1. Today's Commodity Lineup ──
    latest_date = max(s["trading_date"] for s in signals)
    today_rows = [s for s in signals if s["trading_date"] == latest_date]
    today_rows.sort(key=lambda x: _f(x.get("total_turnover_cr")) or 0, reverse=True)

    exchange_turnover = sum(_f(s.get("total_turnover_cr")) or 0 for s in today_rows)

    today = {
        "date": latest_date,
        "exchange_turnover_cr": round(exchange_turnover, 2),
        "commodities": [],
    }
    for s in today_rows:
        today["commodities"].append({
            "commodity": s["commodity"],
            "head": s["commodity_head"],
            "turnover_cr": round(_f(s.get("total_turnover_cr")) or 0, 2),
            "weight": round(_f(s.get("weight_of_turnover")) or 0, 4),
            "signal": s.get("commodity_signal"),
            "composite_z": _f(s.get("composite_z")),
            "turnover_z": _f(s.get("turnover_zscore")),
            "oi_z": _f(s.get("oi_zscore")),
            "volume_z": _f(s.get("volume_zscore")),
        })

    # ── 2. Sector Rotation (sector weight over time) ──
    # Group by date, then compute per-head share
    dates = sorted(set(s["trading_date"] for s in signals))
    # Keep last 60 dates
    dates = dates[-60:]

    sector_rotation = []
    for dt in dates:
        day_rows = [s for s in signals if s["trading_date"] == dt]
        total_to = sum(_f(s.get("total_turnover_cr")) or 0 for s in day_rows)
        if total_to <= 0:
            continue

        heads = {}
        for s in day_rows:
            head = s["commodity_head"]
            if head not in heads:
                heads[head] = 0
            heads[head] += _f(s.get("total_turnover_cr")) or 0

        entry = {"date": dt}
        for head, to in heads.items():
            key = head.lower().replace(" ", "_") + "_pct"
            entry[key] = round(to / total_to * 100, 1)
        sector_rotation.append(entry)

    # ── 3. Commodity Momentum (60-day rolling composite trend) ──
    # For each commodity, track last 60 days of composite_z
    commodity_names = sorted(set(s["commodity"] for s in today_rows))
    commodity_momentum = []

    for c in commodity_names:
        c_rows = [s for s in signals if s["commodity"] == c]
        c_rows.sort(key=lambda x: x["trading_date"])
        recent = c_rows[-60:]

        composites = [_f(r.get("composite_z")) for r in recent]
        valid = [z for z in composites if z is not None]

        if len(valid) < 10:
            continue

        # Avg composite z
        avg_z = sum(valid) / len(valid)
        # Trend: positive days / total days
        positive_days = sum(1 for z in valid if z > 0)
        trend = positive_days / len(valid) if valid else 0
        # Latest
        latest_z = composites[-1] if composites[-1] is not None else 0

        commodity_momentum.append({
            "commodity": c,
            "head": recent[-1]["commodity_head"] if recent else "",
            "avg_composite_z": round(avg_z, 3),
            "positive_day_pct": round(trend * 100, 1),
            "latest_z": latest_z,
            "signal": recent[-1].get("commodity_signal") if recent else "NO_DATA",
            "days": len(valid),
        })

    commodity_momentum.sort(key=lambda x: x["avg_composite_z"], reverse=True)

    # ── 4. Top Movers (biggest z-score changes from prior day) ──
    prev_date = dates[-2] if len(dates) >= 2 else None
    top_movers = []
    if prev_date:
        prev_map = {s["commodity"]: _f(s.get("composite_z"))
                    for s in signals if s["trading_date"] == prev_date}
        for s in today_rows:
            c = s["commodity"]
            curr_z = _f(s.get("composite_z"))
            prev_z = prev_map.get(c)
            if curr_z is not None and prev_z is not None:
                delta = round(curr_z - prev_z, 3)
                top_movers.append({
                    "commodity": c,
                    "head": s["commodity_head"],
                    "prev_z": prev_z,
                    "curr_z": curr_z,
                    "delta_z": delta,
                    "signal": s.get("commodity_signal"),
                })
        top_movers.sort(key=lambda x: abs(x["delta_z"]), reverse=True)

    return {
        "success": True,
        "as_of": ist_now.strftime("%Y-%m-%d %H:%M IST"),
        "today": today,
        "sector_rotation": sector_rotation,
        "commodity_momentum": commodity_momentum,
        "top_movers": top_movers[:10],
        "data_quality": {
            "signal_rows": len(signals),
            "commodities_today": len(today_rows),
            "rotation_days": len(sector_rotation),
            "latest_date": latest_date,
        },
    }


# ─── Vercel handler ──────────────────────────────────────────────────────

class handler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        pass

    def _cors(self):
        origin = self.headers.get("Origin", "")
        hdrs = make_cors_headers(origin)
        for k, v in hdrs.items():
            self.send_header(k, v)
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def send_json(self, data, status=200):
        body = json.dumps(data, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        try:
            result = generate_commodity_analytics()
            self.send_json(result)
        except Exception as e:
            self.send_json({"success": False, "error": str(e)[:200]}, 500)
