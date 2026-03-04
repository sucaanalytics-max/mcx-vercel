"""
/api/cron_commodity_signals — Daily refresh: commodity-level z-scores & signals

Reads mcx_commodity_daily, aggregates by (date, commodity), computes rolling
60-day z-scores on turnover, OI, and volume, then upserts to mcx_commodity_signals.

Schedule: 19:50 IST (after cron_models at 19:45).
"""
from http.server import BaseHTTPRequestHandler
import json, math, urllib.request, urllib.error
from urllib.parse import urlparse, parse_qs

try:
    from api._mcx_config import (
        SUPABASE_URL, SUPABASE_ANON_KEY,
        now_ist, make_cors_headers,
    )
except ImportError:
    from _mcx_config import (
        SUPABASE_URL, SUPABASE_ANON_KEY,
        now_ist, make_cors_headers,
    )

import os

CRON_SECRET = os.environ.get("CRON_SECRET", "")
ROLLING_WINDOW = 60

# Top commodities by turnover — skip tiny ones to keep compute fast
MIN_AVG_TURNOVER_CR = 50  # minimum avg daily turnover to process


def sb_get(table, params=""):
    url = f"{SUPABASE_URL}/rest/v1/{table}{params}"
    req = urllib.request.Request(url, headers={
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
    })
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode())


def sb_upsert(table, rows):
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }
    errors = []
    for i in range(0, len(rows), 50):
        chunk = rows[i:i + 50]
        body = json.dumps(chunk).encode()
        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                pass
        except urllib.error.HTTPError as e:
            err_body = e.read().decode()[:200] if e.fp else ""
            errors.append(f"batch {i}: HTTP {e.code} — {err_body}")
    return errors


def fetch_all(table, select, order="trading_date"):
    all_rows, offset = [], 0
    while True:
        rows = sb_get(table, f"?select={select}&order={order}.asc&limit=1000&offset={offset}")
        all_rows.extend(rows)
        if len(rows) < 1000:
            break
        offset += 1000
    return all_rows


def _f(v):
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _zscore(values, idx, window):
    """Rolling z-score for values[idx] over `window` trailing observations."""
    start = max(0, idx - window + 1)
    w = [v for v in values[start:idx + 1] if v is not None]
    if len(w) < 30:
        return None
    mean = sum(w) / len(w)
    var = sum((x - mean) ** 2 for x in w) / len(w)
    sd = math.sqrt(var) if var > 0 else 0
    if sd == 0:
        return None
    return round((values[idx] - mean) / sd, 3)


def compute_commodity_signals(mode="recent"):
    log = []

    # Fetch raw commodity daily data
    raw = fetch_all(
        "mcx_commodity_daily",
        "trading_date,commodity,commodity_head,instrument_type,"
        "contracts,volume_lots,turnover_cr,premium_turnover_cr,"
        "open_interest,oi_value_cr"
    )
    log.append(f"Raw commodity rows: {len(raw)}")

    # ── Aggregate by (trading_date, commodity) ──
    # Some commodities have separate Futures + Options rows per day
    agg = {}
    for r in raw:
        key = (r["trading_date"], r["commodity"])
        if key not in agg:
            agg[key] = {
                "trading_date": r["trading_date"],
                "commodity": r["commodity"],
                "commodity_head": r["commodity_head"],
                "total_contracts": 0,
                "total_volume_lots": 0,
                "total_turnover_cr": 0.0,
                "total_premium_cr": 0.0,
                "total_open_interest": 0,
                "total_oi_value_cr": 0.0,
            }
        a = agg[key]
        a["total_contracts"] += int(r.get("contracts") or 0)
        a["total_volume_lots"] += int(r.get("volume_lots") or 0)
        a["total_turnover_cr"] += float(r.get("turnover_cr") or 0)
        a["total_premium_cr"] += float(r.get("premium_turnover_cr") or 0)
        a["total_open_interest"] += int(r.get("open_interest") or 0)
        a["total_oi_value_cr"] += float(r.get("oi_value_cr") or 0)

    # Group aggregated rows by commodity, sorted by date
    by_commodity = {}
    for key, row in agg.items():
        c = row["commodity"]
        if c not in by_commodity:
            by_commodity[c] = []
        by_commodity[c].append(row)

    for c in by_commodity:
        by_commodity[c].sort(key=lambda x: x["trading_date"])

    # Filter out commodities with too few rows or too little turnover
    filtered = {}
    for c, hist in by_commodity.items():
        if len(hist) < 60:
            continue
        avg_to = sum(h["total_turnover_cr"] for h in hist) / len(hist)
        if avg_to < MIN_AVG_TURNOVER_CR:
            continue
        filtered[c] = hist

    log.append(f"Commodities passing filter: {len(filtered)} of {len(by_commodity)}")

    # ── Compute daily exchange totals for weight calculation ──
    daily_totals = {}  # date → total turnover across all commodities
    for c, hist in by_commodity.items():
        for h in hist:
            dt = h["trading_date"]
            daily_totals[dt] = daily_totals.get(dt, 0) + h["total_turnover_cr"]

    # ── Compute z-scores for each commodity ──
    results = []
    for c, hist in filtered.items():
        turnovers = [h["total_turnover_cr"] for h in hist]
        ois = [float(h["total_open_interest"]) for h in hist]
        volumes = [float(h["total_volume_lots"]) for h in hist]

        # Determine slice based on mode
        if mode == "backfill":
            process_range = range(len(hist))
        elif mode == "recent":
            process_range = range(max(0, len(hist) - 30), len(hist))
        else:  # latest
            process_range = range(len(hist) - 1, len(hist))

        for i in process_range:
            h = hist[i]
            dt = h["trading_date"]

            to_z = _zscore(turnovers, i, ROLLING_WINDOW)
            oi_z = _zscore(ois, i, ROLLING_WINDOW)
            vol_z = _zscore(volumes, i, ROLLING_WINDOW)

            # Composite = average of available z-scores
            zs = [z for z in [to_z, oi_z, vol_z] if z is not None]
            composite = round(sum(zs) / len(zs), 3) if zs else None

            # Discrete signal
            signal = "NO_DATA"
            if composite is not None:
                if composite > 1.5:
                    signal = "STRONG_BUY"
                elif composite > 0.5:
                    signal = "BUY"
                elif composite > -0.5:
                    signal = "NEUTRAL"
                elif composite > -1.5:
                    signal = "SELL"
                else:
                    signal = "STRONG_SELL"

            # Weight of daily exchange turnover
            daily_total = daily_totals.get(dt, 0)
            weight = round(h["total_turnover_cr"] / daily_total, 4) if daily_total > 0 else 0

            results.append({
                "trading_date": dt,
                "commodity": c,
                "commodity_head": h["commodity_head"],
                "total_contracts": h["total_contracts"],
                "total_volume_lots": h["total_volume_lots"],
                "total_turnover_cr": round(h["total_turnover_cr"], 2),
                "total_premium_cr": round(h["total_premium_cr"], 2),
                "total_open_interest": h["total_open_interest"],
                "total_oi_value_cr": round(h["total_oi_value_cr"], 2),
                "turnover_zscore": to_z,
                "oi_zscore": oi_z,
                "volume_zscore": vol_z,
                "composite_z": composite,
                "commodity_signal": signal,
                "weight_of_turnover": weight,
            })

    log.append(f"Upserting {len(results)} rows ({mode})")
    errors = sb_upsert("mcx_commodity_signals", results)
    if errors:
        log.extend([f"Error: {e}" for e in errors])

    # Summarise latest signals
    latest_date = max(r["trading_date"] for r in results) if results else None
    latest_signals = {r["commodity"]: r["commodity_signal"]
                      for r in results if r["trading_date"] == latest_date}

    return {
        "success": len(errors) == 0,
        "mode": mode,
        "commodities_processed": len(filtered),
        "rows_upserted": len(results),
        "latest_date": latest_date,
        "latest_signals": latest_signals,
        "log": log,
        "errors": errors,
    }


class handler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        pass

    def _cors(self):
        origin = self.headers.get("Origin", "")
        hdrs = make_cors_headers(origin)
        for k, v in hdrs.items():
            self.send_header(k, v)

    def send_json(self, data, status=200):
        body = json.dumps(data, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def _check_auth(self):
        auth_header = self.headers.get("Authorization", "")
        if auth_header.startswith("Bearer ") and auth_header[7:] == CRON_SECRET:
            return True
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)
        if qs.get("secret", [None])[0] == CRON_SECRET:
            return True
        if not CRON_SECRET:
            return True
        return False

    def do_GET(self):
        if not self._check_auth():
            self.send_json({"success": False, "error": "Unauthorized"}, 401)
            return

        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)
        mode = qs.get("mode", ["recent"])[0]
        if mode not in ("recent", "latest", "backfill"):
            mode = "recent"

        try:
            result = compute_commodity_signals(mode=mode)
            result["as_of"] = now_ist().strftime("%Y-%m-%d %H:%M IST")
            self.send_json(result, 200 if result.get("success") else 500)
        except Exception as e:
            self.send_json({"success": False, "error": str(e)[:200]}, 500)
