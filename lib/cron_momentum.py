"""
/api/cron_momentum — Daily Momentum Entry/Exit Signal refresh

Computes two signal layers from raw exchange data:
  Layer 1: F&O Revenue Regime (10D/45D MA ratio → HOT / NEUTRAL / COLD)
  Layer 2: ADR Divergence Overlay (price volatility + momentum → BREAKOUT / BULL_CONT / OVERSOLD / NEUTRAL)
  Composite: STRONG_BUY / BUY / HOLD / WATCH / SELL

Data sources: mcx_daily_revenue (total_rev_cr) + mcx_share_price (close)
Output: mcx_momentum_signals table

Vercel Cron: runs after cron_valuation (which refreshes share prices first).
"""
from http.server import BaseHTTPRequestHandler
import json, urllib.request, urllib.error
from urllib.parse import urlparse, parse_qs

try:
    from lib.mcx_config import (
        SUPABASE_URL, SUPABASE_ANON_KEY,
        now_ist, make_cors_headers,
    )
except ImportError:
    from lib.mcx_config import (
        SUPABASE_URL, SUPABASE_ANON_KEY,
        now_ist, make_cors_headers,
    )

import os

CRON_SECRET = os.environ.get("CRON_SECRET", "")

# ─── Moving average windows ───────────────────────────────────────────────
MA_SHORT = 10     # 10-day revenue MA (short-term activity)
MA_LONG  = 45     # 45-day revenue MA (long-term baseline)

# ─── Regime thresholds ────────────────────────────────────────────────────
REGIME_HOT_THRESHOLD  = 1.05   # ratio > 1.05 → HOT
REGIME_COLD_THRESHOLD = 0.95   # ratio < 0.95 → COLD

# ─── ADR windows ──────────────────────────────────────────────────────────
ADR_SHORT = 5     # 5-day average daily range (short-term volatility)
ADR_LONG  = 20    # 20-day average daily range (medium-term baseline)
PRICE_MOM_WINDOW = 5  # 5-day price momentum lookback

# ─── ADR signal thresholds ────────────────────────────────────────────────
BREAKOUT_MOM   = 0.015   # price mom > +1.5%
BREAKOUT_ADR   = 1.50    # ADR ratio > 1.50
BULLCONT_MOM   = 0.01    # price mom > +1.0%
BULLCONT_ADR   = 0.80    # ADR ratio < 0.80
OVERSOLD_MOM   = -0.01   # price mom < -1.0%
OVERSOLD_ADR   = 1.30    # ADR ratio > 1.30


# ─── Supabase helpers (self-contained, matching cron_models.py) ───────────

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


# ─── Core computation ─────────────────────────────────────────────────────

def _classify_regime(ratio):
    """Classify revenue regime from 10D/45D ratio."""
    if ratio > REGIME_HOT_THRESHOLD:
        return "HOT"
    elif ratio < REGIME_COLD_THRESHOLD:
        return "COLD"
    return "NEUTRAL"


def _classify_adr_signal(price_mom, adr_ratio):
    """Classify ADR divergence signal from price momentum and ADR ratio."""
    if price_mom > BREAKOUT_MOM and adr_ratio > BREAKOUT_ADR:
        return "BREAKOUT"
    if price_mom > BULLCONT_MOM and adr_ratio < BULLCONT_ADR:
        return "BULL_CONT"
    if price_mom < OVERSOLD_MOM and adr_ratio > OVERSOLD_ADR:
        return "OVERSOLD"
    return "NEUTRAL"


def _classify_composite(regime, adr_signal):
    """Combine regime and ADR signal into composite entry/exit signal."""
    if regime == "HOT":
        if adr_signal in ("BULL_CONT", "BREAKOUT", "NEUTRAL"):
            return "STRONG_BUY"
        # HOT + OVERSOLD
        return "BUY"
    elif regime == "NEUTRAL":
        if adr_signal in ("BULL_CONT", "BREAKOUT"):
            return "BUY"
        # NEUTRAL + NEUTRAL or NEUTRAL + OVERSOLD
        return "HOLD"
    else:  # COLD
        if adr_signal == "OVERSOLD":
            return "WATCH"
        # COLD + NEUTRAL, BREAKOUT, BULL_CONT
        return "SELL"


def compute_momentum(mode="recent"):
    log = []

    # Fetch raw data
    rev_rows = fetch_all("mcx_daily_revenue", "trading_date,total_rev_cr")
    log.append(f"Revenue rows: {len(rev_rows)}")

    price_rows = fetch_all("mcx_share_price", "trading_date,close")
    log.append(f"Price rows: {len(price_rows)}")

    # Build lookup maps
    rev_map = {}
    for r in rev_rows:
        rev = _f(r.get("total_rev_cr"))
        if rev is not None:
            rev_map[r["trading_date"]] = rev

    price_map = {}
    for p in price_rows:
        close = _f(p.get("close"))
        if close is not None and close > 0:
            price_map[p["trading_date"]] = close

    # Build aligned date series (intersection, sorted)
    common_dates = sorted(set(rev_map.keys()) & set(price_map.keys()))
    log.append(f"Aligned dates: {len(common_dates)}")

    if len(common_dates) < MA_LONG + 1:
        return {"success": False, "error": f"Need {MA_LONG + 1}+ aligned dates, have {len(common_dates)}", "log": log}

    # Extract aligned arrays
    dates = common_dates
    revs = [rev_map[d] for d in dates]
    prices = [price_map[d] for d in dates]

    # Compute daily ranges (need i >= 1)
    daily_ranges = [None]  # first day has no previous
    for i in range(1, len(dates)):
        daily_ranges.append(abs(prices[i] - prices[i - 1]))

    # Compute all signals
    results = []
    # Minimum index: MA_LONG-1 (for 45-day MA) = index 44
    # Also need 20 daily ranges for ADR 20D, and 5 for price momentum
    # The binding constraint is MA_LONG - 1 = 44 (since ranges need i >= 20 which is < 44)
    start_idx = MA_LONG - 1  # index 44

    for i in range(start_idx, len(dates)):
        # Layer 1: Revenue Regime
        window_short = revs[i - MA_SHORT + 1: i + 1]
        window_long = revs[i - MA_LONG + 1: i + 1]
        ma10 = sum(window_short) / len(window_short)
        ma45 = sum(window_long) / len(window_long)

        if ma45 == 0:
            continue

        ratio = ma10 / ma45
        regime = _classify_regime(ratio)

        # Layer 2: ADR Divergence
        # Daily range at index i
        dr = daily_ranges[i]

        # ADR 5D: average of daily_ranges[i-4 .. i]
        adr5_window = [daily_ranges[j] for j in range(max(1, i - ADR_SHORT + 1), i + 1)
                       if daily_ranges[j] is not None]
        adr5 = sum(adr5_window) / len(adr5_window) if len(adr5_window) >= ADR_SHORT else None

        # ADR 20D: average of daily_ranges[i-19 .. i]
        adr20_window = [daily_ranges[j] for j in range(max(1, i - ADR_LONG + 1), i + 1)
                        if daily_ranges[j] is not None]
        adr20 = sum(adr20_window) / len(adr20_window) if len(adr20_window) >= ADR_LONG else None

        # ADR ratio
        adr_ratio = (adr5 / adr20) if (adr5 is not None and adr20 is not None and adr20 > 0) else None

        # Price momentum 5D
        if i >= PRICE_MOM_WINDOW and prices[i - PRICE_MOM_WINDOW] > 0:
            price_mom = (prices[i] - prices[i - PRICE_MOM_WINDOW]) / prices[i - PRICE_MOM_WINDOW]
        else:
            price_mom = None

        # ADR signal
        if price_mom is not None and adr_ratio is not None:
            adr_signal = _classify_adr_signal(price_mom, adr_ratio)
        else:
            adr_signal = None

        # Composite signal
        if adr_signal is not None:
            composite = _classify_composite(regime, adr_signal)
        else:
            composite = None

        results.append({
            "trading_date": dates[i],
            "fno_rev_cr": round(revs[i], 4),
            "close_price": round(prices[i], 2),
            "ma10_rev_cr": round(ma10, 4),
            "ma45_rev_cr": round(ma45, 4),
            "ratio_10d_45d": round(ratio, 4),
            "regime": regime,
            "daily_range": round(dr, 2) if dr is not None else None,
            "adr_5d": round(adr5, 4) if adr5 is not None else None,
            "adr_20d": round(adr20, 4) if adr20 is not None else None,
            "adr_ratio": round(adr_ratio, 4) if adr_ratio is not None else None,
            "price_mom_5d": round(price_mom, 4) if price_mom is not None else None,
            "adr_signal": adr_signal,
            "composite_signal": composite,
        })

    log.append(f"Computed signals: {len(results)}")

    # Filter by mode
    if mode == "latest":
        results = results[-1:] if results else []
    elif mode == "recent":
        results = results[-30:] if results else []
    # "backfill" = all

    log.append(f"Upserting {len(results)} rows ({mode})")
    errors = sb_upsert("mcx_momentum_signals", results)
    if errors:
        log.extend([f"Error: {e}" for e in errors])

    latest = results[-1] if results else {}
    return {
        "success": len(errors) == 0,
        "mode": mode,
        "rows_upserted": len(results),
        "latest": {
            "date": latest.get("trading_date"),
            "regime": latest.get("regime"),
            "ratio": latest.get("ratio_10d_45d"),
            "adr_signal": latest.get("adr_signal"),
            "composite_signal": latest.get("composite_signal"),
        },
        "log": log,
        "errors": errors,
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
            result = compute_momentum(mode=mode)
            result["as_of"] = now_ist().strftime("%Y-%m-%d %H:%M IST")
            self.send_json(result, 200 if result.get("success") else 500)
        except Exception as e:
            self.send_json({"success": False, "error": str(e)[:200]}, 500)
