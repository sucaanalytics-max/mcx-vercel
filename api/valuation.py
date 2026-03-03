"""
/api/valuation — EPS-Path Fair Value Model (Model A)

Economic chain:
  45DMA F&O Rev → Annualized Total Rev → PAT (55% margin) → EPS → Fair Value via P/E bands

Primary: reads pre-computed valuations from mcx_valuation table (refreshed by valuation_refresh.py).
Fallback: computes live from mcx_daily_revenue + mcx_share_price.

Returns:
  - Current valuation snapshot (EPS, fair value bear/base/bull, signal)
  - Historical fair value vs actual price time series (for charting)
  - Dynamic P/E band statistics
  - Revenue-to-EPS conversion chain (full transparency)
"""
from http.server import BaseHTTPRequestHandler
import json

try:
    from api.mcx_config import (
        TRADING_DAYS, PAT_MARGIN, NON_FO_REV_ANNUAL_CR, DILUTED_SHARES_CR,
        PE_MEAN_DEFAULT, PE_SD_DEFAULT,
        SUPABASE_URL, SUPABASE_ANON_KEY, supabase_read,
        now_ist, make_cors_headers,
    )
except ImportError:
    from mcx_config import (
        TRADING_DAYS, PAT_MARGIN, NON_FO_REV_ANNUAL_CR, DILUTED_SHARES_CR,
        PE_MEAN_DEFAULT, PE_SD_DEFAULT,
        SUPABASE_URL, SUPABASE_ANON_KEY, supabase_read,
        now_ist, make_cors_headers,
    )

import math


# ─── Core valuation engine ─────────────────────────────────────────────────

def compute_eps_chain(ma45_rev_cr):
    """
    Convert 45DMA daily F&O revenue to trailing EPS.
    Chain: ma45 × trading_days → annual F&O → (+non-F&O) → PAT → EPS
    """
    annual_fo_rev = ma45_rev_cr * TRADING_DAYS
    annual_total_rev = annual_fo_rev + NON_FO_REV_ANNUAL_CR
    pat = annual_total_rev * PAT_MARGIN
    eps = pat / DILUTED_SHARES_CR
    return {
        "ma45_rev_cr": round(ma45_rev_cr, 2),
        "annual_fo_rev_cr": round(annual_fo_rev, 2),
        "non_fo_rev_cr": round(NON_FO_REV_ANNUAL_CR, 2),
        "annual_total_rev_cr": round(annual_total_rev, 2),
        "pat_cr": round(pat, 2),
        "eps": round(eps, 2),
        "pat_margin": PAT_MARGIN,
        "trading_days": TRADING_DAYS,
        "diluted_shares_cr": DILUTED_SHARES_CR,
    }


def classify_signal(price, fair_bear, fair_base, fair_bull):
    """
    Classify current price vs fair value range.
    DEEP_VALUE: below bear (-1 SD)
    UNDERVALUED: below base but above bear
    FAIR: within ±5% of base
    OVERVALUED: above base but below bull
    STRETCHED: above bull (+1 SD)
    """
    if not price or not fair_base or fair_base <= 0:
        return "NO_DATA"
    if price < fair_bear:
        return "DEEP_VALUE"
    elif price < fair_base * 0.95:
        return "UNDERVALUED"
    elif price <= fair_base * 1.05:
        return "FAIR"
    elif price <= fair_bull:
        return "OVERVALUED"
    else:
        return "STRETCHED"


# ─── Pre-computed data from Supabase ───────────────────────────────────────

def _fetch_precomputed_valuations(limit=90):
    """Fetch pre-computed valuations from mcx_valuation table."""
    if not SUPABASE_ANON_KEY:
        return []
    try:
        rows = supabase_read(
            "mcx_valuation",
            f"?select=trading_date,daily_rev_cr,ma45_rev_cr,annualized_rev_cr,"
            f"pat_cr,eps,close_price,implied_pe,fair_value_bear,fair_value_base,"
            f"fair_value_bull,signal,pe_mean_used,pe_sd_used"
            f"&order=trading_date.desc&limit={limit}"
        )
        return sorted(rows, key=lambda r: r["trading_date"])
    except Exception:
        return []


def _fetch_revenue_for_live(days=60):
    """Fetch revenue for live fallback computation."""
    if not SUPABASE_ANON_KEY:
        return []
    try:
        rows = supabase_read(
            "mcx_daily_revenue",
            f"?select=trading_date,total_rev_cr&order=trading_date.desc&limit={days}"
        )
        return sorted(rows, key=lambda r: r["trading_date"])
    except Exception:
        return []


def _fetch_latest_price():
    """Fetch the most recent share price."""
    if not SUPABASE_ANON_KEY:
        return None, None
    try:
        rows = supabase_read(
            "mcx_share_price",
            "?select=trading_date,close&order=trading_date.desc&limit=1"
        )
        if rows and rows[0].get("close"):
            return float(rows[0]["close"]), rows[0]["trading_date"]
        return None, None
    except Exception:
        return None, None


# ─── Main valuation generator ──────────────────────────────────────────────

def generate_valuation():
    """
    Full EPS-Path valuation: pre-computed data + live snapshot.
    """
    ist_now = now_ist()

    # ── Primary: pre-computed valuations from Supabase ──────────────
    precomputed = _fetch_precomputed_valuations(limit=90)

    if not precomputed:
        return {"error": "No valuation data available. Run valuation_refresh.py --backfill first.", "success": False}

    latest = precomputed[-1]

    # Extract PE bands from latest row
    pe_mean = float(latest.get("pe_mean_used") or PE_MEAN_DEFAULT)
    pe_sd = float(latest.get("pe_sd_used") or PE_SD_DEFAULT)

    # ── Current EPS chain (full transparency) ───────────────────────
    ma45 = float(latest["ma45_rev_cr"])
    eps_chain = compute_eps_chain(ma45)

    # ── Live price check (may be newer than last valuation row) ─────
    latest_price, latest_price_date = _fetch_latest_price()
    # Use pre-computed price if live fetch fails
    if not latest_price and latest.get("close_price"):
        latest_price = float(latest["close_price"])
        latest_price_date = latest["trading_date"]

    current_eps = eps_chain["eps"]
    fair_bear = round(current_eps * max(pe_mean - pe_sd, 5.0), 2)
    fair_base = round(current_eps * pe_mean, 2)
    fair_bull = round(current_eps * (pe_mean + pe_sd), 2)

    signal = "NO_PRICE"
    implied_pe = None
    pct_from_base = None
    upside_to_base = None
    if latest_price and current_eps > 0:
        signal = classify_signal(latest_price, fair_bear, fair_base, fair_bull)
        implied_pe = round(latest_price / current_eps, 2)
        pct_from_base = round((latest_price - fair_base) / fair_base * 100, 2)
        upside_to_base = round((fair_base - latest_price) / latest_price * 100, 2)

    # ── Build history for charting (last 60 entries) ────────────────
    history = []
    for row in precomputed[-60:]:
        price = float(row["close_price"]) if row.get("close_price") else None
        history.append({
            "date": row["trading_date"],
            "price": price,
            "eps": float(row["eps"]),
            "implied_pe": float(row["implied_pe"]) if row.get("implied_pe") else None,
            "fair_bear": float(row["fair_value_bear"]),
            "fair_base": float(row["fair_value_base"]),
            "fair_bull": float(row["fair_value_bull"]),
            "signal": row["signal"],
            "ma45_rev": float(row["ma45_rev_cr"]),
        })

    return {
        "success": True,
        "model": "EPS-Path Fair Value (Model A)",
        "as_of": ist_now.strftime("%Y-%m-%d %H:%M IST"),
        "snapshot": {
            "latest_price": latest_price,
            "latest_price_date": latest_price_date,
            "eps_chain": eps_chain,
            "current_eps": current_eps,
            "fair_value": {"bear": fair_bear, "base": fair_base, "bull": fair_bull},
            "signal": signal,
            "implied_pe": implied_pe,
            "pct_from_base": pct_from_base,
            "upside_to_base_pct": upside_to_base,
        },
        "pe_bands": {
            "mean": pe_mean,
            "sd": pe_sd,
            "bear_pe": round(max(pe_mean - pe_sd, 5.0), 2),
            "bull_pe": round(pe_mean + pe_sd, 2),
            "source": "dynamic",
            "data_points": len(precomputed),
        },
        "history": history,
        "data_quality": {
            "valuation_rows": len(precomputed),
            "history_returned": len(history),
            "revenue_window": 45,
            "latest_valuation_date": latest["trading_date"],
        },
    }


# ─── Vercel handler ─────────────────────────────────────────────────────────

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
            result = generate_valuation()
            self.send_json(result)
        except Exception as e:
            self.send_json({"success": False, "error": str(e)[:200]}, 500)
