"""
/api/models — Multi-Model Signal Dashboard (Models A + B + C + Ensemble)

Returns:
  - Latest snapshot: all model signals, z-scores, ensemble recommendation
  - History: 60-day time series for charting (ECM spread, MF factors, ensemble)
  - Model metadata: descriptions, factor weights, window sizes

Data: pre-computed in mcx_model_signals table (refreshed by cron_models.py).
"""
from http.server import BaseHTTPRequestHandler
import json

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


# ─── Model metadata ─────────────────────────────────────────────────────

MODEL_META = {
    "B": {
        "name": "Error Correction Model (ECM)",
        "description": "Measures price deviation from Model A fair value. "
                       "Z-score of the spread identifies statistically extreme "
                       "divergences likely to mean-revert.",
        "window": 60,
        "signals": {
            "STRONG_REVERT_UP": "Price far below fair value — strong reversion expected",
            "MILD_REVERT_UP": "Price moderately below — mild upward reversion likely",
            "NEUTRAL": "Spread near historical average — no directional pressure",
            "MILD_EXTEND_DOWN": "Price moderately above — mild downward pressure",
            "STRONG_EXTEND_DOWN": "Price far above fair value — strong downward reversion expected",
        },
    },
    "C": {
        "name": "Multi-Factor Momentum",
        "description": "Composite score from 2 exchange-level factors: "
                       "daily revenue (43%) and total turnover (57%). "
                       "Phase 3 analysis dropped volume and intraday volatility "
                       "(zero marginal IC contribution). "
                       "Positive composite = bullish exchange activity momentum.",
        "window": 60,
        "weights": {"revenue": 0.4286, "turnover": 0.5714},
        "signals": {
            "STRONG_BUY": "All factors strongly positive",
            "BUY": "Net positive factor momentum",
            "NEUTRAL": "Mixed or average factor readings",
            "SELL": "Net negative factor momentum",
            "STRONG_SELL": "All factors strongly negative",
        },
    },
    "ensemble": {
        "name": "Ensemble Signal",
        "description": "Blended recommendation: ECM reversion (30%) + "
                       "Multi-Factor momentum (70%). "
                       "Expands to: Revenue 30% + Turnover 40% + ECM 30%.",
        "signals": {
            "STRONG_BUY": "Undervalued + reversion likely + positive momentum",
            "BUY": "Net positive across models",
            "NEUTRAL": "Conflicting or balanced signals",
            "SELL": "Net negative across models",
            "STRONG_SELL": "Overvalued + extension likely + negative momentum",
        },
    },
}


def _fetch_model_signals(limit=90):
    """Fetch pre-computed model signals from Supabase."""
    if not SUPABASE_ANON_KEY:
        return []
    try:
        rows = supabase_read(
            "mcx_model_signals",
            f"?select=trading_date,close_price,fair_value_base,"
            f"ecm_spread,ecm_spread_pct,ecm_spread_zscore,ecm_half_life_days,ecm_signal,"
            f"mf_revenue_z,mf_turnover_z,mf_volume_z,mf_volatility_z,mf_composite_z,mf_signal,"
            f"ensemble_score,ensemble_signal"
            f"&order=trading_date.desc&limit={limit}"
        )
        return sorted(rows, key=lambda r: r["trading_date"])
    except Exception:
        return []


def generate_models_response():
    ist_now = now_ist()
    rows = _fetch_model_signals(limit=90)

    if not rows:
        return {"success": False, "error": "No model signals available. Run cron_models first."}

    latest = rows[-1]

    # Build snapshot
    snapshot = {
        "date": latest["trading_date"],
        "price": _f(latest.get("close_price")),
        "fair_value_base": _f(latest.get("fair_value_base")),
        "ecm": {
            "spread": _f(latest.get("ecm_spread")),
            "spread_pct": _f(latest.get("ecm_spread_pct")),
            "z_score": _f(latest.get("ecm_spread_zscore")),
            "half_life_days": _f(latest.get("ecm_half_life_days")),
            "signal": latest.get("ecm_signal"),
        },
        "multi_factor": {
            "revenue_z": _f(latest.get("mf_revenue_z")),
            "turnover_z": _f(latest.get("mf_turnover_z")),
            "volume_z": _f(latest.get("mf_volume_z")),
            "volatility_z": _f(latest.get("mf_volatility_z")),
            "composite_z": _f(latest.get("mf_composite_z")),
            "signal": latest.get("mf_signal"),
        },
        "ensemble": {
            "score": _f(latest.get("ensemble_score")),
            "signal": latest.get("ensemble_signal"),
        },
    }

    # Build history (last 60 entries for charting)
    history = []
    for r in rows[-60:]:
        history.append({
            "date": r["trading_date"],
            "price": _f(r.get("close_price")),
            "fair_value": _f(r.get("fair_value_base")),
            "ecm_spread_pct": _f(r.get("ecm_spread_pct")),
            "ecm_z": _f(r.get("ecm_spread_zscore")),
            "ecm_signal": r.get("ecm_signal"),
            "mf_revenue_z": _f(r.get("mf_revenue_z")),
            "mf_turnover_z": _f(r.get("mf_turnover_z")),
            "mf_volume_z": _f(r.get("mf_volume_z")),
            "mf_volatility_z": _f(r.get("mf_volatility_z")),
            "mf_composite_z": _f(r.get("mf_composite_z")),
            "mf_signal": r.get("mf_signal"),
            "ensemble_score": _f(r.get("ensemble_score")),
            "ensemble_signal": r.get("ensemble_signal"),
        })

    return {
        "success": True,
        "as_of": ist_now.strftime("%Y-%m-%d %H:%M IST"),
        "snapshot": snapshot,
        "history": history,
        "models": MODEL_META,
        "data_quality": {
            "total_rows": len(rows),
            "history_returned": len(history),
            "latest_date": latest["trading_date"],
            "rolling_window": 60,
        },
    }


def _f(v):
    """Safe float conversion."""
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


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
            result = generate_models_response()
            self.send_json(result)
        except Exception as e:
            self.send_json({"success": False, "error": str(e)[:200]}, 500)
