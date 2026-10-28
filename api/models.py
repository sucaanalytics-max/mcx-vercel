"""
/api/models — Multi-Model Signal Dashboard (Models A + B + C + Ensemble)

Returns:
  - Latest snapshot: all model signals, z-scores, ensemble recommendation
  - History: 60-day time series for charting (ECM spread, MF factors, ensemble)
  - Model metadata: descriptions, factor weights, window sizes

Data: pre-computed in mcx_model_signals table (refreshed by cron_models.py).
"""
from http.server import BaseHTTPRequestHandler
import json, math

try:
    from lib.mcx_config import (
        SUPABASE_URL, SUPABASE_ANON_KEY, supabase_read, supabase_read_all,
        now_ist, make_cors_headers,
    )
except ImportError:
    from lib.mcx_config import (
        SUPABASE_URL, SUPABASE_ANON_KEY, supabase_read, supabase_read_all,
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


# Range key → trading-day window. Used both to size the Supabase fetch and to
# slice the tail of the sorted result for chart history.
RANGE_DAYS = {
    "30D": 30,
    "60D": 60,
    "Q":   63,
    "1Y":  252,
    "2Y":  504,
    "Max": None,  # all rows
}
DEFAULT_RANGE = "60D"


def _fetch_model_signals(limit=120):
    """Fetch pre-computed model signals from Supabase. limit=None fetches all."""
    if not SUPABASE_ANON_KEY:
        return []
    try:
        q = (
            f"?select=trading_date,close_price,fair_value_base,"
            f"ecm_spread,ecm_spread_pct,ecm_spread_zscore,ecm_half_life_days,ecm_signal,"
            f"mf_revenue_z,mf_turnover_z,mf_volume_z,mf_volatility_z,mf_composite_z,mf_signal,"
            f"ensemble_score,ensemble_signal,"
            f"position_score,conviction,signal_momentum,position_velocity,conviction_2d_ma"
            f"&order=trading_date.desc"
        )
        if limit is not None:
            q += f"&limit={limit}"
            rows = supabase_read("mcx_model_signals", q)
        else:
            rows = supabase_read_all("mcx_model_signals", q, max_rows=5000)
        rows = [r for r in rows if (r.get("trading_date") or "") >= "2020-01-01"]
        return sorted(rows, key=lambda r: r["trading_date"])
    except Exception:
        return []


def generate_models_response(range_key=DEFAULT_RANGE):
    if range_key not in RANGE_DAYS:
        range_key = DEFAULT_RANGE
    window = RANGE_DAYS[range_key]
    # +60 warm-up rows so the rolling σ-bands are well-formed at the window start
    fetch_limit = None if window is None else max(120, window + 60)

    ist_now = now_ist()
    rows = _fetch_model_signals(limit=fetch_limit)

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
        "position": {
            "score": _f(latest.get("position_score")),
            "conviction": _f(latest.get("conviction")),
            "momentum": _f(latest.get("signal_momentum")),
            "velocity_1d": _f(latest.get("position_velocity")),
            "conviction_2d_ma": _f(latest.get("conviction_2d_ma")),
        },
    }

    # Build history (sized by range window)
    # Pre-extract all spreads from the full fetch for rolling band computation
    all_spreads = [_f(r.get("ecm_spread_pct")) for r in rows]
    display_rows = rows if window is None else rows[-window:]
    offset = len(rows) - len(display_rows)   # index offset into full array

    history = []
    for i, r in enumerate(display_rows):
        entry = {
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
            "position_score": _f(r.get("position_score")),
            "conviction": _f(r.get("conviction")),
            "signal_momentum": _f(r.get("signal_momentum")),
            "position_velocity": _f(r.get("position_velocity")),
        }

        # Rolling 60-day ±σ bands matching backend z-score window
        gi = offset + i                       # global index in all_spreads
        ws = max(0, gi - 59)                   # window start (60 obs max)
        window = [v for v in all_spreads[ws:gi + 1] if v is not None]
        if len(window) >= 30:
            m = sum(window) / len(window)
            sd = math.sqrt(sum((v - m) ** 2 for v in window) / len(window))
            entry["ecm_band_mean"] = round(m, 3)
            entry["ecm_band_1up"]  = round(m + sd, 3)
            entry["ecm_band_1dn"]  = round(m - sd, 3)
            entry["ecm_band_15up"] = round(m + 1.5 * sd, 3)
            entry["ecm_band_15dn"] = round(m - 1.5 * sd, 3)

        history.append(entry)

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
            "range": range_key,
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


# ─── Momentum view ───────────────────────────────────────────────────────

MOMENTUM_META = {
    "revenue_regime": {
        "name": "F&O Revenue Regime (10D/45D Ratio)",
        "description": "Compares trailing 10-day avg F&O revenue against 45-day baseline. "
                       "Ratio > 1.05 = HOT (elevated activity), < 0.95 = COLD (declining).",
        "thresholds": {"HOT": "> 1.05", "NEUTRAL": "0.95 – 1.05", "COLD": "< 0.95"},
        "backtested": {"HOT_avg_10d_return": "+5.44%", "HOT_win_rate": "82.5%",
                       "COLD_avg_10d_return": "-1.07%", "COLD_win_rate": "35.3%"},
    },
    "adr_divergence": {
        "name": "ADR Divergence Overlay",
        "description": "Detects divergence between short-term price momentum and volatility. "
                       "Sharpens entry/exit timing within the regime signal.",
        "signals": {
            "BREAKOUT": "PriceMom5D > +1.5% AND ADR Ratio > 1.50 — genuine breakout",
            "BULL_CONT": "PriceMom5D > +1.0% AND ADR Ratio < 0.80 — quiet momentum",
            "OVERSOLD": "PriceMom5D < -1.0% AND ADR Ratio > 1.30 — capitulation bounce",
            "NEUTRAL": "None of the above — follow regime only",
        },
    },
    "composite": {
        "name": "Composite Signal",
        "description": "Combines regime + ADR divergence into actionable output.",
        "signals": {
            "STRONG_BUY": "HOT + (BULL_CONT / BREAKOUT / NEUTRAL)",
            "BUY": "HOT + OVERSOLD  OR  NEUTRAL + (BULL_CONT / BREAKOUT)",
            "HOLD": "NEUTRAL regime + no special ADR signal",
            "WATCH": "COLD + OVERSOLD — potential reversal, wait for confirmation",
            "SELL": "COLD + (NEUTRAL / BREAKOUT / BULL_CONT)",
        },
    },
}


def _fetch_momentum_signals(limit=120):
    """Fetch pre-computed momentum signals from Supabase. limit=None fetches all."""
    if not SUPABASE_ANON_KEY:
        return []
    try:
        q = (
            "?select=trading_date,fno_rev_cr,close_price,"
            "ma10_rev_cr,ma45_rev_cr,ratio_10d_45d,regime,"
            "daily_range,adr_5d,adr_20d,adr_ratio,price_mom_5d,"
            "adr_signal,composite_signal"
            "&order=trading_date.desc"
        )
        if limit is not None:
            q += f"&limit={limit}"
            rows = supabase_read("mcx_momentum_signals", q)
        else:
            rows = supabase_read_all("mcx_momentum_signals", q, max_rows=5000)
        rows = [r for r in rows if (r.get("trading_date") or "") >= "2020-01-01"]
        return sorted(rows, key=lambda r: r["trading_date"])
    except Exception:
        return []


def generate_momentum_response(range_key=DEFAULT_RANGE):
    if range_key not in RANGE_DAYS:
        range_key = DEFAULT_RANGE
    window = RANGE_DAYS[range_key]

    ist_now = now_ist()
    # Need at least 120 rows so regime_stats / streaks reflect a meaningful
    # window even on short ranges; otherwise size to the requested window
    # with a small buffer.
    if window is None:
        fetch_limit = None
    else:
        fetch_limit = max(120, window + 10)
    rows = _fetch_momentum_signals(limit=fetch_limit)

    if not rows:
        return {"success": False, "error": "No momentum signals available. Run cron_momentum first."}

    latest = rows[-1]

    # Build snapshot
    snapshot = {
        "date": latest["trading_date"],
        "close_price": _f(latest.get("close_price")),
        "fno_rev_cr": _f(latest.get("fno_rev_cr")),
        "regime": latest.get("regime"),
        "ratio_10d_45d": _f(latest.get("ratio_10d_45d")),
        "ma10_rev_cr": _f(latest.get("ma10_rev_cr")),
        "ma45_rev_cr": _f(latest.get("ma45_rev_cr")),
        "adr_signal": latest.get("adr_signal"),
        "adr_ratio": _f(latest.get("adr_ratio")),
        "price_mom_5d": _f(latest.get("price_mom_5d")),
        "composite_signal": latest.get("composite_signal"),
    }

    # Build history for charting — sized by the requested range.
    display_rows = rows if window is None else rows[-window:]
    history = []
    for r in display_rows:
        history.append({
            "date": r["trading_date"],
            "close_price": _f(r.get("close_price")),
            "fno_rev_cr": _f(r.get("fno_rev_cr")),
            "ma10_rev_cr": _f(r.get("ma10_rev_cr")),
            "ma45_rev_cr": _f(r.get("ma45_rev_cr")),
            "ratio_10d_45d": _f(r.get("ratio_10d_45d")),
            "regime": r.get("regime"),
            "daily_range": _f(r.get("daily_range")),
            "adr_5d": _f(r.get("adr_5d")),
            "adr_20d": _f(r.get("adr_20d")),
            "adr_ratio": _f(r.get("adr_ratio")),
            "price_mom_5d": _f(r.get("price_mom_5d")),
            "adr_signal": r.get("adr_signal"),
            "composite_signal": r.get("composite_signal"),
        })

    # Regime stats from full dataset
    all_regimes = [r.get("regime") for r in rows if r.get("regime")]
    hot_count = sum(1 for r in all_regimes if r == "HOT")
    cold_count = sum(1 for r in all_regimes if r == "COLD")
    neutral_count = sum(1 for r in all_regimes if r == "NEUTRAL")

    # Current streak
    streak_regime = latest.get("regime")
    streak_count = 0
    for r in reversed(rows):
        if r.get("regime") == streak_regime:
            streak_count += 1
        else:
            break

    return {
        "success": True,
        "as_of": ist_now.strftime("%Y-%m-%d %H:%M IST"),
        "snapshot": snapshot,
        "history": history,
        "regime_stats": {
            "hot_days": hot_count,
            "neutral_days": neutral_count,
            "cold_days": cold_count,
            "current_streak": streak_count,
            "streak_regime": streak_regime,
        },
        "methodology": MOMENTUM_META,
        "data_quality": {
            "total_rows": len(rows),
            "history_returned": len(history),
            "latest_date": latest["trading_date"],
            "range": range_key,
        },
    }


# ─── Vercel handler ──────────────────────────────────────────────────────

# ─── Options feed (Signals → Options) ─────────────────────────────────────
# Written by scripts/options_paper.py after each 15:10 IST run (tables:
# scripts/sql/create_options_feed.sql). Paper trades only; no orders are placed.
# Backtest references: Docs/ADR_Edge_StressTest_Results.md — Schedule B, signal > 0,
# 2.5% half-spread per leg plus costs, 2022–2026.
OPTIONS_BACKTEST = {
    "PCS": {"label": "Put credit spread", "mean": 0.220, "hit": 0.75, "n": 28},
    "CDS": {"label": "Call debit spread", "mean": 0.320, "hit": 0.607, "n": 28},
}


def summarise_trades(trades):
    """Closed paper trades grouped by (variant, book, shadow): n, mean return on risk, hit rate, P&L per lot."""
    groups = {}
    for t in trades:
        if t.get("status") != "closed" or t.get("ret") is None:
            continue
        key = (t.get("variant") or "event", t.get("book"), bool(t.get("shadow")))
        groups.setdefault(key, []).append(t)
    out = []
    for (variant, book, shadow), ts in sorted(groups.items(), key=lambda kv: (kv[0][2], kv[0][0], kv[0][1] or "")):
        rets = [float(t["ret"]) for t in ts]
        out.append({
            "variant": variant, "book": book, "shadow": shadow, "n": len(ts),
            "mean_ret": round(sum(rets) / len(rets), 4),
            "hit": round(sum(r > 0 for r in rets) / len(rets), 3),
            "pnl_per_lot": round(sum(float(t.get("pnl_per_lot") or 0) for t in ts)),
            "worst": round(min(rets), 4), "best": round(max(rets), 4),
        })
    return out


def _trade_row(r):
    rec = r.get("record") or {}
    legs = [{"strike": l.get("strike"), "type": l.get("type"), "qty": l.get("qty"),
             "fill": l.get("entry_fill"), "exit": l.get("exit_fill")} for l in rec.get("legs") or []]
    keep = ("id", "variant", "book", "direction", "shadow", "event", "status", "expiry",
            "entry_date", "exit_date", "signal", "z", "net_entry", "risk", "ret", "pnl_per_lot")
    row = {k: r.get(k) for k in keep}
    row.update(legs=legs, flags=rec.get("flags") or [], reading=rec.get("reading"))
    return row


def generate_options_response():
    try:
        feed = supabase_read("mcx_options_feed", "?select=*&order=trading_date.desc&limit=60")
    except Exception as e:
        return {"success": False, "setup": True, "error": f"Options feed tables are not set up: {str(e)[:120]}"}
    if not feed:
        return {"success": False, "setup": True, "error": "No options feed published yet"}
    try:
        ledger = supabase_read_all("mcx_options_paper", "?select=*&order=entry_date.asc", max_rows=5000)
    except Exception:
        ledger = []
    trades = [_trade_row(r) for r in ledger]
    return {
        "success": True,
        "as_of": now_ist().strftime("%Y-%m-%d %H:%M IST"),
        "latest": feed[0],
        "history": [{"date": f["trading_date"], "spot": f.get("spot"), "signals": f.get("signals")} for f in reversed(feed)],
        "trades": trades,
        "track": summarise_trades(trades),
        "backtest": OPTIONS_BACKTEST,
    }


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
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)
        view = qs.get("view", [None])[0]

        try:
            if view == "momentum":
                range_key = qs.get("range", [DEFAULT_RANGE])[0]
                result = generate_momentum_response(range_key=range_key)
            elif view == "options":
                result = generate_options_response()
            else:
                range_key = qs.get("range", [DEFAULT_RANGE])[0]
                result = generate_models_response(range_key=range_key)
            self.send_json(result)
        except Exception as e:
            self.send_json({"success": False, "error": str(e)[:200]}, 500)
