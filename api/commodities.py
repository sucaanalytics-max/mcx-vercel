"""
/api/commodities — Commodity prices + volatility from Alpha Vantage.
Fixes: F-07 (API key from env), F-06 (remove uncalibrated vol adjustment),
       F-13 (restricted CORS), F-02/F-22 (shared config).
Adds: Supabase caching to reduce AV calls (25/day free tier).
"""
from http.server import BaseHTTPRequestHandler
import json, math, urllib.request, urllib.error
from datetime import datetime, timedelta

from urllib.parse import urlparse, parse_qs

try:
    from api._mcx_config import (
        AV_KEY, make_cors_headers, now_ist,
        SUPABASE_URL, SUPABASE_ANON_KEY, supabase_read, supabase_upsert,
    )
except ImportError:
    from _mcx_config import (
        AV_KEY, make_cors_headers, now_ist,
        SUPABASE_URL, SUPABASE_ANON_KEY, supabase_read, supabase_upsert,
    )


def _av_fetch(function: str, extra: str = "", timeout: int = 12) -> dict:
    """Fetch from Alpha Vantage. Returns parsed JSON or empty dict on failure."""
    if not AV_KEY:
        return {}
    url = f"https://www.alphavantage.co/query?function={function}&apikey={AV_KEY}{extra}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "MCX-Model/4.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception:
        return {}


def _parse_av_daily(data: dict) -> list:
    rows = data.get("data", [])
    result = []
    for r in rows:
        try:
            val = r.get("value", ".")
            if val and val != ".":
                result.append({"date": r["date"], "value": float(val)})
        except (ValueError, KeyError):
            continue
    return result


def _try_supabase_cache():
    """Read cached commodity prices from Supabase (< 4 hours old)."""
    if not SUPABASE_ANON_KEY:
        return None
    try:
        cutoff = (now_ist() - timedelta(hours=4)).strftime("%Y-%m-%dT%H:%M:%S")
        rows = supabase_read(
            "mcx_commodity_prices",
            f"?order=price_date.desc&limit=10&fetched_at=gte.{cutoff}"
        )
        if rows and len(rows) >= 2:
            return rows
    except Exception:
        pass
    return None


def get_commodity_prices():
    """
    Fetch real commodity prices from Alpha Vantage:
      - WTI Crude Oil (daily, $/bbl)
      - Natural Gas Henry Hub (daily, $/MMBtu)
      - USD/INR exchange rate (real-time)
    Then convert to approximate MCX-equivalent INR prices.
    """
    results = {
        "source": "Alpha Vantage (free tier)",
        "api_note": "WTI=NYMEX, NatGas=HenryHub. MCX prices differ by ±2-5% due to INR premium + logistics.",
        "fetched_at": now_ist().strftime("%Y-%m-%d %H:%M IST"),
    }

    if not AV_KEY:
        results["success"] = False
        results["error"] = "Alpha Vantage API key not configured"
        return results

    # ── 1. USD/INR ─────────────────────────────────────────────────────
    fx_data = _av_fetch("CURRENCY_EXCHANGE_RATE",
                        "&from_currency=USD&to_currency=INR")
    fx_quote = fx_data.get("Realtime Currency Exchange Rate", {})
    usd_inr = float(fx_quote.get("5. Exchange Rate", "0")) if fx_quote else 0
    results["usd_inr"] = round(usd_inr, 2)
    results["usd_inr_source"] = "Alpha Vantage real-time"

    # ── 2. WTI Crude Oil (daily) ───────────────────────────────────────
    wti_data = _av_fetch("WTI", "&interval=daily")
    wti_prices = _parse_av_daily(wti_data)
    if wti_prices:
        latest = wti_prices[0]
        results["crude_oil"] = {
            "wti_usd": latest["value"],
            "date": latest["date"],
            "mcx_approx_inr": round(latest["value"] * usd_inr, 0) if usd_inr else None,
            "note": "MCX CrudeOil (₹/bbl) ≈ WTI × USD/INR. Actual MCX price has INR premium of 2-5%.",
        }
        recent_45 = wti_prices[:45]
        if len(recent_45) >= 5:
            prices = [p["value"] for p in recent_45 if p["value"] > 0]
            if len(prices) >= 5:
                daily_returns = [(prices[i] / prices[i+1] - 1)
                                 for i in range(len(prices)-1)]
                vol = (sum(r**2 for r in daily_returns) / len(daily_returns)) ** 0.5
                results["crude_oil"]["volatility_daily"] = round(vol * 100, 2)
                results["crude_oil"]["volatility_annualized"] = round(vol * math.sqrt(250) * 100, 1)
        results["crude_oil"]["history"] = [
            {"date": p["date"], "usd": p["value"],
             "inr": round(p["value"] * usd_inr, 0) if usd_inr else None}
            for p in recent_45
        ]

    # ── 3. Natural Gas (daily) ─────────────────────────────────────────
    ng_data = _av_fetch("NATURAL_GAS", "&interval=daily")
    ng_prices = _parse_av_daily(ng_data)
    if ng_prices:
        latest = ng_prices[0]
        results["natural_gas"] = {
            "henry_hub_usd": latest["value"],
            "date": latest["date"],
            "mcx_approx_inr": round(latest["value"] * usd_inr, 1) if usd_inr else None,
            "note": "MCX NatGas (₹/MMBtu) ≈ HenryHub × USD/INR. MCX typically trades at 5-15% premium.",
        }
        recent_45 = ng_prices[:45]
        if len(recent_45) >= 5:
            prices = [p["value"] for p in recent_45 if p["value"] > 0]
            if len(prices) >= 5:
                daily_returns = [(prices[i] / prices[i+1] - 1)
                                 for i in range(len(prices)-1)]
                vol = (sum(r**2 for r in daily_returns) / len(daily_returns)) ** 0.5
                results["natural_gas"]["volatility_daily"] = round(vol * 100, 2)
                results["natural_gas"]["volatility_annualized"] = round(vol * math.sqrt(250) * 100, 1)
        results["natural_gas"]["history"] = [
            {"date": p["date"], "usd": p["value"],
             "inr": round(p["value"] * usd_inr, 1) if usd_inr else None}
            for p in recent_45
        ]

    # ── 4. Volatility summary (F-06: descriptive only, no uncalibrated adjustment) ──
    crude_vol = results.get("crude_oil", {}).get("volatility_daily", 0)
    ng_vol = results.get("natural_gas", {}).get("volatility_daily", 0)
    combined_vol = crude_vol * 0.6 + ng_vol * 0.4
    results["volatility_summary"] = {
        "combined_vol_pct": round(combined_vol, 2),
        "baseline_vol_pct": 2.1,
        "interpretation": (
            f"Current commodity volatility is "
            f"{'above' if combined_vol > 2.2 else 'near' if combined_vol > 1.9 else 'below'} "
            f"the historical baseline of 2.1% daily. Higher volatility typically correlates "
            f"with higher MCX trading volumes."
        ),
        "note": "Volatility is reported for context only. Revenue projection uses the intraday curve model, not volatility.",
    }

    # ── 5. MCX Ltd stock price (BSE:532374) ────────────────────────────
    mcx_stock = _av_fetch("GLOBAL_QUOTE", "&symbol=532374.BSE")
    gq = mcx_stock.get("Global Quote", {})
    if gq:
        results["mcx_stock"] = {
            "price": float(gq.get("05. price", 0)),
            "change_pct": gq.get("10. change percent", "0%"),
            "date": gq.get("07. latest trading day", ""),
            "note": "MCX India Ltd stock price (BSE:532374).",
        }

    # ── Cache to Supabase ──────────────────────────────────────────────
    if SUPABASE_ANON_KEY:
        today_str = now_ist().strftime("%Y-%m-%d")
        try:
            if wti_prices:
                supabase_upsert("mcx_commodity_prices", {
                    "price_date": today_str,
                    "commodity": "WTI",
                    "value_usd": wti_prices[0]["value"],
                    "value_inr": round(wti_prices[0]["value"] * usd_inr, 2) if usd_inr else None,
                })
            if ng_prices:
                supabase_upsert("mcx_commodity_prices", {
                    "price_date": today_str,
                    "commodity": "NATGAS",
                    "value_usd": ng_prices[0]["value"],
                    "value_inr": round(ng_prices[0]["value"] * usd_inr, 2) if usd_inr else None,
                })
        except Exception:
            pass

    results["success"] = True
    return results


# ═══════════════════════════════════════════════════════════════════════════
#  Commodity Signal Analytics (merged from commodity_analytics.py)
#  Called via ?view=signals
# ═══════════════════════════════════════════════════════════════════════════

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

    # ── 2. Sector Rotation ──
    dates = sorted(set(s["trading_date"] for s in signals))
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

    # ── 3. Commodity Momentum ──
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
        avg_z = sum(valid) / len(valid)
        positive_days = sum(1 for z in valid if z > 0)
        trend = positive_days / len(valid) if valid else 0
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

    # ── 4. Top Movers ──
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
                    "commodity": c, "head": s["commodity_head"],
                    "prev_z": prev_z, "curr_z": curr_z,
                    "delta_z": delta, "signal": s.get("commodity_signal"),
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
        self.send_header("Cache-Control", "public, max-age=1800")

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
            qs = parse_qs(urlparse(self.path).query)
            view = qs.get("view", ["prices"])[0]
            if view == "signals":
                data = generate_commodity_analytics()
            else:
                data = get_commodity_prices()
            self.send_json(data)
        except Exception as e:
            self.send_json({"success": False, "error": str(e)[:200]}, 500)
