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
    from lib.mcx_config import (
        AV_KEY, make_cors_headers, now_ist, COMMODITY_MAP,
        SUPABASE_URL, SUPABASE_ANON_KEY, supabase_read, supabase_upsert,
    )
except ImportError:
    from lib.mcx_config import (
        AV_KEY, make_cors_headers, now_ist, COMMODITY_MAP,
        SUPABASE_URL, SUPABASE_ANON_KEY, supabase_read, supabase_upsert,
    )

# Legacy fragmented commodity symbols whose data is now consolidated into the
# parent (per lib/mcx_config.COMMODITY_MAP). Old signal rows for these may
# still exist in mcx_commodity_signals until the cleanup SQL has been run; we
# defensively skip them at the API layer so the UI only ever sees parents.
LEGACY_CHILD_SYMBOLS = frozenset(COMMODITY_MAP.keys())


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


def _read_prices(commodity, limit=90):
    """Read recent rows for a commodity from mcx_commodity_prices (desc by date)."""
    try:
        rows = supabase_read(
            "mcx_commodity_prices",
            f"?commodity=eq.{commodity}&order=price_date.desc&limit={limit}"
        )
        return rows or []
    except Exception:
        return []


def _mcx_stock_from_db():
    """Latest MCX Ltd share price from mcx_share_price (populated by price_refresh.py)."""
    try:
        rows = supabase_read(
            "mcx_share_price",
            "?select=trading_date,close&order=trading_date.desc&limit=2"
        )
        if rows:
            latest = rows[0]
            price = float(latest.get("close") or 0)
            chg = "0%"
            if len(rows) > 1 and rows[1].get("close"):
                p0 = float(rows[1]["close"])
                if p0:
                    chg = f"{(price / p0 - 1) * 100:+.2f}%"
            return {
                "price": round(price, 2),
                "change_pct": chg,
                "date": latest.get("trading_date", ""),
                "note": "MCX India Ltd share price (NSE:MCX).",
            }
    except Exception:
        pass
    return None


def _vol_block(prices):
    """(daily_pct, annualized_pct) from a desc-ordered price list; (None, None) if <5 pts."""
    prices = [p for p in prices if p and p > 0]
    if len(prices) < 5:
        return None, None
    rets = [(prices[i] / prices[i + 1] - 1) for i in range(len(prices) - 1)]
    vol = (sum(r ** 2 for r in rets) / len(rets)) ** 0.5
    return round(vol * 100, 2), round(vol * math.sqrt(250) * 100, 1)


def get_commodity_prices():
    """
    Commodity prices from mcx_commodity_prices, populated by the local, keyless
    scripts/commodity_price_refresh.py (yfinance: CL=F / NG=F / INR=X). No API
    key required and no fetch from Vercel's rate-limited IPs. The response shape
    is unchanged from the previous Alpha Vantage implementation, so the UI
    contract (crude_oil / natural_gas / usd_inr / volatility_summary / mcx_stock)
    is preserved.
    """
    results = {
        "source": "Yahoo Finance (yfinance) via local refresh",
        "api_note": "WTI=NYMEX CL=F, NatGas=HenryHub NG=F, FX=INR=X. MCX prices differ by ±2-5% (INR premium + logistics).",
        "fetched_at": now_ist().strftime("%Y-%m-%d %H:%M IST"),
    }

    fx_rows = _read_prices("USDINR", 90)
    wti_rows = _read_prices("WTI", 90)
    ng_rows = _read_prices("NATGAS", 90)

    if not wti_rows and not ng_rows:
        results["success"] = False
        results["error"] = ("Commodity price table is empty — run "
                            "scripts/commodity_price_refresh.py "
                            "(see scripts/sql/enable_commodity_prices_writes.sql if writes are denied).")
        return results

    usd_inr = float(fx_rows[0]["value_usd"]) if fx_rows else 0
    results["usd_inr"] = round(usd_inr, 2)
    results["usd_inr_source"] = "Yahoo Finance (INR=X)"

    def _pack(rows, usd_key, decimals):
        latest = rows[0]
        recent = rows[:45]

        def _inr(r):
            if r.get("value_inr") is not None:
                return round(float(r["value_inr"]), decimals)
            return round(float(r["value_usd"]) * usd_inr, decimals) if usd_inr else None

        block = {
            usd_key: round(float(latest["value_usd"]), 3),
            "date": latest["price_date"],
            "mcx_approx_inr": _inr(latest),
        }
        vd, va = _vol_block([float(r["value_usd"]) for r in recent])
        if vd is not None:
            block["volatility_daily"] = vd
            block["volatility_annualized"] = va
        block["history"] = [
            {"date": r["price_date"], "usd": round(float(r["value_usd"]), 3), "inr": _inr(r)}
            for r in recent
        ]
        return block

    if wti_rows:
        b = _pack(wti_rows, "wti_usd", 0)
        b["note"] = "MCX CrudeOil (₹/bbl) ≈ WTI × USD/INR. Actual MCX price has INR premium of 2-5%."
        results["crude_oil"] = b
    if ng_rows:
        b = _pack(ng_rows, "henry_hub_usd", 1)
        b["note"] = "MCX NatGas (₹/MMBtu) ≈ HenryHub × USD/INR. MCX typically trades at 5-15% premium."
        results["natural_gas"] = b

    # Volatility summary (descriptive only)
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

    stock = _mcx_stock_from_db()
    if stock:
        results["mcx_stock"] = stock

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


def _fetch_all(table, select, limit=20000, where=""):
    all_rows, offset = [], 0
    while True:
        rows = supabase_read(
            table,
            f"?select={select}{where}&order=trading_date.asc&limit=1000&offset={offset}"
        )
        all_rows.extend(rows)
        if len(rows) < 1000 or len(all_rows) >= limit:
            break
        offset += 1000
    return all_rows


def generate_commodity_analytics():
    ist_now = now_ist()

    # Only the recent window is needed (today's lineup, 60-day rotation/momentum,
    # prev-day movers). Filtering by date avoids the old bug where an ascending
    # row-cap returned the OLDEST rows and reported a years-stale "today".
    cutoff = (ist_now - timedelta(days=150)).strftime("%Y-%m-%d")
    signals = _fetch_all(
        "mcx_commodity_signals",
        "trading_date,commodity,commodity_head,"
        "total_turnover_cr,total_oi_value_cr,total_volume_lots,"
        "turnover_zscore,oi_zscore,volume_zscore,"
        "composite_z,commodity_signal,weight_of_turnover",
        where=f"&trading_date=gte.{cutoff}",
    )

    # Drop any legacy fragmented child rows (SILVERM, GOLDM, ...) that pre-date
    # the consolidation fix — their data is now reflected in the parent row.
    signals = [s for s in signals if s["commodity"] not in LEGACY_CHILD_SYMBOLS]
    # Drop OI-only stub rows (from refresh_dhan_oi running ahead of bhav_refresh).
    # Without turnover/volume the lineup is meaningless and "latest_date" gets
    # polluted by partial in-progress sessions.
    signals = [s for s in signals if _f(s.get("total_turnover_cr")) and _f(s.get("total_turnover_cr")) > 0]

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
