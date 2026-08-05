"""
/api/history — 45-day rolling revenue history with 3-tier data quality.
Tier 1: Supabase (gold standard — relay EOD, historical API, Excel backfill).
Tier 2: Commodity-derived (Alpha Vantage volatility). Tier 3: Synthetic fallback.
Uses shared config, Supabase caching (F-08), restricted CORS (F-13).
"""
from http.server import BaseHTTPRequestHandler
import json, math, random
from datetime import datetime, timedelta

try:
    from lib.mcx_config import (
        FUTURES_RATE, OPTIONS_RATE, NONTX_DAILY, TRADING_DAYS,
        AV_KEY, MCX_HOLIDAYS_2026,
        get_day_type, calc_revenue, now_ist, make_cors_headers,
        DAY_MULTIPLIER,
        SUPABASE_URL, SUPABASE_ANON_KEY, supabase_read, supabase_read_all, supabase_upsert,
    )
except ImportError:
    from lib.mcx_config import (
        FUTURES_RATE, OPTIONS_RATE, NONTX_DAILY, TRADING_DAYS,
        AV_KEY, MCX_HOLIDAYS_2026,
        get_day_type, calc_revenue, now_ist, make_cors_headers,
        DAY_MULTIPLIER,
        SUPABASE_URL, SUPABASE_ANON_KEY, supabase_read, supabase_read_all, supabase_upsert,
    )

import urllib.request


def _av_fetch(function: str, extra: str = "", timeout: int = 10) -> dict:
    if not AV_KEY:
        return {}
    url = f"https://www.alphavantage.co/query?function={function}&apikey={AV_KEY}{extra}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "MCX-Model/4.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception:
        return {}


def _fetch_commodity_prices() -> dict:
    """Fetch WTI + NatGas from Alpha Vantage (fallback tier)."""
    prices = {}
    wti_raw = _av_fetch("WTI", "&interval=daily")
    for r in wti_raw.get("data", [])[:90]:
        try:
            val = r.get("value", ".")
            if val and val != ".":
                prices.setdefault(r["date"], {})["wti"] = float(val)
        except (ValueError, KeyError):
            continue
    ng_raw = _av_fetch("NATURAL_GAS", "&interval=daily")
    for r in ng_raw.get("data", [])[:90]:
        try:
            val = r.get("value", ".")
            if val and val != ".":
                prices.setdefault(r["date"], {})["ng"] = float(val)
        except (ValueError, KeyError):
            continue
    return prices


def _fetch_supabase_history(limit=60):
    """Fetch cached daily revenue from Supabase (F-08). limit=None fetches all rows.
    Returns dict: {date_str: {"rev": total_rev_cr, "source": source_tag}}"""
    if not SUPABASE_ANON_KEY:
        return {}
    try:
        q = "?select=trading_date,total_rev_cr,source&order=trading_date.desc"
        if limit is not None:
            q += f"&limit={limit}"
            rows = supabase_read("mcx_daily_revenue", q)
        else:
            rows = supabase_read_all("mcx_daily_revenue", q, max_rows=5000)
        rows = [r for r in rows if (r.get("trading_date") or "") >= "2020-01-01"]
        return {
            r["trading_date"]: {
                "rev": r["total_rev_cr"],
                "source": r.get("source", "unknown"),
            }
            for r in rows if r.get("total_rev_cr")
        }
    except Exception:
        return {}


def generate_history(days=60):
    """
    Rolling revenue history with 3-tier data quality:
      1. SUPABASE    — refreshed daily by bhav_refresh.py / relay EOD (gold standard)
      2. COMMODITY   — Alpha Vantage volatility-based estimate
      3. SYNTHETIC   — deterministic random fallback

    days: window size in trading days (30/60/63/252/504); 0 or None = all history.
    """
    ist_now = now_ist()
    today = ist_now.replace(hour=0, minute=0, second=0, microsecond=0)
    day_seed = int(today.strftime("%Y%m%d"))
    rng = random.Random(day_seed)

    window = None if days in (0, None) else int(days)

    # ── Tier 1 fetch first: actual trading dates come from Supabase ────
    fetch_limit = None if window is None else max(60, window + 15)
    supabase_cache = _fetch_supabase_history(limit=fetch_limit)

    # Recent walk (last 75 calendar days) supplies today + recent gap dates;
    # MCX_HOLIDAYS_2026 only covers the current period so keep the walk short.
    recent_walk = []
    d = today - timedelta(days=75)
    while d <= today:
        ds = d.strftime("%Y-%m-%d")
        if d.weekday() < 5 and ds not in MCX_HOLIDAYS_2026:
            recent_walk.append(ds)
        d += timedelta(days=1)

    all_date_strs = sorted(set(list(supabase_cache.keys()) + recent_walk))
    if window is not None:
        all_date_strs = all_date_strs[-window:]
    trading_days = [datetime.strptime(ds, "%Y-%m-%d") for ds in all_date_strs]

    # ── Tier 2: Alpha Vantage commodity prices (fallback) ──────────────
    commodity_prices = _fetch_commodity_prices()
    has_commodities = len(commodity_prices) >= 10
    price_changes = {}
    if has_commodities:
        sorted_dates = sorted(commodity_prices.keys(), reverse=True)
        for i in range(len(sorted_dates) - 1):
            d_now = sorted_dates[i]
            d_prev = sorted_dates[i + 1]
            p_now = commodity_prices[d_now]
            p_prev = commodity_prices[d_prev]
            wti_chg = abs(p_now.get("wti", 0) / p_prev.get("wti", 1) - 1) if p_prev.get("wti") else 0
            ng_chg  = abs(p_now.get("ng", 0) / p_prev.get("ng", 1) - 1) if p_prev.get("ng") else 0
            price_changes[d_now] = wti_chg * 0.6 + ng_chg * 0.4

    Q4_BASELINE = 12.40  # Q4 FY26: Excel TX last-50d (Exchanges Dashboard audit). Non-TX removed.

    history = []
    supabase_used = 0
    commodity_used = 0
    synthetic_used = 0

    # ── Pass 1: Collect actual data from Supabase ──────────────────────
    deferred_indices = []

    for td in trading_days:
        date_str = td.strftime("%Y-%m-%d")
        idx = len(history)

        if td.strftime("%Y-%m-%d") == today.strftime("%Y-%m-%d"):
            history.append({
                "date": date_str, "label": td.strftime("%a %d %b"),
                "adr": None, "is_actual": False, "is_today": True,
                "source": "pending",
            })
            continue

        # Tier 1: Supabase (refreshed by bhav_refresh.py or relay EOD — gold standard)
        if date_str in supabase_cache:
            sc = supabase_cache[date_str]
            history.append({
                "date": date_str, "label": td.strftime("%a %d %b"),
                "adr": sc["rev"], "is_actual": True, "is_today": False,
                "source": sc.get("source", "supabase"),
            })
            supabase_used += 1
            continue

        # No actual data — placeholder; will be filled in pass 2
        history.append({
            "date": date_str, "label": td.strftime("%a %d %b"),
            "adr": None, "is_actual": False, "is_today": False,
            "source": "deferred",
        })
        deferred_indices.append(idx)

    # ── Dynamic trailing ADR from actuals (self-correcting baseline) ─────
    actuals = [h["adr"] for h in history if h.get("is_actual") and h["adr"] is not None]
    if len(actuals) >= 10:
        dynamic_baseline = round(sum(actuals[-10:]) / len(actuals[-10:]), 2)
    elif actuals:
        dynamic_baseline = round(sum(actuals) / len(actuals), 2)
    else:
        dynamic_baseline = Q4_BASELINE  # Fall back to static if no actuals

    # ── Pass 2: Fill deferred days with commodity-derived or synthetic ────
    for idx in deferred_indices:
        entry = history[idx]
        date_str = entry["date"]
        td = datetime.strptime(date_str, "%Y-%m-%d")

        # Use dynamic baseline instead of stale quarterly constant
        base = dynamic_baseline
        dtype = get_day_type(td)
        base *= DAY_MULTIPLIER.get(dtype, 1.00)

        # Tier 4: Commodity-derived
        if date_str in price_changes:
            chg = price_changes[date_str]
            vol_factor = math.sqrt(max(chg, 0.001) / 0.010)
            vol_factor = max(0.85, min(1.25, vol_factor))
            noise = rng.uniform(-0.03, 0.03)
            adr = round(base * vol_factor * (1 + noise), 2)
            adr = max(3.0, min(35.0, adr))
            commodity_used += 1
            entry["adr"] = adr
            entry["source"] = "commodity_derived"
            continue

        # Tier 5: Synthetic fallback
        noise = rng.uniform(-0.08, 0.08)
        adr = round(base * (1 + noise), 2)
        adr = max(3.0, min(35.0, adr))
        synthetic_used += 1
        entry["adr"] = adr
        entry["source"] = "synthetic"

    valid = [h["adr"] for h in history if h["adr"] is not None]
    ma_45 = round(sum(valid[-45:]) / len(valid[-45:]), 2) if valid else 0.0
    period_avg = round(sum(valid) / len(valid), 2) if valid else 0.0

    real_cnt = supabase_used + commodity_used
    total_cnt = max(len(history), 1)

    # Break down Supabase sources for quality tracking
    relay_eod_cnt = sum(1 for h in history if h.get("source") == "mcx_relay_eod")
    excel_cal_cnt = sum(1 for h in history if h.get("source") == "excel_calibrated")
    excel_daily_cnt = sum(1 for h in history if h.get("source") == "excel_daily_data")
    mcx_hist_cnt = sum(1 for h in history if h.get("source") == "mcx_historical")
    official_cnt = relay_eod_cnt + excel_cal_cnt + excel_daily_cnt + mcx_hist_cnt

    return {
        "history": history,
        "ma_45": ma_45,
        "period_avg": period_avg,
        "days_requested": days,
        "today_label": today.strftime("%a %d %b %Y"),
        "today_iso": today.strftime("%Y-%m-%d"),
        "data_quality": {
            "supabase_cache": supabase_used,
            "commodity_derived": commodity_used,
            "synthetic": synthetic_used,
            "total": len(history),
            "real_pct": round(real_cnt / total_cnt * 100, 1),
            "relay_eod": relay_eod_cnt,
            "excel_calibrated": excel_cal_cnt,
            "excel_daily_data": excel_daily_cnt,
            "mcx_historical": mcx_hist_cnt,
            "official_pct": round(official_cnt / total_cnt * 100, 1),
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
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(self.path).query)
            try:
                days = int(qs.get("days", ["60"])[0])
            except ValueError:
                days = 60
            if days not in (0, 30, 60, 63, 252, 504):
                days = 60
            result = generate_history(days=days)
            last3 = [h for h in result["history"] if not h.get("is_today")][-3:]
            result["last3"] = last3
            result["days"] = len(result["history"])
            self.send_json(result)
        except Exception as e:
            self.send_json({"success": False, "error": str(e)[:200]})
