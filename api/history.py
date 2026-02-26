"""
/api/history — 45-day rolling revenue history with 4-tier data quality.
Uses shared config (F-02, F-22), Supabase caching (F-08), restricted CORS (F-13).
Removes hardcoded API key (F-07), duplicate constants, and stale calendar.
"""
from http.server import BaseHTTPRequestHandler
import json, math, random
from datetime import datetime, timedelta

try:
    from api.mcx_config import (
        FUTURES_RATE, OPTIONS_RATE, NONTX_DAILY, TRADING_DAYS,
        AV_KEY, MCX_HOLIDAYS_2026, BHAV_MANUAL,
        get_day_type, calc_revenue, now_ist, make_cors_headers,
        DAY_MULTIPLIER,
        SUPABASE_URL, SUPABASE_ANON_KEY, supabase_read, supabase_upsert,
    )
except ImportError:
    from mcx_config import (
        FUTURES_RATE, OPTIONS_RATE, NONTX_DAILY, TRADING_DAYS,
        AV_KEY, MCX_HOLIDAYS_2026, BHAV_MANUAL,
        get_day_type, calc_revenue, now_ist, make_cors_headers,
        DAY_MULTIPLIER,
        SUPABASE_URL, SUPABASE_ANON_KEY, supabase_read, supabase_upsert,
    )

import urllib.request

try:
    import mcxpy as mcx
    import pandas as pd
    HAS_MCXPY = True
except ImportError:
    HAS_MCXPY = False


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


def _compute_bhav_revenue(bhav_df):
    """
    Compute daily revenue from mcxpy bhav copy DataFrame.
    Uses audit-verified formula: prem = (opt_close / underlying_close) × Value_Lacs
    """
    df = bhav_df.copy()
    for col in ["Value(Lacs)", "Volume(Lots)", "Close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    df["Symbol"] = df["Symbol"].str.strip()
    df["Expiry Date"] = pd.to_datetime(df["Expiry Date"], errors="coerce")

    active = df[df["Volume(Lots)"] > 0]
    futcom = active[active["Instrument Name"] == "FUTCOM"]
    optfut = active[active["Instrument Name"] == "OPTFUT"]

    fut_notl_lacs = futcom["Value(Lacs)"].sum()

    underlying = {}
    for _, r in futcom.iterrows():
        key = (r["Symbol"], r["Expiry Date"])
        underlying[key] = r["Close"]

    opt_prem_lacs = 0
    for _, r in optfut.iterrows():
        opt_close = r["Close"]
        val_lacs = r["Value(Lacs)"]
        if opt_close <= 0 or val_lacs <= 0:
            continue
        sym = r["Symbol"]
        exp = r["Expiry Date"]
        undl = underlying.get((sym, exp))
        if undl is None:
            sym_expiries = [(k, v) for k, v in underlying.items() if k[0] == sym]
            if sym_expiries:
                sym_expiries.sort(
                    key=lambda x: abs((x[0][1] - exp).total_seconds())
                    if pd.notna(x[0][1]) and pd.notna(exp) else float("inf")
                )
                undl = sym_expiries[0][1]
        if undl and undl > 0:
            opt_prem_lacs += (opt_close / undl) * val_lacs

    fn_cr = fut_notl_lacs / 100
    op_cr = opt_prem_lacs / 100
    fut_fee, opt_fee, tx_rev, total_rev = calc_revenue(fn_cr, op_cr)
    return round(total_rev, 2)


def _fetch_bhav_revenues(trading_days):
    """Attempt to fetch bhav copy revenue for each trading day via mcxpy."""
    diag = []
    if not HAS_MCXPY:
        return {}, ["mcxpy not available"]

    results = {}
    for td in trading_days:
        ds_api = td.strftime("%d-%m-%Y")
        ds_iso = td.strftime("%Y-%m-%d")
        try:
            bhav = mcx.mcx_bhavcopy(ds_api)
            if bhav is not None and len(bhav) > 1000:
                rev = _compute_bhav_revenue(bhav)
                if 1.0 < rev < 50.0:
                    results[ds_iso] = rev
                    diag.append(f"{ds_iso}: OK rev={rev}")
                else:
                    diag.append(f"{ds_iso}: rev={rev} out of range")
            else:
                rows = len(bhav) if bhav is not None else 0
                diag.append(f"{ds_iso}: bhav={rows} rows (too small or None)")
        except Exception as e:
            diag.append(f"{ds_iso}: error={str(e)[:80]}")
    return results, diag


def _fetch_supabase_history():
    """Fetch cached daily revenue from Supabase (F-08)."""
    if not SUPABASE_ANON_KEY:
        return {}
    try:
        rows = supabase_read(
            "mcx_daily_revenue",
            "?order=trading_date.desc&limit=60"
        )
        return {r["trading_date"]: r["total_rev_cr"] for r in rows if r.get("total_rev_cr")}
    except Exception:
        return {}


def generate_history_45d():
    """
    45-day rolling history with 5-tier data quality:
      1. SUPABASE    — refreshed daily by bhav_refresh.py cron (gold standard)
      2. BHAV_MCXPY  — live from MCX via mcxpy (fallback on Vercel)
      3. BHAV_MANUAL — hardcoded verified actuals from config (legacy)
      4. COMMODITY   — Alpha Vantage volatility-based estimate
      5. SYNTHETIC   — deterministic random fallback
    """
    ist_now = now_ist()
    today = ist_now.replace(hour=0, minute=0, second=0, microsecond=0)
    day_seed = int(today.strftime("%Y%m%d"))
    rng = random.Random(day_seed)

    # Collect trading days
    trading_days = []
    d = today - timedelta(days=75)
    while d <= today:
        ds = d.strftime("%Y-%m-%d")
        if d.weekday() < 5 and ds not in MCX_HOLIDAYS_2026:
            trading_days.append(d)
        d += timedelta(days=1)
    trading_days = trading_days[-45:]

    # ── Tier 1: Fetch bhav revenues via mcxpy ──────────────────────────
    days_to_fetch = [
        td for td in trading_days
        if td.strftime("%Y-%m-%d") not in BHAV_MANUAL
        and td < today
    ]
    days_to_fetch = days_to_fetch[-7:]
    bhav_mcxpy, bhav_diag = _fetch_bhav_revenues(days_to_fetch)

    # ── Tier 2.5: Supabase cached revenue ──────────────────────────────
    supabase_cache = _fetch_supabase_history()

    # ── Tier 3: Alpha Vantage commodity prices (fallback) ──────────────
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
    Q3_BASELINE =  9.30  # Q3 FY26: Jul – Sep 2025 (was 10.25 incl 0.95 non-TX)
    q3_end = datetime(2025, 9, 30)  # Q3 ends Sep 30; Oct+ uses Q4

    history = []
    bhav_mcxpy_used = 0
    bhav_manual_used = 0
    supabase_used = 0
    commodity_used = 0
    synthetic_used = 0

    # ── Pass 1: Collect all actual data (tiers 1-3) ──────────────────────
    # Deferred indices track days that need synthetic/commodity estimation
    deferred_indices = []

    for td in trading_days:
        date_str = td.strftime("%Y-%m-%d")
        idx = len(history)

        if td == today:
            history.append({
                "date": date_str, "label": td.strftime("%a %d %b"),
                "adr": None, "is_actual": False, "is_today": True,
                "source": "pending",
            })
            continue

        # Tier 1: Supabase (refreshed by bhav_refresh.py cron — gold standard)
        if date_str in supabase_cache:
            history.append({
                "date": date_str, "label": td.strftime("%a %d %b"),
                "adr": supabase_cache[date_str], "is_actual": True, "is_today": False,
                "source": "supabase",
            })
            supabase_used += 1
            continue

        # Tier 2: mcxpy bhav copy (live fetch, rare on Vercel)
        if date_str in bhav_mcxpy:
            rev = bhav_mcxpy[date_str]
            history.append({
                "date": date_str, "label": td.strftime("%a %d %b"),
                "adr": rev, "is_actual": True, "is_today": False,
                "source": "bhav_mcxpy",
            })
            bhav_mcxpy_used += 1
            # Push to Supabase for caching
            if SUPABASE_ANON_KEY:
                try:
                    supabase_upsert("mcx_daily_revenue", {
                        "trading_date": date_str,
                        "fut_notl_cr": 0,
                        "fut_rev_cr": 0,
                        "total_rev_cr": rev,
                        "source": "bhav_mcxpy",
                        "is_actual": True,
                        "data_source": "bhav_mcxpy",
                    })
                except Exception:
                    pass
            continue

        # Tier 3: Hardcoded manual bhav actuals (legacy fallback)
        if date_str in BHAV_MANUAL:
            history.append({
                "date": date_str, "label": td.strftime("%a %d %b"),
                "adr": BHAV_MANUAL[date_str], "is_actual": True, "is_today": False,
                "source": "bhav_manual",
            })
            bhav_manual_used += 1
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
    ma_45 = round(sum(valid) / len(valid), 2) if valid else 0.0

    bhav_total = bhav_mcxpy_used + bhav_manual_used + supabase_used
    real_cnt = bhav_total + commodity_used
    total_cnt = max(len(history), 1)

    return {
        "history": history,
        "ma_45": ma_45,
        "today_label": today.strftime("%a %d %b %Y"),
        "today_iso": today.strftime("%Y-%m-%d"),
        "data_quality": {
            "bhav_mcxpy": bhav_mcxpy_used,
            "bhav_manual": bhav_manual_used,
            "supabase_cache": supabase_used,
            "bhav_total": bhav_total,
            "commodity_derived": commodity_used,
            "synthetic": synthetic_used,
            "total": len(history),
            "real_pct": round(real_cnt / total_cnt * 100, 1),
            "mcxpy_available": HAS_MCXPY,
            "mcxpy_note": (
                "MCX blocks cloud IPs; mcxpy works locally. Use local relay to push bhav data to Supabase."
                if HAS_MCXPY and bhav_mcxpy_used == 0 and len(days_to_fetch) > 0
                else None
            ),
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
            result = generate_history_45d()
            last3 = [h for h in result["history"] if not h.get("is_today")][-3:]
            result["last3"] = last3
            result["days"] = len(result["history"])
            self.send_json(result)
        except Exception as e:
            self.send_json({"success": False, "error": str(e)[:200]})
