"""
/api/mcxlive — MCX live data via mcxpy (market watch, bhav copies, PCR).
Fixes: F-02/F-22 (shared config), F-13 (restricted CORS), F-08 (Supabase push).
Note: mcxpy is blocked from cloud IPs. This endpoint works locally or when
data is pushed to Supabase via the local relay script.
"""
from http.server import BaseHTTPRequestHandler
import json
from datetime import datetime, timedelta

try:
    from api._mcx_config import (
        FUTURES_RATE, OPTIONS_RATE, NONTX_DAILY,
        calc_revenue, now_ist, is_market_open, make_cors_headers,
        SUPABASE_ANON_KEY, supabase_read, supabase_upsert,
    )
except ImportError:
    from _mcx_config import (
        FUTURES_RATE, OPTIONS_RATE, NONTX_DAILY,
        calc_revenue, now_ist, is_market_open, make_cors_headers,
        SUPABASE_ANON_KEY, supabase_read, supabase_upsert,
    )

try:
    import mcxpy as mcx
    import pandas as pd
    HAS_MCXPY = True
except ImportError:
    HAS_MCXPY = False

# MCX Contract Lot Sizes (back-calculated from bhav copy data)
LOT_SIZES = {
    "CRUDEOIL": 100, "CRUDEOILM": 10,
    "NATURALGAS": 1250, "NATGASMINI": 250,
    "GOLD": 100, "GOLDM": 10, "GOLDGUINEA": 1, "GOLDPETAL": 1, "GOLDTEN": 10,
    "SILVER": 30, "SILVERM": 5, "SILVERMIC": 1,
    "COPPER": 2500, "ALUMINIUM": 5000, "ALUMINI": 1000,
    "ZINC": 5000, "ZINCMINI": 1000,
    "LEAD": 5000, "LEADMINI": 1000,
    "NICKEL": 1500, "MENTHAOIL": 360, "COTTON": 25,
}


def _compute_revenue_from_marketwatch(mw_df):
    """Compute running daily revenue from mcxpy market watch DataFrame."""
    active = mw_df[mw_df["Volume"] > 0].copy()
    futcom = active[active["InstrumentName"] == "FUTCOM"]
    optfut = active[active["InstrumentName"] == "OPTFUT"]

    fut_notl_cr = futcom["ValueInLacs"].sum() / 100
    opt_prem_cr = optfut["PremiumValue"].sum() / 100
    opt_notl_cr = optfut["ValueInLacs"].sum() / 100

    fut_fee, opt_fee, tx_rev, total_rev = calc_revenue(fut_notl_cr, opt_prem_cr)
    pn_ratio = (opt_prem_cr / opt_notl_cr * 100) if opt_notl_cr > 0 else 0

    commodity_breakdown = {}
    for name, grp in futcom.groupby("Symbol"):
        val = grp["ValueInLacs"].sum() / 100
        if val >= 0.01:
            commodity_breakdown[name] = round(val, 2)

    opt_breakdown = {}
    for name, grp in optfut.groupby("Symbol"):
        val = grp["PremiumValue"].sum() / 100
        if val >= 0.001:
            opt_breakdown[name] = round(val, 4)

    return {
        "futures_notional_cr": round(fut_notl_cr, 2),
        "options_premium_cr": round(opt_prem_cr, 2),
        "options_notional_cr": round(opt_notl_cr, 2),
        "pn_ratio_pct": round(pn_ratio, 3),
        "futures_fee_cr": round(fut_fee, 4),
        "options_fee_cr": round(opt_fee, 4),
        "non_tx_income_cr": NONTX_DAILY,
        "total_revenue_cr": round(total_rev, 2),
        "active_instruments": len(active),
        "futures_instruments": len(futcom),
        "options_instruments": len(optfut),
        "futures_by_commodity": commodity_breakdown,
        "options_by_commodity": opt_breakdown,
    }


def _compute_revenue_from_bhav(bhav_df):
    """Compute daily revenue from bhav copy DataFrame (end-of-day data)."""
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

    return {
        "futures_notional_cr": round(fn_cr, 2),
        "options_premium_cr": round(op_cr, 2),
        "futures_fee_cr": round(fut_fee, 4),
        "options_fee_cr": round(opt_fee, 4),
        "non_tx_income_cr": NONTX_DAILY,
        "total_revenue_cr": round(total_rev, 2),
    }


def _get_pcr():
    """Fetch Put-Call Ratio from MCX."""
    try:
        pcr_df = mcx.mcx_pcr()
        if pcr_df is not None and len(pcr_df) > 0:
            result = {}
            for _, r in pcr_df.iterrows():
                sym = str(r.get("Symbol", "")).strip()
                ratio = r.get("Ratio", 0)
                if sym:
                    result[sym] = round(float(ratio), 2)
            return result
    except Exception:
        pass
    return {}


def get_live_data():
    """Main function: fetch live MCX data and compute revenue."""
    ist = now_ist()
    market_open = is_market_open(ist)

    result = {
        "success": True,
        "timestamp": ist.strftime("%Y-%m-%d %H:%M:%S IST"),
        "market_open": market_open,
        "data_source": "mcxpy (MCX direct)",
    }

    if not HAS_MCXPY:
        # Fall back to Supabase cached data
        if SUPABASE_ANON_KEY:
            try:
                today = ist.strftime("%Y-%m-%d")
                rows = supabase_read(
                    "mcx_snapshots",
                    f"?trading_date=eq.{today}&order=elapsed_min.desc&limit=1"
                )
                if rows:
                    row = rows[0]
                    result["data_source"] = "supabase_cache"
                    result["live"] = {
                        "futures_notional_cr": row.get("fut_notl_cr", 0),
                        "options_premium_cr": row.get("opt_prem_cr", 0),
                        "total_revenue_cr": row.get("total_rev_cr", 0),
                        "status": "cached",
                        "cache_elapsed_min": row.get("elapsed_min", 0),
                    }
                    return result
            except Exception:
                pass
        result["success"] = False
        result["error"] = "mcxpy not available and no Supabase cache"
        return result

    # 1. Live market watch
    try:
        mw = mcx.mcx_marketwatch()
        if mw is not None and len(mw) > 0:
            revenue = _compute_revenue_from_marketwatch(mw)
            result["live"] = revenue
            result["live"]["status"] = "running" if market_open else "final"

            # Push to Supabase for caching
            if SUPABASE_ANON_KEY:
                try:
                    elapsed = ist.hour * 60 + ist.minute - 540
                    elapsed = max(0, min(elapsed, 870))
                    supabase_upsert("mcx_snapshots", {
                        "trading_date": ist.strftime("%Y-%m-%d"),
                        "elapsed_min": elapsed,
                        "session_closed": elapsed >= 870,
                        "fut_notl_cr": revenue["futures_notional_cr"],
                        "opt_notl_cr": revenue.get("options_notional_cr", 0),
                        "opt_prem_cr": revenue["options_premium_cr"],
                        "fut_rev_cr": revenue["futures_fee_cr"],
                        "opt_rev_cr": revenue["options_fee_cr"],
                        "nontx_rev_cr": NONTX_DAILY,
                        "total_rev_cr": revenue["total_revenue_cr"],
                        "data_source": "mcxpy_live",
                    })
                except Exception:
                    pass
        else:
            result["live"] = {
                "error": "MCX data unavailable — MCX blocks cloud IPs. Use local relay script.",
                "note": "Run mcx_relay.py locally to push data to Supabase"
            }
    except Exception as e:
        result["live"] = {"error": str(e)[:200]}

    # 2. PCR data
    try:
        result["pcr"] = _get_pcr()
    except Exception:
        result["pcr"] = {}

    # 3. Recent bhav copies (last 2 trading days)
    bhav_results = {}
    for days_back in range(1, 8):
        d = ist - timedelta(days=days_back)
        if d.weekday() >= 5:
            continue
        ds_api = d.strftime("%d-%m-%Y")
        ds_iso = d.strftime("%Y-%m-%d")
        try:
            bhav = mcx.mcx_bhavcopy(ds_api)
            if bhav is not None and len(bhav) > 1000:
                rev = _compute_revenue_from_bhav(bhav)
                bhav_results[ds_iso] = rev

                # Cache bhav revenue to Supabase
                if SUPABASE_ANON_KEY:
                    try:
                        supabase_upsert("mcx_daily_revenue", {
                            "trading_date": ds_iso,
                            "fut_notl_cr": rev["futures_notional_cr"],
                            "fut_rev_cr": rev["futures_fee_cr"],
                            "opt_prem_cr": rev["options_premium_cr"],
                            "opt_rev_cr": rev["options_fee_cr"],
                            "total_rev_cr": rev["total_revenue_cr"],
                            "source": "bhav_mcxpy",
                            "is_actual": True,
                            "data_source": "bhav_mcxpy",
                        })
                    except Exception:
                        pass
        except Exception:
            continue
        if len(bhav_results) >= 2:
            break

    result["recent_bhav"] = bhav_results
    return result


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
        self.send_header("Cache-Control", "public, max-age=120, s-maxage=120")

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
        data = get_live_data()
        self.send_json(data)
