#!/usr/bin/env python3
"""
MCX Live Relay — scheduled task
Fetches MCX MarketWatch data locally (bypasses cloud IP blocks),
computes intraday revenue projection, and upserts snapshot to Supabase.
The Vercel /api/refresh GET endpoint reads from Supabase to serve the frontend.

Runs every 15 minutes during MCX trading hours (09:00–23:30 IST).
Designed to be called by a cron/scheduled task starting at 8:00 AM IST.

Usage:
  python3 scripts/mcx_relay.py          # single snapshot, push to Supabase
  python3 scripts/mcx_relay.py --loop   # loop every 15 min until session ends
"""
import sys, os, json, math, time, urllib.request, urllib.error
from curl_cffi import requests as cfreq
from datetime import datetime, timedelta, timezone
from collections import defaultdict

# Add parent to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ── Import shared config from single source of truth ────────────────────────
from lib.mcx_config import (
    SUPABASE_URL, SUPABASE_ANON_KEY,
    FUTURES_RATE, OPTIONS_RATE, NONTX_DAILY, TRADING_DAYS,
    SESSION_START, SESSION_END, SESSION_TOTAL,
    INTRADAY_BUCKETS, DAY_MULTIPLIER, DAY_DESCRIPTION,
    MCX_HOLIDAYS_2026,
    now_ist, get_day_type, get_intraday_weight,
    project_full_day, calc_revenue,
)

LOOP_INTERVAL = 900  # 15 minutes in seconds

# Alias for backward compat (relay uses MCX_HOLIDAYS directly)
MCX_HOLIDAYS = MCX_HOLIDAYS_2026


def is_trading_day(d):
    return d.weekday() < 5 and d.strftime("%Y-%m-%d") not in MCX_HOLIDAYS


def calc_uncertainty(time_pct, day_type, dual_call=False):
    base = 0.35 * math.exp(-3.0 * time_pct) + 0.03
    if day_type == "EXPIRY":
        base *= 1.15
    if dual_call:
        base *= 0.92
    return min(base, 0.40)


# ── MCX API (curl_cffi with Chrome TLS impersonation) ───────────────────────
_mw_session = None
_hist_session = None


def _get_mw_session():
    """Shared curl_cffi session for MarketWatch API (bypasses Akamai)."""
    global _mw_session
    if _mw_session is None:
        _mw_session = cfreq.Session(impersonate="chrome142")
        _mw_session.get("https://www.mcxindia.com/market-data/market-watch", timeout=30)
    return _mw_session


def _get_hist_session():
    """Shared curl_cffi session for Historical API (bypasses Akamai)."""
    global _hist_session
    if _hist_session is None:
        _hist_session = cfreq.Session(impersonate="chrome142")
        _hist_session.get("https://www.mcxindia.com/market-data/historical-data", timeout=30)
    return _hist_session


def fetch_market_watch(session=None, timeout=18):
    """Call MCX GetMarketWatch API via curl_cffi."""
    if session is None:
        session = _get_mw_session()
    resp = session.post(
        "https://www.mcxindia.com/backpage.aspx/GetMarketWatch",
        data="",
        headers={
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Content-Type": "application/json",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://www.mcxindia.com/market-data/market-watch",
        },
        timeout=timeout,
    )
    resp.raise_for_status()
    return resp.json()


def extract_notionals(raw_json):
    contracts = raw_json.get("d", {}).get("Data", [])
    futures = [c for c in contracts if c.get("InstrumentName") == "FUTCOM" and c.get("Volume", 0) > 0]
    options = [c for c in contracts if c.get("InstrumentName") == "OPTFUT" and c.get("Volume", 0) > 0]
    fut_notl = sum(c.get("NotionalValue", 0) for c in futures) / 100
    opt_notl = sum(c.get("NotionalValue", 0) for c in options) / 100
    opt_prem = sum(c.get("PremiumValue", 0) for c in options) / 100
    return fut_notl, opt_notl, opt_prem, futures, options


def supabase_upsert(table, data):
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    body = json.dumps(data).encode()
    req = urllib.request.Request(url, data=body, method="POST", headers={
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=representation",
    })
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode())


def run_snapshot():
    """Fetch MCX data, compute projection, push to Supabase. Returns True on success."""
    capture = now_ist()
    current_min = capture.hour * 60 + capture.minute

    # Check if within trading window (with 30-min pre-buffer)
    if current_min < SESSION_START - 30 or current_min > SESSION_END + 15:
        print(f"  ⏸ {capture.strftime('%H:%M IST')} — outside trading hours, skipping")
        return False

    elapsed = max(0, min(current_min - SESSION_START, SESSION_TOTAL))
    time_pct = elapsed / SESSION_TOTAL

    # Fetch market data via curl_cffi session
    print(f"  Fetching market data...")
    session = _get_mw_session()
    raw1 = fetch_market_watch(session)

    # Dual call for late session
    raw2 = None
    if time_pct > 0.80:
        try:
            time.sleep(2)
            raw2 = fetch_market_watch(session)
        except Exception:
            pass

    fut_n1, opt_n1, opt_p1, futures, options = extract_notionals(raw1)
    if raw2:
        fut_n2, opt_n2, opt_p2, _, _ = extract_notionals(raw2)
        fut_notl = max(fut_n1, fut_n2)
        opt_notl = max(opt_n1, opt_n2)
        opt_prem = max(opt_p1, opt_p2)
        dual_call = True
    else:
        fut_notl, opt_notl, opt_prem = fut_n1, opt_n1, opt_p1
        dual_call = False

    day_type = get_day_type(capture)
    proj_fut, proj_opt, conf = project_full_day(fut_notl, opt_prem, elapsed, day_type)
    fut_rev, opt_rev, tx_rev, total_rev = calc_revenue(proj_fut, proj_opt)

    unc = calc_uncertainty(time_pct, day_type, dual_call)

    # Per-commodity breakdown
    sym_fut = defaultdict(float)
    sym_opt = defaultdict(float)
    sym_optN = defaultdict(float)
    for c in futures:
        sym_fut[c["Symbol"]] += c.get("NotionalValue", 0) / 100
    for c in options:
        base = c["Symbol"].replace("OPT", "")
        sym_opt[base] += c.get("PremiumValue", 0) / 100
        sym_optN[base] += c.get("NotionalValue", 0) / 100

    top_fut = sorted(
        [{"sym": k, "notl": round(v, 2)} for k, v in sym_fut.items()],
        key=lambda x: -x["notl"]
    )[:12]
    top_opt = sorted(
        [{"sym": k, "prem": round(v, 2), "notl": round(sym_optN[k], 2),
          "ratio": round(v / sym_optN[k] * 100, 3) if sym_optN[k] > 0 else 0}
         for k, v in sym_opt.items()],
        key=lambda x: -x["prem"]
    )[:12]

    snapshot = {
        "trading_date": capture.strftime("%Y-%m-%d"),
        "elapsed_min": elapsed,
        "session_closed": elapsed >= SESSION_TOTAL,
        "fut_notl_cr": round(fut_notl, 2),
        "opt_notl_cr": round(opt_notl, 2),
        "opt_prem_cr": round(opt_prem, 2),
        "fut_rev_cr": round(fut_rev, 4),
        "opt_rev_cr": round(opt_rev, 4),
        "nontx_rev_cr": NONTX_DAILY,
        "total_rev_cr": round(tx_rev, 4),
        "proj_fut_cr": round(proj_fut, 2),
        "proj_opt_cr": round(proj_opt, 2),
        "proj_total_rev": round(total_rev, 4),
        "uncertainty_pct": round(unc * 100, 2),
        "confidence": conf,
        "day_type": day_type,
        "day_multiplier": DAY_MULTIPLIER[day_type],
        "active_futures": len(futures),
        "active_options": len(options),
        "prem_notl_pct": round(opt_prem / opt_notl * 100, 3) if opt_notl > 0 else 0,
        "dual_call": dual_call,
        "data_source": "mcx_relay_local",
        "top_futures": top_fut,
        "top_options": top_opt,
    }

    print(f"  Pushing to Supabase...")
    result = supabase_upsert("mcx_snapshots", snapshot)
    print(f"  ✓ {capture.strftime('%H:%M IST')} — Rev: ₹{total_rev:.2f} Cr "
          f"(Fut: {fut_notl:.0f} | Opt Prem: {opt_prem:.0f}) "
          f"[{conf}, ±{unc*100:.0f}%] "
          f"({len(futures)}F/{len(options)}O)")
    return True


def fetch_mcx_historical(date_iso):
    """Fetch daily revenue from MCX Historical Detailed Report API.
    Returns revenue dict with exact PremiumTurnover (no proxy), or None on failure.
    Uses curl_cffi with Chrome TLS impersonation to bypass Akamai bot detection."""
    date_compact = date_iso.replace("-", "")

    payload = {
        "GroupBy": "D", "Segment": "ALL", "CommodityHead": "ALL",
        "Commodity": "ALL", "Startdate": date_compact,
        "EndDate": date_compact, "InstrumentName": "ALL",
    }

    url = "https://www.mcxindia.com/backpage.aspx/GetHistoricalDataDetails"

    try:
        session = _get_hist_session()
        resp = session.post(url, json=payload, headers={
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://www.mcxindia.com/market-data/historical-data",
        }, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        rows = data.get("d", {}).get("Data")
        if not rows or len(rows) < 5:
            return None

        fut_notl_lacs = 0.0
        opt_prem_lacs = 0.0
        n_fut = n_opt = 0

        for r in rows:
            inst = r.get("InstrumentName", "")
            total_val = float(r.get("TotalValue", 0) or 0)
            prem_str = str(r.get("PremiumTurnover", "-")).strip()

            if inst in ("FUTCOM", "FUTIDX"):
                fut_notl_lacs += total_val
                if total_val > 0:
                    n_fut += 1
            elif inst in ("OPTFUT", "OPTIDX"):
                if prem_str not in ("-", ""):
                    try:
                        opt_prem_lacs += float(prem_str)
                    except ValueError:
                        pass
                if total_val > 0:
                    n_opt += 1

        if fut_notl_lacs <= 0 and opt_prem_lacs <= 0:
            return None

        fn_cr = fut_notl_lacs / 100
        op_cr = opt_prem_lacs / 100
        fut_rev = 2 * fn_cr * FUTURES_RATE / 1e7
        opt_rev = 2 * op_cr * OPTIONS_RATE / 1e7
        total = fut_rev + opt_rev + NONTX_DAILY

        return {
            "fut_notl_cr": round(fn_cr, 2),
            "opt_prem_cr": round(op_cr, 2),
            "fut_rev_cr": round(fut_rev, 4),
            "opt_rev_cr": round(opt_rev, 4),
            "nontx_rev_cr": NONTX_DAILY,
            "total_rev_cr": round(total, 4),
            "active_futures": n_fut,
            "active_options": n_opt,
        }
    except Exception:
        return None


def capture_eod():
    """Capture final EOD data and upsert to mcx_daily_revenue as authoritative record.
    Priority: market watch API (direct PremiumValue) > historical detailed report.
    This replaces/supersedes BHAV proxy data for the day."""
    t = now_ist()
    date_iso = t.strftime("%Y-%m-%d")
    print(f"\n  ── EOD Capture for {date_iso} ──")

    eod_record = None
    source_tag = None

    # Priority 1: Market watch API (direct PremiumValue)
    try:
        session = _get_mw_session()
        raw = fetch_market_watch(session)
        time.sleep(2)
        raw2 = fetch_market_watch(session)

        fut_n1, opt_n1, opt_p1, futures1, options1 = extract_notionals(raw)
        fut_n2, opt_n2, opt_p2, _, _ = extract_notionals(raw2)
        fut_notl = max(fut_n1, fut_n2)
        opt_prem = max(opt_p1, opt_p2)

        fut_rev = 2 * fut_notl * FUTURES_RATE / 1e7
        opt_rev = 2 * opt_prem * OPTIONS_RATE / 1e7
        total_rev = fut_rev + opt_rev + NONTX_DAILY

        if 1.0 <= total_rev <= 50.0:
            eod_record = {
                "trading_date": date_iso,
                "fut_notl_cr": round(fut_notl, 2),
                "opt_prem_cr": round(opt_prem, 2),
                "fut_rev_cr": round(fut_rev, 4),
                "opt_rev_cr": round(opt_rev, 4),
                "nontx_rev_cr": NONTX_DAILY,
                "total_rev_cr": round(total_rev, 4),
                "source": "mcx_relay_eod",
                "is_actual": True,
                "active_futures": len(futures1),
                "active_options": len(options1),
            }
            source_tag = "mcx_relay_eod"
        else:
            print(f"  ⚠ Market watch revenue {total_rev:.4f} out of range")
    except Exception as e:
        print(f"  ⓘ Market watch failed ({e}), trying historical API...")

    # Priority 2: MCX Historical Detailed Report (exact PremiumTurnover)
    if eod_record is None:
        hist = fetch_mcx_historical(date_iso)
        if hist and 1.0 <= hist["total_rev_cr"] <= 50.0:
            eod_record = {
                "trading_date": date_iso,
                "source": "mcx_historical",
                "is_actual": True,
                **hist,
            }
            source_tag = "mcx_historical"
        else:
            print("  ✗ Historical API also unavailable")

    if eod_record is None:
        print(f"  ✗ EOD capture failed for {date_iso} — no source available")
        return False

    result = supabase_upsert("mcx_daily_revenue", eod_record)
    tr = eod_record["total_rev_cr"]
    fr = eod_record["fut_rev_cr"]
    opr = eod_record["opt_rev_cr"]
    print(f"  ✓ EOD {date_iso}: ₹{tr:.4f} Cr "
          f"(Fut: {fr:.4f} | Opt: {opr:.4f}) "
          f"→ mcx_daily_revenue [source={source_tag}]")
    return True


def run_loop():
    """Run snapshots every 15 minutes until trading session ends.
    After session close, captures authoritative EOD record for mcx_daily_revenue."""
    print(f"MCX Relay — loop mode (every {LOOP_INTERVAL//60} min)")
    print(f"Trading hours: 09:00–23:30 IST")

    while True:
        t = now_ist()
        current_min = t.hour * 60 + t.minute

        if not is_trading_day(t.date()):
            print(f"{t.strftime('%H:%M IST')} — Not a trading day. Exiting.")
            break

        if current_min > SESSION_END + 30:
            print(f"{t.strftime('%H:%M IST')} — Session ended. Exiting.")
            break

        if current_min < SESSION_START - 30:
            wait = (SESSION_START - 30 - current_min) * 60
            print(f"{t.strftime('%H:%M IST')} — Waiting {wait//60} min until pre-market...")
            time.sleep(min(wait, LOOP_INTERVAL))
            continue

        try:
            run_snapshot()
        except Exception as e:
            print(f"  ✗ Error: {e}")

        # Sleep until next interval
        t2 = now_ist()
        current_min2 = t2.hour * 60 + t2.minute
        if current_min2 > SESSION_END + 15:
            # Session ended — capture EOD authoritative record
            print("\nSession ended. Capturing EOD record...")
            capture_eod()
            break

        print(f"  Next snapshot in {LOOP_INTERVAL//60} min...")
        time.sleep(LOOP_INTERVAL)

    print("MCX Relay finished.")


def catchup_missing(days=7):
    """Check last N days for missing revenue data and fill from Historical API.
    Runs automatically before snapshot to self-heal gaps."""
    t = now_ist()
    filled = 0
    print(f"\n  ── Catch-up: scanning last {days} days for gaps ──")

    for i in range(1, days + 1):  # start at 1 to skip today
        d = (t - timedelta(days=i)).date()
        ds = d.strftime("%Y-%m-%d")

        if not is_trading_day(d):
            continue

        # Check if we already have data for this date
        url = (f"{SUPABASE_URL}/rest/v1/mcx_daily_revenue"
               f"?trading_date=eq.{ds}&limit=1")
        req = urllib.request.Request(url, headers={
            "apikey": SUPABASE_ANON_KEY,
            "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        })
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                rows = json.loads(resp.read().decode())
                if rows:
                    continue  # Already have data
        except Exception:
            continue  # Skip on error, don't block main flow

        # Missing — try Historical API
        print(f"    Gap found: {ds} — fetching from Historical API...")
        hist = fetch_mcx_historical(ds)
        if hist and 0.5 <= hist["total_rev_cr"] <= 50.0:
            record = {
                "trading_date": ds,
                "source": "mcx_historical",
                "is_actual": True,
                **hist,
            }
            try:
                supabase_upsert("mcx_daily_revenue", record)
                print(f"    ✓ {ds}: ₹{hist['total_rev_cr']:.4f} Cr (backfilled)")
                filled += 1
            except Exception as e:
                print(f"    ✗ {ds}: upsert failed — {e}")
        else:
            print(f"    · {ds}: no data from Historical API")

        time.sleep(0.5)

    if filled > 0:
        print(f"  Catch-up done — filled {filled} gaps")
    else:
        print(f"  Catch-up done — no gaps found")
    return filled


if __name__ == "__main__":
    if "--loop" in sys.argv:
        catchup_missing(7)  # Self-heal before starting loop
        run_loop()
    elif "--catchup" in sys.argv:
        days = 7
        for i, arg in enumerate(sys.argv):
            if arg == "--catchup" and i + 1 < len(sys.argv):
                try:
                    days = int(sys.argv[i + 1])
                except ValueError:
                    pass
        catchup_missing(days)
    else:
        try:
            success = run_snapshot()
            if not success:
                print("No snapshot taken (outside trading hours or fetch failed).")
        except Exception as e:
            print(f"ERROR: {e}")
            sys.exit(1)
