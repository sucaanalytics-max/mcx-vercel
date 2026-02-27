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
from datetime import datetime, timedelta, timezone
from collections import defaultdict

# Add parent to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ── Config ──────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://avqwpebveqetwwzkmtux.supabase.co")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImF2cXdwZWJ2ZXFldHd3emttdHV4Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzE0MDkwMzMsImV4cCI6MjA4Njk4NTAzM30.U_Ug61Fp1NSCesXBkYU7GJGTbuATFtXsz6GTi5948Rw")

FUTURES_RATE = 210.0
OPTIONS_RATE = 4180.0
NONTX_DAILY = 0.00
TRADING_DAYS = 252

SESSION_START = 540   # 09:00 IST
SESSION_END   = 1410  # 23:30 IST
SESSION_TOTAL = SESSION_END - SESSION_START  # 870 min

LOOP_INTERVAL = 900  # 15 minutes in seconds

# Intraday volume curve (same as mcx_config.py)
INTRADAY_BUCKETS = [
    ( 540,  630, 0.06),
    ( 630,  750, 0.10),
    ( 750,  900, 0.07),
    ( 900, 1020, 0.10),
    (1020, 1170, 0.18),
    (1170, 1320, 0.34),
    (1320, 1410, 0.15),
]

# Day multipliers (same as mcx_config.py)
DAY_MULTIPLIER = {"HIGH": 1.15, "MEDIUM": 1.05, "EXPIRY": 1.00, "LOW": 1.00}
DAY_DESCRIPTION = {
    "HIGH": "High-volume day (Mon/Tue/post-holiday)",
    "MEDIUM": "Normal trading day",
    "EXPIRY": "Expiry day",
    "LOW": "Low-volume day (Fri/pre-holiday)",
}

MCX_HOLIDAYS = {
    "2025-12-25", "2026-01-26", "2026-04-03", "2026-10-02", "2026-12-25",
}


def now_ist():
    return datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)


def is_trading_day(d):
    return d.weekday() < 5 and d.strftime("%Y-%m-%d") not in MCX_HOLIDAYS


def get_day_type(dt):
    wd = dt.weekday()
    if wd in (0, 1):
        return "HIGH"
    elif wd == 4:
        return "LOW"
    return "MEDIUM"


def get_intraday_weight(elapsed_min):
    """Cumulative weight at given elapsed minutes."""
    cum = 0.0
    for start, end, w in INTRADAY_BUCKETS:
        bucket_start = start - SESSION_START
        bucket_end = end - SESSION_START
        if elapsed_min <= bucket_start:
            break
        elif elapsed_min >= bucket_end:
            cum += w
        else:
            frac = (elapsed_min - bucket_start) / (bucket_end - bucket_start)
            cum += w * frac
            break
    return max(cum, 0.001)


def project_full_day(fut_notl, opt_prem, elapsed_min, day_type):
    if elapsed_min >= SESSION_TOTAL:
        return fut_notl, opt_prem, "final"
    w = get_intraday_weight(elapsed_min)
    mult = DAY_MULTIPLIER.get(day_type, 1.0)
    proj_fut = (fut_notl / w) * mult
    proj_opt = (opt_prem / w) * mult
    if elapsed_min < 120:
        conf = "low"
    elif elapsed_min < 480:
        conf = "medium"
    else:
        conf = "high"
    return proj_fut, proj_opt, conf


def calc_revenue(proj_fut, proj_opt):
    fut_rev = 2 * proj_fut * FUTURES_RATE / 1e7
    opt_rev = 2 * proj_opt * OPTIONS_RATE / 1e7
    tx_rev = fut_rev + opt_rev
    total = tx_rev + NONTX_DAILY
    return fut_rev, opt_rev, tx_rev, total


def calc_uncertainty(time_pct, day_type, dual_call=False):
    base = 0.35 * math.exp(-3.0 * time_pct) + 0.03
    if day_type == "EXPIRY":
        base *= 1.15
    if dual_call:
        base *= 0.92
    return min(base, 0.40)


# ── MCX API ─────────────────────────────────────────────────────────────────

def fetch_cookies(timeout=12):
    """Get fresh MCX session cookies."""
    import http.cookiejar
    cj = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    req = urllib.request.Request(
        "https://www.mcxindia.com/market-data/market-watch",
        headers={
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        },
    )
    opener.open(req, timeout=timeout)
    parts = [f"{c.name}={c.value}" for c in cj]
    cookie_str = "; ".join(parts)
    if "ASP.NET_SessionId" in cookie_str or len(parts) >= 2:
        return cookie_str
    return ""


def fetch_market_watch(cookie, timeout=18):
    """Call MCX GetMarketWatch API."""
    req = urllib.request.Request(
        "https://www.mcxindia.com/backpage.aspx/GetMarketWatch",
        data=b"",
        method="POST",
        headers={
            "accept": "application/json, text/javascript, */*; q=0.01",
            "content-type": "application/json",
            "origin": "https://www.mcxindia.com",
            "referer": "https://www.mcxindia.com/market-data/market-watch",
            "x-requested-with": "XMLHttpRequest",
            "cookie": cookie,
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
        enc = resp.info().get("Content-Encoding", "")
        if "gzip" in enc:
            import gzip
            raw = gzip.decompress(raw)
        return json.loads(raw.decode("utf-8"))


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

    # Fetch cookies + market data
    print(f"  Fetching MCX cookies...")
    cookie = fetch_cookies()
    if not cookie:
        print(f"  ✗ Cookie fetch failed")
        return False

    print(f"  Fetching market data...")
    raw1 = fetch_market_watch(cookie)

    # Dual call for late session
    raw2 = None
    if time_pct > 0.80:
        try:
            time.sleep(2)
            raw2 = fetch_market_watch(cookie)
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
    Returns revenue dict with exact PremiumTurnover, or None on failure.
    Endpoint: /backpage.aspx/GetHistoricalDataDetails"""
    date_compact = date_iso.replace("-", "")

    payload = json.dumps({
        "GroupBy": "D", "Segment": "ALL", "CommodityHead": "ALL",
        "Commodity": "ALL", "Startdate": date_compact,
        "EndDate": date_compact, "InstrumentName": "ALL",
    }).encode()

    url = "https://www.mcxindia.com/backpage.aspx/GetHistoricalDataDetails"
    hdrs = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/json; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Origin": "https://www.mcxindia.com",
        "Referer": "https://www.mcxindia.com/market-data/historical-data",
    }

    try:
        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor())
        init_req = urllib.request.Request(
            "https://www.mcxindia.com/market-data/historical-data",
            headers={"User-Agent": hdrs["User-Agent"]},
        )
        opener.open(init_req, timeout=10)

        req = urllib.request.Request(url, data=payload, method="POST")
        for k, v in hdrs.items():
            req.add_header(k, v)
        with opener.open(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())

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
        cookie = fetch_cookies()
        if cookie:
            raw = fetch_market_watch(cookie)
            time.sleep(2)
            raw2 = fetch_market_watch(cookie)

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
        else:
            print("  ⓘ Cookie fetch failed, trying historical API...")
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


if __name__ == "__main__":
    if "--loop" in sys.argv:
        run_loop()
    else:
        try:
            success = run_snapshot()
            if not success:
                print("No snapshot taken (outside trading hours or fetch failed).")
        except Exception as e:
            print(f"ERROR: {e}")
            sys.exit(1)
