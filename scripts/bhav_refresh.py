#!/usr/bin/env python3
"""
MCX Daily Revenue Refresh — scheduled task
Fetches MCX daily revenue data and upserts to Supabase.
Run daily after 7:30 PM IST (market close + buffer).

Priority chain:
  1. relay EOD data (direct PremiumValue from live API) — authoritative
  2. MCX Historical Detailed Report (GetHistoricalDataDetails) — exact premium

Uses curl_cffi with Chrome TLS impersonation to bypass Akamai bot detection.

Usage:
  python3 scripts/bhav_refresh.py              # refresh today + missing last 5 days
  python3 scripts/bhav_refresh.py 2026-02-20   # refresh specific date
  python3 scripts/bhav_refresh.py --backfill 30 # backfill last 30 days
"""
import sys, os, json, urllib.request
from curl_cffi import requests as cfreq
from datetime import datetime, timedelta, timezone

# Add parent to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ── Config ──────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://avqwpebveqetwwzkmtux.supabase.co")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImF2cXdwZWJ2ZXFldHd3emttdHV4Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzE0MDkwMzMsImV4cCI6MjA4Njk4NTAzM30.U_Ug61Fp1NSCesXBkYU7GJGTbuATFtXsz6GTi5948Rw")
FUTURES_RATE = 210.0
OPTIONS_RATE = 4180.0
NONTX_DAILY = 0.00

# MCX holidays (full-day closures only — no trading at all)
MCX_HOLIDAYS = {
    "2025-12-25", "2026-01-26", "2026-04-03", "2026-10-02", "2026-12-25",
}


def now_ist():
    return datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)


def is_trading_day(d):
    """Check if date is a weekday and not a full-closure holiday."""
    return d.weekday() < 5 and d.strftime("%Y-%m-%d") not in MCX_HOLIDAYS


def check_relay_eod(date_iso):
    """Check if relay has already captured EOD data for this date.
    Returns the row dict if found (source='mcx_relay_eod'), else None."""
    url = (f"{SUPABASE_URL}/rest/v1/mcx_daily_revenue"
           f"?trading_date=eq.{date_iso}&source=eq.mcx_relay_eod&limit=1")
    req = urllib.request.Request(url, headers={
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
    })
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            rows = json.loads(resp.read().decode())
            return rows[0] if rows else None
    except Exception:
        return None


# Shared curl_cffi session for Chrome TLS impersonation (bypasses Akamai)
_hist_session = None

def _get_hist_session():
    global _hist_session
    if _hist_session is None:
        _hist_session = cfreq.Session(impersonate="chrome")
        _hist_session.get("https://www.mcxindia.com/market-data/historical-data", timeout=15)
    return _hist_session

def fetch_mcx_historical(date_iso):
    """Fetch daily revenue from MCX Historical Detailed Report API.
    Returns revenue dict with exact PremiumTurnover (no proxy), or None on failure.
    Uses curl_cffi with Chrome TLS impersonation to bypass Akamai bot detection."""
    date_compact = date_iso.replace("-", "")  # YYYYMMDD

    payload = {
        "GroupBy": "D",
        "Segment": "ALL",
        "CommodityHead": "ALL",
        "Commodity": "ALL",
        "Startdate": date_compact,
        "EndDate": date_compact,
        "InstrumentName": "ALL",
    }

    url = "https://www.mcxindia.com/backpage.aspx/GetHistoricalDataDetails"

    try:
        session = _get_hist_session()
        resp = session.post(url, json=payload, headers={
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://www.mcxindia.com/market-data/historical-data",
        }, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        rows = data.get("d", {}).get("Data")
        if not rows or len(rows) < 5:
            return None

        # Sum futures notional (FUTCOM + FUTIDX) and options premium (OPTFUT + OPTIDX)
        fut_notl_lacs = 0.0
        opt_prem_lacs = 0.0
        opt_notl_lacs = 0.0
        n_fut = 0
        n_opt = 0

        for r in rows:
            inst = r.get("InstrumentName", "")
            total_val = float(r.get("TotalValue", 0) or 0)
            prem_str = str(r.get("PremiumTurnover", "-")).strip()

            if inst in ("FUTCOM", "FUTIDX"):
                fut_notl_lacs += total_val
                if total_val > 0:
                    n_fut += 1
            elif inst in ("OPTFUT", "OPTIDX"):
                opt_notl_lacs += total_val
                if prem_str != "-" and prem_str != "":
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

    except Exception as e:
        print(f"  ⓘ Historical API unavailable: {e}")
        return None


def get_existing_dates():
    """Fetch all dates already in Supabase."""
    url = f"{SUPABASE_URL}/rest/v1/mcx_daily_revenue?select=trading_date,source&order=trading_date.desc&limit=200"
    req = urllib.request.Request(url, headers={
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
    })
    with urllib.request.urlopen(req, timeout=10) as resp:
        rows = json.loads(resp.read().decode())
    return {r["trading_date"]: r.get("source", "") for r in rows}


def upsert_rows(rows):
    """Upsert revenue rows to Supabase."""
    url = f"{SUPABASE_URL}/rest/v1/mcx_daily_revenue"
    body = json.dumps(rows).encode()
    req = urllib.request.Request(url, data=body, method="POST", headers={
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=representation",
    })
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode())


def fetch_and_compute(date_iso):
    """Fetch daily revenue for one date using the priority chain:
    1. relay EOD (already in Supabase) — skip if found
    2. MCX Historical Detailed Report — exact premium, no proxy"""

    # Priority 1: Check if relay already captured authoritative EOD data
    relay = check_relay_eod(date_iso)
    if relay:
        print(f"  ✓ {date_iso}: relay EOD found ({relay['total_rev_cr']} Cr) — skipping")
        return None  # Already in Supabase with correct data

    # Priority 2: MCX Historical Detailed Report (exact PremiumTurnover)
    hist = fetch_mcx_historical(date_iso)
    if hist:
        if hist["total_rev_cr"] < 1.0 or hist["total_rev_cr"] > 50.0:
            print(f"  ⚠ {date_iso}: historical API revenue {hist['total_rev_cr']} out of range")
        else:
            return {
                "trading_date": date_iso,
                "source": "mcx_historical",
                "data_source": "mcx_historical_detailed",
                "is_actual": True,
                **hist,
            }

    print(f"  ✗ {date_iso}: no source available (relay EOD + historical API both missed)")
    return None


def refresh(lookback_days=5, force_dates=None):
    """Main refresh: find missing dates and fill them."""
    existing = get_existing_dates()
    print(f"Supabase has {len(existing)} dates (latest: {max(existing) if existing else 'none'})")

    if force_dates:
        targets = force_dates
    else:
        today = now_ist().date()
        targets = []
        for i in range(lookback_days):
            d = today - timedelta(days=i)
            iso = d.strftime("%Y-%m-%d")
            if is_trading_day(d) and iso not in existing:
                targets.append(iso)

    if not targets:
        print("All dates up to date — nothing to refresh.")
        return

    print(f"Dates to fetch: {targets}")
    rows = []
    for date_iso in sorted(targets):
        print(f"Fetching {date_iso}...")
        row = fetch_and_compute(date_iso)
        if row:
            rows.append(row)
            print(f"  ✓ {date_iso}: {row['total_rev_cr']} Cr "
                  f"({row['active_futures']}F/{row['active_options']}O) [{row['source']}]")

    if rows:
        print(f"\nUpserting {len(rows)} rows...")
        result = upsert_rows(rows)
        print(f"Done — {len(result)} rows upserted.")
    else:
        print("No new data fetched.")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        if arg == "--backfill":
            days = int(sys.argv[2]) if len(sys.argv) > 2 else 30
            refresh(lookback_days=days)
        else:
            # Specific date(s)
            refresh(force_dates=[arg])
    else:
        refresh(lookback_days=5)
