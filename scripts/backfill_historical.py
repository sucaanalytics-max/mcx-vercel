#!/usr/bin/env python3
"""
MCX Revenue Backfill — gap-filling via Historical API
Scans Supabase for missing trading dates and fills them from MCX Historical
Detailed Report API. Run on LOCAL machine (MCX blocks cloud IPs).

Usage:
  python3 scripts/backfill_historical.py              # scan last 14 days
  python3 scripts/backfill_historical.py --days 30    # scan last 30 days
  python3 scripts/backfill_historical.py --full        # scan entire date range
"""
import sys, os, json, urllib.request, time
from curl_cffi import requests as cfreq
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ── Config ──────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://avqwpebveqetwwzkmtux.supabase.co")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImF2cXdwZWJ2ZXFldHd3emttdHV4Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzE0MDkwMzMsImV4cCI6MjA4Njk4NTAzM30.U_Ug61Fp1NSCesXBkYU7GJGTbuATFtXsz6GTi5948Rw")
FUTURES_RATE = 210.0
OPTIONS_RATE = 4180.0
NONTX_DAILY = 0.00

# MCX holidays (full-day closures only)
MCX_HOLIDAYS = {
    "2024-10-02", "2024-11-01", "2024-11-15", "2024-12-25",
    "2025-02-26", "2025-03-14", "2025-03-31", "2025-04-10",
    "2025-04-14", "2025-04-18", "2025-05-01", "2025-05-12",
    "2025-06-27", "2025-08-15", "2025-08-16", "2025-08-27",
    "2025-10-02", "2025-10-21", "2025-10-23", "2025-11-05",
    "2025-12-25",
    "2026-01-26", "2026-04-03", "2026-10-02", "2026-12-25",
}


def now_ist():
    return datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)


def is_trading_day(d):
    """Weekday + not a full-closure holiday."""
    return d.weekday() < 5 and d.strftime("%Y-%m-%d") not in MCX_HOLIDAYS


# ── Supabase helpers ────────────────────────────────────────────────────────
def get_existing_dates():
    """Fetch all trading_dates from Supabase mcx_daily_revenue."""
    dates = set()
    offset = 0
    while True:
        url = (f"{SUPABASE_URL}/rest/v1/mcx_daily_revenue"
               f"?select=trading_date&order=trading_date&offset={offset}&limit=1000")
        req = urllib.request.Request(url, headers={
            "apikey": SUPABASE_ANON_KEY,
            "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            rows = json.loads(resp.read().decode())
            if not rows:
                break
            for r in rows:
                dates.add(r["trading_date"])
            offset += len(rows)
            if len(rows) < 1000:
                break
    return dates


def supabase_upsert(row):
    """Upsert a single revenue row."""
    body = json.dumps(row).encode()
    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }
    url = f"{SUPABASE_URL}/rest/v1/mcx_daily_revenue"
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return True
    except urllib.error.HTTPError as e:
        if e.code == 409:
            patch_url = f"{url}?trading_date=eq.{row['trading_date']}"
            req2 = urllib.request.Request(patch_url, data=body, headers=headers, method="PATCH")
            with urllib.request.urlopen(req2, timeout=10) as resp2:
                return True
        raise


# ── MCX Historical API ─────────────────────────────────────────────────────
_session = None

def _get_session():
    global _session
    if _session is None:
        _session = cfreq.Session(impersonate="chrome")
        _session.get("https://www.mcxindia.com/market-data/historical-data", timeout=15)
    return _session


def fetch_mcx_historical(date_iso):
    """Fetch daily revenue from MCX Historical Detailed Report API.
    Returns dict ready for Supabase upsert, or None."""
    date_compact = date_iso.replace("-", "")
    payload = {
        "GroupBy": "D", "Segment": "ALL", "CommodityHead": "ALL",
        "Commodity": "ALL", "Startdate": date_compact,
        "EndDate": date_compact, "InstrumentName": "ALL",
    }
    url = "https://www.mcxindia.com/backpage.aspx/GetHistoricalDataDetails"
    session = _get_session()

    try:
        resp = session.post(url, json=payload, headers={
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://www.mcxindia.com/market-data/historical-data",
        }, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"    API error: {e}")
        return None

    rows = data.get("d", {}).get("Data")
    if not rows:
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
        "trading_date": date_iso,
        "fut_notl_cr": round(fn_cr, 2),
        "opt_prem_cr": round(op_cr, 2),
        "fut_rev_cr": round(fut_rev, 4),
        "opt_rev_cr": round(opt_rev, 4),
        "nontx_rev_cr": NONTX_DAILY,
        "total_rev_cr": round(total, 4),
        "source": "mcx_historical",
        "is_actual": True,
        "active_futures": n_fut,
        "active_options": n_opt,
    }


# ── Main ────────────────────────────────────────────────────────────────────
def find_gaps(start_date, end_date, existing_dates):
    """Find trading dates in range that are NOT in Supabase."""
    gaps = []
    d = start_date
    while d <= end_date:
        ds = d.strftime("%Y-%m-%d")
        if is_trading_day(d) and ds not in existing_dates:
            gaps.append(ds)
        d += timedelta(days=1)
    return gaps


def main():
    print("═══ MCX Revenue Backfill (Historical API) ═══")
    print(f"    Time: {now_ist().strftime('%Y-%m-%d %H:%M IST')}")

    # Parse args
    if "--full" in sys.argv:
        start = datetime(2024, 9, 1)
        mode = "full scan"
    elif "--days" in sys.argv:
        idx = sys.argv.index("--days")
        days = int(sys.argv[idx + 1]) if idx + 1 < len(sys.argv) else 14
        start = now_ist() - timedelta(days=days)
        mode = f"last {days} days"
    else:
        start = now_ist() - timedelta(days=14)
        mode = "last 14 days"

    end = now_ist() - timedelta(days=1)  # Don't fetch today (may be incomplete)
    print(f"    Mode: {mode} ({start.strftime('%Y-%m-%d')} → {end.strftime('%Y-%m-%d')})")

    # Step 1: Get all existing dates from Supabase
    print("\n  Fetching existing dates from Supabase...")
    existing = get_existing_dates()
    print(f"  Found {len(existing)} existing rows")

    # Step 2: Find gaps
    gaps = find_gaps(start, end, existing)
    if not gaps:
        print(f"\n  ✓ No gaps found — all trading dates covered!")
        return

    print(f"  Found {len(gaps)} missing trading dates:")
    for g in gaps[:10]:
        print(f"    • {g}")
    if len(gaps) > 10:
        print(f"    ... and {len(gaps) - 10} more")

    # Step 3: Fill gaps from Historical API
    print(f"\n  Filling {len(gaps)} gaps from MCX Historical API...")
    filled = 0
    failed = []

    for date_iso in gaps:
        result = fetch_mcx_historical(date_iso)
        if result and 0.5 <= result["total_rev_cr"] <= 50.0:
            try:
                supabase_upsert(result)
                print(f"    ✓ {date_iso}: ₹{result['total_rev_cr']:.4f} Cr")
                filled += 1
            except Exception as e:
                print(f"    ✗ {date_iso}: upsert failed — {e}")
                failed.append(date_iso)
        else:
            print(f"    · {date_iso}: no data from API (holiday or too recent)")
            failed.append(date_iso)

        time.sleep(0.5)  # Rate limiting

    print(f"\n  Done — filled {filled}/{len(gaps)} gaps")
    if failed:
        print(f"  Remaining gaps ({len(failed)}): {', '.join(failed[:10])}")


if __name__ == "__main__":
    main()
