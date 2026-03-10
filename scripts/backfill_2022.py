#!/usr/bin/env python3
"""
MCX Historical Backfill — Jan 2022 → Aug 2024
Fetches daily commodity-level turnover from MCX Historical Detailed Report API
and stores in Supabase (mcx_commodity_daily + mcx_daily_revenue aggregate).

Run on LOCAL machine only — MCX blocks cloud IPs.
Requires: pip install curl_cffi

Usage:
  python3 scripts/backfill_2022.py                    # full backfill Jan 2022 → Aug 2024
  python3 scripts/backfill_2022.py --resume            # resume from last successful date
  python3 scripts/backfill_2022.py --from 2023-01-01   # start from specific date
  python3 scripts/backfill_2022.py --from 2023-01-01 --to 2023-06-30  # date range
  python3 scripts/backfill_2022.py --dry-run            # preview dates without writing
"""
import sys, os, json, urllib.request, urllib.error, time, argparse
from curl_cffi import requests as cfreq
from datetime import datetime, timedelta, timezone

# ── Config ──────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://avqwpebveqetwwzkmtux.supabase.co")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImF2cXdwZWJ2ZXFldHd3emttdHV4Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzE0MDkwMzMsImV4cCI6MjA4Njk4NTAzM30.U_Ug61Fp1NSCesXBkYU7GJGTbuATFtXsz6GTi5948Rw")

# Default date range
DEFAULT_START = "2022-01-03"  # First trading day of Jan 2022
DEFAULT_END   = "2024-08-30"  # Last day before existing mcx_daily_revenue starts (Sep 2024)

# Rate limiting
REQUEST_DELAY = 0.8  # seconds between API calls (be respectful)

# MCX holidays 2022-2024 (full-day closures — no trading)
# Source: MCX annual circulars. If API returns no data, we skip gracefully anyway.
MCX_HOLIDAYS = {
    # 2022
    "2022-01-26", "2022-03-01", "2022-03-18", "2022-04-14", "2022-04-15",
    "2022-05-03", "2022-08-09", "2022-08-15", "2022-08-31",
    "2022-10-05", "2022-10-24", "2022-10-26", "2022-11-08",
    "2022-12-26",  # Christmas observed
    # 2023
    "2023-01-26", "2023-03-07", "2023-03-30", "2023-04-04", "2023-04-07",
    "2023-04-14", "2023-04-22",  # Eid (Saturday but MCX was closed)
    "2023-05-01", "2023-06-29", "2023-08-15",
    "2023-09-19", "2023-09-28",
    "2023-10-02", "2023-10-24", "2023-11-14", "2023-11-27",
    "2023-12-25",
    # 2024
    "2024-01-26", "2024-03-08", "2024-03-25", "2024-03-29",
    "2024-04-11", "2024-04-14", "2024-04-17", "2024-04-21",
    "2024-05-01", "2024-05-23", "2024-06-17",
    "2024-07-17", "2024-08-15",
}


def now_ist():
    return datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)


def is_trading_day(d):
    """Weekday and not a known full-closure holiday."""
    return d.weekday() < 5 and d.strftime("%Y-%m-%d") not in MCX_HOLIDAYS


# ── MCX Historical API ─────────────────────────────────────────────────────
_session = None

def _get_session():
    """Lazily create a curl_cffi session with Chrome TLS impersonation.
    Seeds with a GET to the historical-data page (sets cookies/headers for Akamai)."""
    global _session
    if _session is None:
        print("  Initializing Chrome-impersonation session...")
        _session = cfreq.Session(impersonate="chrome142")
        resp = _session.get("https://www.mcxindia.com/market-data/historical-data", timeout=20)
        print(f"  Session seeded (status {resp.status_code})")
    return _session


def fetch_mcx_day(date_iso):
    """Fetch all contract-level data for one trading date from MCX Historical API.
    Returns list of raw row dicts from MCX, or None if no data / error."""
    date_compact = date_iso.replace("-", "")
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
    session = _get_session()

    try:
        resp = session.post(url, json=payload, headers={
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://www.mcxindia.com/market-data/historical-data",
        }, timeout=20)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        return None, str(e)

    rows = data.get("d", {}).get("Data")
    if not rows or len(rows) < 2:
        return None, "no data"

    return rows, None


def aggregate_commodity_rows(date_iso, raw_rows):
    """Aggregate raw contract-level MCX rows into commodity-level summaries.

    Groups by (commodity, instrument_type) where instrument_type is one of:
      FUTCOM, FUTIDX, OPTFUT, OPTIDX

    Returns:
      commodity_rows: list of dicts for mcx_commodity_daily table
      aggregate:      dict for mcx_daily_revenue table (exchange-wide totals)
    """
    # Group by (Commodity, InstrumentName)
    # API fields: Commodity (not Symbol), Segment (not CommodityHead),
    #   TradedContract (not NoOfContract), Quantity (not Volume),
    #   TotalValue, PremiumTurnover, InstrumentName
    groups = {}
    for r in raw_rows:
        symbol = (r.get("Symbol") or r.get("Commodity") or "").strip()
        inst = (r.get("InstrumentName") or "").strip()
        chead = (r.get("CommodityHead") or r.get("Segment") or "").strip()

        if not symbol or not inst:
            continue

        key = (symbol, inst)
        if key not in groups:
            groups[key] = {
                "commodity": symbol,
                "commodity_head": chead,
                "instrument_type": inst,
                "contracts": 0,
                "volume_lots": 0,
                "turnover_lacs": 0.0,
                "premium_turnover_lacs": 0.0,
                "open_interest": 0,
                "oi_value_lacs": 0.0,
            }
        g = groups[key]

        g["contracts"] += int(r.get("NoOfContract", 0) or r.get("TradedContract", 0) or 0)
        g["volume_lots"] += int(r.get("Volume", 0) or r.get("Quantity", 0) or 0)
        g["turnover_lacs"] += float(r.get("TotalValue", 0) or 0)

        # PremiumTurnover (only meaningful for options)
        prem_str = str(r.get("PremiumTurnover", "-")).strip()
        if prem_str not in ("-", "", "0"):
            try:
                g["premium_turnover_lacs"] += float(prem_str)
            except ValueError:
                pass

        g["open_interest"] += int(r.get("OpenInterest", 0) or 0)
        oi_val = r.get("OIValue", 0)
        if oi_val:
            try:
                g["oi_value_lacs"] += float(oi_val)
            except (ValueError, TypeError):
                pass

    # Convert to Supabase rows (lakhs → crores)
    commodity_rows = []
    total_fut_notl_cr = 0.0
    total_opt_prem_cr = 0.0
    n_fut = 0
    n_opt = 0

    for key, g in groups.items():
        turnover_cr = round(g["turnover_lacs"] / 100, 2)
        prem_cr = round(g["premium_turnover_lacs"] / 100, 2) if g["premium_turnover_lacs"] > 0 else None

        commodity_rows.append({
            "trading_date": date_iso,
            "commodity": g["commodity"],
            "commodity_head": g["commodity_head"] or None,
            "instrument_type": g["instrument_type"],
            "contracts": g["contracts"],
            "volume_lots": g["volume_lots"],
            "turnover_cr": turnover_cr,
            "premium_turnover_cr": prem_cr,
            "open_interest": g["open_interest"],
            "oi_value_cr": round(g["oi_value_lacs"] / 100, 2),
            "source": "mcx_historical",
        })

        # Exchange-wide aggregation
        if g["instrument_type"] in ("FUTCOM", "FUTIDX"):
            total_fut_notl_cr += turnover_cr
            if turnover_cr > 0:
                n_fut += 1
        elif g["instrument_type"] in ("OPTFUT", "OPTIDX"):
            total_opt_prem_cr += (prem_cr or 0)
            if turnover_cr > 0:
                n_opt += 1

    # Exchange-wide aggregate row (turnover only — no revenue computation)
    aggregate = {
        "trading_date": date_iso,
        "fut_notl_cr": round(total_fut_notl_cr, 2),
        "opt_prem_cr": round(total_opt_prem_cr, 2),
        "fut_rev_cr": None,   # Will be computed later during fee calibration
        "opt_rev_cr": None,   # Will be computed later during fee calibration
        "nontx_rev_cr": None,
        "total_rev_cr": None,
        "source": "mcx_historical_backfill",
        "is_actual": False,   # Turnover is actual, but revenue not yet computed
        "active_futures": n_fut,
        "active_options": n_opt,
    }

    return commodity_rows, aggregate


# ── Supabase helpers ────────────────────────────────────────────────────────
def supabase_request(endpoint, data, method="POST"):
    """Make a Supabase REST API request with upsert semantics."""
    url = f"{SUPABASE_URL}/rest/v1/{endpoint}"
    body = json.dumps(data).encode()
    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return True, resp.status
    except urllib.error.HTTPError as e:
        return False, f"HTTP {e.code}: {e.read().decode()[:200]}"
    except Exception as e:
        return False, str(e)


def upsert_commodity_batch(rows):
    """Upsert a batch of commodity rows to mcx_commodity_daily."""
    return supabase_request("mcx_commodity_daily", rows)


def upsert_daily_revenue(row):
    """Upsert a single aggregate row to mcx_daily_revenue.
    Uses on_conflict=trading_date to avoid duplicate key errors."""
    url = f"{SUPABASE_URL}/rest/v1/mcx_daily_revenue"
    body = json.dumps(row).encode()
    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return True, resp.status
    except urllib.error.HTTPError as e:
        return False, f"HTTP {e.code}: {e.read().decode()[:200]}"
    except Exception as e:
        return False, str(e)


def get_existing_commodity_dates():
    """Fetch distinct trading_dates already in mcx_commodity_daily."""
    dates = set()
    offset = 0
    while True:
        url = (f"{SUPABASE_URL}/rest/v1/mcx_commodity_daily"
               f"?select=trading_date&order=trading_date&offset={offset}&limit=1000")
        req = urllib.request.Request(url, headers={
            "apikey": SUPABASE_ANON_KEY,
            "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        })
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                rows = json.loads(resp.read().decode())
                if not rows:
                    break
                for r in rows:
                    dates.add(r["trading_date"])
                offset += len(rows)
                if len(rows) < 1000:
                    break
        except Exception:
            break
    return dates


def get_existing_revenue_dates():
    """Fetch trading_dates already in mcx_daily_revenue."""
    dates = set()
    offset = 0
    while True:
        url = (f"{SUPABASE_URL}/rest/v1/mcx_daily_revenue"
               f"?select=trading_date&order=trading_date&offset={offset}&limit=1000")
        req = urllib.request.Request(url, headers={
            "apikey": SUPABASE_ANON_KEY,
            "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        })
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                rows = json.loads(resp.read().decode())
                if not rows:
                    break
                for r in rows:
                    dates.add(r["trading_date"])
                offset += len(rows)
                if len(rows) < 1000:
                    break
        except Exception:
            break
    return dates


# ── Progress tracking ───────────────────────────────────────────────────────
PROGRESS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".backfill_2022_progress.json")

def load_progress():
    """Load last successful date from progress file."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {"last_date": None, "filled": 0, "skipped": 0, "errors": []}

def save_progress(state):
    """Save progress state."""
    with open(PROGRESS_FILE, "w") as f:
        json.dump(state, f, indent=2)


# ── Main ────────────────────────────────────────────────────────────────────
def generate_trading_dates(start_iso, end_iso):
    """Generate list of potential trading dates in range."""
    start = datetime.strptime(start_iso, "%Y-%m-%d")
    end = datetime.strptime(end_iso, "%Y-%m-%d")
    dates = []
    d = start
    while d <= end:
        if is_trading_day(d):
            dates.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    return dates


def main():
    parser = argparse.ArgumentParser(description="MCX Historical Backfill — Jan 2022 → Aug 2024")
    parser.add_argument("--from", dest="start_date", default=DEFAULT_START,
                        help=f"Start date (default: {DEFAULT_START})")
    parser.add_argument("--to", dest="end_date", default=DEFAULT_END,
                        help=f"End date (default: {DEFAULT_END})")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from last successful date")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview dates without writing to Supabase")
    parser.add_argument("--skip-existing", action="store_true", default=True,
                        help="Skip dates already in mcx_commodity_daily (default: True)")
    parser.add_argument("--no-skip-existing", action="store_false", dest="skip_existing",
                        help="Re-fetch and overwrite existing dates")
    parser.add_argument("--delay", type=float, default=REQUEST_DELAY,
                        help=f"Seconds between API calls (default: {REQUEST_DELAY})")
    args = parser.parse_args()

    print("═" * 60)
    print("  MCX Historical Backfill — Commodity-Level Data")
    print("═" * 60)
    print(f"  Time:  {now_ist().strftime('%Y-%m-%d %H:%M IST')}")
    print(f"  Range: {args.start_date} → {args.end_date}")
    print(f"  Delay: {args.delay}s between requests")
    print(f"  Mode:  {'DRY RUN' if args.dry_run else 'LIVE'}")
    print()

    # Resume support
    if args.resume:
        progress = load_progress()
        if progress["last_date"]:
            # Start from day after last successful date
            resume_date = (datetime.strptime(progress["last_date"], "%Y-%m-%d")
                          + timedelta(days=1)).strftime("%Y-%m-%d")
            args.start_date = resume_date
            print(f"  Resuming from {resume_date} (last success: {progress['last_date']})")
            print(f"  Previous progress: {progress['filled']} filled, {progress['skipped']} skipped")
        else:
            print("  No previous progress found — starting fresh")
        print()

    # Generate trading dates
    all_dates = generate_trading_dates(args.start_date, args.end_date)
    print(f"  Potential trading dates in range: {len(all_dates)}")

    # Filter out dates already in mcx_commodity_daily (the primary target)
    if args.skip_existing and not args.dry_run:
        print("  Checking mcx_commodity_daily for existing data...")
        existing_commodity = get_existing_commodity_dates()
        before = len(all_dates)
        all_dates = [d for d in all_dates if d not in existing_commodity]
        print(f"  Skipping {before - len(all_dates)} dates already in mcx_commodity_daily")

    print(f"  Dates to fetch: {len(all_dates)}")
    if not all_dates:
        print("\n  ✓ Nothing to fetch — all dates covered!")
        return

    # Preview first/last dates
    print(f"  First: {all_dates[0]}  Last: {all_dates[-1]}")
    print()

    if args.dry_run:
        print("  DRY RUN — dates that would be fetched:")
        for i, d in enumerate(all_dates):
            print(f"    {i+1:>3}. {d}")
            if i >= 19 and len(all_dates) > 25:
                print(f"    ... and {len(all_dates) - 20} more")
                break
        return

    # Load or init progress
    progress = load_progress() if args.resume else {
        "last_date": None, "filled": 0, "skipped": 0, "errors": []
    }

    # Pre-fetch existing revenue dates to avoid overwriting computed revenue
    print("  Checking existing mcx_daily_revenue dates...")
    existing_revenue = get_existing_revenue_dates()
    print(f"  Found {len(existing_revenue)} existing revenue rows (will skip these for revenue upsert)")
    print()

    # Fetch loop
    total = len(all_dates)
    filled = progress["filled"]
    skipped = progress["skipped"]
    errors = progress.get("errors", [])
    consecutive_errors = 0
    MAX_CONSECUTIVE_ERRORS = 5

    for i, date_iso in enumerate(all_dates):
        pct = (i + 1) / total * 100
        prefix = f"  [{i+1:>3}/{total}] {pct:5.1f}%  {date_iso}"

        # Fetch from MCX
        raw_rows, err = fetch_mcx_day(date_iso)

        if err:
            if err == "no data":
                # Could be a holiday we didn't know about — skip silently
                print(f"{prefix}  · no data (likely holiday)")
                skipped += 1
            else:
                print(f"{prefix}  ✗ ERROR: {err}")
                errors.append({"date": date_iso, "error": err})
                consecutive_errors += 1
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    print(f"\n  ⚠ {MAX_CONSECUTIVE_ERRORS} consecutive errors — stopping.")
                    print(f"    Use --resume to continue from {date_iso}")
                    break

            # Save progress periodically
            if (i + 1) % 10 == 0:
                progress.update({"last_date": date_iso, "filled": filled,
                                 "skipped": skipped, "errors": errors[-20:]})
                save_progress(progress)

            time.sleep(args.delay)
            continue

        # Reset consecutive error counter on success
        consecutive_errors = 0

        # Aggregate into commodity-level + exchange-wide
        commodity_rows, aggregate = aggregate_commodity_rows(date_iso, raw_rows)

        # Write to Supabase
        # 1. Commodity-level data
        ok1, msg1 = upsert_commodity_batch(commodity_rows)
        if not ok1:
            print(f"{prefix}  ✗ commodity upsert failed: {msg1}")
            errors.append({"date": date_iso, "error": f"commodity: {msg1}"})

        # 2. Exchange-wide aggregate (only if not already in mcx_daily_revenue
        #    to avoid overwriting existing computed revenue from Sep 2024+)
        ok2 = True
        if date_iso not in existing_revenue:
            ok2, msg2 = upsert_daily_revenue(aggregate)
            if not ok2:
                print(f"{prefix}  ✗ revenue upsert failed: {msg2}")
                errors.append({"date": date_iso, "error": f"revenue: {msg2}"})

        if ok1:
            n_commodities = len(commodity_rows)
            fut_cr = aggregate["fut_notl_cr"]
            opt_cr = aggregate["opt_prem_cr"]
            print(f"{prefix}  ✓ {n_commodities} commodities | "
                  f"Fut ₹{fut_cr:,.0f}Cr  Opt ₹{opt_cr:,.1f}Cr prem")
            filled += 1

        # Save progress every 10 dates
        if (i + 1) % 10 == 0:
            progress.update({"last_date": date_iso, "filled": filled,
                             "skipped": skipped, "errors": errors[-20:]})
            save_progress(progress)

        time.sleep(args.delay)

    # Final progress save
    progress.update({
        "last_date": all_dates[-1] if all_dates else None,
        "filled": filled,
        "skipped": skipped,
        "errors": errors[-50:],
        "completed_at": now_ist().strftime("%Y-%m-%d %H:%M IST"),
    })
    save_progress(progress)

    # Summary
    print()
    print("═" * 60)
    print(f"  BACKFILL COMPLETE")
    print(f"  Filled:  {filled}")
    print(f"  Skipped: {skipped} (no data / holidays)")
    print(f"  Errors:  {len(errors)}")
    if errors:
        print(f"  Recent errors:")
        for e in errors[-5:]:
            print(f"    • {e['date']}: {e['error'][:80]}")
    print(f"  Progress saved to: {PROGRESS_FILE}")
    print("═" * 60)


if __name__ == "__main__":
    main()
