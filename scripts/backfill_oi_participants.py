#!/usr/bin/env python3
"""
Backfill MCX OI Participant Category data from historical XLSX files.

Uses MCX's Sitefinity document API to discover download URLs for each date,
then downloads via curl_cffi (Akamai bypass), parses, and upserts to Supabase.

Usage:
  python3 scripts/backfill_oi_participants.py --from 2024-09-02 --to 2026-04-10
  python3 scripts/backfill_oi_participants.py --from 2024-09-02   # defaults --to today
  python3 scripts/backfill_oi_participants.py --recent 30         # last 30 calendar days
  python3 scripts/backfill_oi_participants.py --retry-failed      # retry previously failed

Requires: curl_cffi, pandas, openpyxl
"""
import sys, os, json, time, argparse
from datetime import datetime, date, timedelta
from curl_cffi import requests as cfreq

# Add parent to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.cron_oi_participants import _parse_participants, sb_upsert
from lib.mcx_config import now_ist, SUPABASE_URL, SUPABASE_ANON_KEY

# Sitefinity document API — discovered from archive page JS
SITEFINITY_API = (
    "https://www.mcxindia.com/api/default/documents"
    "?$filter=FolderId eq f409d346-57fb-64e3-bdfd-ff00007acb35"
    " and PublicationDate gt {from_date}T18:30:01Z"
    " and PublicationDate lt {to_date}T18:30:01Z"
    "&$orderby=Ordinal asc"
)

# MCX holidays (combined from mcx_config)
try:
    from lib.mcx_config import MCX_HOLIDAYS_2026
    _HOLIDAYS = set(MCX_HOLIDAYS_2026)
except ImportError:
    _HOLIDAYS = set()

# Additional known holidays for 2024-2025 backfill period
_HOLIDAYS_2024_2025 = {
    # 2024
    "2024-01-26", "2024-03-25", "2024-03-29", "2024-04-11", "2024-04-14",
    "2024-04-17", "2024-04-21", "2024-05-20", "2024-05-23", "2024-06-17",
    "2024-07-17", "2024-08-15", "2024-09-16", "2024-10-02", "2024-10-12",
    "2024-10-31", "2024-11-01", "2024-11-15", "2024-12-25",
    # 2025
    "2025-01-26", "2025-02-26", "2025-03-14", "2025-03-31", "2025-04-06",
    "2025-04-10", "2025-04-14", "2025-04-18", "2025-05-01", "2025-05-12",
    "2025-06-07", "2025-07-06", "2025-08-15", "2025-08-16", "2025-08-27",
    "2025-10-02", "2025-10-20", "2025-10-21", "2025-10-22", "2025-11-05",
    "2025-11-26", "2025-12-25",
}
_HOLIDAYS.update(_HOLIDAYS_2024_2025)

RATE_LIMIT = 3  # seconds between downloads
PROGRESS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".oi_backfill_progress.json")


def is_trading_day(d):
    return d.weekday() < 5 and d.strftime("%Y-%m-%d") not in _HOLIDAYS


def get_existing_dates():
    """Fetch distinct report_dates already in Supabase."""
    import urllib.request
    url = (f"{SUPABASE_URL}/rest/v1/mcx_oi_participants"
           f"?select=report_date&order=report_date.desc&limit=2000")
    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
    }
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            rows = json.loads(resp.read().decode())
            return set(r["report_date"] for r in rows)
    except Exception as e:
        print(f"  Warning: could not fetch existing dates: {e}")
        return set()


def discover_url(session, target_date):
    """Use Sitefinity document API to find the XLSX download URL for a date."""
    from_date = (target_date - timedelta(days=1)).strftime("%Y-%m-%d")
    to_date = target_date.strftime("%Y-%m-%d")
    api_url = SITEFINITY_API.format(from_date=from_date, to_date=to_date)

    resp = session.get(api_url, timeout=15)
    if resp.status_code != 200:
        return None

    data = resp.json()
    items = data.get("value", [])
    if not items:
        return None

    # Return the first matching document URL
    return items[0].get("Url")


def load_progress():
    try:
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"completed": [], "failed": [], "skipped": []}


def save_progress(progress):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


def backfill(start_date, end_date):
    """Download and upsert OI participant data for date range."""
    print(f"MCX OI Participants Backfill")
    print(f"  Range: {start_date} to {end_date}")

    # Get existing dates to skip
    existing = get_existing_dates()
    print(f"  Already in Supabase: {len(existing)} dates")

    progress = load_progress()
    completed_set = set(progress["completed"])

    # Create curl_cffi session (try chrome142 for newer curl_cffi, fallback to chrome)
    session = cfreq.Session(impersonate="chrome")
    print("  Warming up session...")
    try:
        session.get("https://www.mcxindia.com/", timeout=30)
    except Exception:
        # Retry with different impersonate if first attempt fails
        try:
            session = cfreq.Session(impersonate="chrome142")
            session.get("https://www.mcxindia.com/", timeout=30)
        except Exception as e:
            print(f"  Warning: session warmup failed: {e}")

    # Iterate trading days
    d = start_date
    total = 0
    success = 0
    failed = 0
    skipped = 0

    while d <= end_date:
        ds = d.strftime("%Y-%m-%d")

        if not is_trading_day(d):
            d += timedelta(days=1)
            continue

        total += 1

        if ds in existing or ds in completed_set:
            skipped += 1
            d += timedelta(days=1)
            continue

        print(f"  [{ds}] ", end="", flush=True)

        try:
            # Step 1: Discover download URL via Sitefinity API
            url = discover_url(session, d)
            if not url:
                print("No document found in archive")
                progress["failed"].append(ds)
                failed += 1
                d += timedelta(days=1)
                time.sleep(1)
                continue

            # Step 2: Download the XLSX
            print(f"Downloading...", end=" ", flush=True)
            resp = session.get(url, timeout=30)

            if resp.status_code != 200 or resp.content[:2] != b'PK':
                print(f"Download failed (status={resp.status_code}, is_xlsx={resp.content[:2] == b'PK'})")
                progress["failed"].append(ds)
                failed += 1
                d += timedelta(days=1)
                time.sleep(RATE_LIMIT)
                continue

            # Step 3: Parse
            report_date, rows = _parse_participants(resp.content)
            if not rows:
                print("No rows parsed")
                progress["failed"].append(ds)
                failed += 1
                d += timedelta(days=1)
                time.sleep(RATE_LIMIT)
                continue

            # Step 4: Upsert
            errors = sb_upsert("mcx_oi_participants", rows)
            if errors:
                print(f"Upsert errors: {errors[:1]}")
                progress["failed"].append(ds)
                failed += 1
            else:
                print(f"OK ({len(rows)} rows, date: {report_date})")
                progress["completed"].append(ds)
                completed_set.add(ds)
                success += 1

        except Exception as e:
            print(f"Error: {e}")
            progress["failed"].append(ds)
            failed += 1

        save_progress(progress)
        d += timedelta(days=1)
        time.sleep(RATE_LIMIT)

    print(f"\nBackfill complete:")
    print(f"  Total trading days: {total}")
    print(f"  Successful: {success}")
    print(f"  Skipped (already exists): {skipped}")
    print(f"  Failed: {failed}")
    if failed > 0:
        print(f"  Failed dates saved to: {PROGRESS_FILE}")
        print(f"  Re-run with --retry-failed to retry")


def main():
    parser = argparse.ArgumentParser(description="Backfill MCX OI Participant data")
    parser.add_argument("--from", dest="from_date", help="Start date (YYYY-MM-DD)", default="2024-09-02")
    parser.add_argument("--to", dest="to_date", help="End date (YYYY-MM-DD)", default=None)
    parser.add_argument("--recent", type=int, help="Backfill last N calendar days", default=None)
    parser.add_argument("--retry-failed", action="store_true", help="Retry previously failed dates")
    args = parser.parse_args()

    if args.retry_failed:
        progress = load_progress()
        if not progress["failed"]:
            print("No failed dates to retry.")
            return
        dates = sorted(set(progress["failed"]))
        print(f"Retrying {len(dates)} failed dates...")
        progress["failed"] = []
        save_progress(progress)
        start = datetime.strptime(dates[0], "%Y-%m-%d").date()
        end = datetime.strptime(dates[-1], "%Y-%m-%d").date()
        backfill(start, end)
        return

    if args.recent:
        end = now_ist().date()
        start = end - timedelta(days=args.recent)
    else:
        start = datetime.strptime(args.from_date, "%Y-%m-%d").date()
        end = datetime.strptime(args.to_date, "%Y-%m-%d").date() if args.to_date else now_ist().date()

    backfill(start, end)


if __name__ == "__main__":
    main()
