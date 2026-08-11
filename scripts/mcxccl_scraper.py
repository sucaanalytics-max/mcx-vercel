#!/usr/bin/env python3
"""
MCXCCL Daily Margin Scraper — Backfill historical margin data.

MCXCCL migrated to Sitefinity (~Jun 2026): the old browser-driven
/backpage.aspx/GetDailyMargin POST (page JS `GetData`/`OnSucessDM`) is gone.
The replacement is a plain XHR the page's RiskManagement.js makes:

    GET https://www.mcxccl.com/DailyMargin/GetDailyMargin?fromDate=dd/mm/yyyy
    → {"Data": [{fileID, instrumentID, symbol, expiryDate ("31JUL2026"),
                 initialMargin, tenderMargin, totalMargin, additionalLongMargin,
                 additionalShortMargin, specialLongMargin, specialShortMargin,
                 elmLong, elmShort, deliveryMargin, ...}, ...]}

curl_cffi with Chrome TLS impersonation passes Akamai — no Playwright needed.
Upserts FUTCOM rows to mcx_margin_daily in Supabase.

Usage:
  python3 scripts/mcxccl_scraper.py                         # weekly from 2024-01 to today
  python3 scripts/mcxccl_scraper.py --from 2024-01-02 --to 2024-01-02  # single date
  python3 scripts/mcxccl_scraper.py --frequency monthly     # monthly sampling
  python3 scripts/mcxccl_scraper.py --dry-run               # parse only, no upsert

Requires: curl_cffi (use /opt/homebrew/bin/python3)
"""
import sys, os, json, argparse, time, urllib.request, urllib.error
from datetime import datetime, date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://avqwpebveqetwwzkmtux.supabase.co")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImF2cXdwZWJ2ZXFldHd3emttdHV4Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzE0MDkwMzMsImV4cCI6MjA4Njk4NTAzM30.U_Ug61Fp1NSCesXBkYU7GJGTbuATFtXsz6GTi5948Rw")

MCXCCL_URL = "https://www.mcxccl.com/risk-management/daily-margin"

# Known margin change dates — always include these in the scrape
CHANGE_DATES = [
    "2024-02-15",  # Silver IM revised (10% -> 13%)
    "2024-06-21",  # Crude Oil IM set at 30%
    "2024-08-16",  # Silver 11.5%->13%, Crude Oil 30%->33%
    "2025-10-14",  # Gold 7.25%->8.25%, Silver 11.25%->12.75%
    "2025-10-17",  # Silver +2% additional
    "2026-02-05",  # Gold +1%, Silver +4.5% additional
    "2026-02-06",  # Gold +2% more, Silver +2.5% more
    "2026-02-19",  # Gold/Silver additional withdrawn
]


def generate_dates(start, end, frequency="weekly"):
    """Generate target scrape dates: weekly Mondays + change dates."""
    if start == end:
        return [start]

    dates = set()
    for cd in CHANGE_DATES:
        d = datetime.strptime(cd, "%Y-%m-%d").date()
        if start <= d <= end:
            dates.add(d)

    current = start
    if frequency == "weekly":
        while current.weekday() != 0:
            current += timedelta(days=1)
        while current <= end:
            dates.add(current)
            current += timedelta(weeks=1)
    elif frequency == "monthly":
        while current <= end:
            d = date(current.year, current.month, 1)
            while d.weekday() >= 5:
                d += timedelta(days=1)
            if start <= d <= end:
                dates.add(d)
            if current.month == 12:
                current = date(current.year + 1, 1, 1)
            else:
                current = date(current.year, current.month + 1, 1)
    elif frequency == "daily":
        while current <= end:
            if current.weekday() < 5:
                dates.add(current)
            current += timedelta(days=1)

    return sorted(dates)


def parse_ajax_data(data, snapshot_date):
    """Parse GetDailyMargin JSON (dict) into list of margin row dicts for Supabase."""
    rows = []
    records = data.get("Data") or []

    for r in records:
        if r.get("fileID") != 1:  # 1 = FUTCOM
            continue

        symbol = (r.get("symbol") or "").strip()
        if not symbol:
            continue

        # expiryDate is "31JUL2026" (DDMMMYYYY, case-insensitive for strptime)
        expiry = None
        expiry_display = (r.get("expiryDate") or "").strip()
        if expiry_display:
            try:
                expiry = datetime.strptime(expiry_display, "%d%b%Y").strftime("%Y-%m-%d")
            except ValueError:
                pass

        def _num(val):
            try:
                v = float(val)
                return round(v, 2) if v == v else None
            except (ValueError, TypeError):
                return None

        rows.append({
            "snapshot_date": snapshot_date,
            "symbol": symbol,
            "instrument": "FUTCOM",
            "expiry": expiry,
            "initial_margin_pct": _num(r.get("initialMargin")),
            "tender_margin_pct": _num(r.get("tenderMargin")),
            "total_margin_pct": _num(r.get("totalMargin")),
            "additional_long_pct": _num(r.get("additionalLongMargin")),
            "additional_short_pct": _num(r.get("additionalShortMargin")),
            "special_long_pct": _num(r.get("specialLongMargin")),
            "special_short_pct": _num(r.get("specialShortMargin")),
            "elm_long_pct": _num(r.get("elmLong")),
            "elm_short_pct": _num(r.get("elmShort")),
            "delivery_margin_pct": _num(r.get("deliveryMargin")),
            "source": "mcxccl_daily",
        })

    return rows


def sb_upsert(table, rows):
    """Upsert rows to Supabase in batches of 50."""
    if not SUPABASE_ANON_KEY:
        print("  WARNING: SUPABASE_ANON_KEY not set, skipping upsert")
        return ["No API key"]

    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }
    errors = []
    for i in range(0, len(rows), 50):
        chunk = rows[i:i + 50]
        body = json.dumps(chunk).encode()
        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                pass
        except urllib.error.HTTPError as e:
            err_body = e.read().decode()[:200] if e.fp else ""
            errors.append(f"batch {i}: HTTP {e.code} — {err_body}")
    return errors


def make_session():
    """curl_cffi session with Chrome TLS impersonation, warmed on the margin page."""
    from curl_cffi import requests as cfreq
    session = cfreq.Session(impersonate="chrome142")
    resp = session.get(MCXCCL_URL, timeout=30)
    resp.raise_for_status()
    return session


def scrape_date(session, target_date):
    """Fetch margin data for a single date via GetDailyMargin. Returns (rows, record_count)."""
    resp = session.get(
        "https://www.mcxccl.com/DailyMargin/GetDailyMargin",
        params={"fromDate": target_date.strftime("%d/%m/%Y")},
        headers={
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": MCXCCL_URL,
        },
        timeout=30,
    )
    resp.raise_for_status()
    if "json" not in (resp.headers.get("content-type") or ""):
        # Akamai challenge / HTML error page served with HTTP 200
        raise RuntimeError(f"non-JSON response: {resp.text[:120]!r}")

    data = resp.json()
    total_count = len(data.get("Data") or [])

    snapshot_str = target_date.strftime("%Y-%m-%d")
    rows = parse_ajax_data(data, snapshot_str)

    return rows, total_count


def main():
    parser = argparse.ArgumentParser(description="Scrape MCXCCL daily margin data")
    parser.add_argument("--from", dest="from_date", default="2024-01-01",
                        help="Start date YYYY-MM-DD (default: 2024-01-01)")
    parser.add_argument("--to", dest="to_date", default=None,
                        help="End date YYYY-MM-DD (default: today)")
    parser.add_argument("--frequency", choices=["weekly", "monthly", "daily"],
                        default="weekly", help="Sampling frequency (default: weekly)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse only, don't upsert to Supabase")
    parser.add_argument("--delay", type=float, default=2.0,
                        help="Delay between dates in seconds (default: 2.0)")
    args = parser.parse_args()

    start = datetime.strptime(args.from_date, "%Y-%m-%d").date()
    end = datetime.strptime(args.to_date, "%Y-%m-%d").date() if args.to_date else date.today()

    dates = generate_dates(start, end, args.frequency)
    print(f"Target: {len(dates)} dates from {dates[0]} to {dates[-1]} ({args.frequency})")

    total_rows = 0
    total_errors = []
    dates_with_data = 0
    dates_no_data = 0

    print(f"Warming up session on {MCXCCL_URL}...")
    session = make_session()
    print("Session ready.\n")

    for i, target_date in enumerate(dates):
        label = target_date.strftime("%Y-%m-%d (%a)")
        print(f"[{i+1}/{len(dates)}] {label}...", end=" ", flush=True)

        try:
            rows, record_count = scrape_date(session, target_date)
        except Exception as e:
            err_msg = str(e)[:100]
            print(f"ERROR: {err_msg}")
            total_errors.append(f"{target_date}: {err_msg}")
            # Rebuild session on error (expired Akamai cookies etc.)
            try:
                session = make_session()
            except Exception:
                pass
            time.sleep(args.delay)
            continue

        if not rows:
            print(f"no data (total={record_count})")
            dates_no_data += 1
            time.sleep(args.delay)
            continue

        print(f"{len(rows)} FUTCOM rows (of {record_count} total)", end="")

        if not args.dry_run:
            errors = sb_upsert("mcx_margin_daily", rows)
            if errors:
                print(f" — {len(errors)} errors!")
                total_errors.extend(errors)
            else:
                print(" — upserted OK")
        else:
            print(" — dry run")

        total_rows += len(rows)
        dates_with_data += 1
        time.sleep(args.delay)

    print(f"\n{'='*50}")
    print(f"Dates scraped: {dates_with_data} with data, {dates_no_data} no data")
    print(f"Total FUTCOM rows: {total_rows}")
    if total_errors:
        print(f"Errors ({len(total_errors)}):")
        for e in total_errors[:10]:
            print(f"  {e}")
    else:
        print("No errors.")


if __name__ == "__main__":
    main()
