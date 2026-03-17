#!/usr/bin/env python3
"""
MCX Margin Refresh — CLI for daily margin capture + historical backfill.

Usage:
  python3 scripts/margin_refresh.py              # today via Sharekhan (default)
  python3 scripts/margin_refresh.py --gaps       # show missing dates only
  python3 scripts/margin_refresh.py --backfill 30  # fill last 30 days via MCXCCL
  python3 scripts/margin_refresh.py 2026-03-10   # specific date via MCXCCL
"""
import sys, os, argparse
from datetime import datetime, date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.cron_margins import refresh_margins, get_existing_margin_dates, sb_upsert
from lib.mcx_config import now_ist, is_trading_day


def show_gaps(lookback_days=30):
    """Print missing trading days in last N days."""
    existing = get_existing_margin_dates()
    today = now_ist().date()
    missing = []
    for i in range(lookback_days):
        d = today - timedelta(days=i)
        ds = d.strftime("%Y-%m-%d")
        if is_trading_day(d) and ds not in existing:
            missing.append(ds)
    missing.sort()

    print(f"Supabase has {len(existing)} margin snapshot dates (latest: {max(existing) if existing else 'none'})")
    if missing:
        print(f"\nMissing {len(missing)} trading days in last {lookback_days} days:")
        for d in missing:
            print(f"  {d}")
        print(f"\nBackfill with: python3 scripts/margin_refresh.py --backfill {lookback_days}")
    else:
        print(f"No gaps in last {lookback_days} days.")
    return missing


def backfill_mcxccl(target_dates):
    """Backfill specific dates via MCXCCL Playwright scraper."""
    if not target_dates:
        print("No dates to backfill.")
        return

    # Skip dates already in DB
    existing = get_existing_margin_dates()
    to_scrape = [d for d in target_dates if d not in existing]
    skipped = len(target_dates) - len(to_scrape)
    if skipped:
        print(f"Skipping {skipped} dates already in DB")
    if not to_scrape:
        print("All dates already have data.")
        return

    print(f"Backfilling {len(to_scrape)} dates via MCXCCL scraper...")
    print("(Requires Playwright + Chromium — launching browser)\n")

    try:
        from scripts.mcxccl_scraper import scrape_date, MCXCCL_URL
        from playwright.sync_api import sync_playwright
    except ImportError as e:
        print(f"ERROR: {e}")
        print("Install with: pip install playwright && playwright install chromium")
        return

    total_rows = 0
    total_errors = []
    dates_ok = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/131.0.0.0 Safari/537.36",
        )
        page = context.new_page()

        print(f"Navigating to {MCXCCL_URL}...")
        page.goto(MCXCCL_URL, wait_until="networkidle", timeout=30000)

        title = page.title()
        if "Access Denied" in title:
            print(f"ERROR: Akamai blocked access. Title: {title}")
            browser.close()
            return
        print(f"Page loaded: {title}\n")

        import time
        for i, ds in enumerate(sorted(to_scrape)):
            target = datetime.strptime(ds, "%Y-%m-%d").date()
            print(f"[{i+1}/{len(to_scrape)}] {ds}...", end=" ", flush=True)

            try:
                rows, count = scrape_date(page, target)
            except Exception as e:
                print(f"ERROR: {str(e)[:100]}")
                total_errors.append(f"{ds}: {str(e)[:100]}")
                try:
                    page.goto(MCXCCL_URL, wait_until="networkidle", timeout=30000)
                except Exception:
                    pass
                time.sleep(2)
                continue

            if not rows:
                print(f"no data (count={count})")
                time.sleep(2)
                continue

            errors = sb_upsert("mcx_margin_daily", rows)
            if errors:
                print(f"{len(rows)} rows — {len(errors)} upsert errors!")
                total_errors.extend(errors)
            else:
                print(f"{len(rows)} rows — OK")
                dates_ok += 1

            total_rows += len(rows)
            time.sleep(2)

        browser.close()

    print(f"\nDone: {dates_ok} dates backfilled, {total_rows} rows total")
    if total_errors:
        print(f"Errors ({len(total_errors)}):")
        for e in total_errors[:10]:
            print(f"  {e}")


def main():
    parser = argparse.ArgumentParser(description="MCX Margin Refresh")
    parser.add_argument("date", nargs="?", help="Specific date YYYY-MM-DD (backfill via MCXCCL)")
    parser.add_argument("--gaps", action="store_true", help="Show missing dates only")
    parser.add_argument("--backfill", type=int, metavar="DAYS",
                        help="Backfill last N days via MCXCCL scraper")
    args = parser.parse_args()

    if args.gaps:
        show_gaps(lookback_days=30)
        return

    if args.backfill:
        # Find missing trading days in last N days
        existing = get_existing_margin_dates()
        today = now_ist().date()
        missing = []
        for i in range(args.backfill):
            d = today - timedelta(days=i)
            ds = d.strftime("%Y-%m-%d")
            if is_trading_day(d) and ds not in existing:
                missing.append(ds)
        missing.sort()
        if missing:
            print(f"Found {len(missing)} missing dates in last {args.backfill} days")
            backfill_mcxccl(missing)
        else:
            print(f"No gaps in last {args.backfill} days.")
        return

    if args.date:
        # Specific date via MCXCCL
        backfill_mcxccl([args.date])
        return

    # Default: today via Sharekhan XLS
    result = refresh_margins()
    print(f"\nSuccess: {result['success']}")
    if result.get("snapshot_date"):
        print(f"Date: {result['snapshot_date']}")
        print(f"Rows: {result['rows_upserted']}")
        print(f"Symbols: {', '.join(result.get('symbols', []))}")
    for line in result.get("log", []):
        print(f"  {line}")
    if result.get("gaps"):
        print(f"\nGaps: {', '.join(result['gaps'])}")
    if result.get("errors"):
        print("\nErrors:")
        for e in result["errors"]:
            print(f"  {e}")


if __name__ == "__main__":
    main()
