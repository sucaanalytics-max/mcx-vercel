#!/usr/bin/env python3
"""
MCX Share Price Refresh — yfinance → Supabase
Fetches MCX Ltd (NSE: MCX) daily OHLCV from Yahoo Finance and upserts to Supabase.

Usage:
  python3 scripts/price_refresh.py                # refresh last 7 days
  python3 scripts/price_refresh.py --backfill 400 # backfill N calendar days
  python3 scripts/price_refresh.py 2026-02-20     # refresh specific date
"""
import sys, os, json, urllib.request, time
from datetime import datetime, timedelta, timezone

# ── Config ──────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://avqwpebveqetwwzkmtux.supabase.co")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImF2cXdwZWJ2ZXFldHd3emttdHV4Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzE0MDkwMzMsImV4cCI6MjA4Njk4NTAzM30.U_Ug61Fp1NSCesXBkYU7GJGTbuATFtXsz6GTi5948Rw")
TICKER = "MCX.NS"
TABLE = "mcx_share_price"

# Sanity bounds for MCX Ltd share price (INR)
PRICE_MIN = 500   # MCX dipped to ~904 in Mar 2025
PRICE_MAX = 20000


def now_ist():
    return datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)


# ── Supabase helpers ────────────────────────────────────────────────────────
def supabase_get(date_iso):
    """Check if we already have price data for this date."""
    url = (f"{SUPABASE_URL}/rest/v1/{TABLE}"
           f"?trading_date=eq.{date_iso}&limit=1")
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


def supabase_upsert(row):
    """Upsert a single price row to Supabase."""
    date_iso = row["trading_date"]
    body = json.dumps(row).encode()
    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }
    # Try POST with upsert
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}"
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return True
    except urllib.error.HTTPError as e:
        if e.code == 409:
            # Already exists — PATCH instead
            patch_url = f"{url}?trading_date=eq.{date_iso}"
            req2 = urllib.request.Request(patch_url, data=body, headers=headers, method="PATCH")
            with urllib.request.urlopen(req2, timeout=10) as resp2:
                return True
        raise


# ── yfinance fetch ──────────────────────────────────────────────────────────
def fetch_prices(start_date, end_date, max_retries=3):
    """Fetch MCX.NS OHLCV data from yfinance for the given date range.
    Returns list of dicts with trading_date, open, high, low, close, adj_close, volume."""
    import yfinance as yf

    for attempt in range(max_retries):
        try:
            df = yf.download(
                TICKER,
                start=start_date,
                end=end_date,
                auto_adjust=False,
                progress=False,
            )
            if df is None or df.empty:
                print(f"  ⚠ yfinance returned no data (attempt {attempt+1}/{max_retries})")
                time.sleep(3 * (attempt + 1))
                continue

            results = []
            for idx, row in df.iterrows():
                date_str = idx.strftime("%Y-%m-%d") if hasattr(idx, 'strftime') else str(idx)[:10]
                close_val = float(row["Close"].iloc[0]) if hasattr(row["Close"], 'iloc') else float(row["Close"])
                open_val = float(row["Open"].iloc[0]) if hasattr(row["Open"], 'iloc') else float(row["Open"])
                high_val = float(row["High"].iloc[0]) if hasattr(row["High"], 'iloc') else float(row["High"])
                low_val = float(row["Low"].iloc[0]) if hasattr(row["Low"], 'iloc') else float(row["Low"])
                adj_val = float(row["Adj Close"].iloc[0]) if hasattr(row["Adj Close"], 'iloc') else float(row["Adj Close"])
                vol_val = int(row["Volume"].iloc[0]) if hasattr(row["Volume"], 'iloc') else int(row["Volume"])

                # Sanity check
                if close_val < PRICE_MIN or close_val > PRICE_MAX:
                    print(f"  ⚠ {date_str}: close ₹{close_val:.2f} outside bounds — skipped")
                    continue

                results.append({
                    "trading_date": date_str,
                    "open": round(open_val, 2),
                    "high": round(high_val, 2),
                    "low": round(low_val, 2),
                    "close": round(close_val, 2),
                    "adj_close": round(adj_val, 2),
                    "volume": vol_val,
                    "source": "yfinance",
                })
            return results

        except Exception as e:
            print(f"  ✗ yfinance error (attempt {attempt+1}/{max_retries}): {e}")
            time.sleep(3 * (attempt + 1))

    return []


# ── Main logic ──────────────────────────────────────────────────────────────
def refresh_prices(start_date, end_date):
    """Fetch prices and upsert to Supabase, skipping dates we already have."""
    prices = fetch_prices(start_date, end_date)
    if not prices:
        print("  ✗ No prices fetched from yfinance")
        return 0

    inserted = 0
    for p in prices:
        date_iso = p["trading_date"]
        existing = supabase_get(date_iso)
        if existing:
            print(f"  · {date_iso}: ₹{p['close']:.2f} — already in DB, skipping")
            continue

        try:
            supabase_upsert(p)
            print(f"  ✓ {date_iso}: ₹{p['close']:.2f} (vol: {p['volume']:,})")
            inserted += 1
        except Exception as e:
            print(f"  ✗ {date_iso}: upsert failed — {e}")

    return inserted


def main():
    print(f"═══ MCX Share Price Refresh ({TICKER}) ═══")
    print(f"    Time: {now_ist().strftime('%Y-%m-%d %H:%M IST')}")

    if len(sys.argv) > 1 and sys.argv[1] == "--backfill":
        days = int(sys.argv[2]) if len(sys.argv) > 2 else 400
        end_date = now_ist().strftime("%Y-%m-%d")
        start_date = (now_ist() - timedelta(days=days)).strftime("%Y-%m-%d")
        print(f"    Mode: backfill {days} days ({start_date} → {end_date})")
    elif len(sys.argv) > 1:
        # Specific date
        target = sys.argv[1]
        start_date = target
        end_date = (datetime.strptime(target, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"    Mode: single date {target}")
    else:
        # Default: last 7 days
        end_date = (now_ist() + timedelta(days=1)).strftime("%Y-%m-%d")
        start_date = (now_ist() - timedelta(days=7)).strftime("%Y-%m-%d")
        print(f"    Mode: last 7 days ({start_date} → {end_date})")

    count = refresh_prices(start_date, end_date)
    print(f"\n    Done — {count} new rows inserted")


if __name__ == "__main__":
    main()
