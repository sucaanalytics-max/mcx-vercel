#!/usr/bin/env python3
"""
Commodity Price Refresh — yfinance → Supabase (keyless replacement for Alpha Vantage)

Fetches WTI Crude (CL=F), Henry Hub Natural Gas (NG=F) and USD/INR (INR=X)
daily closes from Yahoo Finance and upserts them into mcx_commodity_prices.
The /api/commodities endpoint then reads this table (local fetch → Supabase →
Vercel reads), so the Commodities price panel works without any API key and
without hitting Yahoo from Vercel's rate-limited datacenter IPs.

Rows written (commodity ∈ WTI | NATGAS | USDINR):
  price_date (YYYY-MM-DD), commodity, value_usd, value_inr, fetched_at

Usage:
  python3 scripts/commodity_price_refresh.py             # last ~90 calendar days
  python3 scripts/commodity_price_refresh.py --days 400  # backfill N days
"""
import sys, os, json, urllib.request, urllib.error, argparse
from datetime import datetime, timedelta, timezone

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://avqwpebveqetwwzkmtux.supabase.co")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImF2cXdwZWJ2ZXFldHd3emttdHV4Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzE0MDkwMzMsImV4cCI6MjA4Njk4NTAzM30.U_Ug61Fp1NSCesXBkYU7GJGTbuATFtXsz6GTi5948Rw")
SUPABASE_WRITE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "") or SUPABASE_ANON_KEY
TABLE = "mcx_commodity_prices"

TICKERS = {"CL=F": "WTI", "NG=F": "NATGAS", "INR=X": "USDINR"}


def now_ist():
    return datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)


def _headers(write=False):
    key = SUPABASE_WRITE_KEY if write else SUPABASE_ANON_KEY
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }


def upsert_rows(rows):
    """Bulk upsert on (price_date, commodity). Falls back to per-row POST/PATCH
    if the table has no matching unique constraint (PostgREST 42P10)."""
    if not rows:
        return 0
    body = json.dumps(rows).encode()
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}?on_conflict=price_date,commodity"
    h = _headers(write=True)
    h["Prefer"] = "resolution=merge-duplicates,return=minimal"
    req = urllib.request.Request(url, data=body, headers=h, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30):
            return len(rows)
    except urllib.error.HTTPError as e:
        err = e.read().decode() if e.fp else ""
        if "42P10" in err or "no unique or exclusion constraint" in err:
            print("  ⚠ no (price_date,commodity) constraint — using POST/PATCH fallback")
            return _upsert_fallback(rows)
        raise Exception(f"Supabase {e.code}: {err[:200]}")


def _upsert_fallback(rows):
    n = 0
    for row in rows:
        body = json.dumps(row).encode()
        h = _headers(write=True)
        h["Prefer"] = "return=minimal"
        try:
            req = urllib.request.Request(f"{SUPABASE_URL}/rest/v1/{TABLE}",
                                         data=body, headers=h, method="POST")
            with urllib.request.urlopen(req, timeout=15):
                n += 1
        except urllib.error.HTTPError as e:
            if e.code in (409, 400):
                flt = f"?price_date=eq.{row['price_date']}&commodity=eq.{row['commodity']}"
                req2 = urllib.request.Request(f"{SUPABASE_URL}/rest/v1/{TABLE}{flt}",
                                              data=body, headers=h, method="PATCH")
                with urllib.request.urlopen(req2, timeout=15):
                    n += 1
            else:
                raise
    return n


def fetch(days):
    import warnings
    warnings.filterwarnings("ignore")
    import yfinance as yf
    period = f"{max(days, 7)}d"
    df = yf.download(list(TICKERS.keys()), period=period, interval="1d",
                     progress=False, auto_adjust=True)
    close = df["Close"]
    # Forward/back-fill FX so every commodity date has a rate; commodities keep NaN.
    fx = close["INR=X"].ffill().bfill()
    fetched = now_ist().strftime("%Y-%m-%dT%H:%M:%S")
    rows = []
    for ts, r in close.iterrows():
        date_iso = ts.strftime("%Y-%m-%d")
        rate = fx.get(ts)
        rate = round(float(rate), 4) if rate == rate else None  # NaN check
        for tkr, name in TICKERS.items():
            v = r.get(tkr)
            if v != v or v is None:  # skip NaN (e.g. US holiday)
                continue
            v = float(v)
            if name == "USDINR":
                rows.append({"price_date": date_iso, "commodity": "USDINR",
                             "value_usd": round(v, 4), "value_inr": None,
                             "fetched_at": fetched})
            else:
                rows.append({"price_date": date_iso, "commodity": name,
                             "value_usd": round(v, 4),
                             "value_inr": round(v * rate, 2) if rate else None,
                             "fetched_at": fetched})
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=90)
    args = ap.parse_args()
    print(f"Fetching WTI/NatGas/USDINR ({args.days}d) from Yahoo Finance...")
    rows = fetch(args.days)
    if not rows:
        print("✗ No rows fetched"); sys.exit(1)
    n = upsert_rows(rows)
    latest = {}
    for r in rows:
        c = r["commodity"]
        if c not in latest or r["price_date"] > latest[c]["price_date"]:
            latest[c] = r
    print(f"✓ Upserted {n} rows")
    for c, r in sorted(latest.items()):
        print(f"  {c:7s} {r['price_date']}  usd={r['value_usd']}  inr={r['value_inr']}")


if __name__ == "__main__":
    main()
