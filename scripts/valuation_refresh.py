#!/usr/bin/env python3
"""
MCX EPS-Path Valuation Refresh — Compute & store daily valuations in Supabase.

Reads mcx_daily_revenue + mcx_share_price, computes:
  45DMA Rev → Annualized Rev → PAT → EPS → Fair Value (Bear/Base/Bull)
  → Implied P/E → Signal (DEEP_VALUE / UNDERVALUED / FAIR / OVERVALUED / STRETCHED)

Usage:
  python3 scripts/valuation_refresh.py              # refresh last 30 days
  python3 scripts/valuation_refresh.py --backfill   # full historical backfill
  python3 scripts/valuation_refresh.py --latest     # just today / latest
"""
import sys, os, json, math, urllib.request, urllib.error
from datetime import datetime, timedelta, timezone

# ── Config ──────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://avqwpebveqetwwzkmtux.supabase.co")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImF2cXdwZWJ2ZXFldHd3emttdHV4Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzE0MDkwMzMsImV4cCI6MjA4Njk4NTAzM30.U_Ug61Fp1NSCesXBkYU7GJGTbuATFtXsz6GTi5948Rw")

# Model parameters (same as mcx_config.py)
TRADING_DAYS = 252
PAT_MARGIN = 0.55
NON_FO_REV_ANNUAL_CR = 527.0
DILUTED_SHARES_CR = 25.451
PE_MEAN_DEFAULT = 34.79
PE_SD_DEFAULT = 3.49
MA_WINDOW = 45
PE_LOOKBACK = 252   # trailing trading days for dynamic PE (1 year)


def now_ist():
    return datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)


# ── Supabase helpers ────────────────────────────────────────────────────────
def sb_get(table, params=""):
    url = f"{SUPABASE_URL}/rest/v1/{table}{params}"
    req = urllib.request.Request(url, headers={
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
    })
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode())


def sb_upsert(table, rows):
    """Batch upsert rows to Supabase."""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }
    # Batch in chunks of 50
    for i in range(0, len(rows), 50):
        chunk = rows[i:i+50]
        body = json.dumps(chunk).encode()
        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                pass
        except urllib.error.HTTPError as e:
            err = e.read().decode()[:200] if e.fp else ""
            print(f"  ✗ Upsert error batch {i}: {e.code} — {err}")


def fetch_all_paginated(table, select, order_col="trading_date"):
    """Fetch all rows with pagination (1000/page)."""
    all_rows = []
    offset = 0
    while True:
        params = f"?select={select}&order={order_col}.asc&limit=1000&offset={offset}"
        rows = sb_get(table, params)
        all_rows.extend(rows)
        if len(rows) < 1000:
            break
        offset += 1000
    return all_rows


# ── Valuation computation ──────────────────────────────────────────────────

def compute_valuations(rev_rows, price_rows, pe_mean=None, pe_sd=None):
    """
    Compute daily valuations for each date that has both revenue (45DMA) and price.
    Returns list of valuation dicts ready for Supabase upsert.
    """
    # Build price lookup: date → close
    price_map = {}
    for r in price_rows:
        if r.get("close"):
            price_map[r["trading_date"]] = float(r["close"])

    # Build daily revenue list (ascending by date)
    rev_list = []
    for r in rev_rows:
        if r.get("total_rev_cr") is not None:
            rev_list.append({
                "date": r["trading_date"],
                "rev": float(r["total_rev_cr"]),
            })

    if len(rev_list) < MA_WINDOW:
        print(f"  ⚠ Only {len(rev_list)} revenue days — need at least {MA_WINDOW}")
        return []

    # First pass: compute EPS series to derive dynamic P/E
    eps_series = []
    for i in range(MA_WINDOW - 1, len(rev_list)):
        window_revs = [rev_list[j]["rev"] for j in range(i - MA_WINDOW + 1, i + 1)]
        ma45 = sum(window_revs) / MA_WINDOW
        annual_fo = ma45 * TRADING_DAYS
        annual_total = annual_fo + NON_FO_REV_ANNUAL_CR
        pat = annual_total * PAT_MARGIN
        eps = pat / DILUTED_SHARES_CR

        date_str = rev_list[i]["date"]
        price = price_map.get(date_str)

        eps_series.append({
            "date": date_str,
            "daily_rev": rev_list[i]["rev"],
            "ma45": ma45,
            "annual_total": annual_total,
            "pat": pat,
            "eps": eps,
            "price": price,
            "implied_pe": round(price / eps, 2) if (price and eps > 0) else None,
        })

    # Compute dynamic P/E: robust median + scaled MAD (matches cron_valuation.py)
    if pe_mean is None or pe_sd is None:
        all_pes = [e["implied_pe"] for e in eps_series if e["implied_pe"] is not None]
        recent_pes = all_pes[-PE_LOOKBACK:] if len(all_pes) > PE_LOOKBACK else all_pes
        if len(recent_pes) >= 30:
            recent_sorted = sorted(recent_pes)
            n = len(recent_sorted)
            pe_median = recent_sorted[n // 2] if n % 2 else (recent_sorted[n // 2 - 1] + recent_sorted[n // 2]) / 2
            abs_devs = sorted(abs(p - pe_median) for p in recent_sorted)
            mad_raw = abs_devs[len(abs_devs) // 2] if len(abs_devs) % 2 else (abs_devs[len(abs_devs) // 2 - 1] + abs_devs[len(abs_devs) // 2]) / 2
            mad_scaled = mad_raw * 1.4826  # scale factor for normal-equivalent SD
            pe_mean = round(pe_median, 2)
            pe_sd = round(mad_scaled, 2) if mad_scaled > 0.5 else round(math.sqrt(sum((p - pe_median) ** 2 for p in recent_pes) / n), 2)
            print(f"  Dynamic P/E: median={pe_mean:.2f}, MAD-sd={pe_sd:.2f} (n={len(recent_pes)}, lookback={PE_LOOKBACK})")
        else:
            pe_mean = PE_MEAN_DEFAULT
            pe_sd = PE_SD_DEFAULT
            print(f"  Using default P/E: mean={pe_mean}, sd={pe_sd} (only {len(recent_pes)} points)")

    pe_bear = max(pe_mean - pe_sd, 5.0)
    pe_bull = pe_mean + pe_sd

    # Second pass: build valuation rows
    valuations = []
    for e in eps_series:
        eps = e["eps"]
        price = e["price"]

        fv_bear = round(eps * pe_bear, 2)
        fv_base = round(eps * pe_mean, 2)
        fv_bull = round(eps * pe_bull, 2)

        signal = "NO_PRICE"
        if price and eps > 0:
            if price < fv_bear:
                signal = "DEEP_VALUE"
            elif price < fv_base * 0.95:
                signal = "UNDERVALUED"
            elif price <= fv_base * 1.05:
                signal = "FAIR"
            elif price <= fv_bull:
                signal = "OVERVALUED"
            else:
                signal = "STRETCHED"

        valuations.append({
            "trading_date": e["date"],
            "daily_rev_cr": round(e["daily_rev"], 2),
            "ma45_rev_cr": round(e["ma45"], 2),
            "annualized_rev_cr": round(e["annual_total"], 2),
            "pat_cr": round(e["pat"], 2),
            "eps": round(eps, 2),
            "close_price": price,
            "implied_pe": e["implied_pe"],
            "fair_value_bear": fv_bear,
            "fair_value_base": fv_base,
            "fair_value_bull": fv_bull,
            "signal": signal,
            "pe_mean_used": round(pe_mean, 2),
            "pe_sd_used": round(pe_sd, 2),
        })

    return valuations


# ── Main ──────────────────────────────────────────────────────────────────

def main():
    print(f"═══ MCX EPS-Path Valuation Refresh ═══")
    print(f"    Time: {now_ist().strftime('%Y-%m-%d %H:%M IST')}")

    mode = "recent"
    if "--backfill" in sys.argv:
        mode = "backfill"
    elif "--latest" in sys.argv:
        mode = "latest"

    # Fetch all revenue and price data
    print("  Fetching revenue data...")
    rev_rows = fetch_all_paginated("mcx_daily_revenue", "trading_date,total_rev_cr,source")
    print(f"    → {len(rev_rows)} revenue rows")

    print("  Fetching price data...")
    price_rows = fetch_all_paginated("mcx_share_price", "trading_date,close")
    print(f"    → {len(price_rows)} price rows")

    # Compute all valuations
    print("  Computing valuations...")
    valuations = compute_valuations(rev_rows, price_rows)
    print(f"    → {len(valuations)} valuation rows computed")

    if not valuations:
        print("  ✗ No valuations to write")
        return

    # Filter based on mode
    if mode == "latest":
        valuations = valuations[-1:]
    elif mode == "recent":
        valuations = valuations[-30:]
    # backfill = all

    print(f"  Upserting {len(valuations)} rows ({mode} mode)...")
    sb_upsert("mcx_valuation", valuations)

    # Print summary
    latest = valuations[-1]
    print(f"\n  ── Latest Valuation ({latest['trading_date']}) ──")
    print(f"     45DMA Rev:     ₹{latest['ma45_rev_cr']:.2f} Cr/day")
    print(f"     Annual Rev:    ₹{latest['annualized_rev_cr']:.0f} Cr")
    print(f"     PAT:           ₹{latest['pat_cr']:.0f} Cr")
    print(f"     EPS:           ₹{latest['eps']:.2f}")
    print(f"     Close Price:   ₹{latest['close_price']}")
    print(f"     Implied P/E:   {latest['implied_pe']}x")
    print(f"     Fair Value:    ₹{latest['fair_value_bear']} / ₹{latest['fair_value_base']} / ₹{latest['fair_value_bull']}")
    print(f"     Signal:        {latest['signal']}")
    print(f"     P/E Band:      {latest['pe_mean_used']}x ± {latest['pe_sd_used']}x")

    print(f"\n    Done — {len(valuations)} rows upserted")


if __name__ == "__main__":
    main()
