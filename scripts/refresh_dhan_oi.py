#!/usr/bin/env python3
"""
Daily EOD: fetch live per-commodity OI from Dhan and upsert into
mcx_commodity_daily so the signals cron's OI z-score factor stops being null.

Why this exists:
  The MCX historical detailed-report endpoint that bhav_refresh uses does NOT
  return open interest, so mcx_commodity_daily.open_interest is 0 for every
  commodity on every date. This script fills today's row using Dhan's live
  quote API (sum of OI across all live FUTCOM expiries per commodity).

Scope: covers the top-turnover MCX commodities that drive ~99% of exchange
revenue. Historical OI cannot be backfilled (no source exists); only forward.

Usage:
  python3 scripts/refresh_dhan_oi.py              # today's IST date
  python3 scripts/refresh_dhan_oi.py 2026-05-27   # specific date
"""
from __future__ import annotations
import sys, os, datetime as dt
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lib.dhan_client import fetch_mcx_commodity_oi
from lib.mcx_config import supabase_upsert, now_ist, MCX_HOLIDAYS_2026, COMMODITY_HEAD


def closed_session_date() -> str:
    """Return the most recent CLOSED MCX trading session date (IST).
    MCX session is 09:00-23:30 IST. Between 23:30 today and 09:00 tomorrow,
    today's date is the closed session. Between 00:00 and 09:00, it's yesterday.
    Weekends/holidays: walk back to the prior trading day.
    """
    ist = now_ist()
    d = ist.date()
    if ist.hour < 9:
        d = d - dt.timedelta(days=1)
    # Walk back over weekends / full-day holidays
    while d.weekday() >= 5 or d.strftime("%Y-%m-%d") in MCX_HOLIDAYS_2026:
        d = d - dt.timedelta(days=1)
    return d.strftime("%Y-%m-%d")


COMMODITIES = [
    "SILVER", "SILVERM", "SILVERMIC",
    "GOLD",   "GOLDM",   "GOLDGUINEA", "GOLDPETAL", "GOLDTEN",
    "CRUDEOIL", "CRUDEOILM",
    "NATURALGAS", "NATGASMINI",
    "COPPER",
    "ZINC", "ZINCMINI",
    "ALUMINIUM", "ALUMINI",
    "LEAD", "LEADMINI",
    "NICKEL",
    "COTTON", "MENTHAOIL", "CARDAMOM",
]


def main() -> int:
    trading_date = sys.argv[1] if len(sys.argv) > 1 else closed_session_date()
    print(f"Fetching live Dhan OI for {len(COMMODITIES)} commodities on {trading_date}...")

    rows = fetch_mcx_commodity_oi(COMMODITIES, trading_date=trading_date)
    rows = [r for r in rows if r["open_interest"] > 0]
    print(f"  got {len(rows)} commodities with non-zero OI")

    if not rows:
        print("nothing to upsert"); return 1

    # Upsert into mcx_commodity_daily. We do not overwrite turnover/volume/etc.
    # added by bhav_refresh — Supabase merge-duplicates leaves untouched columns
    # alone when only OI fields are provided, BUT we need to be careful: if
    # this row didn't exist yet (today's row may be empty until bhav_refresh
    # runs at 19:30 IST), the insert creates a stub. That's fine — bhav_refresh
    # will update other fields when it runs.
    payload = [{
        "trading_date": r["trading_date"],
        "commodity":    r["commodity"],
        # commodity_head has a NOT-NULL constraint downstream; fall back to the
        # static taxonomy when this row is being created fresh by the OI cron
        # (bhav_refresh will overwrite it correctly when it runs later).
        "commodity_head": COMMODITY_HEAD.get(r["commodity"], "OTHER"),
        "instrument_type": r["instrument_type"],
        "open_interest": r["open_interest"],
        "oi_value_cr":   r["oi_value_cr"],
        "source":        r["source"],
    } for r in rows]

    try:
        supabase_upsert("mcx_commodity_daily", payload)
        print(f"  ✓ upserted {len(payload)} rows to mcx_commodity_daily")
    except Exception as e:
        print(f"  ✗ upsert failed: {e}")
        return 2

    # Summary
    print(f"\n{'commodity':12s}{'OI(lots)':>12s}")
    for r in sorted(rows, key=lambda x: -x["open_interest"])[:10]:
        print(f"{r['commodity']:12s}{r['open_interest']:>12,d}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
