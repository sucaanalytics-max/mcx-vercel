#!/usr/bin/env python3
"""
Find and repair dates where mcx_commodity_daily is missing or incomplete.

A date is a GAP when mcx_daily_revenue has real exchange turnover for it but
mcx_commodity_daily has no rows with turnover_cr > 0. That covers both shapes
seen in production:

  * no rows at all      — bhav_refresh never reached the date (relay wrote the
                          aggregate, or the MCX endpoint was down)
  * OI-only stub rows   — refresh_dhan_oi created the date first; turnover_cr
                          defaults to 0 and the old coverage check then treated
                          the date as done

Usage:
  python3 scripts/backfill_commodity_gaps.py --check         # report only
  python3 scripts/backfill_commodity_gaps.py                 # repair all gaps
  python3 scripts/backfill_commodity_gaps.py 2026-06-04 ...  # repair specific dates
  python3 scripts/backfill_commodity_gaps.py --since 2026-01-01
"""
import sys, os, json, time, urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.bhav_refresh import (
    SUPABASE_URL, SUPABASE_ANON_KEY,
    _fetch_mcx_raw, _aggregate_commodity_rows, _upsert_commodity_batch,
)

FUT_RATE, OPT_RATE, BOTH = 210.0, 4180.0, 2.0


def _get(path):
    req = urllib.request.Request(f"{SUPABASE_URL}/rest/v1/{path}", headers={
        "apikey": SUPABASE_ANON_KEY, "Authorization": f"Bearer {SUPABASE_ANON_KEY}"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode())


def _paged(path, page=1000):
    out, off = [], 0
    while True:
        chunk = _get(f"{path}&limit={page}&offset={off}")
        out += chunk
        if len(chunk) < page:
            return out
        off += page


def find_gaps(since="2022-01-01"):
    """Dates with real exchange revenue but no real commodity breakdown."""
    rev = _paged(f"mcx_daily_revenue?select=trading_date,fut_notl_cr,opt_prem_cr"
                 f"&trading_date=gte.{since}&order=trading_date.asc")
    have = {r["trading_date"] for r in _paged(
        f"mcx_commodity_daily?select=trading_date&turnover_cr=gt.0"
        f"&trading_date=gte.{since}&order=trading_date.asc")}
    gaps = []
    for r in rev:
        if (r.get("fut_notl_cr") or 0) <= 0:
            continue
        if r["trading_date"] not in have:
            gaps.append(r)
    return gaps


def repair(date_iso):
    """Re-fetch and write the per-commodity breakdown for one date."""
    raw, err = _fetch_mcx_raw(date_iso)
    if not raw:
        return False, err or "no data"
    rows = _aggregate_commodity_rows(date_iso, raw)
    if not rows:
        return False, "no commodity rows parsed"
    if not _upsert_commodity_batch(rows):
        return False, "write failed"
    return True, f"{len(rows)} rows"


def verify(date_iso):
    """Compare the rebuilt breakdown against the exchange aggregate."""
    rev = _get(f"mcx_daily_revenue?trading_date=eq.{date_iso}"
               f"&select=fut_notl_cr,opt_prem_cr")
    cd = _paged(f"mcx_commodity_daily?trading_date=eq.{date_iso}"
                f"&select=instrument_type,turnover_cr,premium_turnover_cr&order=commodity.asc")
    if not rev or not cd:
        return None
    t = rev[0]
    fut = sum(r["turnover_cr"] or 0 for r in cd if not r["instrument_type"].startswith("OPT"))
    prem = sum(r["premium_turnover_cr"] or 0 for r in cd if r["instrument_type"].startswith("OPT"))
    fe = 100 * (fut - t["fut_notl_cr"]) / t["fut_notl_cr"] if t["fut_notl_cr"] else None
    pe = (100 * (prem - t["opt_prem_cr"]) / t["opt_prem_cr"]) if t.get("opt_prem_cr") else None
    return {"rows": len(cd), "fut_err_pct": fe, "prem_err_pct": pe}


def main():
    args = [a for a in sys.argv[1:]]
    check_only = "--check" in args
    args = [a for a in args if a != "--check"]
    since = "2022-01-01"
    if "--since" in args:
        i = args.index("--since"); since = args[i + 1]; args = args[:i] + args[i + 2:]

    targets = args if args else [g["trading_date"] for g in find_gaps(since)]

    if not targets:
        print("No gaps found — mcx_commodity_daily is complete.")
        return 0

    print(f"{len(targets)} date(s) with missing commodity detail:")
    for d in targets:
        print(f"   {d}")
    if check_only:
        return 1

    print()
    ok_n = 0
    for d in targets:
        print(f"Repairing {d}...")
        ok, msg = repair(d)
        if not ok:
            print(f"  ✗ {msg}")
            continue
        v = verify(d)
        if v and v["fut_err_pct"] is not None and abs(v["fut_err_pct"]) < 0.5:
            pe = f"{v['prem_err_pct']:+.3f}%" if v["prem_err_pct"] is not None else "n/a"
            print(f"  ✓ {msg} | futures {v['fut_err_pct']:+.3f}% vs exchange total, premium {pe}")
            ok_n += 1
        else:
            print(f"  ⚠ wrote rows but reconciliation looks off: {v}")
        time.sleep(1.0)  # be polite to MCX

    print(f"\n{ok_n}/{len(targets)} repaired and reconciled.")
    return 0 if ok_n == len(targets) else 1


if __name__ == "__main__":
    sys.exit(main())
