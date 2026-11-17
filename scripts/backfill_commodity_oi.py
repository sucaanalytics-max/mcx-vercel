#!/usr/bin/env python3
"""
Backfill (or nightly-refresh) futures open interest in mcx_commodity_daily from
MCX's own daily Bhavcopy (lib/mcx_bhavcopy.py).

Why: MCX's Historical Detailed Report, which bhav_refresh writes the breakdown
from, has no OI column, so those rows carry open_interest = 0 until this runs.
Until Nov 2026 the OI came from Dhan's stitched history, which needed a Dhan
login: a PIN+TOTP mint on one machine revokes the token on any other, and the
relay Mac and the options-paper laptop share the account. The Bhavcopy needs no
login, is the exchange's own total over every open contract, and goes back to
2022 (see lib/mcx_bhavcopy.py for the validation against the Dhan figures).

Writes ONLY open_interest and oi_value_cr, and only onto futures rows (FUTCOM,
FUTIDX) that already exist with turnover_cr > 0, so it never creates stub rows.
A date MCX has no Bhavcopy for (holiday, not yet published) is left as stored.

Usage (deps from pyproject.toml + uv.lock via scripts/mcx_uv.sh):
  scripts/mcx_uv.sh python scripts/backfill_commodity_oi.py --dry-run
  scripts/mcx_uv.sh python scripts/backfill_commodity_oi.py            # 2022 → today
  scripts/mcx_uv.sh python scripts/backfill_commodity_oi.py --nightly  # last 30 days
  ... --from 2026-11-01 --to 2026-11-16 --only GOLD SILVER --report before_after.csv
"""
from __future__ import annotations
import argparse, csv, sys, time, datetime as dt
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lib.supabase import supabase_read_all, upsert_batches   # noqa: E402

TABLE = "mcx_commodity_daily"
HISTORY_START = "2022-01-01"   # first mcx_commodity_daily row is 2022-01-03
NIGHTLY_DAYS = 30              # re-fetch window: a late or re-published Bhavcopy is picked up
FUT_TYPES = ("FUTCOM", "FUTIDX")
PAUSE_S = 0.5                  # between dates: one Bhavcopy request per instrument per date


def existing_futures_rows(frm: str, to: str, only=None) -> dict[str, dict[tuple, dict]]:
    """date -> {(commodity, instrument_type): stored row} for real futures rows
    (turnover > 0) in [frm, to]."""
    rows = supabase_read_all(
        TABLE,
        "?select=trading_date,commodity,instrument_type,open_interest,oi_value_cr"
        f"&instrument_type=in.({','.join(FUT_TYPES)})&turnover_cr=gt.0"
        f"&trading_date=gte.{frm}&trading_date=lte.{to}",
        max_rows=None, timeout=30)
    out: dict[str, dict[tuple, dict]] = {}
    for r in rows:
        if only and r["commodity"] not in only:
            continue
        out.setdefault(r["trading_date"], {})[(r["commodity"], r["instrument_type"])] = r
    return out


def build_payload(have: dict, fetch, pause=PAUSE_S, sleep=time.sleep):
    """(payload, changes, failed_dates, empty_dates) for the dates in `have`.

    fetch(date_iso, fallback_mult) -> (futures OI dict, lot multipliers), as
    lib.mcx_bhavcopy.fetch_futures_oi with its session bound. Lot multipliers
    carry forward across dates (oldest first) so an untraded contract can still
    be valued. changes: one dict per row with the stored and new figures."""
    payload, changes, failed, empty = [], [], [], []
    mult: dict[str, float] = {}
    for i, d in enumerate(sorted(have)):
        if i:
            sleep(pause)
        try:
            oi, day_mult = fetch(d, mult)
        except Exception as e:   # noqa: BLE001 — reported, the run goes on
            print(f"  {d}: ✗ {type(e).__name__}: {str(e)[:120]}")
            failed.append(d)
            continue
        mult.update(day_mult)
        if not oi:
            empty.append(d)
            continue
        for key, stored in have[d].items():
            new = oi.get(key)
            if new is None:
                continue          # symbol absent from MCX's report: keep what is stored
            row = {"trading_date": d, "commodity": key[0], "instrument_type": key[1],
                   "open_interest": new["open_interest"],
                   "oi_value_cr": new["oi_value_cr"] if new["oi_value_cr"] is not None
                   else stored.get("oi_value_cr") or 0}
            payload.append(row)
            changes.append({**row, "old_open_interest": stored.get("open_interest"),
                            "old_oi_value_cr": stored.get("oi_value_cr")})
    return payload, changes, failed, empty


def summarize(changes) -> None:
    by_c: dict[str, list] = {}
    for c in changes:
        by_c.setdefault(c["commodity"], []).append(c)
    print(f"{'commodity':12s}{'rows':>6s}{'was 0':>7s}{'changed':>8s}{'median new/old':>16s}"
          f"{'last date':>12s}{'last OI':>10s}")
    for name in sorted(by_c):
        cs = by_c[name]
        zero = sum(1 for c in cs if not c["old_open_interest"])
        changed = sum(1 for c in cs if c["old_open_interest"] != c["open_interest"])
        ratios = sorted(c["open_interest"] / c["old_open_interest"] for c in cs
                        if c["old_open_interest"] and c["open_interest"])
        med = f"{ratios[len(ratios) // 2]:.3f}" if ratios else "—"
        last = max(cs, key=lambda c: c["trading_date"])
        print(f"{name:12s}{len(cs):6d}{zero:7d}{changed:8d}{med:>16s}"
              f"{last['trading_date']:>12s}{last['open_interest']:>10,d}")


def upsert(payload: list[dict]) -> None:
    """Write OI onto existing rows (merge on the table's unique key: only the
    columns sent are updated). Raises if any batch fails."""
    errors = upsert_batches(TABLE, payload, on_conflict="trading_date,commodity,instrument_type",
                            batch=500, timeout=60)
    if errors:
        raise RuntimeError(f"upsert: {len(errors)} batch(es) failed; first: {errors[0][:300]}")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="fetch and report, write nothing")
    ap.add_argument("--nightly", action="store_true", help=f"only the last {NIGHTLY_DAYS} days")
    ap.add_argument("--from", dest="frm", default=HISTORY_START)
    ap.add_argument("--to", dest="to", default=None, help="last date (default today)")
    ap.add_argument("--only", nargs="*", help="limit to these commodities")
    ap.add_argument("--report", help="write every row's old and new OI to this CSV")
    a = ap.parse_args(argv)

    today = dt.date.today()
    frm = (today - dt.timedelta(days=NIGHTLY_DAYS)).isoformat() if a.nightly else a.frm
    to = a.to or today.isoformat()
    print(f"OI from MCX Bhavcopy, {'dry run' if a.dry_run else 'write'}: {frm} → {to}")

    have = existing_futures_rows(frm, to, set(a.only) if a.only else None)
    print(f"{sum(len(v) for v in have.values())} futures rows on {len(have)} dates")
    if not have:
        return 0

    from lib.mcx_bhavcopy import new_bhavcopy_session, fetch_futures_oi
    session = new_bhavcopy_session()
    payload, changes, failed, empty = build_payload(
        have, lambda d, m: fetch_futures_oi(session, d, m))

    summarize(changes)
    if a.report:
        with open(a.report, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(changes[0]) if changes else ["trading_date"])
            w.writeheader()
            w.writerows(changes)
        print(f"report: {a.report}")
    if empty:
        print(f"no Bhavcopy yet / holiday ({len(empty)}): {', '.join(empty[-5:])}"
              + (" …" if len(empty) > 5 else ""))
    print(f"{len(payload)} rows to write")
    if not a.dry_run and payload:
        upsert(payload)
        print(f"✓ wrote open_interest / oi_value_cr on {len(payload)} rows")
    if failed:
        print(f"✗ fetch failed for {len(failed)} date(s): {', '.join(failed[:10])}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
