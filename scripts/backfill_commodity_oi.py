#!/usr/bin/env python3
"""
Backfill (or nightly-refresh) futures open interest in mcx_commodity_daily
from Dhan's stitched historical OI (see lib/dhan_client.fetch_commodity_oi_history).

Why: MCX's historical report has no OI column, and refresh_dhan_oi.py (live
quotes) only ever ran on 27–28 May 2026, so open_interest was 0 on every other
date and the signals cron's OI z-score had no input.

Writes ONLY open_interest and oi_value_cr, and only onto FUTCOM rows that
already exist with turnover_cr > 0, so it never creates OI-only stub rows.
Glitch days (one-day spikes/dips that revert) are written as 0 = missing,
which cron_commodity_signals skips.

Usage (deps from pyproject.toml + uv.lock via scripts/mcx_uv.sh):
  scripts/mcx_uv.sh python scripts/backfill_commodity_oi.py --dry-run
  scripts/mcx_uv.sh python scripts/backfill_commodity_oi.py            # 2022 → today
  scripts/mcx_uv.sh python scripts/backfill_commodity_oi.py --nightly  # last 30 days
"""
from __future__ import annotations
import argparse, sys, datetime as dt
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lib.dhan_client import fetch_commodity_oi_history, list_front_futures
from lib.supabase import supabase_read_all, upsert_batches

TABLE = "mcx_commodity_daily"
HISTORY_START = "2022-01-01"   # first mcx_commodity_daily row is 2022-01-03
NIGHTLY_DAYS = 30              # re-fetch window: lets yesterday's last day be glitch-checked


def _get(params: str) -> list[dict]:
    """Every matching row of the table, paged over a total order."""
    return supabase_read_all(TABLE, "?" + params, max_rows=None, timeout=30)


def existing_futcom_rows(frm: str, to: str) -> dict[str, set[str]]:
    """commodity -> trading dates that have a real FUTCOM row (turnover > 0)."""
    rows = _get(f"select=trading_date,commodity&instrument_type=eq.FUTCOM&turnover_cr=gt.0"
                f"&trading_date=gte.{frm}&trading_date=lt.{to}&order=trading_date")
    out: dict[str, set[str]] = {}
    for r in rows:
        out.setdefault(r["commodity"], set()).add(r["trading_date"])
    return out


def upsert(payload: list[dict]) -> None:
    """Write OI onto existing rows (merge on the table's unique key). Raises if
    any batch fails."""
    errors = upsert_batches(TABLE, payload, on_conflict="trading_date,commodity,instrument_type",
                            batch=500, timeout=60)
    if errors:
        raise RuntimeError(f"upsert: {len(errors)} batch(es) failed; first: {errors[0][:300]}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="fetch and report, write nothing")
    ap.add_argument("--nightly", action="store_true", help=f"only the last {NIGHTLY_DAYS} days")
    ap.add_argument("--from", dest="frm", default=HISTORY_START)
    ap.add_argument("--only", nargs="*", help="limit to these commodities")
    a = ap.parse_args()

    today = dt.date.today()
    frm = (today - dt.timedelta(days=NIGHTLY_DAYS)).isoformat() if a.nightly else a.frm
    print(f"OI {'dry run' if a.dry_run else 'write'}: {frm} → {today}")
    payload, failed = build_payload(frm, a.only)
    print(f"{len(payload)} rows to write")
    if not a.dry_run and payload:
        upsert(payload)
        print(f"✓ wrote open_interest / oi_value_cr on {len(payload)} rows")
    if failed:
        print(f"✗ fetch failed for: {', '.join(failed)}")
        return 1
    return 0


def build_payload(frm: str, only: list[str] | None = None) -> tuple[list[dict], list[str]]:
    """OI rows to upsert from `frm` to today, plus commodities whose fetch failed."""
    today = dt.date.today()
    to = (today + dt.timedelta(days=1)).isoformat()      # exclusive
    # Fetch a little earlier than we write so the first written day has a
    # left neighbour for the glitch check.
    fetch_from = (dt.date.fromisoformat(frm) - dt.timedelta(days=10)).isoformat()

    have = existing_futcom_rows(frm, to)
    commodities = sorted(only or have)

    payload, skipped, failed = [], [], []
    print(f"{'commodity':12s}{'rows':>6s}{'matched':>8s}{'glitch':>7s}{'first':>12s}{'last':>12s}{'last OI':>10s}")
    for c in commodities:
        if not list_front_futures(c):
            skipped.append(c)
            continue
        try:
            h = fetch_commodity_oi_history(c, fetch_from, to)
        except Exception as e:
            print(f"{c:12s}  ✗ {e}")
            failed.append(c)
            continue
        dates = have.get(c, set())
        h = h[h.index.isin(dates)] if len(h) else h
        if not len(h):
            skipped.append(c)
            continue
        for d, r in h.iterrows():
            glitch = bool(r.glitch)
            payload.append({
                "trading_date": d, "commodity": c, "instrument_type": "FUTCOM",
                "open_interest": 0 if glitch else int(r.open_interest),
                "oi_value_cr": 0 if glitch else float(r.oi_value_cr),
            })
        print(f"{c:12s}{len(dates):6d}{len(h):8d}{int(h.glitch.sum()):7d}"
              f"{h.index[0]:>12s}{h.index[-1]:>12s}{int(h.raw_oi.iloc[-1]):>10,d}")

    if skipped:
        print(f"no Dhan OI for: {', '.join(skipped)}")
    return payload, failed


if __name__ == "__main__":
    sys.exit(main())
