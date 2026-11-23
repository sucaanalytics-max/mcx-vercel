#!/usr/bin/env python3
"""
Per-contract MCX futures EOD files from MCX's Bhavcopy (lib/contract_eod.py):
one CSV per session in ${MCX_CONTRACT_EOD_DIR:-~/tradingone/data/mcx_contract_eod},
every FUTCOM contract and expiry with MCX's official close. Files only:
nothing goes to Supabase.

Resumable: a date that already has a file is skipped without a fetch, so
re-running the same command carries on where it stopped. One Bhavcopy request
per date, paced by --pause (at least 1 s). It stops cleanly, keeping every
file written so far, when:
  - a fetch still fails after one fresh session (a block, an HTML page, a timeout);
  - MAX_NO_REPORT sessions in a row have no report (MCX answering every date
    with another day's report, i.e. ignoring the date);
  - a report has rows but none usable (a format change);
  - a file can't be written.
Exit 0 when done, 1 when stopped early. A session with no report (a holiday,
or a report not published yet) is counted, not written, and retried next run.

Usage (deps from pyproject.toml + uv.lock via scripts/mcx_uv.sh):
  scripts/mcx_uv.sh python scripts/backfill_contract_eod.py --start 2022-01-01   # history (resumable)
  scripts/mcx_uv.sh python scripts/backfill_contract_eod.py --recent 10          # daily_verify, 07:00 IST
  ... --end 2026-11-20 (default yesterday)  --dir DIR  --pause 2  --weekends
"""
from __future__ import annotations
import argparse, sys, time
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lib.calendar import has_session, now_ist  # noqa: E402
from lib.contract_eod import (  # noqa: E402
    bhavcopy_rows, contract_eod_dir, pending_dates, recent_session_dates,
    session_dates, write_contract_eod,
)

HISTORY_START = "2022-01-01"
PAUSE_S = 1.5
MIN_PAUSE_S = 1.0
MAX_NO_REPORT = 5   # MCX never closes more than 2-3 sessions in a row


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description="Per-contract MCX futures EOD CSVs from the Bhavcopy")
    ap.add_argument("--start", default=HISTORY_START, help=f"first date (default {HISTORY_START})")
    ap.add_argument("--end", default=None, help="last date (default yesterday, IST)")
    ap.add_argument("--recent", type=int, default=None,
                    help="instead of --start/--end: the last N sessions before today")
    ap.add_argument("--dir", default=None, help="output folder (default $MCX_CONTRACT_EOD_DIR "
                                                "or ~/tradingone/data/mcx_contract_eod)")
    ap.add_argument("--pause", type=float, default=PAUSE_S,
                    help=f"seconds between requests (default {PAUSE_S}, at least {MIN_PAUSE_S})")
    ap.add_argument("--weekends", action="store_true",
                    help="also try Saturdays and Sundays (special sessions lib.calendar doesn't list)")
    a = ap.parse_args(argv)
    a.pause = max(a.pause, MIN_PAUSE_S)
    return a


def plan_dates(a, today):
    """The session dates to cover, oldest first."""
    if a.recent:
        return recent_session_dates(today, a.recent)
    end = a.end or (today - timedelta(days=1)).isoformat()
    return session_dates(a.start, end, weekends=a.weekends)


def _stop(s, d, reason, log):
    s["stopped"], s["stopped_at"] = reason, d
    log(f"  {d}: ✗ {reason}")
    return s


def run(dates, fetch, out_dir, pause=PAUSE_S, sleep=time.sleep, now=now_ist,
        reconnect=None, max_no_report=MAX_NO_REPORT, log=print):
    """Write the files for `dates` that have none yet, oldest first.

    fetch(date_iso) -> the Bhavcopy's raw rows for that date, [] when MCX has
    no report for it (lib.mcx_bhavcopy.fetch_day). reconnect() replaces the
    session after a failed fetch; the date is then tried once more. Never
    raises. Returns a summary: written / kept / no_report (date lists),
    present (files already there), contracts, stopped (reason or None),
    stopped_at."""
    s = {"written": [], "kept": [], "no_report": [], "present": 0, "contracts": 0,
         "stopped": None, "stopped_at": None}
    todo = pending_dates(dates, out_dir)
    s["present"] = len(dates) - len(todo)
    streak = 0
    for i, d in enumerate(todo):
        if i:
            sleep(pause)
        try:
            raw = fetch(d)
        except Exception as e:  # noqa: BLE001 — one fresh session, then stop
            log(f"  {d}: {type(e).__name__}: {str(e)[:120]} — retrying with a fresh session")
            try:
                if reconnect:
                    reconnect()
                sleep(pause)
                raw = fetch(d)
            except Exception as e2:  # noqa: BLE001
                return _stop(s, d, f"fetch failed twice ({type(e2).__name__}: {str(e2)[:160]})", log)
        if not raw:
            s["no_report"].append(d)
            if has_session(date.fromisoformat(d)):
                streak += 1
                if streak >= max_no_report:
                    return _stop(s, d, f"{streak} sessions in a row with no report: MCX may be "
                                       "ignoring the date (check lib/mcx_bhavcopy.py)", log)
            log(f"  {d}: no report (holiday, or not published yet)")
            continue
        streak = 0
        rows = bhavcopy_rows(raw, d, now().strftime("%Y-%m-%dT%H:%M:%S+05:30"))
        if not rows:
            return _stop(s, d, f"report has {len(raw)} rows but none usable (format change?)", log)
        try:
            status = write_contract_eod(rows, d, out_dir)
        except Exception as e:  # noqa: BLE001
            return _stop(s, d, f"write failed ({type(e).__name__}: {e})", log)
        s[status].append(d)
        s["contracts"] += len(rows)
        log(f"  {d}: {status} — {len(rows)} contracts, {len({r['symbol'] for r in rows})} commodities")
    return s


def summarize(s, log=print):
    log(f"\n{len(s['written'])} days written ({s['contracts']:,} contracts), "
        f"{s['present']} already there, {len(s['no_report'])} with no report"
        + (f", {len(s['kept'])} kept (existing file larger)" if s["kept"] else ""))
    if s["no_report"]:
        nr = s["no_report"]
        log(f"no report (holiday / not yet published; retried next run): "
            f"{', '.join(nr[:12])}{' …' if len(nr) > 12 else ''}")
    if s["stopped"]:
        log(f"STOPPED at {s['stopped_at']}: {s['stopped']}")
        log("Every file up to there is kept; re-run the same command to resume.")


def main(argv=None) -> int:
    a = parse_args(argv)
    dates = plan_dates(a, now_ist().date())
    out_dir = a.dir or contract_eod_dir()
    todo = pending_dates(dates, out_dir)
    print(f"Contract EOD files from MCX Bhavcopy → {out_dir}")
    if not dates:
        print("no sessions in range")
        return 0
    print(f"{len(dates)} sessions {dates[0]} → {dates[-1]}: {len(dates) - len(todo)} already written, "
          f"{len(todo)} to fetch" + (f" (~{len(todo) * (a.pause + 1.5) / 60:.0f} min)" if len(todo) > 20 else ""))
    if not todo:
        return 0

    from lib.mcx_bhavcopy import fetch_day, new_bhavcopy_session
    try:
        holder = {"s": new_bhavcopy_session()}
    except Exception as e:  # noqa: BLE001
        print(f"✗ could not open an MCX session ({type(e).__name__}: {str(e)[:160]}); nothing fetched")
        return 1

    def reconnect():
        holder["s"] = new_bhavcopy_session()

    s = run(dates, lambda d: fetch_day(holder["s"], d, "FUTCOM", timeout=60, retries=1),
            out_dir, pause=a.pause, reconnect=reconnect)
    summarize(s)
    return 1 if s["stopped"] else 0


if __name__ == "__main__":
    sys.exit(main())
