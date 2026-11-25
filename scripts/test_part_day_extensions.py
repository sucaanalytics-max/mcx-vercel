#!/usr/bin/env python3
"""
Part-day sessions beyond the live projection (extends PR #19):

  1. session_weight: full 1.0, morning-closed 0.80, evening-closed 0.15, closed 0.
  2. Quarterly: a remaining part-day counts at its weight; full-day rates skip
     part days and the Muhurat; an evening-only day is pre-open before 17:00.
  3. Revenue tab (exchange_dashboard): the Muhurat keeps its bar but is in no
     average.
  4. Intraday curve training skips part days.
  5. Today page: an evening-closed day is final from 17:00.

Pure: every Supabase read is stubbed.
"""
import os, sys
from datetime import date, datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

FAILED = []


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}" + (f"  ({detail})" if detail and not cond else ""))
    if not cond:
        FAILED.append(name)


def test_session_weight():
    print("\n[1] session_weight")
    from lib.mcx_config import session_weight
    check("full day (Tue 17 Nov 2026)", session_weight("2026-11-17") == 1.0)
    check("morning closed (Tue 24 Nov 2026)", session_weight("2026-11-24") == 0.80)
    check("evening closed (1 Jan 2026)", session_weight("2026-01-01") == 0.15)
    check("holiday (25 Dec 2026)", session_weight("2026-12-25") == 0.0)
    check("weekend Muhurat (Sun 8 Nov 2026)", session_weight("2026-11-08") == 0.0)


def test_quarterly_classify():
    print("\n[2] quarterly remaining days")
    import api.quarterly as q
    # Mon 23 Nov 2026 10:00 IST, rows through Fri 20 Nov; window to Fri 27 Nov.
    rows = [{"trading_date": "2026-11-20", "total_rev_cr": 14.0, "source": "mcx_relay_eod"}]
    done, remaining, missing, status, weight = q._classify_days(
        date(2026, 11, 20), date(2026, 11, 27), datetime(2026, 11, 23, 10, 0), rows)
    check("5 sessions remain (23-27 Nov)", remaining == 5, str(remaining))
    check("24 Nov counts 0.80", abs(weight - 4.80) < 1e-9, str(weight))
    check("today is live", status == "live", status)
    _, _, _, status, _ = q._classify_days(
        date(2026, 11, 24), date(2026, 11, 24), datetime(2026, 11, 24, 12, 0), [])
    check("evening-only day is pre-open at 12:00", status == "pre_open", status)
    _, _, _, status, _ = q._classify_days(
        date(2026, 1, 1), date(2026, 1, 1), datetime(2026, 1, 1, 17, 30),
        [{"trading_date": "2026-01-01", "total_rev_cr": 1.6, "source": "mcx_relay_eod"}])
    check("evening-closed day is final after 17:00", status == "final", status)


def test_exchange_dashboard_muhurat():
    print("\n[3] Revenue tab averages skip the Muhurat")
    import api.exchange_dashboard as ex
    days = ["2026-11-02", "2026-11-03", "2026-11-04", "2026-11-05", "2026-11-06",
            "2026-11-08", "2026-11-09"]
    rows = [{"trading_date": d, "fut_rev_cr": 0.1 if d == "2026-11-08" else 2.0,
             "opt_rev_cr": 0.2 if d == "2026-11-08" else 10.0, "total_rev_cr": None} for d in days]
    real = ex.supabase_read_all
    ex.supabase_read_all = lambda *a, **k: rows
    try:
        d = ex.generate_exchange_dashboard(today=date(2026, 11, 9))
    finally:
        ex.supabase_read_all = real
    last5 = next(t for t in d["trailing"] if t["label"] == "Last 5 Trading Days")
    check("5-day average is 12.0 (Muhurat not a day)", last5["avg_total"] == 12.0, str(last5))
    check("45-day average is 12.0", d["avg_45d"]["avg_total"] == 12.0, str(d["avg_45d"]))
    bar = [x for x in d["daily_trend"] if x["date"] == "2026-11-08"]
    check("Muhurat keeps its bar, flagged special", len(bar) == 1 and "special" in bar[0], str(bar))
    check("latest day is the last regular session", d["latest_day"]["date"] == "2026-11-09")


def test_intraday_curves_skip_part_days():
    print("\n[4] intraday curve training skips part days")
    import lib.intraday_curves as ic
    seen = {}
    real_derive, real_read = ic._derive_bucket_weights, ic.supabase_read_all

    def fake_derive(snaps, d):
        seen[d] = True
        return [1 / 7] * 7, 100.0
    snaps = []
    for d in ("2026-11-09", "2026-11-10", "2026-11-11"):     # 10 Nov is morning-closed
        for m in (60, 300, 600, 895):
            snaps.append({"trading_date": d, "elapsed_min": m, "fut_notl_cr": m, "opt_prem_cr": m})
    ic._derive_bucket_weights = fake_derive
    ic.supabase_read_all = lambda *a, **k: snaps
    real_now = ic.now_ist
    ic.now_ist = lambda: datetime(2026, 11, 12, 10, 0)
    try:
        ic.generate_intraday_curves(days=30)
    except Exception as e:      # downstream math on the fake data is not under test
        print(f"    (generate_intraday_curves raised after selection: {type(e).__name__})")
    finally:
        ic._derive_bucket_weights, ic.supabase_read_all, ic.now_ist = real_derive, real_read, real_now
    check("full days used", seen.get("2026-11-09") and seen.get("2026-11-11"), str(seen))
    check("10 Nov (morning closed) skipped", "2026-11-10" not in seen, str(seen))


def test_home_close():
    print("\n[5] Today page close time")
    from lib.home_view import _close_min
    check("evening-closed day closes 17:00", _close_min(date(2026, 1, 1)) == 1020)
    check("winter full day closes 23:55", _close_min(date(2026, 11, 17)) == 1435)
    check("morning-closed day closes 23:55", _close_min(date(2026, 11, 24)) == 1435)


if __name__ == "__main__":
    print("=" * 72)
    print("part-day extension tests")
    print("=" * 72)
    test_session_weight()
    test_quarterly_classify()
    test_exchange_dashboard_muhurat()
    test_intraday_curves_skip_part_days()
    test_home_close()
    print("\n" + "=" * 72)
    if FAILED:
        print(f"{len(FAILED)} FAILED: {', '.join(FAILED)}")
        sys.exit(1)
    print("All tests passed.")
