#!/usr/bin/env python3
"""
Regression tests for the seasonal MCX close (23:30 / 23:55 IST).

MCX's non-agri contracts close at 23:55 IST while the US is on standard time
and at 23:30 otherwise. Until Sep 2026 the code assumed 23:30 all year, so from
2 Nov 2026 every relay EOD row would have missed the last 25 minutes (~4% of a
winter day), and bhav_refresh never corrects relay rows. Covers:
  1. session_end() reproduces every switch date in MCX circulars 643/2024,
     071/2025, 491/2025 and 068/2026, and the expected 2026-27 dates.
  2. The intraday buckets, weights and elapsed-minute maths stay consistent
     with the session length (870 / 895 min).
  3. The relay wakes just after the close, takes its EOD row for the right date
     before midnight, and marks the session closed only at the real close.
  4. daily_verify replaces a relay EOD row that is >1% off MCX's historical
     report, and leaves unsettled reports and non-relay sources alone.
  5. Part-day sessions (morning or evening closed): session_shape() builds on
     the seasonal close, the projection is time-only over the day's real
     hours, and the relay sleeps through the closed half and closes on time.

Run:  python3 scripts/test_session_hours.py            # offline, no network
      python3 scripts/test_session_hours.py --network  # + read-only MCX/Supabase checks
"""
import sys, os, time, types
from datetime import datetime, date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib.mcx_config as cfg

FAILED = []


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}" + (f"  — {detail}" if detail else ""))
    if not cond:
        FAILED.append(name)


# Keep the projection offline: no adaptive curve, so the static one is used.
_real_adaptive_weights = cfg._compute_adaptive_weights
cfg._compute_adaptive_weights = lambda vol_fn=None, close_min=None: None


def test_switch_dates_match_circulars():
    print("\n[1] Session close vs MCX circulars")
    cases = [
        # MCX/TRD/643/2024: 23:55 from 2024-11-04 to 2025-03-07 (both inclusive)
        ("2024-11-01", 1410), ("2024-11-04", 1435), ("2025-03-07", 1435),
        # MCX/TRD/071/2025: 23:30 from 2025-03-10
        ("2025-03-10", 1410),
        # MCX/TRD/491/2025: 23:55 from 2025-11-03 to 2026-03-06
        ("2025-10-31", 1410), ("2025-11-03", 1435), ("2026-03-06", 1435),
        # MCX/TRD/068/2026: 23:30 from 2026-03-09
        ("2026-03-09", 1410),
        # 2026-27, expected (no circular yet): US DST ends 1 Nov 2026, starts 14 Mar 2027
        ("2026-10-30", 1410), ("2026-11-02", 1435), ("2027-03-12", 1435), ("2027-03-15", 1410),
    ]
    for d, want in cases:
        check(f"{d} closes {cfg.fmt_ist_minutes(want)}", cfg.session_end(d) == want,
              f"got {cfg.fmt_ist_minutes(cfg.session_end(d))}")
    check("accepts datetime", cfg.session_end(datetime(2026, 11, 2, 23, 50)) == 1435)
    check("accepts date", cfg.session_end(date(2026, 11, 2)) == 1435)
    check("session_total winter = 895", cfg.session_total("2026-11-02") == 895)
    check("session_total summer = 870", cfg.session_total("2026-09-29") == 870)

    cfg.SESSION_END_OVERRIDES["2026-11-02"] = 1410
    try:
        check("override wins over the US rule", cfg.session_end("2026-11-02") == 1410)
    finally:
        del cfg.SESSION_END_OVERRIDES["2026-11-02"]


def test_buckets_consistent():
    print("\n[2] Intraday buckets and weights")
    summer = cfg.intraday_buckets("2026-09-29")
    legacy = [(540, 630, 0.06), (630, 750, 0.10), (750, 900, 0.07), (900, 1020, 0.10),
              (1020, 1170, 0.18), (1170, 1320, 0.34), (1320, 1410, 0.15)]
    check("summer buckets unchanged", summer == legacy)
    for d in ("2026-09-29", "2026-11-02"):
        b = cfg.intraday_buckets(d)
        check(f"{d}: 7 buckets", len(b) == 7)
        check(f"{d}: weights sum to 1", abs(sum(w for *_, w in b) - 1) < 1e-9)
        check(f"{d}: contiguous 09:00→close",
              b[0][0] == 540 and b[-1][1] == cfg.session_end(d)
              and all(b[i][1] == b[i + 1][0] for i in range(6)))
    check("winter label 22:00–23:55", cfg.bucket_labels("2026-11-02")[-1] == "22:00–23:55")

    w = cfg.get_intraday_weight
    check("summer weight at 870 = 1", abs(w(870, "2026-09-29") - 1) < 1e-9)
    check("winter weight at 895 = 1", abs(w(895, "2026-11-02") - 1) < 1e-9)
    at_2330 = w(870, "2026-11-02")
    check("winter weight at 23:30 ≈ 0.96 (4% still to trade)", abs(at_2330 - 0.96) < 0.005, f"{at_2330:.4f}")
    series = [w(m, "2026-11-02") for m in range(0, 900, 5)]
    check("winter curve monotonic", all(a <= b + 1e-12 for a, b in zip(series, series[1:])))
    dyn = cfg.get_intraday_weight_dynamic(870, [1 / 7] * 7, "2026-11-02")
    check("dynamic weights use winter edges", dyn < 1.0, f"{dyn:.4f}")


def test_projection():
    print("\n[3] project_full_day")
    pf, po, conf = cfg.project_full_day(1000, 100, 870, trading_date="2026-09-29")
    check("summer 23:30 is CERTAIN", conf == "CERTAIN" and pf == 1000)
    pf, po, conf = cfg.project_full_day(1000, 100, 870, trading_date="2026-11-02")
    check("winter 23:30 still projects upward", conf != "CERTAIN" and pf > 1000 and po > 100,
          f"{pf:.1f}/{po:.1f} {conf}")
    pf, po, conf = cfg.project_full_day(1000, 100, 895, trading_date="2026-11-02")
    check("winter 23:55 is CERTAIN", conf == "CERTAIN" and pf == 1000)
    check("market open 23:50 in winter", cfg.is_market_open(datetime(2026, 11, 2, 23, 50)))
    check("market shut 23:50 in summer", not cfg.is_market_open(datetime(2026, 9, 29, 23, 50)))


# ── Relay ────────────────────────────────────────────────────────────────────

class FakeClock:
    def __init__(self, start):
        self.t = start

    def __call__(self):
        return self.t

    def sleep(self, seconds):
        self.t += timedelta(seconds=seconds)


def _market_watch(fut_cr, prem_cr):
    # NotionalValue / PremiumValue are in lakhs; extract_notionals divides by 100.
    return {"data": {"Data": [
        {"InstrumentName": "FUTCOM", "Volume": 1, "NotionalValue": fut_cr * 100, "ProductCode": "CRUDEOIL"},
        {"InstrumentName": "OPTFUT", "Volume": 1, "NotionalValue": prem_cr * 1000,
         "PremiumValue": prem_cr * 100, "ProductCode": "CRUDEOIL"},
    ]}}


def _patch_relay(relay, clock, writes):
    relay.now_ist = clock
    relay._get_mw_session = lambda force_new=False: None
    relay.fetch_market_watch = lambda session=None, timeout=18: _market_watch(60000, 9000)
    relay._hard_call = lambda fn, *a, hard_timeout=25, **k: fn(*a, **k)
    relay.supabase_upsert = lambda table, row, **k: writes.append((table, dict(row))) or [row]
    relay.send_heartbeat = lambda *a, **k: None
    relay.time = types.SimpleNamespace(sleep=lambda s: None, time=time.time)  # not the real module
    cfg.supabase_read = lambda *a, **k: []   # today's snapshots for drift


def test_relay_snapshot_close():
    print("\n[4] Relay snapshot closes at the real close")
    import scripts.mcx_relay as relay
    for when, want_elapsed, want_closed in [
        (datetime(2026, 9, 29, 23, 32), 870, True),    # summer, after 23:30
        (datetime(2026, 11, 2, 23, 32), 872, False),   # winter, still trading
        (datetime(2026, 11, 2, 23, 57), 895, True),    # winter, after 23:55
    ]:
        writes = []
        _patch_relay(relay, FakeClock(when), writes)
        result = relay.run_snapshot()
        snap = [r for t, r in writes if t == "mcx_snapshots"][-1]
        check(f"{when:%Y-%m-%d %H:%M}: elapsed {want_elapsed}, closed={want_closed}",
              snap["elapsed_min"] == want_elapsed and snap["session_closed"] == want_closed
              and (result == "closed") == want_closed,
              f"elapsed={snap['elapsed_min']} closed={snap['session_closed']} result={result}")

    t = datetime
    ns = relay.next_sleep_seconds
    check("summer 23:20 sleeps to 23:32", ns(t(2026, 9, 29, 23, 20), 900) == 720)
    check("winter 23:47:30 sleeps to 23:57", ns(t(2026, 11, 2, 23, 47, 30), 900) == 570)
    check("midday sleeps the full interval", ns(t(2026, 11, 2, 12, 0), 900) == 900)
    check("retry cadence kept when close is far", ns(t(2026, 11, 2, 23, 40), 120) == 120)


def test_relay_loop_winter_eod():
    print("\n[5] Relay loop takes the winter EOD before midnight")
    import scripts.mcx_relay as relay
    clock = FakeClock(datetime(2026, 11, 2, 22, 47))
    writes, eod_calls = [], []
    _patch_relay(relay, clock, writes)
    relay._watchful_sleep = clock.sleep
    relay._start_stall_watchdog = lambda: None
    relay.refresh_margins_local = lambda: None
    relay.refresh_oi_participants_local = lambda: None
    relay.capture_eod = lambda date_iso=None: eod_calls.append((date_iso, clock()))
    relay.run_loop()
    check("EOD captured once", len(eod_calls) == 1, str(eod_calls))
    if eod_calls:
        d, at = eod_calls[0]
        check("EOD for the session date", d == "2026-11-02", d)
        check("EOD after 23:55 and before midnight",
              datetime(2026, 11, 2, 23, 55) <= at < datetime(2026, 11, 3), f"{at:%H:%M:%S}")
    snaps = [r for t, r in writes if t == "mcx_snapshots"]
    check("last snapshot at 895 min", snaps and snaps[-1]["elapsed_min"] == 895,
          str([s["elapsed_min"] for s in snaps]))


def _run_loop_with_failing_close(start, fail="raise"):
    """Run the loop with every snapshot after the close failing: raising, or
    returning False (no data / outside hours)."""
    import importlib
    import scripts.mcx_relay as relay
    relay = importlib.reload(relay)
    clock = FakeClock(start)
    writes, eod_calls = [], []
    _patch_relay(relay, clock, writes)
    real_snapshot = relay.run_snapshot

    def flaky_snapshot():
        t = clock()
        if t.hour * 60 + t.minute >= relay.session_end(t):
            if fail == "raise":
                raise RuntimeError("MCX unreachable")
            return False
        return real_snapshot()
    relay.run_snapshot = flaky_snapshot
    relay._watchful_sleep = clock.sleep
    relay._start_stall_watchdog = lambda: None
    relay._reset_net_state = lambda: None
    relay.refresh_margins_local = lambda: None
    relay.refresh_oi_participants_local = lambda: None
    relay.capture_eod = lambda date_iso=None: eod_calls.append((date_iso, clock()))
    try:
        relay.run_loop()
    except SystemExit as e:   # the 4-failure watchdog; report it, don't die
        eod_calls.append(("exit", e.code))
    return eod_calls


def test_relay_fallback_when_close_snapshot_fails():
    print("\n[5b] Clock fallback when the post-close snapshot fails")
    calls = _run_loop_with_failing_close(datetime(2026, 11, 2, 23, 40))
    check("winter: fallback EOD by 23:57, same date",
          len(calls) == 1 and calls[0][0] == "2026-11-02"
          and calls[0][1] < datetime(2026, 11, 2, 23, 58), str(calls))
    calls = _run_loop_with_failing_close(datetime(2026, 9, 29, 23, 20), fail="false")
    check("summer: keeps trying until 23:45 before falling back",
          len(calls) == 1 and calls[0][0] == "2026-09-29"
          and datetime(2026, 9, 29, 23, 45) <= calls[0][1] < datetime(2026, 9, 29, 23, 50), str(calls))


def test_capture_eod_uses_session_date():
    print("\n[6] capture_eod labels the session it closes")
    import importlib
    import scripts.mcx_relay as relay
    relay = importlib.reload(relay)   # undo the loop test's capture_eod stub
    writes = []
    _patch_relay(relay, FakeClock(datetime(2026, 11, 3, 0, 0, 30)), writes)
    relay.capture_eod("2026-11-02")
    rows = [r for t, r in writes if t == "mcx_daily_revenue"]
    check("row dated 2026-11-02 despite a 00:00 clock",
          rows and rows[0]["trading_date"] == "2026-11-02" and rows[0]["source"] == "mcx_relay_eod",
          str(rows[0]["trading_date"] if rows else None))


# ── daily_verify reconciliation ──────────────────────────────────────────────

def _hist(total, fut=60000.0, prem=9000.0, opt_rev=7.5):
    return {"total_rev_cr": total, "fut_notl_cr": fut, "opt_prem_cr": prem, "opt_rev_cr": opt_rev,
            "fut_rev_cr": total - opt_rev, "nontx_rev_cr": 0.0, "active_futures": 90, "active_options": 800}


def test_reconcile_decision():
    print("\n[7] daily_verify.reconcile_decision")
    import scripts.daily_verify as dv
    relay = {"source": "mcx_relay_eod", "total_rev_cr": 10.0, "fut_notl_cr": 58000.0, "opt_prem_cr": 8700.0}
    cases = [
        ("relay 0.8% short → ok", relay, _hist(10.08), "ok"),
        ("relay 4% short (winter 23:30 capture) → replace", relay, _hist(10.42), "replace"),
        ("relay 3% high → replace", relay, _hist(9.70), "replace"),
        ("options leg missing → unsettled", relay, _hist(2.5, prem=0.0, opt_rev=0.0), "unsettled"),
        ("options leg partial → unsettled", relay, _hist(6.0, prem=3000.0), "unsettled"),
        ("historical row stale → replace (self-heal)",
         {**relay, "source": "mcx_historical"}, _hist(10.5), "replace"),
        ("excel row 2% off → ok", {**relay, "source": "excel_daily_data"}, _hist(10.2), "ok"),
        ("excel row 8% off → review", {**relay, "source": "excel_daily_data"}, _hist(10.9), "review"),
    ]
    for name, stored, hist, want in cases:
        got, delta, _ = dv.reconcile_decision(stored, hist)
        check(name, got == want, f"got {got} ({delta if delta is None else round(delta, 2)})")


def test_replace_confirms_write():
    print("\n[8] daily_verify.replace_with_historical")
    import scripts.daily_verify as dv
    calls = []
    dv.supabase_upsert = lambda table, row, **k: calls.append((table, row, k)) or [dict(row)]
    err = dv.replace_with_historical("2026-11-02", _hist(10.42))
    table, row, kw = calls[-1]
    check("writes mcx_daily_revenue on trading_date",
          err is None and table == "mcx_daily_revenue" and kw.get("on_conflict") == "trading_date")
    check("row marked historical", row["source"] == "mcx_historical" and row["total_rev_cr"] == 10.42)
    # The guard trigger (scripts/sql/guard_mcx_daily_revenue.sql) admits a
    # replacement of a relay row only with this exact pair.
    check("row carries the guard's reconciliation marker",
          row["data_source"] == "mcx_historical_reconciled" == dv.RECONCILED_DATA_SOURCE)
    check("backfill rows keep the ordinary marker",
          dv.historical_record("2026-11-02", _hist(10.42))["data_source"] == "mcx_historical_detailed")
    dv.supabase_upsert = lambda table, row, **k: []
    check("RLS no-op reported as failure", dv.replace_with_historical("2026-11-02", _hist(10.42)) is not None)


def test_curves_and_freshness():
    print("\n[9] Intraday curve split and freshness cutoff")
    from lib.intraday_curves import _derive_bucket_weights, _bucket_edges
    # Uniform pace: 1 Cr per minute from 09:00 to 23:55.
    snaps = [{"elapsed_min": m, "fut_notl_cr": float(m), "opt_prem_cr": 0.0} for m in range(15, 900, 15)]
    snaps[-1]["elapsed_min"] = 895
    snaps[-1]["fut_notl_cr"] = 895.0
    w, total = _derive_bucket_weights(snaps, "2026-11-02")
    check("winter edges end at 895", _bucket_edges("2026-11-02")[-1] == 895)
    check("winter day total uses 23:55", abs(total - 895) < 1e-6, f"{total}")
    check("late bucket = 115/895 at uniform pace", abs(w[-1] - 115 / 895) < 1e-6, f"{w[-1]:.4f}")

    import lib.intraday_curves as ic
    real_read, real_now = ic.supabase_read_all, ic.now_ist
    def day(d):
        return [{"trading_date": d, "elapsed_min": m, "fut_notl_cr": float(m), "opt_prem_cr": 0.0}
                for m in range(15, cfg.session_total(d) + 1, 15)] + \
               [{"trading_date": d, "elapsed_min": cfg.session_total(d),
                 "fut_notl_cr": float(cfg.session_total(d)), "opt_prem_cr": 0.0}]
    ic.supabase_read_all = lambda *a, **k: day("2026-10-29") + day("2026-10-30") + day("2026-11-02")
    ic.now_ist = lambda: datetime(2026, 11, 3, 12, 0)
    try:
        c = ic.generate_intraday_curves(days=30, include_today=False)
    finally:
        ic.supabase_read_all, ic.now_ist = real_read, real_now
    ra = c["rolling_average"]
    check("curve average uses winter days only",
          ra["days_used"] == 1 and ra["other_season_days_excluded"] == 2, str({k: ra[k] for k in ra if k != "buckets"}))

    from scripts.freshness_scan import expected_trading_day
    check("winter 23:40: today not complete",
          expected_trading_day(datetime(2026, 11, 3, 23, 40)) == date(2026, 11, 2))
    check("winter 23:56: today complete",
          expected_trading_day(datetime(2026, 11, 3, 23, 56)) == date(2026, 11, 3))
    check("summer 23:31: today complete",
          expected_trading_day(datetime(2026, 9, 29, 23, 31)) == date(2026, 9, 29))


# ── Part-day sessions ────────────────────────────────────────────────────────
# 2026-11-10 Diwali-Balipratipada: morning closed, evening 17:00–23:55 (winter).
# 2026-10-20 Dassera: morning closed, evening 17:00–23:30 (summer).
# 2026-01-01 New Year: evening closed, morning 09:00–17:00.
# 2026-11-02 / 2026-09-29: normal winter / summer days.

def test_session_shape():
    print("\n[10] session_shape and session_clock")
    cases = [
        ("2026-11-10", 1020, 1435, 415, "evening_only"),
        ("2026-10-20", 1020, 1410, 390, "evening_only"),
        ("2026-01-01",  540, 1020, 480, "morning_only"),
        ("2026-11-02",  540, 1435, 895, "full"),
        ("2026-09-29",  540, 1410, 870, "full"),
        ("2026-10-02",  540,  540,   0, "closed"),    # Gandhi Jayanti
        ("2026-11-07",  540,  540,   0, "closed"),    # Saturday
        ("2026-11-08",  540,  540,   0, "closed"),    # Muhurat Sunday: never projected live
    ]
    for d, start, end, total, kind in cases:
        sh = cfg.session_shape(d)
        check(f"{d}: {kind} {cfg.fmt_ist_minutes(start)}–{cfg.fmt_ist_minutes(end)} ({total} min)",
              (sh["start"], sh["end"], sh["total"], sh["type"]) == (start, end, total, kind), str(sh))
    for d in ("2026-11-02", "2026-09-29", "2026-11-10", "2026-10-20"):
        check(f"{d}: close is session_end()", cfg.session_shape(d)["end"] == cfg.session_end(d))
    cfg.SESSION_END_OVERRIDES["2026-11-10"] = 1410
    try:
        check("override moves the evening-only close too", cfg.session_shape("2026-11-10")["end"] == 1410)
    finally:
        del cfg.SESSION_END_OVERRIDES["2026-11-10"]

    clk = cfg.session_clock
    for when, want in [
        (datetime(2026, 11, 10, 12, 0), (180, 0)),     # morning closed: nothing traded
        (datetime(2026, 11, 10, 20, 0), (660, 180)),
        (datetime(2026, 11, 10, 23, 58), (895, 415)),
        (datetime(2026, 1, 1, 18, 0), (480, 480)),     # evening closed: done at 17:00
        (datetime(2026, 11, 2, 20, 0), (660, 660)),    # full days: the two agree
        (datetime(2026, 9, 29, 23, 40), (870, 870)),
    ]:
        e, tr, _ = clk(when)
        check(f"{when:%Y-%m-%d %H:%M}: elapsed {want[0]}, traded {want[1]}", (e, tr) == want, f"{e}, {tr}")
    check("traded_minutes from a stored elapsed (11-10, 660)", cfg.traded_minutes(660, "2026-11-10") == 180)
    check("traded_minutes = elapsed on a full day", cfg.traded_minutes(660, "2026-11-02") == 660)


def test_part_day_projection():
    print("\n[11] Projection on part-day sessions")
    # 20:00 on 2026-11-10: 180 of 415 minutes traded, so time-only gives ×415/180.
    pf, po, conf = cfg.project_full_day(1000, 100, 660, trading_date="2026-11-10")
    check("11-10 20:00 is time-only (×415/180)",
          abs(pf - 1000 * 415 / 180) < 1e-6 and abs(po - 100 * 415 / 180) < 1e-6, f"{pf:.2f}/{po:.2f} {conf}")
    check("11-10 20:00 confidence MEDIUM (43% gone)", conf == "MEDIUM", conf)
    pf_full, _, _ = cfg.project_full_day(1000, 100, 660, trading_date="2026-11-02")
    check("a full winter day at 20:00 still uses the curve", abs(pf_full - pf) > 1, f"{pf_full:.2f}")
    pf, po, conf = cfg.project_full_day(1000, 100, 180, trading_date="2026-11-10")
    check("11-10 12:00 (closed half) returns realized", (pf, po, conf) == (1000, 100, "LOW"))
    pf, po, conf = cfg.project_full_day(1000, 100, 895, trading_date="2026-11-10")
    check("11-10 23:55 is CERTAIN", conf == "CERTAIN" and pf == 1000)
    pf, po, conf = cfg.project_full_day(1000, 100, 870, trading_date="2026-11-10")
    check("11-10 23:30 still projects (winter close)", conf != "CERTAIN" and abs(pf - 1000 * 415 / 390) < 1e-6,
          f"{pf:.2f} {conf}")
    pf, po, conf = cfg.project_full_day(1000, 100, 240, trading_date="2026-01-01")
    check("01-01 13:00 is ×480/240", abs(pf - 2000) < 1e-6, f"{pf:.2f}")
    pf, po, conf = cfg.project_full_day(1000, 100, 480, trading_date="2026-01-01")
    check("01-01 17:00 is CERTAIN", conf == "CERTAIN" and pf == 1000)
    # Day-type prior still applies and fades, as on full days.
    pf_hi, _, _ = cfg.project_full_day(1000, 100, 660, day_type="HIGH", trading_date="2026-11-10")
    tp = 180 / 415
    check("day-type prior fades with share gone",
          abs(pf_hi - 1000 / tp * (1 + 0.15 * (1 - tp))) < 1e-6, f"{pf_hi:.2f}")

    op = cfg.is_market_open
    check("11-10 12:00 market shut", not op(datetime(2026, 11, 10, 12, 0)))
    check("11-10 17:00 market open", op(datetime(2026, 11, 10, 17, 0)))
    check("11-10 23:50 market open (winter)", op(datetime(2026, 11, 10, 23, 50)))
    check("01-01 18:00 market shut", not op(datetime(2026, 1, 1, 18, 0)))
    check("10-02 12:00 holiday shut", not op(datetime(2026, 10, 2, 12, 0)))

    # The adaptive curve learns only from full days: an evening-only day's
    # snapshots would teach it that 09:00–17:00 trades nothing.
    real_read = cfg.supabase_read_all
    def day(d, start):
        return [{"trading_date": d, "elapsed_min": m, "fut_notl_cr": float(max(0, m - (start - 540))),
                 "opt_prem_cr": 0.0} for m in range(15, cfg.session_total(d) + 1, 15)]
    rows = day("2026-11-10", 1020)
    cfg.supabase_read_all = lambda *a, **k: rows
    real_now = cfg.now_ist
    cfg.now_ist = lambda: datetime(2026, 11, 11, 12, 0)
    try:
        used = _real_adaptive_weights(close_min=1435)
    finally:
        cfg.supabase_read_all, cfg.now_ist = real_read, real_now
    check("adaptive curve ignores part-day sessions", used is None, str(used))


def test_refresh_part_day():
    print("\n[12] /api/refresh on a part-day session")
    import api.refresh as rf
    writes = []
    rf.supabase_upsert = lambda table, row, **k: writes.append((table, dict(row))) or [row]
    raw = _market_watch(60000, 9000)
    r = rf.process_market_data(raw, datetime(2026, 11, 10, 20, 0))
    tp = 180 / 415
    check("elapsed_min stays clock-based (660)", r["elapsed_min"] == 660, str(r["elapsed_min"]))
    check("session 415 min, evening_only", r["session_total_min"] == 415 and r["session_type"] == "evening_only")
    check("elapsed_pct is share of the real session", r["elapsed_pct"] == round(tp * 100, 1), str(r["elapsed_pct"]))
    check("not closed", r["session_closed"] is False)
    check("projection ×415/180", abs(r["proj_fut_cr"] - round(60000 * 415 / 180, 2)) < 0.01, str(r["proj_fut_cr"]))
    check("uncertainty from share gone",
          r["uncertainty_pct"] == round(cfg.calc_uncertainty(tp, r["day_type"], False) * 100, 1))
    check("range read at minutes traded, not the clock", r["range_min"] == 180, str(r["range_min"]))
    snap = [row for t, row in writes if t == "mcx_snapshots"]
    check("snapshot not marked closed", snap and snap[-1]["session_closed"] is False and snap[-1]["elapsed_min"] == 660)

    r = rf.process_market_data(raw, datetime(2026, 11, 2, 20, 0))
    check("normal winter day: 895 min, full, range at clock", r["session_total_min"] == 895
          and r["session_type"] == "full" and r["range_min"] == 660 and r["elapsed_pct"] == round(660 / 895 * 100, 1))
    r = rf.process_market_data(raw, datetime(2026, 9, 29, 20, 0))
    check("normal summer day: 870 min", r["session_total_min"] == 870 and r["elapsed_pct"] == round(660 / 870 * 100, 1))
    r = rf.process_market_data(raw, datetime(2026, 1, 1, 17, 1))
    check("evening-closed day closed at 17:00", r["session_closed"] is True and r["elapsed_pct"] == 100.0)


def test_relay_part_day():
    print("\n[13] Relay on a part-day session")
    import importlib
    import scripts.mcx_relay as relay
    relay = importlib.reload(relay)
    for when, want in [
        (datetime(2026, 11, 10, 12, 0), None),          # morning closed: skip, no write
        (datetime(2026, 11, 10, 16, 45), None),         # pre-open: yesterday's totals, stale guard
        (datetime(2026, 11, 10, 20, 0), (660, False)),
        (datetime(2026, 11, 10, 23, 57), (895, True)),
        (datetime(2026, 1, 1, 17, 2), (480, True)),     # evening closed: EOD at 17:00
    ]:
        writes = []
        _patch_relay(relay, FakeClock(when), writes)
        result = relay.run_snapshot()
        snaps = [r for t, r in writes if t == "mcx_snapshots"]
        if want is None:
            check(f"{when:%Y-%m-%d %H:%M}: skipped", result is False and not snaps, str(result))
            continue
        snap = snaps[-1] if snaps else {}
        check(f"{when:%Y-%m-%d %H:%M}: elapsed {want[0]}, closed={want[1]}",
              snap.get("elapsed_min") == want[0] and snap.get("session_closed") == want[1]
              and (result == "closed") == want[1], f"{snap.get('elapsed_min')} {snap.get('session_closed')} {result}")
        if when.hour == 20:
            check("relay projection ×415/180",
                  abs(snap["proj_fut_cr"] - round(60000 * 415 / 180, 2)) < 0.01, str(snap["proj_fut_cr"]))

    ns = relay.next_sleep_seconds
    check("01-01 16:50 sleeps to 17:02", ns(datetime(2026, 1, 1, 16, 50), 900) == 720)
    check("11-10 23:47:30 sleeps to 23:57", ns(datetime(2026, 11, 10, 23, 47, 30), 900) == 570)

    # Whole day on 2026-11-10 from an 08:30 launch.
    clock = FakeClock(datetime(2026, 11, 10, 8, 30))
    writes, eod_calls = [], []
    _patch_relay(relay, clock, writes)
    relay._watchful_sleep = clock.sleep
    relay._start_stall_watchdog = lambda: None
    relay.refresh_margins_local = lambda: None
    relay.refresh_oi_participants_local = lambda: None
    relay.capture_eod = lambda date_iso=None: eod_calls.append((date_iso, clock()))
    relay.run_loop()
    snaps = [r for t, r in writes if t == "mcx_snapshots"]
    check("no snapshots before 16:30", snaps and min(s["elapsed_min"] for s in snaps) >= 450,
          str([s["elapsed_min"] for s in snaps][:3]))
    check("EOD once, for 11-10, after 23:55",
          len(eod_calls) == 1 and eod_calls[0][0] == "2026-11-10"
          and datetime(2026, 11, 10, 23, 55) <= eod_calls[0][1] < datetime(2026, 11, 11), str(eod_calls))


def test_network_relay_matches_historical():
    """Read-only: recent relay EOD rows agree with MCX's report within 1%, so
    the reconciliation won't churn normal summer days."""
    print("\n[net] Recent relay EOD rows vs MCX historical")
    import importlib
    importlib.reload(cfg)
    import scripts.mcx_relay as relay
    fetch_mcx_historical = importlib.reload(relay).fetch_mcx_historical   # drop test stubs
    import scripts.daily_verify as dv
    rows = cfg.supabase_read("mcx_daily_revenue",
                             "?source=eq.mcx_relay_eod&order=trading_date.desc&limit=6"
                             "&select=trading_date,source,total_rev_cr,fut_notl_cr,opt_prem_cr")
    for r in rows[1:]:   # skip the newest: its report may not be settled yet
        hist = fetch_mcx_historical(r["trading_date"])
        if not hist:
            check(f"{r['trading_date']} historical available", False)
            continue
        action, delta, why = dv.reconcile_decision(r, hist)
        check(f"{r['trading_date']} within 1% ({action})", action in ("ok", "unsettled"),
              f"{delta:+.2f}%" if delta is not None else why)


if __name__ == "__main__":
    print("=" * 72)
    print("Seasonal MCX close regression tests")
    print("=" * 72)
    test_switch_dates_match_circulars()
    test_buckets_consistent()
    test_projection()
    test_relay_snapshot_close()
    test_relay_loop_winter_eod()
    test_relay_fallback_when_close_snapshot_fails()
    test_capture_eod_uses_session_date()
    test_reconcile_decision()
    test_replace_confirms_write()
    test_curves_and_freshness()
    test_session_shape()
    test_part_day_projection()
    test_refresh_part_day()
    test_relay_part_day()
    if "--network" in sys.argv:
        test_network_relay_matches_historical()
    print("\n" + "=" * 72)
    if FAILED:
        print(f"{len(FAILED)} FAILED: {', '.join(FAILED)}")
        sys.exit(1)
    print("All tests passed.")
