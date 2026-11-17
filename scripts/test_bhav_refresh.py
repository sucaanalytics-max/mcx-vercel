#!/usr/bin/env python3
"""
Regression tests for the daily commodity pipeline.

Covers the defects found on 28 Jul 2026 (special sessions now go through
has_session(), see [4]):
  1. MCX moved GetHistoricalDataDetails from POST /backpage.aspx/... (JSON body,
     {"d":{"Data":[...]}}) to GET /GetHistoricalDataDetails (query string,
     {"IsSuccess":..,"Data":{"Data":[...]}}), and renamed InstrumentName ->
     Instrumentname / Symbol -> Commodity / CommodityHead -> Segment.
  2. ensure_commodity_breakdown() treated "any row exists for this date" as
     "breakdown is complete". The Dhan OI cron inserts OI-only stub rows with
     turnover_cr = 0, so real dates were skipped forever.
  3. is_trading_day() rejected Saturdays, dropping the Feb-1 Union Budget
     special sessions (2025-02-01, 2026-02-01).
  4. anon cannot UPDATE mcx_commodity_daily (RLS USING clause). merge-duplicates
     upserts 42501 and plain PATCH silently no-ops, so existing rows could never
     be corrected.

Run:  python3 scripts/test_bhav_refresh.py
Network-dependent (hits MCX + Supabase read APIs).
"""
import sys, os, json, urllib.request
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import scripts.bhav_refresh as br

FAILED = []


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}" + (f"  — {detail}" if detail else ""))
    if not cond:
        FAILED.append(name)


def db_revenue(date_iso):
    url = (f"{br.SUPABASE_URL}/rest/v1/mcx_daily_revenue"
           f"?trading_date=eq.{date_iso}&select=fut_notl_cr,opt_prem_cr")
    req = urllib.request.Request(url, headers={
        "apikey": br.SUPABASE_ANON_KEY, "Authorization": f"Bearer {br.SUPABASE_ANON_KEY}"})
    with urllib.request.urlopen(req, timeout=15) as r:
        rows = json.loads(r.read().decode())
    return rows[0] if rows else None


def test_endpoint_returns_rows():
    print("\n[1] MCX historical endpoint")
    rows, err = br._fetch_mcx_raw("2026-06-05")
    check("fetch returns rows", rows is not None, err or f"{len(rows or [])} rows")
    if not rows:
        return None
    check("row count plausible", len(rows) > 20, f"{len(rows)} rows")
    inst = {br._field(r, "InstrumentName") for r in rows}
    check("instrument types parsed", "FUTCOM" in inst and "OPTFUT" in inst, str(sorted(i for i in inst if i)))
    comms = {br._field(r, "Symbol") for r in rows}
    check("commodity names parsed", "CRUDEOIL" in comms, f"{len(comms)} distinct")
    return rows


def test_reconciles_to_db():
    print("\n[2] Parsed totals reconcile to mcx_daily_revenue")
    for d in ["2026-06-05", "2026-05-27"]:
        truth = db_revenue(d)
        if not truth:
            check(f"{d} present in DB", False)
            continue
        hist = br.fetch_mcx_historical(d, write_commodity=False)
        if not hist:
            check(f"{d} parsed", False)
            continue
        fe = abs(hist["fut_notl_cr"] - truth["fut_notl_cr"]) / truth["fut_notl_cr"] * 100
        pe = abs(hist["opt_prem_cr"] - truth["opt_prem_cr"]) / truth["opt_prem_cr"] * 100
        check(f"{d} futures within 0.1%", fe < 0.1, f"{hist['fut_notl_cr']} vs {truth['fut_notl_cr']} ({fe:.3f}%)")
        check(f"{d} premium within 0.1%", pe < 0.1, f"{hist['opt_prem_cr']} vs {truth['opt_prem_cr']} ({pe:.3f}%)")


def test_commodity_rows_have_turnover():
    print("\n[3] Commodity aggregation")
    rows, _ = br._fetch_mcx_raw("2026-06-05")
    if not rows:
        check("raw rows available", False)
        return
    agg = br._aggregate_commodity_rows("2026-06-05", rows)
    check("produces commodity rows", len(agg) > 20, f"{len(agg)} rows")
    check("turnover populated", sum(r["turnover_cr"] for r in agg) > 1000,
          f"total {sum(r['turnover_cr'] for r in agg):,.0f} cr")
    check("premium populated on options",
          any(r["premium_turnover_cr"] for r in agg if r["instrument_type"].startswith("OPT")))
    crude = [r for r in agg if r["commodity"] == "CRUDEOIL"]
    check("CRUDEOIL present with both legs", len({r["instrument_type"] for r in crude}) >= 2,
          str({r["instrument_type"] for r in crude}))
    check("commodity_head populated", all(r["commodity_head"] for r in agg))


def test_special_sessions():
    print("\n[4] Weekend special sessions")
    # Special sessions go through has_session(); is_trading_day() stays strict so
    # they are never projected as a full day (lib/mcx_config.MCX_SPECIAL_SESSIONS).
    check("2026-11-08 Muhurat (Sun) has a session", br.has_session(date(2026, 11, 8)))
    check("2026-11-08 is NOT a trading day", not br.is_trading_day(date(2026, 11, 8)))
    check("2026-02-07 (ordinary Sat) has no session", not br.has_session(date(2026, 2, 7)))
    check("2026-12-25 (holiday) has no session", not br.has_session(date(2026, 12, 25)))


def test_presence_check_ignores_oi_stubs():
    print("\n[5] Coverage set excludes OI-only stub dates")
    covered = br.get_commodity_dates("2026-05-20")
    # 2026-05-27 / 05-28 hold only dhan_live OI stubs (turnover_cr = 0) until
    # the backfill runs; they must NOT count as covered.
    stub_dates = {"2026-05-27", "2026-05-28"}
    still_stub = {d for d in stub_dates if not _date_has_turnover(d)}
    for d in sorted(still_stub):
        check(f"{d} (stub only) not counted as covered", d not in covered)
    if not still_stub:
        print("      (both dates already backfilled with real turnover — check skipped)")
    real = "2026-06-10"
    if _date_has_turnover(real):
        check(f"{real} (real data) IS counted as covered", real in covered)


def test_presence_query_filters_turnover():
    print("\n[6] Coverage query filters turnover_cr > 0 (offline)")
    # [5] skips once the stub dates are backfilled, so pin the query itself.
    seen = []

    class _Resp:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def read(self): return b"[]"

    real = br.urllib.request.urlopen
    br.urllib.request.urlopen = lambda req, timeout=None: (seen.append(req.full_url), _Resp())[1]
    try:
        br.get_commodity_dates("2026-10-01")
    finally:
        br.urllib.request.urlopen = real
    check("query has turnover_cr=gt.0", bool(seen) and "turnover_cr=gt.0" in seen[0],
          seen[0] if seen else "no request made")


# ── Offline: completeness gate, futures premium, --commodity CLI (Nov 2026) ──

def _raw(n=12, opt_premium=True, fut_premium_copy=False):
    """Synthetic raw MCX rows: n commodities x (FUTCOM, OPTFUT)."""
    rows = []
    for i in range(n):
        sym = f"COMM{i:02d}"
        rows.append({"Commodity": sym, "Instrumentname": "FUTCOM", "Segment": "ENERGY",
                     "TradedContract": 1000, "Quantity": 10, "TotalValue": 50000.0,
                     "PremiumTurnover": "50000.00" if fut_premium_copy else "-"})
        rows.append({"Commodity": sym, "Instrumentname": "OPTFUT", "Segment": "ENERGY",
                     "TradedContract": 5000, "Quantity": 50, "TotalValue": 900000.0,
                     "PremiumTurnover": "2500.00" if opt_premium else "-"})
    return rows


def test_futures_never_carry_premium():
    print("\n[7] Futures rows never carry premium (MCX copies notional into it)")
    agg = br._aggregate_commodity_rows("2026-11-10", _raw(fut_premium_copy=True))
    fut = [r for r in agg if r["instrument_type"] == "FUTCOM"]
    opt = [r for r in agg if r["instrument_type"] == "OPTFUT"]
    check("FUT premium_turnover_cr is None", all(r["premium_turnover_cr"] is None for r in fut),
          str({r["premium_turnover_cr"] for r in fut}))
    check("FUT turnover kept", all(r["turnover_cr"] == 500.0 for r in fut))
    check("OPT premium kept", all(r["premium_turnover_cr"] == 25.0 for r in opt))
    hist_rows = _raw(fut_premium_copy=True)
    real_fetch = br._fetch_mcx_raw
    br._fetch_mcx_raw = lambda d: (hist_rows, None)
    try:
        hist = br.fetch_mcx_historical("2026-11-10", write_commodity=False)
    finally:
        br._fetch_mcx_raw = real_fetch
    check("revenue premium counts options only", hist and hist["opt_prem_cr"] == 12 * 25.0,
          str(hist and hist["opt_prem_cr"]))


def test_completeness_gate():
    print("\n[8] Completeness gate")
    agg = lambda **kw: br._aggregate_commodity_rows("2026-11-10", _raw(**kw))
    ok, why = br.breakdown_completeness(agg())
    check("complete report passes", ok, why)
    ok, why = br.breakdown_completeness(agg(opt_premium=False))
    check("04:31 shape (contracts, no option premium) is refused", not ok and "premium" in why, why)
    ok, why = br.breakdown_completeness(agg(n=5))
    check("too few rows is refused", not ok, why)
    ok, why = br.breakdown_completeness([])
    check("empty report is refused", not ok, why)
    no_fut = [r for r in agg() if r["instrument_type"] != "FUTCOM"] + [
        {**r, "turnover_cr": 0} for r in agg() if r["instrument_type"] == "FUTCOM"]
    ok, why = br.breakdown_completeness(no_fut)
    check("missing futures turnover is refused", not ok and "futures" in why, why)


def _with_fakes(raw, write_ok=True, present=()):
    """Patch MCX + Supabase in bhav_refresh; returns (writes list, restore fn)."""
    writes, saved = [], (br._fetch_mcx_raw, br._upsert_commodity_batch, br.get_commodity_dates)
    br._fetch_mcx_raw = lambda d: raw if isinstance(raw, tuple) else (raw, None)
    br._upsert_commodity_batch = lambda rows: (writes.append(rows), write_ok)[1]
    br.get_commodity_dates = lambda since: set(present)
    def restore():
        br._fetch_mcx_raw, br._upsert_commodity_batch, br.get_commodity_dates = saved
    return writes, restore


def test_commodity_breakdown_statuses():
    print("\n[9] commodity_breakdown() never writes a partial report")
    cases = [
        ("complete -> written", _raw(), True, br.BREAKDOWN_WRITTEN, 1),
        ("no premium -> incomplete, no write", _raw(opt_premium=False), True, br.BREAKDOWN_INCOMPLETE, 0),
        ("no rows -> not_published", (None, "no data"), True, br.BREAKDOWN_NOT_PUBLISHED, 0),
        ("timeout -> fetch_error", (None, "curl: (28) timed out"), True, br.BREAKDOWN_FETCH_ERROR, 0),
        ("upsert fails -> write_failed", _raw(), False, br.BREAKDOWN_WRITE_FAILED, 1),
    ]
    for name, raw, write_ok, want, n_writes in cases:
        writes, restore = _with_fakes(raw, write_ok)
        try:
            status, _, detail = br.commodity_breakdown("2026-11-10")
        finally:
            restore()
        check(name, status == want and len(writes) == n_writes, f"{status}, {len(writes)} writes, {detail}")
    writes, restore = _with_fakes(_raw())
    try:
        status, _, _ = br.commodity_breakdown("2026-11-10", present={"2026-11-10"})
    finally:
        restore()
    check("already present -> present, no fetch/write", status == br.BREAKDOWN_PRESENT and not writes)


def test_commodity_cli_exit_codes_and_retry():
    print("\n[10] --commodity CLI: exit codes and bounded retry")
    from datetime import datetime as dt
    for name, raw, want in [("written -> 0", _raw(), br.EXIT_OK),
                            ("incomplete -> 3", _raw(opt_premium=False), br.EXIT_NOT_READY),
                            ("fetch error -> 1", (None, "boom"), br.EXIT_ERROR)]:
        writes, restore = _with_fakes(raw)
        try:
            rc = br.run_commodity_cli(["2026-11-10"])
        finally:
            restore()
        check(name, rc == want, f"rc={rc}")

    # Report incomplete at 05:30, complete on the retry at 05:50.
    clock = [dt(2026, 11, 11, 5, 30)]
    feed = [_raw(opt_premium=False), _raw()]
    sleeps, writes = [], []
    saved = (br._fetch_mcx_raw, br._upsert_commodity_batch, br.get_commodity_dates)
    br._fetch_mcx_raw = lambda d: (feed.pop(0), None)
    br._upsert_commodity_batch = lambda rows: (writes.append(rows), True)[1]
    br.get_commodity_dates = lambda since: set()
    def fake_sleep(s):
        sleeps.append(s)
        clock[0] = clock[0].replace(minute=clock[0].minute + s // 60)
    try:
        rc = br.run_commodity_cli(["2026-11-10"], retry_until="06:55", retry_every_min=20,
                                  now=lambda: clock[0], sleep=fake_sleep)
    finally:
        br._fetch_mcx_raw, br._upsert_commodity_batch, br.get_commodity_dates = saved
    check("retries until complete, then writes once", rc == 0 and sleeps == [1200] and len(writes) == 1,
          f"rc={rc}, sleeps={sleeps}, writes={len(writes)}")

    # Never complete: stops at the deadline instead of looping forever.
    clock = [dt(2026, 11, 11, 6, 40)]
    writes, restore = _with_fakes(_raw(opt_premium=False))
    try:
        rc = br.run_commodity_cli(["2026-11-10"], retry_until="06:55", retry_every_min=20,
                                  now=lambda: clock[0], sleep=lambda s: (_ for _ in ()).throw(AssertionError("slept")))
    finally:
        restore()
    check("past the deadline -> exit 3, no sleep", rc == br.EXIT_NOT_READY, f"rc={rc}")


def test_recent_session_dates():
    print("\n[11] recent_session_dates: excludes today, includes Muhurat")
    got = br.recent_session_dates(4, today=date(2026, 11, 10))
    check("6-9 Nov window", got == ["2026-11-06", "2026-11-08", "2026-11-09"], str(got))


def _date_has_turnover(date_iso):
    url = (f"{br.SUPABASE_URL}/rest/v1/mcx_commodity_daily"
           f"?trading_date=eq.{date_iso}&turnover_cr=gt.0&select=trading_date&limit=1")
    req = urllib.request.Request(url, headers={
        "apikey": br.SUPABASE_ANON_KEY, "Authorization": f"Bearer {br.SUPABASE_ANON_KEY}"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return len(json.loads(r.read().decode())) > 0


if __name__ == "__main__":
    print("=" * 72)
    print("bhav_refresh regression tests")
    print("=" * 72)
    test_endpoint_returns_rows()
    test_reconciles_to_db()
    test_commodity_rows_have_turnover()
    test_special_sessions()
    test_presence_check_ignores_oi_stubs()
    test_presence_query_filters_turnover()
    test_futures_never_carry_premium()
    test_completeness_gate()
    test_commodity_breakdown_statuses()
    test_commodity_cli_exit_codes_and_retry()
    test_recent_session_dates()
    print("\n" + "=" * 72)
    if FAILED:
        print(f"{len(FAILED)} FAILED: {', '.join(FAILED)}")
        sys.exit(1)
    print("All tests passed.")
