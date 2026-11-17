#!/usr/bin/env python3
"""
Phase 2 (data correctness) regression tests:

  1. Calendar rollover: calendar_status OK / DUE / STALE; the freshness scan,
     daily_verify and bhav_refresh all see one calendar.
  2. Commodity OI: daily_verify runs the Dhan OI backfill before the signal
     recompute (and valuation before models); bhav_refresh keeps stored
     non-zero OI instead of writing 0 over it.
  3. backfill_commodity_gaps.repair goes through the completeness gate.
  4. Pagination: every paginated order is total (unique tiebreaker), and
     newest_first caps drop the oldest rows, never the newest.
  5. Valuation history is on the snapshot's constants.
  6. Hourly accuracy: entry lag, stale-opening trim, in-sample flag.

Pure: every Supabase / MCX / Dhan call is stubbed.
Run: scripts/run_tests.sh   (needs curl_cffi: imports scripts/bhav_refresh)
"""
import os, sys, json
from datetime import date, datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

FAILED = []


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}" + (f"  ({detail})" if detail and not cond else ""))
    if not cond:
        FAILED.append(name)


# ── 1. Calendar ─────────────────────────────────────────────────────────────

def test_calendar():
    print("\n[1] calendar rollover")
    import lib.mcx_config as m
    st = m.calendar_status(date(2026, 11, 17))
    check("17 Nov 2026: OK", st["status"] == "OK", str(st))
    st = m.calendar_status(date(2026, 12, 15))
    check("15 Dec 2026, 2027 missing: DUE", st["status"] == "DUE" and st["days_to_rollover"] == 17, str(st))
    st = m.calendar_status(date(2027, 1, 1))
    check("1 Jan 2027, 2027 missing: STALE", st["status"] == "STALE" and "2027" in st["message"], str(st))
    st = m.calendar_status(datetime(2027, 3, 1, 10, 0))
    check("accepts a datetime", st["status"] == "STALE", str(st))
    saved = set(m.MCX_CALENDAR_YEARS)
    try:
        m.MCX_CALENDAR_YEARS.add(2027)
        check("2027 loaded: 15 Dec is OK", m.calendar_status(date(2026, 12, 15))["status"] == "OK")
        check("2027 loaded: 1 Jan 2027 is OK", m.calendar_status(date(2027, 1, 1))["status"] == "OK")
    finally:
        m.MCX_CALENDAR_YEARS.clear()
        m.MCX_CALENDAR_YEARS.update(saved)
    check("every covered year has holidays listed",
          all(any(h.startswith(f"{y}-") for h in m.MCX_HOLIDAYS) for y in m.MCX_CALENDAR_YEARS))

    import scripts.freshness_scan as fs
    real = m.calendar_status
    try:
        m.calendar_status = lambda today=None: real(date(2027, 1, 4))
        c = fs._check_calendar()
        check("freshness scan reports STALE", c["status"] == "STALE", str(c))
    finally:
        m.calendar_status = real
    with open(fs.MANIFEST) as f:
        man = json.load(f)
    cal = [d for d in man["domains"] if d["domain"] == "calendar"]
    check("manifest has a yearly calendar domain with no fixer",
          len(cal) == 1 and cal[0]["cadence"] == "yearly" and cal[0]["fixer"] is None, str(cal))

    import scripts.daily_verify as dv
    stale = {"status": "STALE", "message": "x"}
    check("daily_verify: STALE fails the run", dv.calendar_check(lambda: stale) is False)
    check("daily_verify: DUE only warns", dv.calendar_check(lambda: {"status": "DUE", "message": "y"}) is True)

    import scripts.bhav_refresh as br
    check("bhav_refresh uses the shared holiday set", br.MCX_HOLIDAYS is m.MCX_HOLIDAYS)

    # Past sessions filled from MCX's historical report on 17 Nov 2026
    check("2024 and 2025 Muhurat are special sessions (out of averages)",
          {"2024-11-01", "2025-10-21"} <= set(m.MCX_SPECIAL_SESSIONS))
    check("1 Jan 2025 was evening-closed", "2025-01-01" in m.MCX_EVENING_CLOSE
          and m.session_weight("2025-01-01") == 0.15)


# ── 2. Commodity OI ─────────────────────────────────────────────────────────

def test_oi_order():
    print("\n[2a] daily_verify: OI backfill before the signal recompute")
    import scripts.daily_verify as dv
    calls = []
    saved = (dv.dates_to_verify, dv.calendar_check, dv._run_script, dv.run_commodity_oi_backfill)
    try:
        dv.dates_to_verify = lambda t, days: []
        dv.calendar_check = lambda: True
        dv._run_script = lambda name, args, timeout=60: calls.append((name, tuple(args))) or True
        dv.run_commodity_oi_backfill = lambda: calls.append(("backfill_commodity_oi.py", ("--nightly",))) or (True, "")
        rc = dv.main()
    finally:
        dv.dates_to_verify, dv.calendar_check, dv._run_script, dv.run_commodity_oi_backfill = saved
    names = [c[0] if c[0] != "run_cron.py" else c[1][0] for c in calls]
    check("run exits 0 when every step is OK", rc == 0, str(rc))
    check("OI backfill runs", "backfill_commodity_oi.py" in names, str(names))
    check("OI backfill before commodity_signals",
          names.index("backfill_commodity_oi.py") < names.index("commodity_signals"), str(names))
    check("valuation recomputed before models", names.index("valuation") < names.index("models"), str(names))

    cmd = dv.commodity_oi_command(uv="/opt/homebrew/bin/uv")
    check("uv: runs from the project lock (mcx_uv.sh)",
          cmd[:3] == ["bash", os.path.join("scripts", "mcx_uv.sh"), "python"]
          and cmd[-2:] == [os.path.join("scripts", "backfill_commodity_oi.py"), "--nightly"], str(cmd))
    cmd = dv.commodity_oi_command(uv="", py="/x/python3")
    check("no uv: falls back to the helper python", cmd == ["/x/python3", os.path.join("scripts", "backfill_commodity_oi.py"), "--nightly"], str(cmd))

    class R:
        returncode, stdout, stderr = 1, "✗ fetch failed for: GOLD", ""
    ok, tail = dv.run_commodity_oi_backfill(run=lambda *a, **k: R())
    check("a failed backfill is reported, not raised", ok is False and "GOLD" in tail, tail)

    def boom(*a, **k):
        raise FileNotFoundError("uv")
    ok, tail = dv.run_commodity_oi_backfill(run=boom)
    check("a missing interpreter is reported, not raised", ok is False and "FileNotFoundError" in tail, tail)


def test_oi_preserved():
    print("\n[2b] bhav_refresh keeps stored OI")
    import scripts.bhav_refresh as br
    new = [
        {"trading_date": "2026-11-16", "commodity": "GOLD", "instrument_type": "FUTCOM",
         "turnover_cr": 100.0, "open_interest": 0, "oi_value_cr": 0.0},
        {"trading_date": "2026-11-16", "commodity": "SILVER", "instrument_type": "FUTCOM",
         "turnover_cr": 50.0, "open_interest": 0, "oi_value_cr": 0.0},
        {"trading_date": "2026-11-16", "commodity": "COPPER", "instrument_type": "FUTCOM",
         "turnover_cr": 5.0, "open_interest": 7, "oi_value_cr": 1.0},
    ]
    stored = {("2026-11-16", "GOLD", "FUTCOM"): {"open_interest": 12345, "oi_value_cr": 9876.5},
              ("2026-11-16", "COPPER", "FUTCOM"): {"open_interest": 99, "oi_value_cr": 3.0}}
    out = br.preserve_existing_oi(new, stored)
    check("zero OI replaced by the stored figure", out[0]["open_interest"] == 12345 and out[0]["oi_value_cr"] == 9876.5)
    check("other fields untouched", out[0]["turnover_cr"] == 100.0)
    check("no stored OI: stays 0", out[1]["open_interest"] == 0)
    check("a row with its own OI keeps it", out[2]["open_interest"] == 7)
    check("input rows not mutated", new[0]["open_interest"] == 0)

    sent = []

    class Resp:
        def __enter__(self): return self
        def __exit__(self, *a): return False

    real = br.urllib.request.urlopen
    try:
        br.urllib.request.urlopen = lambda req, timeout=None: sent.append(json.loads(req.data)) or Resp()
        ok = br._upsert_commodity_batch(new, read_existing_oi=lambda dates: stored)
        check("upsert payload carries the stored OI", ok and sent and sent[0][0]["open_interest"] == 12345, str(sent)[:200])
        sent.clear()

        def offline(dates):
            raise OSError("offline")
        ok = br._upsert_commodity_batch(new, read_existing_oi=offline)
        check("stored OI unreadable: nothing written", ok is False and not sent)
    finally:
        br.urllib.request.urlopen = real


# ── 3. Gap repair goes through the completeness gate ────────────────────────

def test_gap_repair_gate():
    print("\n[3] backfill_commodity_gaps.repair is gated")
    import scripts.backfill_commodity_gaps as g
    import scripts.bhav_refresh as br
    ok, msg = g.repair("2026-11-16", breakdown=lambda d: (br.BREAKDOWN_INCOMPLETE, 42, "options premium missing (3/20)"))
    check("incomplete breakdown is refused", ok is False and "premium" in msg, msg)
    ok, msg = g.repair("2026-11-16", breakdown=lambda d: (br.BREAKDOWN_WRITTEN, 42, "1,000 contracts"))
    check("complete breakdown is written", ok is True and msg.startswith("42 rows"), msg)

    # End to end through the real commodity_breakdown: a half-published report
    # (contracts, no option premium) must not reach the upsert.
    raw = []
    for i in range(12):
        raw.append({"Symbol": f"F{i}", "InstrumentName": "FUTCOM", "CommodityHead": "X",
                    "TradedContract": 10, "Volume": 10, "TotalValue": 100.0, "PremiumTurnover": "-"})
        raw.append({"Symbol": f"F{i}", "InstrumentName": "OPTFUT", "CommodityHead": "X",
                    "TradedContract": 10, "Volume": 10, "TotalValue": 100.0, "PremiumTurnover": "-"})
    writes = []
    saved = (br._fetch_mcx_raw, br._upsert_commodity_batch)
    try:
        br._fetch_mcx_raw = lambda d: (raw, None)
        br._upsert_commodity_batch = lambda rows, **k: writes.append(rows) or True
        ok, msg = g.repair("2026-11-16")
        check("partial MCX report: no write", ok is False and not writes and "incomplete" in msg, msg)
    finally:
        br._fetch_mcx_raw, br._upsert_commodity_batch = saved


# ── 4. Pagination ───────────────────────────────────────────────────────────

def test_pagination():
    print("\n[4] pagination")
    from lib.mcx_config import paginated_order, supabase_read_all
    p = paginated_order("mcx_commodity_daily", "?select=a&trading_date=gte.2024-01-01&order=trading_date.asc")
    check("commodity_daily gets its unique key",
          p.endswith("order=trading_date.asc,commodity.asc,instrument_type.asc"), p)
    p = paginated_order("mcx_snapshots", "?select=a&order=trading_date.asc,elapsed_min.asc", newest_first=True)
    check("newest_first flips every term", p.endswith("order=trading_date.desc,elapsed_min.desc,id.desc"), p)
    p = paginated_order("mcx_daily_revenue", "?select=a")
    check("no order given: unique key added", p == "?select=a&order=trading_date.asc", p)
    p = paginated_order("mcx_valuation", "?select=a&order=trading_date.desc")
    check("already unique: unchanged", p == "?select=a&order=trading_date.desc", p)

    # A fake table of 2,500 rows, 5 per date, served 1,000 per request at most.
    table = [{"trading_date": f"2026-{1 + i // 150:02d}-{1 + (i // 5) % 30:02d}", "k": i % 5, "i": i}
             for i in range(2500)]
    seen = []

    def read(tbl, params, timeout=10):
        seen.append(params)
        q = dict(x.split("=", 1) for x in params.lstrip("?").split("&"))
        desc = q["order"].startswith("trading_date.desc")
        rows = sorted(table, key=lambda r: (r["trading_date"], r["i"]), reverse=desc)
        off, lim = int(q["offset"]), min(int(q["limit"]), 1000)
        return rows[off:off + lim]

    rows = supabase_read_all("mcx_commodity_daily", "?select=*&order=trading_date.asc", max_rows=1200,
                             newest_first=True, read=read)
    check("newest_first keeps the newest 1,200", [r["i"] for r in rows] == list(range(1300, 2500)),
          f"{rows[0]['i']}..{rows[-1]['i']}")
    check("every page request carries the tiebreaker", all("commodity.desc" in s for s in seen), seen[0])
    rows = supabase_read_all("mcx_commodity_daily", "?select=*&order=trading_date.asc", max_rows=1200, read=read)
    check("default still keeps the oldest (opt-in change)", rows[-1]["i"] == 1199)
    seen.clear()
    rows = supabase_read_all("mcx_commodity_daily", "?select=*&order=trading_date.asc",
                             page_size=5000, max_rows=10000, read=read)
    check("page_size above PostgREST's 1,000 cap still reads everything", len(rows) == 2500, str(len(rows)))

    # The cron copies of fetch_all and api/commodities._fetch_all
    import lib.cron_commodity_signals as ccs, lib.cron_models as cm, lib.cron_momentum as cmo, lib.cron_valuation as cv
    for mod, fn in ((ccs, "fetch_all"), (cm, "fetch_all"), (cmo, "fetch_all"), (cv, "fetch_all_paginated")):
        got = []
        real = mod.sb_get
        try:
            mod.sb_get = lambda tbl, params: got.append(params) or []
            getattr(mod, fn)("mcx_commodity_daily", "trading_date")
        finally:
            mod.sb_get = real
        check(f"{mod.__name__}.{fn} orders by the unique key", got and "commodity.asc,instrument_type.asc" in got[0], str(got))

    import api.commodities as ac
    got = {}
    real = ac.supabase_read_all
    try:
        ac.supabase_read_all = lambda tbl, params, **kw: got.update(kw, params=params) or []
        ac._fetch_all("mcx_commodity_signals", "trading_date", limit=500, where="&trading_date=gte.2026-01-01")
    finally:
        ac.supabase_read_all = real
    check("commodities._fetch_all keeps the newest rows", got.get("newest_first") is True and got.get("max_rows") == 500, str(got))


def _capture_reads(mod, call):
    """Run call() with mod.supabase_read_all recording each (table, kwargs)."""
    got = []
    real = mod.supabase_read_all
    try:
        mod.supabase_read_all = lambda tbl, params="", **kw: got.append((tbl, params, kw)) or []
        try:
            call()
        except Exception:
            pass                    # empty data may fail downstream; only the reads matter
    finally:
        mod.supabase_read_all = real
    return got


def test_capped_reads_keep_newest():
    print("\n[4b] capped ascending reads keep the newest rows")
    import lib.oi_participants_dashboard as oi, api.commodity_dashboard as cd, api.analytics as an
    import lib.intraday_curves as ic, lib.hourly_analysis as ha
    cases = [
        (oi, oi.generate_oi_participants_dashboard),
        (cd, cd.generate_commodity_dashboard),
        (an, an.generate_analytics),
        (ic, lambda: ic.generate_intraday_curves(days=30)),
        (ha, lambda: ha.generate_hourly_accuracy(90)),
    ]
    for mod, call in cases:
        reads = _capture_reads(mod, call)
        bad = [(t, p) for t, p, kw in reads if ".asc" in p and not kw.get("newest_first")]
        check(f"{mod.__name__}: every capped ascending read is newest_first", reads and not bad, str(bad or reads)[:200])
    reads = _capture_reads(cd, cd.generate_commodity_dashboard)
    check("commodity_dashboard cap raised above the ~23k-row window",
          reads and reads[0][2].get("max_rows", 0) >= 30000, str(reads[:1])[:200])


# ── 5. Valuation ────────────────────────────────────────────────────────────

def test_valuation_basis():
    print("\n[5] valuation history on the snapshot's constants")
    import api.valuation as v
    # A row written by the stale writer (252 days, Rs 527 Cr non-F&O) on 16 Nov 2026.
    stale = {"trading_date": "2026-11-16", "ma45_rev_cr": 12.91, "close_price": 3729.6,
             "eps": 81.67, "fair_value_bear": 2705.73, "fair_value_base": 3270.07, "fair_value_bull": 3834.41,
             "implied_pe": 45.67, "signal": "OVERVALUED", "pe_mean_used": 40.04, "pe_sd_used": 6.91}
    pt = v.history_point(stale)
    eps = v.compute_eps_chain(12.91)["eps"]
    check("eps from today's chain, not the stored 81.67", pt["eps"] == eps and abs(eps - 81.67) > 1, str(pt))
    check("fair base = eps x the row's P/E", pt["fair_base"] == round(eps * 40.04, 2), str(pt))
    check("implied P/E recomputed", pt["implied_pe"] == round(3729.6 / eps, 2), str(pt))
    no_ma = v.history_point({"trading_date": "2020-01-01", "close_price": 100.0})
    check("row without a 45-day MA: no fair value, no crash", no_ma["fair_base"] is None and no_ma["eps"] is None)

    saved = (v._fetch_precomputed_valuations, v._fetch_latest_price, v._fetch_regression_pairs)
    try:
        older = dict(stale, trading_date="2026-11-13", ma45_rev_cr=12.82, close_price=3700.0)
        v._fetch_precomputed_valuations = lambda limit=90: [older, stale]
        v._fetch_latest_price = lambda: (3729.6, "2026-11-16")
        v._fetch_regression_pairs = lambda: []
        r = v.generate_valuation("Max")
    finally:
        v._fetch_precomputed_valuations, v._fetch_latest_price, v._fetch_regression_pairs = saved
    last = r["history"][-1]
    check("last history point equals the snapshot",
          last["eps"] == r["snapshot"]["current_eps"] and last["fair_base"] == r["snapshot"]["fair_value"]["base"],
          f"{last} vs {r['snapshot']['fair_value']}")
    check("payload says which basis", r["data_quality"].get("history_basis") == "current_constants")


# ── 6. Hourly accuracy ──────────────────────────────────────────────────────

def test_hourly():
    print("\n[6] hourly accuracy")
    import lib.hourly_analysis as ha
    from lib.mcx_config import trim_stale_open
    dates = ["d0", "d1", "d2", "d3", "d4", "d5", "d6"]
    prices = {d: 100.0 + i for i, d in enumerate(dates)}
    f1 = ha.forward_returns(dates, prices, 1)
    check("1-day return enters at the NEXT close", abs(f1["d0"] - (102 - 101) / 101) < 1e-12, str(f1["d0"]))
    check("1-day return needs lag+hold sessions ahead", "d5" not in f1 and "d4" in f1)
    f5 = ha.forward_returns(dates, prices, 5)
    check("5-day return from the next close", abs(f5["d0"] - (106 - 101) / 101) < 1e-12 and "d1" not in f5)
    f1_0 = ha.forward_returns(dates, prices, 1, lag=0)
    check("lag 0 reproduces the old same-day return", abs(f1_0["d0"] - 0.01) < 1e-12)

    snaps = [{"elapsed_min": 15, "fut_notl_cr": 50000, "opt_prem_cr": 900},   # prior-day cumulative
             {"elapsed_min": 30, "fut_notl_cr": 3000, "opt_prem_cr": 40},
             {"elapsed_min": 45, "fut_notl_cr": 5000, "opt_prem_cr": 70},
             {"elapsed_min": 60, "fut_notl_cr": 7000, "opt_prem_cr": 90},
             {"elapsed_min": 75, "fut_notl_cr": 9000, "opt_prem_cr": 110}]
    t = trim_stale_open(snaps)
    check("stale opening snapshot trimmed", [s["elapsed_min"] for s in t] == [30, 45, 60, 75], str(t))
    check("clean day untouched", trim_stale_open(snaps[1:]) == snaps[1:])
    check("never below 4 snapshots", len(trim_stale_open(snaps[:4])) == 4)

    ov = ha.in_sample_overlap(["2026-08-01", "2026-09-20", "2026-11-16"], date(2026, 11, 17), lookback_days=60)
    check("in-sample = days inside the curve's 60-day window", ov == ["2026-09-20", "2026-11-16"], str(ov))

    real = ha.supabase_read_all
    try:
        ha.supabase_read_all = lambda *a, **k: []
        r = ha.generate_hourly_accuracy(90)
    finally:
        ha.supabase_read_all = real
    m = r.get("methodology", {})
    check("payload carries the methodology",
          m.get("entry_lag_days") == 1 and m.get("curve_training_days") == 60
          and m.get("stale_open_trimmed") is True and "curve_fitted_in_sample" in m, str(m))


if __name__ == "__main__":
    print("=" * 72)
    print("Phase 2 data-correctness tests")
    print("=" * 72)
    for t in (test_calendar, test_oi_order, test_oi_preserved, test_gap_repair_gate,
              test_pagination, test_capped_reads_keep_newest, test_valuation_basis, test_hourly):
        try:
            t()
        except Exception as e:
            import traceback; traceback.print_exc()
            check(f"{t.__name__} raised {type(e).__name__}: {e}", False)
    print("\n" + "=" * 72)
    if FAILED:
        print(f"{len(FAILED)} FAILED: {', '.join(FAILED)}")
        sys.exit(1)
    print("All tests passed.")
