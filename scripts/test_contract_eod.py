#!/usr/bin/env python3
"""
Tests for the per-contract MCX futures EOD files: one CSV per session at
${MCX_CONTRACT_EOD_DIR:-~/tradingone/data/mcx_contract_eod}/YYYY-MM-DD.csv,
built from MCX's Bhavcopy (lib/contract_eod.py, scripts/backfill_contract_eod.py,
daily_verify step 5e) and read by a separate local project.

  1. Bhavcopy rows -> the project's columns: official close and previous
     close, blank ltp (the report has none), zero prices blank, every FUTCOM
     contract and expiry the report lists.
  2. Date -> file mapping, and the date check: another day's rows are never
     filed under the asked date.
  3. Atomic write; an empty or smaller capture never replaces a file.
  4. Backfill: which dates, skip what is written (resume), pacing, a clean
     stop on a block or when MCX stops honouring the date.
  5. daily_verify runs it for the last 10 sessions and a failure there is
     non-fatal; the relay no longer writes these files.

Offline: no network, files only in temp dirs.
Run:  python3 scripts/test_contract_eod.py
"""
import csv, inspect, os, sys, tempfile
from datetime import date, datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

FAILED = []

COLUMNS = ["date", "symbol", "instrument", "expiry", "open", "high", "low", "close",
           "prev_close", "ltp", "oi", "volume", "captured_at"]
DAY = "2026-11-20"
CAPTURED = "2026-11-22T07:01:15+05:30"


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}" + (f"  — {detail}" if detail and not cond else ""))
    if not cond:
        FAILED.append(name)


def _bhav(sym, expiry, close, prev, o=0.0, h=0.0, l=0.0, vol=0, oi=0, day="11/20/2026", inst="FUTCOM"):
    # Shape of a GetDateWiseBhavCopy row (20 Nov 2026).
    return {"sLTT": None, "Date": day, "Symbol": sym.ljust(13), "ExpiryDate": expiry,
            "Open": o, "High": h, "Low": l, "Close": close, "PreviousClose": prev,
            "Volume": vol, "VolumeInThousands": f"{vol}.000 KGS  ", "Value": 0.0,
            "OpenInterest": oi, "DateDisplay": "20 Nov 2026", "InstrumentName": inst,
            "StrikePrice": 0.0, "OptionType": "-"}


def _report(day="11/20/2026"):
    return [
        _bhav("ALUMINI", "30NOV2026", 318.35, 320.55, 316.0, 322.3, 314.2, 3473, 1274, day),
        # untraded: MCX still publishes a Close (= PreviousClose); no O/H/L
        _bhav("ALUMINI", "26FEB2027", 308.45, 308.45, day=day),
        _bhav("CRUDEOIL", "18DEC2026", 5100.0, 5050.0, 5060.0, 5120.0, 5040.0, 20000, 15000, day),
        _bhav("ZINC", "SOMEDAY", 250.0, 249.0, vol=1, day=day),               # unreadable expiry
        _bhav("ALUMINI", "30NOV2026", 1.0, 1.0, day=day),                     # repeat: first wins
        _bhav("GOLD", "05FEB2027", 120000.0, 0.0, day=day),                   # prev 0 -> blank
        _bhav("SILVER", "05MAR2027", 150000.0, 149000.0, day="11/19/2026"),   # another day
        _bhav("MCXBULLDEX", "24DEC2026", 20000.0, 19900.0, day=day, inst="FUTIDX"),
        {**_bhav("COPPER", "31DEC2026", 1200.0, 1190.0, vol=5, oi=9, day=day), "LTP": 1201.5},
    ]


def _read(path):
    with open(path, newline="") as f:
        r = csv.reader(f)
        header = next(r)
        return header, [dict(zip(header, row)) for row in r]


def test_rows():
    print("\n[1] Bhavcopy rows -> contract rows")
    import lib.contract_eod as ce
    rows = ce.bhavcopy_rows(_report(), DAY, CAPTURED)
    by = {(r["symbol"], r["expiry"]): r for r in rows}
    check("column order is the contract", list(ce.CONTRACT_EOD_COLUMNS) == COLUMNS, str(ce.CONTRACT_EOD_COLUMNS))
    check("every row has exactly those columns", rows and all(list(r) == COLUMNS for r in rows))
    check("FUTCOM of the asked day only; bad expiry dropped; repeat kept once",
          sorted(by) == [("ALUMINI", "2026-11-30"), ("ALUMINI", "2027-02-26"), ("COPPER", "2026-12-31"),
                         ("CRUDEOIL", "2026-12-18"), ("GOLD", "2027-02-05")], str(sorted(by)))
    check("sorted by symbol, then expiry", [(r["symbol"], r["expiry"]) for r in rows] == sorted(by))
    a = by.get(("ALUMINI", "2026-11-30"), {})
    check("traded: O/H/L, official close, previous close",
          [a.get(k) for k in ("open", "high", "low", "close", "prev_close")]
          == [316.0, 322.3, 314.2, 318.35, 320.55], str(a))
    check("first of a repeated contract wins", a.get("close") == 318.35, str(a.get("close")))
    check("ltp blank: the Bhavcopy has no last traded price", a.get("ltp") == "", repr(a.get("ltp")))
    check("oi / volume as ints, symbol unpadded, date, instrument, captured_at",
          (a.get("oi"), a.get("volume"), a.get("symbol"), a.get("date"), a.get("instrument"),
           a.get("captured_at")) == (1274, 3473, "ALUMINI", DAY, "FUTCOM", CAPTURED), str(a))
    u = by.get(("ALUMINI", "2027-02-26"), {})
    check("untraded: O/H/L blank, official close kept, volume 0",
          [u.get(k) for k in ("open", "high", "low")] == ["", "", ""]
          and (u.get("close"), u.get("prev_close"), u.get("volume"), u.get("oi")) == (308.45, 308.45, 0, 0), str(u))
    check("previous close 0 -> blank", by.get(("GOLD", "2027-02-05"), {}).get("prev_close") == "")
    check("ltp taken when the report has one", by.get(("COPPER", "2026-12-31"), {}).get("ltp") == 1201.5)
    check("no rows in -> no rows out", ce.bhavcopy_rows([], DAY, CAPTURED) == []
          and ce.bhavcopy_rows(None, DAY, CAPTURED) == [])


def test_paths():
    print("\n[2] Date -> file, and the date check")
    import lib.contract_eod as ce
    saved = os.environ.pop("MCX_CONTRACT_EOD_DIR", None)
    try:
        check("default dir ~/tradingone/data/mcx_contract_eod",
              ce.contract_eod_dir() == os.path.expanduser("~/tradingone/data/mcx_contract_eod"), ce.contract_eod_dir())
        os.environ["MCX_CONTRACT_EOD_DIR"] = "/tmp/x"
        check("MCX_CONTRACT_EOD_DIR overrides (read at call time)", ce.contract_eod_dir() == "/tmp/x")
    finally:
        os.environ.pop("MCX_CONTRACT_EOD_DIR", None)
        if saved is not None:
            os.environ["MCX_CONTRACT_EOD_DIR"] = saved
    check("YYYY-MM-DD.csv in the dir", ce.contract_eod_path(DAY, "/d") == "/d/2026-11-20.csv")
    rows = ce.bhavcopy_rows(_report(), DAY, CAPTURED)
    with tempfile.TemporaryDirectory() as tmp:
        try:
            ce.write_contract_eod(rows, "2026-11-19", tmp)
            raised = False
        except ValueError:
            raised = True
        check("rows of another day are refused, nothing written", raised and os.listdir(tmp) == [],
              str(os.listdir(tmp)))


def test_write():
    print("\n[3] Atomic write, never replaced by an empty or smaller capture")
    import lib.contract_eod as ce
    rows = ce.bhavcopy_rows(_report(), DAY, CAPTURED)
    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "nested", "dir")
        path = os.path.join(out, f"{DAY}.csv")
        check("empty: nothing written, no directory made",
              ce.write_contract_eod([], DAY, out) == "empty" and not os.path.exists(out))
        status = ce.write_contract_eod(rows, DAY, out)
        header, got = _read(path) if os.path.exists(path) else ([], [])
        check("writes YYYY-MM-DD.csv", status == "written" and os.path.exists(path), status)
        check("header is the contract", header == COLUMNS, str(header))
        check("one line per contract", len(got) == len(rows), f"{len(got)} vs {len(rows)}")
        if got:
            check("values round-trip", (got[0]["symbol"], got[0]["expiry"], got[0]["close"], got[0]["ltp"],
                                        got[1]["open"]) == ("ALUMINI", "2026-11-30", "318.35", "", ""), str(got[:2]))
        check("no temp files left", os.listdir(out) == [f"{DAY}.csv"], str(os.listdir(out)))
        before = open(path).read()
        check("smaller capture keeps the file",
              ce.write_contract_eod(rows[:2], DAY, out) == "kept" and open(path).read() == before)
        check("empty capture keeps the file",
              ce.write_contract_eod([], DAY, out) == "empty" and open(path).read() == before)
        newer = [dict(r, captured_at="2026-11-23T07:00:00+05:30") for r in rows]
        check("same-size capture replaces it",
              ce.write_contract_eod(newer, DAY, out) == "written" and "2026-11-23T07" in open(path).read())
        before = open(path).read()
        real = ce.os.replace
        ce.os.replace = lambda *a, **k: (_ for _ in ()).throw(OSError("disk full"))
        try:
            ce.write_contract_eod(rows + rows[:1], DAY, out)
            raised = False
        except OSError:
            raised = True
        finally:
            ce.os.replace = real
        check("failed rename raises; old file intact; no temp left",
              raised and open(path).read() == before and os.listdir(out) == [f"{DAY}.csv"], str(os.listdir(out)))


def test_dates():
    print("\n[4a] Which dates")
    import lib.contract_eod as ce
    from lib.calendar import has_session
    ds = ce.session_dates("2026-11-02", "2026-11-15")
    days = [date.fromisoformat(d) for d in ds]
    check("ascending ISO dates in range", ds == sorted(ds) and ds[0] >= "2026-11-02" and ds[-1] <= "2026-11-15")
    check("only days MCX trades (Muhurat Sunday 8 Nov included, Saturdays not)",
          all(has_session(d) for d in days) and "2026-11-08" in ds and "2026-11-07" not in ds, str(ds))
    check("weekends=True tries every day", "2026-11-07" in ce.session_dates("2026-11-02", "2026-11-15", weekends=True))
    rec = ce.recent_session_dates(date(2026, 11, 22), 3)
    check("recent: the last N sessions before today, oldest first",
          len(rec) == 3 and rec == sorted(rec) and rec[-1] == "2026-11-20" and all(d < "2026-11-22" for d in rec), str(rec))
    with tempfile.TemporaryDirectory() as tmp:
        open(ce.contract_eod_path("2026-11-03", tmp), "w").write("x")
        check("pending skips dates that have a file",
              ce.pending_dates(["2026-11-02", "2026-11-03", "2026-11-04"], tmp) == ["2026-11-02", "2026-11-04"])

    import backfill_contract_eod as bf
    a = bf.parse_args(["--recent", "10"])
    check("--recent 10 -> the last 10 sessions", bf.plan_dates(a, date(2026, 11, 22))
          == ce.recent_session_dates(date(2026, 11, 22), 10))
    a = bf.parse_args(["--start", "2026-11-16"])
    got = bf.plan_dates(a, date(2026, 11, 22))
    check("--start alone runs to yesterday", got and got[0] == "2026-11-16" and got[-1] == "2026-11-20", str(got))
    check("--start default is 2022-01-01", bf.parse_args([]).start == "2022-01-01")
    check("pause has a 1 s floor", bf.parse_args(["--pause", "0.1"]).pause == 1.0
          and bf.parse_args(["--pause", "2"]).pause == 2.0)


class _Fetch:
    """fetch(date) for backfill.run: a report per date; [] = no report;
    an Exception instance is raised."""
    def __init__(self, plan):
        self.plan, self.calls = plan, []

    def __call__(self, d):
        self.calls.append(d)
        got = self.plan.get(d, [])
        if isinstance(got, list) and got and isinstance(got[0], Exception):
            raise got.pop(0)
        if isinstance(got, Exception):
            raise got
        return got


def _day_report(d):
    return _report(day=datetime.strptime(d, "%Y-%m-%d").strftime("%m/%d/%Y"))


def test_run():
    print("\n[4b] Backfill run: resume, pacing, clean stops")
    import backfill_contract_eod as bf
    import lib.contract_eod as ce
    d1, d2, d3, d4, d5 = "2026-11-16", "2026-11-17", "2026-11-18", "2026-11-19", "2026-11-20"
    now = lambda: datetime(2026, 11, 22, 7, 1, 15)
    with tempfile.TemporaryDirectory() as tmp:
        ce.write_contract_eod(ce.bhavcopy_rows(_day_report(d3), d3, CAPTURED), d3, tmp)
        fetch, sleeps = _Fetch({d1: _day_report(d1), d4: _day_report(d4), d5: _day_report(d5)}), []
        s = bf.run([d1, d2, d3, d4, d5], fetch, tmp, pause=1.5, sleep=sleeps.append, now=now)
        check("writes the days with a report", s["written"] == [d1, d4, d5], str(s))
        check("a day with no report is counted, not written", s["no_report"] == [d2]
              and not os.path.exists(ce.contract_eod_path(d2, tmp)), str(s))
        check("an existing file is skipped without a fetch", s["present"] == 1 and d3 not in fetch.calls, str(fetch.calls))
        check("paced between fetches", sleeps == [1.5] * 3, str(sleeps))
        check("not stopped", s["stopped"] is None, str(s["stopped"]))
        _, rows = _read(ce.contract_eod_path(d4, tmp))
        check("file date column = its name; captured_at IST",
              rows and rows[0]["date"] == d4 and rows[0]["captured_at"] == "2026-11-22T07:01:15+05:30", str(rows[:1]))
        fetch2 = _Fetch({})
        s = bf.run([d1, d2, d3, d4, d5], fetch2, tmp, pause=1.5, sleep=lambda x: None, now=now)
        check("resume: only the day without a file is tried again", fetch2.calls == [d2] and s["written"] == [],
              str(fetch2.calls))

    with tempfile.TemporaryDirectory() as tmp:
        reconnects = []
        fetch = _Fetch({d1: _day_report(d1), d2: [RuntimeError("HTTP 403"), RuntimeError("HTTP 403")],
                        d3: _day_report(d3)})
        s = bf.run([d1, d2, d3], fetch, tmp, sleep=lambda x: None, now=now, reconnect=lambda: reconnects.append(1))
        check("blocked: one fresh session, then a clean stop", len(reconnects) == 1 and s["stopped"]
              and "403" in s["stopped"] and s["stopped_at"] == d2, str(s))
        check("blocked: earlier days kept, later days not fetched",
              s["written"] == [d1] and d3 not in fetch.calls, str(fetch.calls))

    with tempfile.TemporaryDirectory() as tmp:
        calls = {"n": 0}

        def flaky(d):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("timeout")
            return _day_report(d)
        s = bf.run([d1, d2], flaky, tmp, sleep=lambda x: None, now=now, reconnect=lambda: None)
        check("a transient error heals with a fresh session", s["stopped"] is None and s["written"] == [d1, d2], str(s))

    with tempfile.TemporaryDirectory() as tmp:
        fetch = _Fetch({})
        s = bf.run([d1, d2, d3, d4, d5], fetch, tmp, sleep=lambda x: None, now=now, max_no_report=3)
        check("sessions in a row with no report: stop (MCX may be ignoring the date)",
              len(fetch.calls) == 3 and s["stopped"] and s["stopped_at"] == d3, str(s))

    with tempfile.TemporaryDirectory() as tmp:
        junk = [dict(r, ExpiryDate="??") for r in _day_report(d1)]
        s = bf.run([d1, d2], _Fetch({d1: junk, d2: _day_report(d2)}), tmp, sleep=lambda x: None, now=now)
        check("a report with no usable rows stops the run (format change)",
              s["stopped"] and s["stopped_at"] == d1 and s["written"] == [] and os.listdir(tmp) == [], str(s))

    with tempfile.NamedTemporaryFile() as not_a_dir:
        s = bf.run([d1], _Fetch({d1: _day_report(d1)}), not_a_dir.name, sleep=lambda x: None, now=now)
        check("a write error stops cleanly, no raise", s["stopped"] and s["stopped_at"] == d1, str(s))


def test_daily_verify_and_relay():
    print("\n[5] daily_verify runs it (non-fatal); the relay no longer writes it")
    import scripts.daily_verify as dv
    calls = []
    saved = (dv.dates_to_verify, dv._run_script, dv.run_commodity_oi_backfill)

    def run_script(name, args, timeout=60):
        calls.append((name, list(args), timeout))
        return name != "backfill_contract_eod.py"
    dv.dates_to_verify = lambda t, days: []
    dv._run_script = run_script
    dv.run_commodity_oi_backfill = lambda *a, **k: (calls.append(("commodity_oi", [], 0)), (True, ""))[1]
    try:
        rc_fail = dv.main()
        dv._run_script = lambda name, args, timeout=60: True
        rc_ok = dv.main()
    finally:
        dv.dates_to_verify, dv._run_script, dv.run_commodity_oi_backfill = saved
    step = [c for c in calls if c[0] == "backfill_contract_eod.py"]
    check("runs backfill_contract_eod.py --recent 10", step and step[0][1] == ["--recent", "10"], str(step))
    check("with a timeout", step and 60 <= step[0][2] <= 600, str(step))
    check("its failure does not change the exit code", rc_fail == rc_ok, f"{rc_fail} vs {rc_ok}")
    names = [c[0] for c in calls]
    check("after the commodity OI step, before the signal recompute",
          step and names.index("commodity_oi") < names.index("backfill_contract_eod.py")
          < names.index("run_cron.py"), str(names))

    import scripts.mcx_relay as relay
    check("relay: no contract-file step", not hasattr(relay, "capture_contract_eod")
          and "contract" not in inspect.getsource(relay.close_session).lower())


if __name__ == "__main__":
    print("=" * 72)
    print("per-contract EOD file tests")
    print("=" * 72)
    for t in (test_rows, test_paths, test_write, test_dates, test_run, test_daily_verify_and_relay):
        try:
            t()
        except Exception as e:      # a missing function is a failure, not a crash
            import traceback; traceback.print_exc()
            print(f"  FAIL  {t.__name__} raised {type(e).__name__}: {e}")
            FAILED.append(t.__name__)
    print("\n" + "=" * 72)
    if FAILED:
        print(f"{len(FAILED)} FAILED: {', '.join(FAILED)}")
        sys.exit(1)
    print("All tests passed.")
