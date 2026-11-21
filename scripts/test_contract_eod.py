#!/usr/bin/env python3
"""
Tests for the relay's per-contract EOD file (scripts/mcx_relay.py,
capture_contract_eod): after the close, every FUTCOM contract in MCX
MarketWatch goes to ${MCX_CONTRACT_EOD_DIR:-~/tradingone/data/mcx_contract_eod}/
YYYY-MM-DD.csv, which a separate local project reads.

The contract that project depends on:
  1. Columns exactly: date, symbol, instrument, expiry, open, high, low, close,
     prev_close, ltp, oi, volume, captured_at. Expiry is YYYY-MM-DD.
  2. Written atomically (temp file + rename); an empty or smaller capture
     never replaces an existing day's file; data for another day never goes
     under the session's date.
  3. Reuses the MarketWatch JSON capture_eod already fetched: no extra MCX
     call, and it runs after every existing close-of-session step.
  4. Never raises into the relay loop.

Offline: market watch, Supabase and every other close-of-session step are faked.
Run:  python3 scripts/test_contract_eod.py
"""
import csv, os, sys, tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import scripts.mcx_relay as relay

FAILED = []

COLUMNS = ["date", "symbol", "instrument", "expiry", "open", "high", "low", "close",
           "prev_close", "ltp", "oi", "volume", "captured_at"]
SESSION = "2026-11-20"
CAPTURED = "2026-11-20T23:57:04+05:30"
AS_OF_MS = 1795199100000          # MarketWatch Summary.AsOn: 2026-11-20 23:55 IST


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}" + (f"  — {detail}" if detail else ""))
    if not cond:
        FAILED.append(name)


def _fut(sym, expiry, ltp=0.0, prev=100.0, vol=0, oi=0, o=0.0, h=0.0, l=0.0,
         ltt="", key="InstrumentName"):
    # Shape of a live GetMarketWatch row (21 Nov 2026), trimmed.
    return {"Symbol": None, "ProductCode": sym, "ExpiryDate": expiry, key: "FUTCOM",
            "Open": o, "High": h, "Low": l, "LTP": ltp, "PreviousClose": prev,
            "Volume": vol, "OpenInterest": oi, "LTTValue": ltt,
            "NotionalValue": 0.0, "PremiumValue": 0.0}


def _market_watch(as_of_ms=AS_OF_MS, extra=()):
    rows = [
        _fut("ALUMINI", "30NOV2026", ltp=319.0, prev=318.35, vol=3473, oi=1274,
             o=316.0, h=322.3, l=314.2, ltt="2026-11-20 23:54:44"),
        _fut("ALUMINI", "31DEC2026", ltp=321.5, prev=320.9, vol=12, oi=88,
             o=320.0, h=322.0, l=319.8, ltt="2026-11-20 23:41:02"),
        # untraded: MarketWatch reports 0 for every price but PreviousClose
        _fut("CARDAMOM", "29JAN2027", prev=3536.0, oi=1),
        # older payloads spell the key Instrumentname
        _fut("GOLDM", "04DEC2026", ltp=97000.0, prev=96500.0, vol=5000, oi=9000,
             o=96600.0, h=97200.0, l=96400.0, ltt="2026-11-20 23:54:59", key="Instrumentname"),
        # unparseable expiry: dropped, not guessed
        _fut("ZINC", "SOMEDAY", ltp=250.0, vol=1),
        # duplicate of the first row: kept once
        _fut("ALUMINI", "30NOV2026", ltp=1.0, vol=1),
        {"InstrumentName": "OPTFUT", "ProductCode": "CRUDEOIL", "ExpiryDate": "15DEC2026",
         "LTP": 120.0, "Volume": 900, "OpenInterest": 50, "PreviousClose": 110.0,
         "StrikePrice": 5000.0, "OptionType": "CE"},
        *extra,
    ]
    summary = {"Count": len(rows)}
    if as_of_ms is not None:
        summary["AsOn"] = f"/Date({as_of_ms})/"
    return {"success": True, "data": {"Summary": summary, "Data": rows}}


def _read(path):
    with open(path, newline="") as f:
        r = csv.reader(f)
        header = next(r)
        return header, [dict(zip(header, row)) for row in r]


class _Env:
    """Point MCX_CONTRACT_EOD_DIR at a temp dir for the duration."""
    def __enter__(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.old = os.environ.get("MCX_CONTRACT_EOD_DIR")
        os.environ["MCX_CONTRACT_EOD_DIR"] = self.tmp.name
        return self.tmp.name

    def __exit__(self, *exc):
        if self.old is None:
            os.environ.pop("MCX_CONTRACT_EOD_DIR", None)
        else:
            os.environ["MCX_CONTRACT_EOD_DIR"] = self.old
        self.tmp.cleanup()


def test_rows():
    print("\n[1] MarketWatch -> contract rows")
    rows = relay.contract_eod_rows(_market_watch(), SESSION, CAPTURED)
    by = {(r["symbol"], r["expiry"]): r for r in rows}
    check("FUTCOM only, bad expiry dropped, duplicate kept once", len(rows) == 4,
          str(sorted(by)))
    check("column order is the contract", list(relay.CONTRACT_EOD_COLUMNS) == COLUMNS,
          str(relay.CONTRACT_EOD_COLUMNS))
    check("every row has exactly the contract's columns",
          all(list(r) == COLUMNS for r in rows), str(list(rows[0]) if rows else None))
    a = by.get(("ALUMINI", "2026-11-30"), {})
    check("expiry 30NOV2026 -> 2026-11-30, symbol from ProductCode", bool(a), str(sorted(by)))
    check("first of a duplicate pair wins", a.get("ltp") == 319.0, str(a.get("ltp")))
    check("traded row: prices, close = LTP",
          (a.get("open"), a.get("high"), a.get("low"), a.get("close"), a.get("ltp"),
           a.get("prev_close")) == (316.0, 322.3, 314.2, 319.0, 319.0, 318.35), str(a))
    check("oi and volume are ints", (a.get("oi"), a.get("volume")) == (1274, 3473)
          and all(isinstance(a.get(k), int) for k in ("oi", "volume")), str(a))
    check("date, instrument, captured_at",
          (a.get("date"), a.get("instrument"), a.get("captured_at")) == (SESSION, "FUTCOM", CAPTURED),
          str(a))
    c = by.get(("CARDAMOM", "2027-01-29"), {})
    check("untraded row: zero prices blank, prev_close and OI kept",
          [c.get(k) for k in ("open", "high", "low", "close", "ltp")] == [""] * 5
          and c.get("prev_close") == 3536.0 and c.get("oi") == 1 and c.get("volume") == 0, str(c))
    check("Instrumentname spelling accepted", ("GOLDM", "2026-12-04") in by, str(sorted(by)))
    check("sorted by symbol, then expiry",
          [(r["symbol"], r["expiry"]) for r in rows] == sorted((r["symbol"], r["expiry"]) for r in rows))
    check("empty / legacy payloads give no rows",
          relay.contract_eod_rows({}, SESSION, CAPTURED) == []
          and relay.contract_eod_rows({"d": {"Data": []}}, SESSION, CAPTURED) == [])


def test_as_of():
    print("\n[2] Which day the MarketWatch data is for")
    from datetime import datetime
    check("Summary.AsOn -> IST", relay.market_watch_as_of(_market_watch())
          == datetime(2026, 11, 20, 23, 55), str(relay.market_watch_as_of(_market_watch())))
    check("no AsOn -> latest LTTValue",
          relay.market_watch_as_of(_market_watch(as_of_ms=None)) == datetime(2026, 11, 20, 23, 54, 59),
          str(relay.market_watch_as_of(_market_watch(as_of_ms=None))))
    check("MCX's year-1 placeholder is not a date",
          relay.market_watch_as_of({"data": {"Summary": {"AsOn": "/Date(-62135596800000)/"},
                                             "Data": []}}) is None)
    check("nothing at all -> None", relay.market_watch_as_of({}) is None)


def test_write():
    print("\n[3] Atomic write, never replaced by a smaller capture")
    rows = relay.contract_eod_rows(_market_watch(), SESSION, CAPTURED)
    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "nested", "dir")
        path = os.path.join(out, f"{SESSION}.csv")

        check("empty capture: nothing written, no directory made",
              relay.write_contract_eod([], SESSION, out) == "empty" and not os.path.exists(out))

        status = relay.write_contract_eod(rows, SESSION, out)
        header, got = _read(path) if os.path.exists(path) else ([], [])
        check("writes YYYY-MM-DD.csv", status == "written" and os.path.exists(path), status)
        check("header is the contract", header == COLUMNS, str(header))
        check("one line per contract", len(got) == len(rows), f"{len(got)} vs {len(rows)}")
        if got:
            check("values round-trip", got[0]["symbol"] == "ALUMINI" and got[0]["expiry"] == "2026-11-30"
                  and got[0]["close"] == "319.0" and got[0]["oi"] == "1274", str(got[0]))
        check("no temp files left", sorted(os.listdir(out)) == [f"{SESSION}.csv"], str(os.listdir(out)))

        before = open(path).read()
        check("smaller capture keeps the existing file",
              relay.write_contract_eod(rows[:2], SESSION, out) == "kept" and open(path).read() == before)
        check("empty capture keeps the existing file",
              relay.write_contract_eod([], SESSION, out) == "empty" and open(path).read() == before)

        newer = [dict(r, captured_at="2026-11-20T23:59:00+05:30") for r in rows]
        check("same-size capture replaces it",
              relay.write_contract_eod(newer, SESSION, out) == "written"
              and "23:59:00" in open(path).read())

        # A crash between the temp write and the rename leaves the old file
        # whole and cleans up the temp file.
        before = open(path).read()
        real_replace = relay.os.replace
        relay.os.replace = lambda *a, **k: (_ for _ in ()).throw(OSError("disk full"))
        try:
            relay.write_contract_eod(rows + rows[:1], SESSION, out)
            raised = None
        except OSError as e:
            raised = e
        finally:
            relay.os.replace = real_replace
        check("failed rename raises to the caller", raised is not None)
        check("failed rename: old file intact, no temp left",
              open(path).read() == before and sorted(os.listdir(out)) == [f"{SESSION}.csv"],
              str(os.listdir(out)))


def test_capture():
    print("\n[4] capture_contract_eod: location, guards, never raises")
    saved_env = os.environ.pop("MCX_CONTRACT_EOD_DIR", None)
    try:
        check("default dir is ~/tradingone/data/mcx_contract_eod",
              relay.contract_eod_dir() == os.path.expanduser("~/tradingone/data/mcx_contract_eod"),
              relay.contract_eod_dir())
    finally:
        if saved_env is not None:
            os.environ["MCX_CONTRACT_EOD_DIR"] = saved_env

    fetches = []
    real_fetch = relay.fetch_market_watch
    relay.fetch_market_watch = lambda *a, **k: fetches.append(1) or _market_watch()
    try:
        with _Env() as out:
            got = relay.capture_contract_eod(SESSION, raw=_market_watch())
            path = os.path.join(out, f"{SESSION}.csv")
            check("writes into MCX_CONTRACT_EOD_DIR", got == path and os.path.exists(path), str(got))
            _, rows = _read(path)
            check("4 contracts, captured_at is IST ISO",
                  len(rows) == 4 and rows[0]["captured_at"].endswith("+05:30")
                  and rows[0]["captured_at"][10] == "T", rows[0]["captured_at"] if rows else "")

        with _Env() as out:
            stale = _market_watch(as_of_ms=AS_OF_MS - 86400_000)     # 19 Nov's data
            check("data for another day is not written",
                  relay.capture_contract_eod(SESSION, raw=stale) is None and os.listdir(out) == [])
            check("unknown as-of day is not written",
                  relay.capture_contract_eod(SESSION, raw={"data": {"Data": [
                      _fut("ALUMINI", "30NOV2026", ltp=319.0, vol=1)]}}) is None
                  and os.listdir(out) == [])
            check("no MarketWatch from the EOD capture: skipped, no fetch of its own",
                  relay.capture_contract_eod(SESSION, raw=None) is None and fetches == []
                  and os.listdir(out) == [])
            got = relay.capture_contract_eod(None, raw=_market_watch())
            check("no session date: uses the data's own day",
                  got == os.path.join(out, f"{SESSION}.csv"), str(got))

        with tempfile.NamedTemporaryFile() as not_a_dir:
            os.environ["MCX_CONTRACT_EOD_DIR"] = not_a_dir.name
            try:
                got = relay.capture_contract_eod(SESSION, raw=_market_watch())
                raised = None
            except BaseException as e:
                got, raised = None, e
            finally:
                os.environ.pop("MCX_CONTRACT_EOD_DIR", None)
            check("unwritable location: logged, never raises", raised is None and got is None, repr(raised))
        for bad in ({"data": None}, {"data": {"Data": None}}, {"data": {"Data": [None, 5, "x"]}}, "junk"):
            try:
                relay.capture_contract_eod(SESSION, raw=bad)
                ok = True
            except BaseException:
                ok = False
            check(f"malformed payload {str(bad)[:24]!r}: never raises", ok)
    finally:
        relay.fetch_market_watch = real_fetch
        if saved_env is not None:
            os.environ["MCX_CONTRACT_EOD_DIR"] = saved_env


def _close(contract_step=None):
    """close_session() with capture_eod real and every network side effect faked.
    Returns (events, fetch count, raised)."""
    events, fetches = [], []
    names = ("_get_mw_session", "fetch_market_watch", "extract_notionals", "supabase_upsert",
             "fetch_mcx_historical", "refresh_margins_local", "refresh_oi_participants_local",
             "send_heartbeat", "capture_commodity_breakdown", "capture_contract_eod")
    saved = {n: getattr(relay, n) for n in names}
    real_sleep = relay.time.sleep
    out = os.environ["MCX_CONTRACT_EOD_DIR"]
    path = os.path.join(out, f"{SESSION}.csv")
    relay._get_mw_session = lambda *a, **k: object()
    relay.fetch_market_watch = lambda *a, **k: fetches.append(1) or _market_watch()
    relay.extract_notionals = lambda raw: (38797.0, 0.0, 10476.0, [1] * 91, [1] * 867)
    relay.fetch_mcx_historical = lambda d: events.append(("historical",)) or None
    relay.supabase_upsert = lambda table, rec, **k: (events.append(("upsert", table)), [rec])[1]
    relay.refresh_margins_local = lambda: events.append(("margins",))
    relay.refresh_oi_participants_local = lambda: events.append(("oi",))
    relay.send_heartbeat = lambda status, **k: events.append(("heartbeat", status))
    relay.capture_commodity_breakdown = lambda d, **k: events.append(
        ("commodity", "file exists" if os.path.exists(path) else "no file yet"))
    real_step = saved["capture_contract_eod"]
    relay.capture_contract_eod = contract_step or (
        lambda d, raw=None, **k: events.append(("contracts", d)) or real_step(d, raw=raw, **k))
    relay.time.sleep = lambda s: None
    try:
        relay.close_session(SESSION)
        raised = None
    except BaseException as e:
        raised = e
    finally:
        relay.time.sleep = real_sleep
        for n, fn in saved.items():
            setattr(relay, n, fn)
    return events, len(fetches), raised


def test_close_session():
    print("\n[5] close_session: contract file last, from the EOD capture's own fetch")
    with _Env() as out:
        events, n_fetch, raised = _close()
        kinds = [e[0] for e in events]
        check("no exception", raised is None, repr(raised))
        check("contract step runs last, after revenue/margins/OI/heartbeat/commodity",
              kinds == ["upsert", "margins", "oi", "heartbeat", "commodity", "contracts"], str(kinds))
        check("commodity step ran before any contract file existed",
              ("commodity", "no file yet") in events, str(events))
        check("no MarketWatch call beyond capture_eod's two", n_fetch == 2, str(n_fetch))
        path = os.path.join(out, f"{SESSION}.csv")
        check("file written for the session", os.path.exists(path))
        if os.path.exists(path):
            check("4 contracts in it", len(_read(path)[1]) == 4)

    with _Env():
        def boom(*a, **k):
            raise RuntimeError("contract step exploded")
        events, _, raised = _close(contract_step=boom)
        kinds = [e[0] for e in events]
        check("a raising contract step never reaches the relay loop", raised is None, repr(raised))
        check("revenue, margins, OI, heartbeat, commodity all still ran",
              kinds == ["upsert", "margins", "oi", "heartbeat", "commodity"], str(kinds))


if __name__ == "__main__":
    print("=" * 72)
    print("relay per-contract EOD file tests")
    print("=" * 72)
    for t in (test_rows, test_as_of, test_write, test_capture, test_close_session):
        try:
            t()
        except Exception as e:      # a missing function is a failure, not a crash
            print(f"  FAIL  {t.__name__} raised {type(e).__name__}: {e}")
            FAILED.append(t.__name__)
    print("\n" + "=" * 72)
    if FAILED:
        print(f"{len(FAILED)} FAILED: {', '.join(FAILED)}")
        sys.exit(1)
    print("All tests passed.")
