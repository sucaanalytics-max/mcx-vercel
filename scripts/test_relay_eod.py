#!/usr/bin/env python3
"""
Regression tests for the relay's close-of-session path (scripts/mcx_relay.py).

After the close the relay now also tries to write the day's per-commodity
breakdown (mcx_commodity_daily) by running `bhav_refresh.py --commodity
<session date>` in a subprocess. MCX's report usually isn't complete then
(10 Nov 2026: empty at 23:48, complete by 06:31), so that attempt must never
cost the revenue row:
  1. capture_eod() still writes mcx_daily_revenue when the commodity fetch
     times out, errors, or can't even start, and it writes FIRST.
  2. capture_commodity_breakdown() never raises and stays inside its timeout.
  3. The session date is passed explicitly (a 23:55 close leaves minutes
     before now_ist() rolls over).

Offline: market watch, Supabase and the subprocess are all faked.
Run:  python3 scripts/test_relay_eod.py
"""
import sys, os, subprocess

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import scripts.mcx_relay as relay

FAILED = []


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}" + (f"  — {detail}" if detail else ""))
    if not cond:
        FAILED.append(name)


class _Res:
    def __init__(self, rc, out="", err=""):
        self.returncode, self.stdout, self.stderr = rc, out, err


def _fake_run(behaviour, calls):
    def run(cmd, **kw):
        calls.append((cmd, kw))
        if behaviour == "timeout":
            raise subprocess.TimeoutExpired(cmd, kw.get("timeout"))
        if behaviour == "oserror":
            raise OSError("exec format error")
        if behaviour == "not_ready":
            return _Res(3, "  2026-11-10: incomplete — 42 rows, options premium missing (0/10)\n")
        if behaviour == "error":
            return _Res(1, "", "curl: (28) Operation timed out")
        return _Res(0, "  2026-11-10: written — 42 rows, 17,843,704 contracts\n")
    return run


def _run_close(behaviour):
    """close_session() with every side effect faked. Returns (events, calls)."""
    events, calls = [], []
    saved = {name: getattr(relay, name) for name in (
        "_get_mw_session", "fetch_market_watch", "extract_notionals", "supabase_upsert",
        "refresh_margins_local", "refresh_oi_participants_local", "send_heartbeat",
        "capture_commodity_breakdown")}
    real_sleep = relay.time.sleep
    real_ccb = relay.capture_commodity_breakdown

    relay._get_mw_session = lambda *a, **k: object()
    relay.fetch_market_watch = lambda *a, **k: {}
    # Rs 38,797 Cr futures notional, Rs 10,476 Cr premium (10 Nov close).
    relay.extract_notionals = lambda raw: (38797.0, 0.0, 10476.0, [1] * 91, [1] * 867)
    relay.supabase_upsert = lambda table, rec, **k: (events.append(("upsert", table, rec)), [rec])[1]
    relay.refresh_margins_local = lambda: events.append(("margins",))
    relay.refresh_oi_participants_local = lambda: events.append(("oi",))
    relay.send_heartbeat = lambda status, **k: events.append(("heartbeat", status))
    relay.capture_commodity_breakdown = lambda d, **k: (
        events.append(("commodity", d)), real_ccb(d, run=_fake_run(behaviour, calls)))[1]
    relay.time.sleep = lambda s: None
    try:
        relay.close_session("2026-11-10")
        raised = None
    except BaseException as e:      # SystemExit included: the relay must not die here
        raised = e
    finally:
        relay.time.sleep = real_sleep
        for name, fn in saved.items():
            setattr(relay, name, fn)
    return events, calls, raised


def test_revenue_survives_commodity_failure():
    print("\n[1] Revenue is written whatever the commodity fetch does")
    for behaviour in ("timeout", "error", "oserror", "not_ready", "ok"):
        events, calls, raised = _run_close(behaviour)
        upserts = [e for e in events if e[0] == "upsert" and e[1] == "mcx_daily_revenue"]
        kinds = [e[0] for e in events]
        check(f"{behaviour}: no exception", raised is None, repr(raised))
        check(f"{behaviour}: mcx_daily_revenue written once", len(upserts) == 1,
              f"{len(upserts)} upserts")
        if upserts:
            rec = upserts[0][2]
            check(f"{behaviour}: revenue row is the relay's EOD",
                  rec["source"] == "mcx_relay_eod" and rec["trading_date"] == "2026-11-10"
                  and abs(rec["total_rev_cr"] - 10.3873) < 1e-3, str(rec["total_rev_cr"]))
        check(f"{behaviour}: revenue, margins, OI and heartbeat all precede the commodity fetch",
              kinds == ["upsert", "margins", "oi", "heartbeat", "commodity"], str(kinds))


def test_commodity_call_shape():
    print("\n[2] Commodity fetch: explicit session date, bounded subprocess")
    calls = []
    rc = relay.capture_commodity_breakdown("2026-11-10", run=_fake_run("ok", calls))
    cmd, kw = calls[0]
    check("passes the session date explicitly", cmd[-2:] == ["--commodity", "2026-11-10"], str(cmd[-2:]))
    check("runs bhav_refresh.py with this interpreter",
          cmd[0] == sys.executable and cmd[1].endswith("bhav_refresh.py"), str(cmd[:2]))
    check("has a timeout inside the stall watchdog",
          0 < kw.get("timeout", 0) < relay.STALL_LIMIT, str(kw.get("timeout")))
    check("returns the exit code", rc == 0)
    check("timeout returns None, no raise",
          relay.capture_commodity_breakdown("2026-11-10", run=_fake_run("timeout", [])) is None)
    check("not-ready returns 3",
          relay.capture_commodity_breakdown("2026-11-10", run=_fake_run("not_ready", [])) == 3)


def test_clock_fallback_path():
    print("\n[3] Clock-fallback close also writes revenue first, no eod_done heartbeat")
    events = []
    saved = (relay.capture_eod, relay.refresh_margins_local, relay.refresh_oi_participants_local,
             relay.send_heartbeat, relay.capture_commodity_breakdown)
    relay.capture_eod = lambda d: events.append(("eod", d))
    relay.refresh_margins_local = lambda: events.append(("margins",))
    relay.refresh_oi_participants_local = lambda: events.append(("oi",))
    relay.send_heartbeat = lambda status, **k: events.append(("heartbeat", status))
    relay.capture_commodity_breakdown = lambda d, **k: (_ for _ in ()).throw(RuntimeError("boom"))
    try:
        relay.close_session("2026-11-10", clock_fallback=True)
        raised = None
    except BaseException as e:
        raised = e
    finally:
        (relay.capture_eod, relay.refresh_margins_local, relay.refresh_oi_participants_local,
         relay.send_heartbeat, relay.capture_commodity_breakdown) = saved
    check("no exception even if the commodity step itself raises", raised is None, repr(raised))
    check("EOD captured for the session date", events[:1] == [("eod", "2026-11-10")], str(events))
    check("no eod_done heartbeat on the fallback path",
          not any(e[0] == "heartbeat" for e in events), str(events))


if __name__ == "__main__":
    print("=" * 72)
    print("relay close-of-session tests")
    print("=" * 72)
    test_revenue_survives_commodity_failure()
    test_commodity_call_shape()
    test_clock_fallback_path()
    print("\n" + "=" * 72)
    if FAILED:
        print(f"{len(FAILED)} FAILED: {', '.join(FAILED)}")
        sys.exit(1)
    print("All tests passed.")
