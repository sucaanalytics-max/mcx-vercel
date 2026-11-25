#!/usr/bin/env python3
"""
Regression tests for the relay's system-sleep detection (scripts/mcx_relay.py).

On battery with the lid closed the Mac sleeps whatever caffeinate says (17 Nov
2026 lost 18:19-23:55; 18-20 Nov lost 25-50 min each). The relay compares the
wall clock with CLOCK_UPTIME_RAW, which stops while the Mac sleeps:
  1. asleep_seconds() is the wall-clock gain over uptime, never negative.
  2. sleep_report() counts only time inside a session, across midnight too.
  3. _check_sleep() ignores short gaps; a long one exits(1) for launchd, and
     only one that cost snapshots writes an error heartbeat and notifies.
  4. _notify_sleep() notifies at most once per SLEEP_NOTIFY_EVERY, never raises.
  5. The stall watchdog ages on the uptime clock, so a sleep is not a stall.

Offline: clocks, Supabase heartbeat and osascript are faked.
Run:  python3 scripts/test_relay_sleep.py
"""
import sys, os, tempfile, time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import scripts.mcx_relay as relay

FAILED = []
IST = relay._IST


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}" + (f"  — {detail}" if detail else ""))
    if not cond:
        FAILED.append(name)


def ist(day, hh, mm):
    return datetime(2026, 11, day, hh, mm, tzinfo=IST)


def test_asleep_seconds():
    print("asleep_seconds")
    check("awake: wall and uptime advance together", relay.asleep_seconds(0, 0, 900, 900) == 0)
    check("slept 20 min", relay.asleep_seconds(0, 0, 2100, 900) == 1200)
    check("never negative (wall clock stepped back)", relay.asleep_seconds(0, 0, 800, 900) == 0)


def test_sleep_report():
    print("sleep_report")
    # 17 Nov 2026 (Tue): full session 09:00-23:55 (US on standard time)
    s17 = relay.sessions_between(ist(17, 18, 19), ist(17, 23, 55))
    check("one session on a same-day stretch", len(s17) == 1)
    check("17 Nov session is 09:00-23:55",
          s17[0] == (ist(17, 9, 0), ist(17, 23, 55)), str(s17))
    msg, missed = relay.sleep_report(ist(17, 18, 19), ist(17, 23, 55), 336 * 60, s17)
    check("17 Nov 18:19-23:55 asleep: 22 snapshots missed", missed == 22, msg)
    check("message names the stretch and the advice",
          "18:19" in msg and "23:55" in msg and "plugged in" in msg, msg)

    night = relay.sessions_between(ist(18, 0, 30), ist(18, 8, 0))
    _, missed = relay.sleep_report(ist(18, 0, 30), ist(18, 8, 0), 450 * 60, night)
    check("asleep before the open: nothing missed", missed == 0)

    # 23:00 on the 17th to 10:00 on the 18th: 55 min of evening + 60 of morning
    both = relay.sessions_between(ist(17, 23, 0), ist(18, 10, 0))
    check("a stretch over midnight covers both days", len(both) == 2)
    msg, missed = relay.sleep_report(ist(17, 23, 0), ist(18, 10, 0), 660 * 60, both)
    check("asleep over midnight: evening and morning both count (115 min -> 7)",
          missed == 7, msg)
    check("a multi-day message shows the dates", "17 Nov" in msg and "18 Nov" in msg, msg)

    _, missed = relay.sleep_report(ist(17, 12, 0), ist(17, 12, 14), 14 * 60, s17)
    check("under one interval asleep: none missed", missed == 0)

    sat = relay.sessions_between(ist(21, 10, 0), ist(21, 12, 0))   # Saturday
    check("a Saturday has no session (open == close)", sat[0][0] == sat[0][1], str(sat))
    _, missed = relay.sleep_report(ist(21, 10, 0), ist(21, 12, 0), 7200, sat)
    check("asleep on a Saturday: nothing missed", missed == 0)

    week = relay.sessions_between(ist(1, 0, 0), ist(30, 0, 0))
    check("sessions_between stops at max_days", len(week) == 7)


def _run_check(wall0, up0, wall1, up1):
    """_check_sleep() with faked clocks; returns (exit code or None, heartbeats, notices)."""
    beats, notices = [], []
    saved = (relay._sleep_mark, relay.time.time, relay._uptime,
             relay.send_heartbeat, relay._notify_sleep)
    relay._sleep_mark = (wall0, up0)
    relay.time.time = lambda: wall1
    relay._uptime = lambda: up1
    relay.send_heartbeat = lambda *a, **k: beats.append((a, k))
    relay._notify_sleep = notices.append
    code = None
    try:
        relay._check_sleep()
    except SystemExit as e:
        code = e.code
    finally:
        mark = relay._sleep_mark
        (relay._sleep_mark, relay.time.time, relay._uptime,
         relay.send_heartbeat, relay._notify_sleep) = saved
    return code, beats, notices, mark


def test_check_sleep():
    print("_check_sleep")
    t0 = ist(17, 18, 19).timestamp()
    code, beats, notices, mark = _run_check(t0, 100.0, t0 + 900, 1000.0)
    check("awake 15 min: no exit", code is None)
    check("the mark moves to now", mark == (t0 + 900, 1000.0))

    code, *_ = _run_check(t0, 100.0, t0 + 900 + relay.SLEEP_SLACK, 1000.0)
    check("asleep exactly SLEEP_SLACK: still no exit", code is None)

    code, beats, notices, _ = _run_check(t0, 100.0, t0 + 336 * 60, 160.0)
    check("asleep 5.6 h in session: exit(1)", code == 1)
    check("...with an error heartbeat",
          len(beats) == 1 and beats[0][0][0] == "error"
          and "system sleep" in beats[0][1].get("error", ""), str(beats))
    check("...and one notification", len(notices) == 1 and "snapshots missed" in notices[0],
          str(notices))

    t1 = ist(18, 0, 30).timestamp()
    code, beats, notices, _ = _run_check(t1, 100.0, t1 + 450 * 60, 160.0)
    check("asleep overnight, out of session: exit(1)", code == 1)
    check("...but no heartbeat or notification", not beats and not notices,
          f"{beats} {notices}")


def test_notify_rate_limit():
    print("_notify_sleep")
    calls = []
    saved_stamp, saved_run = relay.SLEEP_NOTICE_STAMP, None
    import subprocess
    saved_run = subprocess.run
    with tempfile.TemporaryDirectory() as d:
        relay.SLEEP_NOTICE_STAMP = os.path.join(d, "sub", "stamp")
        subprocess.run = lambda cmd, **kw: calls.append(cmd)
        try:
            relay._notify_sleep("first")
            check("first notice goes out", len(calls) == 1)
            check("text is an argument, not AppleScript source",
                  calls and calls[0][-2] == "first" and "first" not in " ".join(calls[0][:-2]),
                  str(calls))
            check("the stamp is written", os.path.exists(relay.SLEEP_NOTICE_STAMP))
            relay._notify_sleep("second")
            check("a second within the half hour is dropped", len(calls) == 1)
            old = time.time() - relay.SLEEP_NOTIFY_EVERY - 5
            os.utime(relay.SLEEP_NOTICE_STAMP, (old, old))
            relay._notify_sleep("third")
            check("after the half hour it goes out again", len(calls) == 2)

            def boom(cmd, **kw):
                raise OSError("no osascript")
            subprocess.run = boom
            os.utime(relay.SLEEP_NOTICE_STAMP, (old, old))
            try:
                relay._notify_sleep("fourth")
                check("a failing osascript does not raise", True)
            except Exception as e:
                check("a failing osascript does not raise", False, repr(e))
        finally:
            relay.SLEEP_NOTICE_STAMP = saved_stamp
            subprocess.run = saved_run


def test_stall_watchdog_uses_uptime():
    print("stall watchdog")
    saved = (relay._uptime, relay.time.time)
    relay._uptime = lambda: 12345.0
    relay.time.time = lambda: 9.9e9
    try:
        relay._mark_progress()
        check("progress is stamped on the uptime clock", relay._last_progress == 12345.0,
              str(relay._last_progress))
    finally:
        relay._uptime, relay.time.time = saved
        relay._mark_progress()


def test_watchful_sleep_checks():
    print("_watchful_sleep")
    seen = []
    saved = (relay.time.sleep, relay._check_sleep)
    relay.time.sleep = lambda s: seen.append(("sleep", s))
    relay._check_sleep = lambda: seen.append(("check",))
    try:
        relay._watchful_sleep(900)
    finally:
        relay.time.sleep, relay._check_sleep = saved
    check("sleeps, then checks", seen == [("sleep", 900), ("check",)], str(seen))


if __name__ == "__main__":
    test_asleep_seconds()
    test_sleep_report()
    test_check_sleep()
    test_notify_rate_limit()
    test_stall_watchdog_uses_uptime()
    test_watchful_sleep_checks()
    print(f"\n{'ALL PASS' if not FAILED else f'{len(FAILED)} FAILED: {FAILED}'}")
    sys.exit(1 if FAILED else 0)
