#!/usr/bin/env python3
"""
Regression tests for the Phase 1 hardening fixes (audit 2026-11-17).

  1. /api/refresh?health=1 reports the snapshot's real age on a UTC host (Vercel);
     it used to add 330 min, so the relay always looked dead in market hours.
  2. CORS echoes only this project's previews / localhost, not evil.vercel.app.
  3. int_param clamps and falls back instead of raising on ?days=abc.
  4. cache_header never lets a {success: false} body be cached.
  5. run_cron.py exits non-zero when the job reports success: false.
  6. close_session still runs margins / OI / commodity when the EOD write raises,
     and sends no eod_done heartbeat for a session whose revenue was not written.
  7. sb_upsert counts 409 batches as skipped instead of silently "upserted".

Pure: Supabase, network and subprocess calls are stubbed.
"""
import os, sys, time, json, subprocess
from datetime import datetime, timedelta, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

FAILED = []


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}" + (f"  ({detail})" if detail and not cond else ""))
    if not cond:
        FAILED.append(name)


def test_health_age_on_utc_host():
    print("\n[1] health age is real minutes on a UTC host")
    old_tz = os.environ.get("TZ")
    os.environ["TZ"] = "UTC"
    time.tzset()
    try:
        import importlib
        import api.refresh as refresh
        importlib.reload(refresh)
        cap = (datetime.now(timezone.utc) - timedelta(minutes=4)).isoformat()
        snap = {"captured_at": cap, "trading_date": "2026-11-17", "total_rev_cr": 20.0, "elapsed_min": 70}

        def fake_read(table, params="", timeout=10):
            return [snap] if table == "mcx_snapshots" else []
        refresh.supabase_read = fake_read
        sent = {}
        h = refresh.handler.__new__(refresh.handler)
        h.send_json = lambda data, status=200: sent.update(data=data, status=status)
        h._health_check()
        age = sent["data"]["last_snapshot"]["age_min"]
        check("age is ~4 min, not ~334", 3.5 <= age <= 5.0, str(age))
    finally:
        if old_tz is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = old_tz
        time.tzset()


def test_cors():
    print("\n[2] CORS origin matching")
    from lib.mcx_config import make_cors_headers, ALLOWED_ORIGINS
    allow = lambda o: make_cors_headers(o)["Access-Control-Allow-Origin"]
    prod = ALLOWED_ORIGINS[0]
    check("production origin echoed", allow(prod) == prod)
    for good in ("https://mcx-vercel-git-fix-abc-team.vercel.app", "http://localhost:8765",
                 "http://127.0.0.1:3000"):
        check(f"echoes {good}", allow(good) == good, allow(good))
    for bad in ("https://evil.vercel.app", "https://evil-localhost.com",
                "https://mcx-vercel.vercel.app.evil.com", "http://mcx-vercel-x.vercel.app",
                "javascript:alert(1)", "https://localhost.evil.com"):
        check(f"rejects {bad}", allow(bad) == prod, allow(bad))


def test_int_param():
    print("\n[3] int_param")
    from lib.mcx_config import int_param
    check("default when missing", int_param({}, "days", 30, 5, 90) == 30)
    check("garbage falls back", int_param({"days": ["abc"]}, "days", 30, 5, 90) == 30)
    check("clamped high", int_param({"days": ["252"]}, "days", 30, 5, 90) == 90)
    check("clamped low", int_param({"days": ["-3"]}, "days", 30, 5, 90) == 5)
    check("passes valid", int_param({"days": ["60"]}, "days", 30, 5, 90) == 60)


def test_cache_header():
    print("\n[4] cache_header")
    from lib.mcx_config import cache_header
    ok = "public, max-age=60"
    check("success payload keeps cache", cache_header({"success": True}, ok) == ok)
    check("payload without success key keeps cache", cache_header({"rows": []}, ok) == ok)
    check("success:false is no-store", cache_header({"success": False}, ok) == "no-store")


def test_run_cron_exit_code():
    print("\n[5] run_cron.py exit code")
    import run_cron
    real = run_cron._run
    try:
        for result, want in (({"success": False, "error": "x"}, 1), ({"success": True}, 0)):
            run_cron._run = lambda job, mode, r=result: r
            sys.argv = ["run_cron.py", "models", "recent"]
            try:
                run_cron.main()
                code = 0
            except SystemExit as e:
                code = e.code or 0
            check(f"success={result['success']} -> exit {want}", code == want, str(code))
    finally:
        run_cron._run = real


def test_close_session_survives_eod_failure():
    print("\n[6] close_session when the EOD write raises")
    import mcx_relay as relay
    events = []
    names = ("capture_eod", "refresh_margins_local", "refresh_oi_participants_local",
             "send_heartbeat", "capture_commodity_breakdown")
    saved = {n: getattr(relay, n) for n in names}

    def boom(d):
        raise Exception("Supabase upsert 409: duplicate key value violates unique constraint")
    relay.capture_eod = boom
    relay.refresh_margins_local = lambda: events.append("margins")
    relay.refresh_oi_participants_local = lambda: events.append("oi")
    relay.send_heartbeat = lambda status, **k: events.append(f"hb:{status}")
    relay.capture_commodity_breakdown = lambda d, **k: events.append("commodity")
    try:
        relay.close_session("2026-11-10")
        raised = None
    except BaseException as e:
        raised = e
    finally:
        for n, fn in saved.items():
            setattr(relay, n, fn)
    check("does not raise", raised is None, repr(raised))
    check("margins, OI and commodity still run",
          all(x in events for x in ("margins", "oi", "commodity")), str(events))
    check("reports the error, no eod_done", "hb:error" in events and "hb:eod_done" not in events, str(events))


def test_sb_upsert_counts_409():
    print("\n[7] sb_upsert counts 409 batches as skipped")
    import urllib.error, io
    import lib.cron_margins as cm
    calls = {"n": 0}

    class _Resp:
        def __enter__(self): return self
        def __exit__(self, *a): return False

    def fake_urlopen(req, timeout=15):
        calls["n"] += 1
        if calls["n"] == 1:
            raise urllib.error.HTTPError(req.full_url, 409, "Conflict", {}, io.BytesIO(b"dup"))
        return _Resp()
    real = cm.urllib.request.urlopen
    cm.urllib.request.urlopen = fake_urlopen
    try:
        stats = {}
        errs = cm.sb_upsert("mcx_margin_daily", [{"i": i} for i in range(80)], stats=stats)
    finally:
        cm.urllib.request.urlopen = real
    check("no errors reported", errs == [], str(errs))
    check("first 50-row batch counted as skipped", stats.get("skipped") == 50, str(stats))


if __name__ == "__main__":
    print("=" * 72)
    print("hardening regression tests")
    print("=" * 72)
    test_health_age_on_utc_host()
    test_cors()
    test_int_param()
    test_cache_header()
    test_run_cron_exit_code()
    test_close_session_survives_eod_failure()
    test_sb_upsert_counts_409()
    print("\n" + "=" * 72)
    if FAILED:
        print(f"{len(FAILED)} FAILED: {', '.join(FAILED)}")
        sys.exit(1)
    print("All tests passed.")
