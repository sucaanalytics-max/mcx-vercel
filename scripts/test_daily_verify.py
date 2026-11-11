#!/usr/bin/env python3
"""
Regression tests for daily_verify's commodity check (step 3) and interpreter
choice, after the 10 Nov 2026 failure:
  - A Homebrew upgrade repointed /opt/homebrew/bin/python3 to a fresh 3.15
    with no packages. _homebrew_python() returned the first interpreter that
    EXISTED, so bhav_refresh died on `import curl_cffi`...
  - ...and step 3 still recorded "Backfilled commodity data" (it never looked
    at run_bhav_refresh's result, and wrapped the check in `except: pass`).
    The run printed "All checks passed" with 9 Nov missing from
    mcx_commodity_daily.

Offline: Supabase, MCX and subprocesses are faked.
Run:  python3 scripts/test_daily_verify.py
"""
import sys, os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import scripts.daily_verify as dv

FAILED = []


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}" + (f"  — {detail}" if detail else ""))
    if not cond:
        FAILED.append(name)


GOOD_REV = {"trading_date": "2026-11-09", "total_rev_cr": 11.933, "fut_rev_cr": 2.0302,
            "opt_rev_cr": 9.9027, "fut_notl_cr": 48339, "opt_prem_cr": 11845, "source": "mcx_relay_eod"}
GOOD_SNAP = {"count": 59, "last_elapsed": 870, "session_closed": True,
             "first_at": "", "last_at": "", "last_rev": 11.93, "confidence": "CERTAIN"}


def _verify(commodity_reads, bhav_result, read_error=None):
    """verify_date('2026-11-09') with everything but step 3 green.
    commodity_reads: successive supabase_read results for mcx_commodity_daily."""
    reads, bhav_calls = list(commodity_reads), []
    saved = (dv.get_daily_revenue, dv.fetch_mcx_historical, dv.get_snapshot_summary,
             dv.supabase_read, dv._run_script_rc)
    dv.get_daily_revenue = lambda d: dict(GOOD_REV)
    dv.fetch_mcx_historical = lambda d: {"total_rev_cr": 11.93, "opt_rev_cr": 9.9,
                                         "fut_notl_cr": 48339, "opt_prem_cr": 11845}
    dv.get_snapshot_summary = lambda d: dict(GOOD_SNAP)

    def fake_read(table, params="", **k):
        assert table == "mcx_commodity_daily", table
        if read_error:
            raise read_error
        return reads.pop(0)
    dv.supabase_read = fake_read
    dv._run_script_rc = lambda name, args, timeout=60: (bhav_calls.append((name, args)), bhav_result)[1]
    try:
        return dv.verify_date("2026-11-09"), bhav_calls
    finally:
        (dv.get_daily_revenue, dv.fetch_mcx_historical, dv.get_snapshot_summary,
         dv.supabase_read, dv._run_script_rc) = saved


ROWS = [{"contracts": 442068}] * 42


def test_failed_backfill_is_an_issue():
    print("\n[1] A failed commodity backfill is an ISSUE, never 'Backfilled'")
    cases = [
        ("bhav_refresh crashed (ModuleNotFoundError)", (1, "ModuleNotFoundError: No module named 'curl_cffi'"), "exit 1"),
        ("report not complete yet", (3, "  2026-11-09: incomplete — options premium missing"), "not complete"),
        ("bhav_refresh timed out", (None, "TimeoutExpired: timed out after 120 seconds"), "did not finish"),
        ("exit 0 but no rows landed", (0, "  2026-11-09: written"), "exit 0"),
    ]
    for name, bhav, want in cases:
        r, calls = _verify([[], []], bhav)
        check(f"{name}: status ISSUES", r["status"] == "ISSUES", str(r["issues"]))
        check(f"{name}: no 'Backfilled' action",
              not any("Backfilled" in a for a in r["actions"]), str(r["actions"]))
        check(f"{name}: issue says why", any(want in i for i in r["issues"]), str(r["issues"]))
        check(f"{name}: ran bhav_refresh --commodity <date>",
              calls == [("bhav_refresh.py", ["--commodity", "2026-11-09"])], str(calls))


def test_backfill_reported_only_when_rows_exist():
    print("\n[2] Backfill is reported once the rows exist")
    r, _ = _verify([[], ROWS], (0, "written"))
    check("status OK", r["status"] == "OK", str(r["issues"]))
    check("action names rows and contracts",
          any("42 rows" in a and "18,566,856" in a for a in r["actions"]), str(r["actions"]))
    r, calls = _verify([ROWS], (1, "should not run"))
    check("already present: no backfill, no action", r["status"] == "OK" and not calls and not r["actions"],
          f"{r['actions']} {calls}")


def test_read_error_is_an_issue():
    print("\n[3] A Supabase read error is an ISSUE, not swallowed")
    r, _ = _verify([], (0, ""), read_error=RuntimeError("HTTP 503"))
    check("status ISSUES", r["status"] == "ISSUES" and any("read failed" in i for i in r["issues"]),
          str(r["issues"]))


def test_presence_ignores_oi_stubs():
    print("\n[4] Presence check counts only real breakdown rows (turnover_cr > 0)")
    seen = []
    saved = dv.supabase_read
    dv.supabase_read = lambda table, params="", **k: (seen.append(params), [])[1]
    try:
        dv.commodity_coverage("2026-11-09")
    finally:
        dv.supabase_read = saved
    check("query filters turnover_cr=gt.0", seen and "turnover_cr=gt.0" in seen[0], str(seen))


def test_main_exits_nonzero():
    print("\n[5] main() exits non-zero when a day still has no commodity rows")
    saved = (dv.dates_to_verify, dv.verify_date, dv._run_script)
    dv.dates_to_verify = lambda t, days: ["2026-11-09"]
    dv.verify_date = lambda d: {"date": d, "status": "ISSUES", "revenue": 11.93, "source": "mcx_relay_eod",
                                "snapshots": 59, "actions": [],
                                "issues": ["No commodity breakdown in mcx_commodity_daily (bhav_refresh exit 1: ...)"]}
    dv._run_script = lambda *a, **k: True
    try:
        rc = dv.main()
    finally:
        dv.dates_to_verify, dv.verify_date, dv._run_script = saved
    check("exit code 1", rc == 1, f"rc={rc}")


def test_interpreter_needs_packages():
    print("\n[6] Helper interpreter = first that can import curl_cffi, not first that exists")
    dv._PY_HAS_DEPS.clear()
    saved_exists, saved_has = dv.os.path.exists, dv._py_has_deps
    saved_exe = dv.sys.executable
    bare = "/opt/homebrew/bin/python3"          # the fresh 3.15, no packages
    good = "/opt/homebrew/bin/python3.14"
    dv.sys.executable = "/usr/bin/python3"      # launchd's 3.9, no packages
    dv.os.path.exists = lambda p: p in (bare, good, "/usr/bin/python3")
    dv._py_has_deps = lambda p, probe=None: p == good
    try:
        got = dv._homebrew_python()
    finally:
        dv.os.path.exists, dv._py_has_deps, dv.sys.executable = saved_exists, saved_has, saved_exe
    check("skips the package-less interpreters", got == good, got)

    calls = []
    probe = lambda cmd, **k: (calls.append(cmd), type("R", (), {"returncode": 1})())[1]
    dv._PY_HAS_DEPS.clear()
    check("probe failure -> no deps", dv._py_has_deps("/x/python", probe=probe) is False)
    check("probe imports curl_cffi", calls and "curl_cffi" in calls[0][-1], str(calls))
    dv._PY_HAS_DEPS.clear()


if __name__ == "__main__":
    print("=" * 72)
    print("daily_verify tests")
    print("=" * 72)
    test_failed_backfill_is_an_issue()
    test_backfill_reported_only_when_rows_exist()
    test_read_error_is_an_issue()
    test_presence_ignores_oi_stubs()
    test_main_exits_nonzero()
    test_interpreter_needs_packages()
    print("\n" + "=" * 72)
    if FAILED:
        print(f"{len(FAILED)} FAILED: {', '.join(FAILED)}")
        sys.exit(1)
    print("All tests passed.")
