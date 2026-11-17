#!/usr/bin/env python3
"""
Phase 3 (structure) guards: one copy of each shared helper, one dependency
manifest, launchd jobs that log outside /tmp and Dropbox, and a Vercel config
that only lists functions that exist. Offline: reads files, no network.

Run: scripts/run_tests.sh
"""
import json, os, re, sys, plistlib, glob
from datetime import date

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
FAILED = []


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}" + (f"  ({detail})" if detail and not cond else ""))
    if not cond:
        FAILED.append(name)


def src_files(*dirs):
    for d in dirs:
        for p in sorted(glob.glob(os.path.join(ROOT, d, "*.py"))):
            if not os.path.basename(p).startswith("test_"):
                yield os.path.relpath(p, ROOT), open(p, encoding="utf-8").read()


def defs(name, *dirs):
    pat = re.compile(rf"^def {re.escape(name)}\(", re.M)
    return [p for p, s in src_files(*dirs) if pat.search(s)]


def test_single_copies():
    print("\n[1] one copy of each shared helper")
    check("now_ist only in lib/calendar.py", defs("now_ist", "api", "lib", "scripts") == ["lib/calendar.py"],
          str(defs("now_ist", "api", "lib", "scripts")))
    # options_paper keeps an NSE calendar (MCX Ltd the stock); backfill_oi_participants
    # skips MCX's OI non-publication days, which include part-day closures.
    allowed = {"lib/calendar.py", "scripts/options_paper.py", "scripts/backfill_oi_participants.py"}
    got = set(defs("is_trading_day", "api", "lib", "scripts"))
    check("is_trading_day only in lib/calendar.py (+ NSE / OI-publication calendars)", got <= allowed, str(got - allowed))
    # (exchange_dashboard keeps a two-line _quarter_key wrapper returning a tuple.)
    for name in ("fy_label", "_fy_label", "quarter_label", "_prev_quarter", "_yoy_quarter"):
        got = set(defs(name, "api", "lib"))
        check(f"{name} defined only in lib/calendar.py", got <= {"lib/calendar.py"}, str(got))
    for name in ("sb_get", "fetch_all", "fetch_all_paginated", "_supabase_request", "supabase_read",
                 "supabase_read_all"):
        got = set(defs(name, "api", "lib", "scripts"))
        check(f"{name} defined only in lib/supabase.py", got <= {"lib/supabase.py"}, str(got))
    # Direct REST calls: only lib/supabase.py (the price writers' POST -> PATCH
    # fallbacks use its post_rows / patch_rows).
    rest = {p for p, s in src_files("api", "lib", "scripts") if "/rest/v1/" in s}
    check("raw Supabase REST calls only in lib/supabase.py", rest <= {"lib/supabase.py"}, str(rest))
    # Shared constants and the float parser: one definition each, in lib/mcx_config.py.
    for name in ("RANGE_DAYS", "DEFAULT_RANGE", "FINAL_ROW_SOURCES"):
        got = {p for p, s in src_files("api", "lib", "scripts")
               if re.search(rf"^{name}\s*=", s, re.M)}
        check(f"{name} assigned only in lib/mcx_config.py", got == {"lib/mcx_config.py"}, str(got))
    for name in ("_f", "safe_float", "_safe_float"):
        got = set(defs(name, "api", "lib", "scripts"))
        check(f"{name} defined only in lib/mcx_config.py", got <= {"lib/mcx_config.py"}, str(got))
    fee = {p for p, s in src_files("api", "lib", "scripts")
           if re.search(r"^\s*(FUTURES_RATE|OPTIONS_RATE|FUT_RATE|OPT_RATE)\s*=", s, re.M)}
    check("fee rates defined only in lib/mcx_config.py", fee == {"lib/mcx_config.py"}, str(fee))
    imp = {p for p, s in src_files("scripts", "lib")
           if re.search(r"cfreq\.Session\(", s)}
    check("curl_cffi sessions built only in lib/curl_session.py", imp <= {"lib/curl_session.py"}, str(imp))
    handlers = {p for p, s in src_files("api")
                if re.search(r"^class handler\(BaseHTTPRequestHandler\)", s, re.M)}
    check("api handlers use JSONHandler (cron delegates, so it is the exception)",
          handlers <= {"api/cron.py"}, str(handlers))


def test_calendar_labels():
    print("\n[2] lib/calendar FY / quarter labels match the copies they replaced")
    from lib import calendar as cal

    def old_fy(d):
        return f"FY{str(d.year + 1)[-2:]}" if d.month >= 4 else f"FY{str(d.year)[-2:]}"

    def old_q(d):
        m, y = d.month, d.year
        if m <= 3:
            return f"Q4 FY{str(y)[-2:]}"
        if m <= 6:
            return f"Q1 FY{str(y + 1)[-2:]}"
        if m <= 9:
            return f"Q2 FY{str(y + 1)[-2:]}"
        return f"Q3 FY{str(y + 1)[-2:]}"

    def old_prev(q):
        n, fy = int(q[1]), q[3:]
        return f"Q4 FY{str(int(fy[2:]) - 1).zfill(2)}" if n == 1 else f"Q{n - 1} {fy}"

    bad = []
    for y in range(2005, 2035):
        for m in range(1, 13):
            d = date(y, m, 15)
            q = cal.quarter_label(d)
            if cal.fy_label(d) != old_fy(d) or q != old_q(d) or cal.prev_quarter(q) != old_prev(q) \
                    or cal.yoy_quarter(q) != f"{q[:2]} FY{str(int(q[-2:]) - 1).zfill(2)}" \
                    or cal.quarter_start(d) > d or (d - cal.quarter_start(d)).days > 92 \
                    or cal.fy_start(d) > d or (d - cal.fy_start(d)).days > 366:
                bad.append(d)
    check("fy / quarter / prev / yoy / starts agree for 2005-2034", not bad, str(bad[:3]))
    check("29 Sep 2026 is Q2 FY27", cal.quarter_label(date(2026, 9, 29)) == "Q2 FY27")
    check("Q1 FY27 -> Q4 FY26", cal.prev_quarter("Q1 FY27") == "Q4 FY26")
    import lib.mcx_config as m
    import scripts.bhav_refresh as br
    check("mcx_config re-exports the calendar", m.now_ist is cal.now_ist and m.MCX_HOLIDAYS is cal.MCX_HOLIDAYS)
    check("bhav_refresh uses the shared calendar", br.has_session is cal.has_session and br.now_ist is cal.now_ist)


def test_manifest_and_vercel():
    print("\n[3] dependencies and Vercel config")
    check("no requirements.txt (pyproject.toml + uv.lock only)", not os.path.exists(os.path.join(ROOT, "requirements.txt")))
    check("uv.lock present", os.path.exists(os.path.join(ROOT, "uv.lock")))
    py = open(os.path.join(ROOT, "pyproject.toml")).read()
    base = re.search(r"^dependencies = \[(.*?)\]", py, re.S | re.M).group(1)
    check("Vercel deps carry no local-only package",
          not re.search(r"curl_cffi|dhanhq|xlrd|openpyxl|mcxpy|scipy", base), base.strip())
    v = json.load(open(os.path.join(ROOT, "vercel.json")))
    fns = sorted(v["functions"])
    missing = [f for f in fns if not os.path.exists(os.path.join(ROOT, f))]
    check("every vercel.json function exists", not missing, str(missing))
    on_disk = sorted(os.path.relpath(p, ROOT) for p in glob.glob(os.path.join(ROOT, "api", "*.py")))
    check("every api/*.py is configured in vercel.json", on_disk == fns, f"{on_disk} vs {fns}")
    check("within the Hobby plan's 12 functions", len(on_disk) <= 12, str(len(on_disk)))
    check("dead history / mcxlive gone", not {"api/history.py", "api/mcxlive.py"} & set(on_disk))
    ign = open(os.path.join(ROOT, ".vercelignore")).read().split()
    check("scripts/ and research/ never deploy", "scripts/" in ign and "research/" in ign)


def test_launchd():
    print("\n[4] launchd plists")
    plists = sorted(glob.glob(os.path.join(ROOT, "scripts", "launchd", "com.mcx.*.plist")))
    stray = glob.glob(os.path.join(ROOT, "scripts", "com.mcx.*.plist"))
    check("all plists live in scripts/launchd/", plists and not stray, str(stray))
    for p in plists:
        d = plistlib.load(open(p, "rb"))
        name = os.path.basename(p)
        logs = [d.get("StandardOutPath", ""), d.get("StandardErrorPath", "")]
        check(f"{name}: logs in ~/Library/Logs/mcx",
              all(l.startswith("/Users/pranayagarwal/Library/Logs/mcx/") for l in logs), str(logs))
        args = " ".join(d.get("ProgramArguments", []))
        check(f"{name}: no pinned interpreter or --with list",
              "python3.14" not in args and "--with " not in args, args)
    check("commodity-oi job removed (daily_verify step 5d)",
          not os.path.exists(os.path.join(ROOT, "scripts", "launchd", "com.mcx.commodity-oi.plist")))


if __name__ == "__main__":
    print("=" * 72)
    print("Phase 3 structure tests")
    print("=" * 72)
    for t in (test_single_copies, test_calendar_labels, test_manifest_and_vercel, test_launchd):
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
