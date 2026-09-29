#!/usr/bin/env python3
"""
MCX Daily Verification & Backfill — runs at 07:00 IST
Checks previous trading day's data integrity and backfills gaps.

Designed for launchd (macOS) or Task Scheduler (Windows).

Each morning it also reconciles recent relay EOD rows with MCX's historical
detailed report and replaces any that differ by more than 1% (see
reconcile_decision).

Usage:
  python3 scripts/daily_verify.py           # verify last 3 trading days
  python3 scripts/daily_verify.py --days 7  # verify last 7 trading days
"""
import sys, os, json, urllib.request, urllib.error
from datetime import datetime, timedelta, timezone

# Force UTF-8 on Windows
if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.mcx_config import (
    SUPABASE_URL, SUPABASE_ANON_KEY,
    FUTURES_RATE, OPTIONS_RATE,
    now_ist, is_trading_day, get_day_type, session_total, supabase_upsert, supabase_read,
)

# ── Relay EOD reconciliation ─────────────────────────────────────────────────
# A relay EOD row is the market-watch cumulative taken just after the close;
# MCX's historical detailed report is the exchange's own full-day figure. On
# summer days they agree to within 1% (101 relay days, Mar–Sep 2026: median
# -0.01%, worst -0.83%). A bigger gap means the stored row is short or stale,
# e.g. a 23:30 capture on a 23:55 winter day, where the last 25 minutes carry
# ~4% of the day. Such rows are replaced with the historical figure.
RECONCILE_TOLERANCE_PCT = 1.0
# Only rows written by these sources are replaced automatically. The historical
# source is included so a row replaced from a not-yet-final report corrects
# itself on a later morning. Imported Excel rows are left for a human.
REPLACEABLE_SOURCES = {"mcx_relay_eod", "mcx_historical"}
REVIEW_THRESHOLD_PCT = 5.0   # flag, don't replace, other sources beyond this
DEFAULT_DAYS = 3             # T-1's options leg is often unsettled at 07:00


def reconcile_decision(stored, hist, tolerance_pct=RECONCILE_TOLERANCE_PCT):
    """Compare a stored mcx_daily_revenue row with MCX's historical report.

    Returns (action, delta_pct, reason). action is one of:
      "unsettled" — the report isn't final yet (a leg missing or far below
                    what's stored); check again tomorrow
      "ok"        — within tolerance
      "replace"   — outside tolerance and the row may be replaced
      "review"    — well outside tolerance but from a source we don't replace
    delta_pct is (historical - stored) / historical × 100, or None.
    """
    total = hist.get("total_rev_cr") or 0
    if (hist.get("opt_rev_cr") or 0) <= 0 or not (1.0 <= total <= 50.0):
        return "unsettled", None, "options leg missing or total out of range"
    # MCX publishes T-1's report in pieces. A leg under half of what the relay
    # saw is a partial report, not a correction.
    for leg in ("fut_notl_cr", "opt_prem_cr"):
        if (stored.get(leg) or 0) > 0 and (hist.get(leg) or 0) < 0.5 * stored[leg]:
            return "unsettled", None, f"{leg} below half of stored"

    delta_pct = (total - stored["total_rev_cr"]) / total * 100
    if abs(delta_pct) <= tolerance_pct:
        return "ok", delta_pct, ""
    if stored.get("source") in REPLACEABLE_SOURCES:
        return "replace", delta_pct, ""
    if abs(delta_pct) > REVIEW_THRESHOLD_PCT:
        return "review", delta_pct, ""
    return "ok", delta_pct, "outside tolerance but source not replaceable"


def historical_record(date_iso, hist):
    """mcx_daily_revenue row built from a historical-report dict."""
    from datetime import datetime as dt
    return {
        "trading_date": date_iso,
        "day_type": get_day_type(dt.strptime(date_iso, "%Y-%m-%d")),
        "source": "mcx_historical",
        "data_source": "mcx_historical_detailed",
        "is_actual": True,
        **hist,
    }


def replace_with_historical(date_iso, hist):
    """Overwrite the stored row with the historical figure. Returns an error
    string, or None on success. Confirms the write took: an UPDATE that RLS
    blocks can come back as success with zero rows."""
    record = historical_record(date_iso, hist)
    try:
        written = supabase_upsert("mcx_daily_revenue", record, on_conflict="trading_date")
    except Exception as e:
        return str(e)[:200]
    if not written or abs((written[0].get("total_rev_cr") or 0) - record["total_rev_cr"]) > 1e-4:
        return "upsert returned no updated row (write blocked by RLS?)"
    return None


# ── Helpers ──────────────────────────────────────────────────────────────────

_hist_unavailable = None   # set when the MCX historical client can't load


def fetch_mcx_historical(date_iso):
    """Fetch from MCX Historical API (reuse relay logic)."""
    global _hist_unavailable
    try:
        from scripts.mcx_relay import fetch_mcx_historical as _fetch
    except Exception as e:
        # Typically curl_cffi missing because launchd ran /usr/bin/python3.
        # Remember why, so the run reports it instead of silently skipping
        # every cross-check.
        _hist_unavailable = f"{type(e).__name__}: {e}"
        return None
    try:
        return _fetch(date_iso)
    except Exception:
        return None


def get_daily_revenue(date_iso):
    """Read daily revenue record from Supabase."""
    try:
        rows = supabase_read(
            "mcx_daily_revenue",
            f"?trading_date=eq.{date_iso}&limit=1"
        )
        return rows[0] if rows else None
    except Exception:
        return None


def get_snapshot_summary(date_iso):
    """Get first/last snapshot + count for a date."""
    try:
        snaps = supabase_read(
            "mcx_snapshots",
            f"?trading_date=eq.{date_iso}&select=captured_at,total_rev_cr,elapsed_min,session_closed,confidence&order=captured_at.asc"
        )
        if not snaps:
            return None
        last = snaps[-1]
        first = snaps[0]
        return {
            "count": len(snaps),
            "first_at": first["captured_at"],
            "last_at": last["captured_at"],
            "last_rev": last["total_rev_cr"],
            "last_elapsed": last["elapsed_min"],
            "session_closed": last["session_closed"],
            "confidence": last["confidence"],
        }
    except Exception:
        return None


def backfill_from_historical(date_iso):
    """Backfill daily revenue + commodities from Historical API."""
    hist = fetch_mcx_historical(date_iso)
    if not hist or hist["total_rev_cr"] < 0.5 or hist["total_rev_cr"] > 50.0:
        return None

    record = historical_record(date_iso, hist)
    try:
        supabase_upsert("mcx_daily_revenue", record)
        return record
    except Exception as e:
        print(f"  Upsert failed: {e}")
        return None


def _homebrew_python():
    """Interpreter with curl_cffi. The homebrew python is required — launchd
    plists may pin /usr/bin/python3 (Python 3.9, no third-party packages)."""
    import shutil
    candidate_pys = [
        "/opt/homebrew/bin/python3",
        "/opt/homebrew/opt/python@3.14/bin/python3.14",
        shutil.which("python3.14"),
        shutil.which("python3"),
        sys.executable,
    ]
    return next((p for p in candidate_pys if p and os.path.exists(p)), sys.executable)


def _run_script(script_name, args, timeout=60):
    """Run a scripts/*.py helper under the homebrew interpreter."""
    try:
        import subprocess
        project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        result = subprocess.run(
            [_homebrew_python(), os.path.join(project_dir, "scripts", script_name), *args],
            capture_output=True, text=True, timeout=timeout, cwd=project_dir,
        )
        if result.returncode == 0:
            return True
        print(f"  {script_name} error: {result.stderr[:200] or result.stdout[-200:]}")
        return False
    except Exception as e:
        print(f"  {script_name} failed: {e}")
        return False


def run_bhav_refresh(date_iso):
    """Run bhav_refresh for commodity-level data."""
    return _run_script("bhav_refresh.py", [date_iso])


# ── Main Verification ────────────────────────────────────────────────────────

def verify_date(date_iso):
    """Verify a single trading date. Returns dict with status."""
    result = {"date": date_iso, "issues": [], "actions": []}

    # 1. Check daily revenue record
    rev = get_daily_revenue(date_iso)
    if rev:
        result["revenue"] = rev["total_rev_cr"]
        result["source"] = rev["source"]

        # Math check
        fut_r = rev.get("fut_rev_cr", 0)
        opt_r = rev.get("opt_rev_cr", 0)
        if abs((fut_r + opt_r) - rev["total_rev_cr"]) > 0.01:
            result["issues"].append(f"Math mismatch: {fut_r}+{opt_r} != {rev['total_rev_cr']}")

        # Range check
        if rev["total_rev_cr"] < 2.0:
            result["issues"].append(f"Revenue unusually low: {rev['total_rev_cr']:.4f}")
        elif rev["total_rev_cr"] > 30.0:
            result["issues"].append(f"Revenue unusually high: {rev['total_rev_cr']:.4f}")

        # Reconcile with MCX's historical report (see reconcile_decision).
        # The report lags ~1 trading day on the OPTIONS leg: for T-1 it often
        # returns futures only, which is incomplete, not a divergence. Such
        # days are skipped and picked up on a later morning (DEFAULT_DAYS).
        hist = fetch_mcx_historical(date_iso)
        if hist:
            action, delta_pct, why = reconcile_decision(rev, hist)
            if action == "unsettled":
                result["actions"].append(
                    f"Historical not yet settled (got {hist['total_rev_cr']:.2f} Cr, "
                    f"opt={hist.get('opt_rev_cr') or 0:.2f}; {why}) — cross-check skipped"
                )
            elif action == "replace":
                err = replace_with_historical(date_iso, hist)
                if err:
                    result["issues"].append(
                        f"Stored {rev['source']} {rev['total_rev_cr']:.4f} differs from historical "
                        f"{hist['total_rev_cr']:.4f} ({delta_pct:+.2f}%) but replace failed: {err}")
                else:
                    result["revenue"] = hist["total_rev_cr"]
                    result["source"] = "mcx_historical"
                    result["actions"].append(
                        f"Replaced {rev['source']} {rev['total_rev_cr']:.4f} with historical "
                        f"{hist['total_rev_cr']:.4f} Cr ({delta_pct:+.2f}%)")
            elif action == "review":
                result["issues"].append(
                    f"Historical API divergence vs stored ({rev['source']}): "
                    f"{hist['total_rev_cr']:.4f} vs {rev['total_rev_cr']:.4f} ({delta_pct:+.1f}%) — review"
                )
        elif _hist_unavailable:
            result["issues"].append(f"MCX historical client unavailable ({_hist_unavailable}) "
                                    f"— cross-check skipped; run under the homebrew python")
        else:
            # Even T-1 has at least its futures leg by 07:00, so no report at all
            # means the fetch failed. Say so: a silent skip hides a short row.
            result["issues"].append("MCX historical report unavailable — cross-check skipped")
    else:
        result["issues"].append("MISSING from mcx_daily_revenue")
        # Try to backfill
        rec = backfill_from_historical(date_iso)
        if rec:
            result["revenue"] = rec["total_rev_cr"]
            result["source"] = "mcx_historical"
            result["actions"].append(f"Backfilled: {rec['total_rev_cr']:.4f} Cr")
        else:
            result["issues"].append("Historical API also unavailable")

    # 2. Check relay snapshots
    snap = get_snapshot_summary(date_iso)
    if snap:
        result["snapshots"] = snap["count"]
        result["relay_last_elapsed"] = snap["last_elapsed"]
        result["relay_closed"] = snap["session_closed"]

        if snap["count"] < 10:
            result["issues"].append(f"Low snapshot count: {snap['count']}")
        if snap["last_elapsed"] < 800 and not snap["session_closed"]:
            result["issues"].append(
                f"Relay died early: {snap['last_elapsed']}/{session_total(date_iso)} min")
    else:
        result["snapshots"] = 0
        result["issues"].append("No relay snapshots")

    # 3. Ensure commodity data exists
    try:
        comm = supabase_read(
            "mcx_commodity_daily",
            f"?trading_date=eq.{date_iso}&select=trading_date&limit=1"
        )
        if not comm:
            run_bhav_refresh(date_iso)
            result["actions"].append("Backfilled commodity data")
    except Exception:
        pass

    result["status"] = "OK" if not result["issues"] else "ISSUES"
    return result


def main():
    days = DEFAULT_DAYS
    for i, arg in enumerate(sys.argv):
        if arg == "--days" and i + 1 < len(sys.argv):
            try:
                days = int(sys.argv[i + 1])
            except ValueError:
                pass

    t = now_ist()
    print(f"MCX Daily Verification — {t.strftime('%Y-%m-%d %H:%M IST')}")
    print(f"Checking last {days} trading day(s)")
    print("=" * 70)

    all_ok = True
    for i in range(1, days + 30):  # scan up to 30 calendar days to find N trading days
        d = (t - timedelta(days=i)).date()
        if not is_trading_day(d):
            continue

        date_iso = d.strftime("%Y-%m-%d")
        result = verify_date(date_iso)

        status_icon = "OK" if result["status"] == "OK" else "!!"
        rev_str = f"{result.get('revenue', 0):.4f} Cr" if result.get("revenue") else "NO DATA"
        snap_str = f"{result.get('snapshots', 0)} snaps"
        source = result.get("source", "?")

        print(f"\n[{status_icon}] {date_iso}  {rev_str}  ({source})  {snap_str}")

        if result["issues"]:
            for issue in result["issues"]:
                print(f"    ISSUE: {issue}")
        if result["actions"]:
            for action in result["actions"]:
                print(f"    FIXED: {action}")

        if result["status"] != "OK":
            all_ok = False

        days -= 1
        if days <= 0:
            break

    # 4. iCOMDEX index levels (BULLDEX & siblings) — trailing-week self-heal.
    # Data publishes T+1, so the 07:00 run picks up yesterday. Soft-fail:
    # the freshness scanner alerts if mcx_icomdex_daily actually goes stale.
    print("\niCOMDEX index refresh (trailing week)...")
    if _run_script("icomdex_refresh.py", [], timeout=120):
        print("  icomdex_refresh OK")
    else:
        print("  icomdex_refresh FAILED (non-fatal; freshness scanner will flag if stale)")

    # 5. Commodity prices (WTI/NatGas/USDINR via yfinance) — nothing else
    # schedules this, so it only stays fresh through this run. 07:00 IST is
    # past the US close (~02:30 IST), so each morning picks up the completed
    # session. Soft-fail like iCOMDEX.
    print("\nCommodity price refresh (yfinance)...")
    if _run_script("commodity_price_refresh.py", [], timeout=120):
        print("  commodity_price_refresh OK")
    else:
        print("  commodity_price_refresh FAILED (non-fatal; freshness scanner will flag if stale)")

    # 5b. Margins. The Vercel cron for this could never work: it downloads a
    # Sharekhan XLS that never completes from datacenter IPs (60s timeout ->
    # HTTP 500 nightly, and zero rows ever written in its 20:xx UTC window).
    # It has been removed from vercel.json; the relay covers same-day margins
    # from Sharekhan but that file is intermittently stale, which is why gaps
    # kept needing hand-backfilling. Fill them here from the MCXCCL API, which
    # works locally. Soft-fail like the blocks above.
    print("\nMargin refresh (MCXCCL backfill, trailing 3 days)...")
    if _run_script("margin_refresh.py", ["--backfill", "3"], timeout=180):
        print("  margin_refresh OK")
    else:
        print("  margin_refresh FAILED (non-fatal; freshness scanner will flag if stale)")

    # 6. Derived signal tables (models / momentum / commodity_signals).
    # The Vercel crons that own these fire at 14:10-14:20 UTC, but mcx_valuation
    # for the same session is not written until ~20:27 UTC — so those runs can
    # only ever see the PREVIOUS session and the tables sit one day behind from
    # ~02:00 IST until ~19:45 IST daily. By 07:00 IST every input is present
    # (valuation 01:57 IST, revenue 23:30 IST, commodity_daily backfilled above),
    # so recomputing here closes that window. Order matters: models/momentum
    # depend on valuation, commodity_signals on the bhav backfill above.
    # Idempotent upserts, soft-fail like the blocks above.
    print("\nDerived signal refresh (models / momentum / commodity_signals)...")
    for _job in ("models", "momentum", "commodity_signals"):
        if _run_script("run_cron.py", [_job, "recent"], timeout=180):
            print(f"  {_job} OK")
        else:
            print(f"  {_job} FAILED (non-fatal; freshness scanner will flag if stale)")

    print("\n" + "=" * 70)
    if all_ok:
        print("All checks passed.")
    else:
        print("Issues found — see above.")

    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
