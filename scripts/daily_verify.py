#!/usr/bin/env python3
"""
MCX Daily Verification & Backfill — runs at 07:00 IST
Checks previous trading day's data integrity and backfills gaps.

Designed for launchd (macOS) or Task Scheduler (Windows).

Usage:
  python3 scripts/daily_verify.py           # verify yesterday
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
    now_ist, is_trading_day, get_day_type, supabase_upsert, supabase_read,
)

# ── Helpers ──────────────────────────────────────────────────────────────────

def fetch_mcx_historical(date_iso):
    """Fetch from MCX Historical API (reuse relay logic)."""
    try:
        from scripts.mcx_relay import fetch_mcx_historical as _fetch
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
    from datetime import datetime as dt
    hist = fetch_mcx_historical(date_iso)
    if not hist or hist["total_rev_cr"] < 0.5 or hist["total_rev_cr"] > 50.0:
        return None

    d = dt.strptime(date_iso, "%Y-%m-%d")
    record = {
        "trading_date": date_iso,
        "day_type": get_day_type(d),
        "source": "mcx_historical",
        "is_actual": True,
        **hist,
    }
    try:
        supabase_upsert("mcx_daily_revenue", record)
        return record
    except Exception as e:
        print(f"  Upsert failed: {e}")
        return None


def run_bhav_refresh(date_iso):
    """Run bhav_refresh for commodity-level data."""
    try:
        import subprocess
        project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        result = subprocess.run(
            [sys.executable, os.path.join(project_dir, "scripts", "bhav_refresh.py"), date_iso],
            capture_output=True, text=True, timeout=60, cwd=project_dir,
        )
        if result.returncode == 0:
            return True
        else:
            print(f"  bhav_refresh error: {result.stderr[:200]}")
            return False
    except Exception as e:
        print(f"  bhav_refresh failed: {e}")
        return False


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

        # Cross-validate with Historical API
        hist = fetch_mcx_historical(date_iso)
        if hist:
            delta_pct = abs(hist["total_rev_cr"] - rev["total_rev_cr"]) / rev["total_rev_cr"] * 100
            if delta_pct > 5.0:
                result["issues"].append(
                    f"Historical API divergence: {hist['total_rev_cr']:.4f} vs {rev['total_rev_cr']:.4f} ({delta_pct:.1f}%)"
                )
                # Update with Historical API data if it's more reliable
                if rev["source"] != "mcx_historical":
                    backfill_from_historical(date_iso)
                    result["actions"].append(f"Updated from Historical API: {hist['total_rev_cr']:.4f}")
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
            result["issues"].append(f"Relay died early: {snap['last_elapsed']}/870 min")
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
    days = 1
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

    print("\n" + "=" * 70)
    if all_ok:
        print("All checks passed.")
    else:
        print("Issues found — see above.")

    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
