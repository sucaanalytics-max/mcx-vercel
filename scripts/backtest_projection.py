#!/usr/bin/env python3
"""
MCX Projection Backtest Harness

For each historical trading day, finds snapshots at 25/50/75% elapsed
and compares the projection to the day's final EOD revenue. Outputs
a CSV log and a stdout summary table.

Usage:
  python3 scripts/backtest_projection.py            # last 30 trading days
  python3 scripts/backtest_projection.py --days 60  # last 60 trading days
  python3 scripts/backtest_projection.py --recompute  # also recompute the
      projection from scratch using current model code (vs the proj_total_rev
      stored in the snapshot, which reflects the model at write-time)
"""
import sys, os, csv, statistics
from datetime import datetime, timedelta
from collections import defaultdict

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
    SESSION_TOTAL,
    now_ist, is_trading_day, get_day_type,
    supabase_read, supabase_read_all,
    project_full_day, calc_revenue,
)


# ── Helpers ────────────────────────────────────────────────────────────────

def fetch_eod(date_iso):
    rows = supabase_read(
        "mcx_daily_revenue",
        f"?trading_date=eq.{date_iso}&select=trading_date,total_rev_cr,fut_notl_cr,opt_prem_cr,source,day_type"
    )
    return rows[0] if rows else None


def fetch_snaps(date_iso):
    return supabase_read_all(
        "mcx_snapshots",
        f"?trading_date=eq.{date_iso}&select=captured_at,elapsed_min,fut_notl_cr,opt_prem_cr,total_rev_cr,proj_total_rev,confidence,day_type,session_closed&order=elapsed_min.asc"
    )


def find_at_elapsed(snaps, target_min, tolerance=20):
    """Find the snapshot closest to target_min elapsed, within tolerance."""
    if not snaps:
        return None
    best = None
    best_dist = float("inf")
    for s in snaps:
        d = abs(s["elapsed_min"] - target_min)
        if d < best_dist and d <= tolerance:
            best = s
            best_dist = d
    return best


def median(xs):
    return statistics.median(xs) if xs else float("nan")


def percentile(xs, p):
    if not xs:
        return float("nan")
    xs_sorted = sorted(xs)
    k = (len(xs_sorted) - 1) * (p / 100)
    f = int(k)
    c = min(f + 1, len(xs_sorted) - 1)
    return xs_sorted[f] + (xs_sorted[c] - xs_sorted[f]) * (k - f)


# ── Backtest ───────────────────────────────────────────────────────────────

ELAPSED_TARGETS = [
    (218, "25%"),  # 870 * 0.25 = 217.5
    (435, "50%"),  # 870 * 0.50
    (652, "75%"),  # 870 * 0.75
]


def backtest(days=30, recompute=False):
    t = now_ist()
    rows = []  # CSV rows
    errors_by_target = defaultdict(list)         # target_label -> [error%]
    errors_by_target_daytype = defaultdict(list) # (target, day_type) -> [error%]
    errors_by_target_session = defaultdict(list) # (target, full|partial) -> [error%]
    relay_died_count = 0
    full_session_count = 0

    print(f"MCX Projection Backtest — {t.strftime('%Y-%m-%d %H:%M IST')}")
    print(f"Scanning last {days} trading days...")
    if recompute:
        print("Mode: RECOMPUTE (re-running project_full_day with current code)")
    else:
        print("Mode: STORED (using proj_total_rev from snapshots)")
    print()

    scanned = 0
    for offset in range(1, days + 30):
        d = (t - timedelta(days=offset)).date()
        if not is_trading_day(d):
            continue
        date_iso = d.strftime("%Y-%m-%d")

        eod = fetch_eod(date_iso)
        if not eod:
            continue
        actual = eod["total_rev_cr"]
        if not actual or actual < 0.5:
            continue

        snaps = fetch_snaps(date_iso)
        if not snaps:
            continue

        # Determine session completeness
        last_elapsed = max(s["elapsed_min"] for s in snaps)
        full_session = last_elapsed >= 800
        if full_session:
            full_session_count += 1
        else:
            relay_died_count += 1
        session_label = "full" if full_session else "partial"

        day_type = eod.get("day_type") or "LOW"

        for target_min, label in ELAPSED_TARGETS:
            snap = find_at_elapsed(snaps, target_min)
            if not snap:
                continue

            if recompute:
                # Recompute projection using current model code
                proj_fut, proj_opt, _ = project_full_day(
                    snap["fut_notl_cr"], snap["opt_prem_cr"],
                    snap["elapsed_min"], snap.get("day_type") or day_type,
                )
                _, _, _, proj = calc_revenue(proj_fut, proj_opt)
            else:
                proj = snap.get("proj_total_rev") or snap.get("total_rev_cr") or 0

            if proj <= 0:
                continue

            err_pct = (proj - actual) / actual * 100
            errors_by_target[label].append(err_pct)
            errors_by_target_daytype[(label, day_type)].append(err_pct)
            errors_by_target_session[(label, session_label)].append(err_pct)

            rows.append({
                "date": date_iso,
                "day_type": day_type,
                "elapsed_label": label,
                "elapsed_min": snap["elapsed_min"],
                "proj_rev_cr": round(proj, 4),
                "actual_rev_cr": round(actual, 4),
                "error_pct": round(err_pct, 2),
                "session_label": session_label,
                "source": eod.get("source", "?"),
                "snap_count": len(snaps),
            })

        scanned += 1
        if scanned >= days:
            break

    # ── Summary ──────────────────────────────────────────────────────────
    print(f"Scanned {scanned} trading days  |  full sessions: {full_session_count}  |  partial: {relay_died_count}")
    print()
    print(f"{'Elapsed':<10} {'N':>4} {'Median%':>10} {'P90%':>10} {'P10%':>10} {'Mean%':>10} {'Bias':<20}")
    print("-" * 80)
    for _, label in ELAPSED_TARGETS:
        errs = errors_by_target[label]
        if not errs:
            continue
        under = sum(1 for e in errs if e < 0)
        over = sum(1 for e in errs if e > 0)
        print(f"{label:<10} {len(errs):>4} {median(errs):>9.2f}% {percentile(errs,90):>9.2f}% {percentile(errs,10):>9.2f}% {sum(errs)/len(errs):>9.2f}% {under} under / {over} over")

    # Day-type breakdown
    print()
    print(f"By day_type at 50% elapsed:")
    print(f"  {'Day type':<10} {'N':>3} {'Median%':>10} {'Mean%':>10}")
    for dt in ("LOW", "MEDIUM", "HIGH", "EXPIRY"):
        errs = errors_by_target_daytype.get(("50%", dt), [])
        if errs:
            print(f"  {dt:<10} {len(errs):>3} {median(errs):>9.2f}% {sum(errs)/len(errs):>9.2f}%")

    # Session completeness breakdown
    print()
    print(f"By session completeness at 50% elapsed:")
    for sess in ("full", "partial"):
        errs = errors_by_target_session.get(("50%", sess), [])
        if errs:
            print(f"  {sess:<10} {len(errs):>3} {median(errs):>9.2f}% (P90 {percentile(errs,90):>6.2f}%)")

    # ── CSV ─────────────────────────────────────────────────────────────
    log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
    os.makedirs(log_dir, exist_ok=True)
    suffix = "_recomputed" if recompute else ""
    csv_path = os.path.join(log_dir, f"backtest_{t.strftime('%Y-%m-%d')}{suffix}.csv")
    if rows:
        with open(csv_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        print(f"\nDetail CSV: {csv_path}")

    # ── Acceptance gates (compared to baseline targets in plan) ─────────
    print()
    print("Acceptance gates (median error %):")
    targets = {"25%": -15.0, "50%": -10.0, "75%": -3.0}
    all_pass = True
    for label, target in targets.items():
        errs = errors_by_target[label]
        if not errs:
            continue
        m = median(errs)
        ok = abs(m) <= abs(target) * 1.5  # within 50% of target = OK
        flag = "PASS" if ok else "FAIL"
        if not ok:
            all_pass = False
        print(f"  {label}: median {m:>7.2f}%  target |median| <= {abs(target):.1f}%  [{flag}]")

    return 0 if all_pass else 1


def main():
    days = 30
    recompute = "--recompute" in sys.argv
    for i, arg in enumerate(sys.argv):
        if arg == "--days" and i + 1 < len(sys.argv):
            try:
                days = int(sys.argv[i + 1])
            except ValueError:
                pass

    return backtest(days=days, recompute=recompute)


if __name__ == "__main__":
    sys.exit(main())
