#!/usr/bin/env python3
"""
Day-Type Multiplier Calibration

Pulls the full mcx_daily_revenue history, recomputes day_type for each day
via the current get_day_type() function (fixes stale field in older rows),
and computes month-normalized mean revenue per day type with confidence
intervals. Outputs a calibration report and proposed DAY_MULTIPLIER values.

Usage:
  python3 scripts/calibrate_day_types.py
  python3 scripts/calibrate_day_types.py --min-date 2025-01-01
"""
import sys, os, csv, math, statistics
from datetime import datetime
from collections import defaultdict

if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.mcx_config import (
    supabase_read_all, get_day_type, is_trading_day,
)


def t_critical(n):
    """Approx t-critical for 95% CI."""
    if n < 2:
        return float("inf")
    if n < 30:
        return 2.0
    return 1.96


def main():
    min_date = "2024-10-01"  # match the comment in mcx_config.py (361-day window starts Oct 2024)
    for i, arg in enumerate(sys.argv):
        if arg == "--min-date" and i + 1 < len(sys.argv):
            min_date = sys.argv[i + 1]

    print(f"Day-Type Calibration — fetching history >= {min_date}")
    rows = supabase_read_all(
        "mcx_daily_revenue",
        f"?trading_date=gte.{min_date}&select=trading_date,total_rev_cr,source&order=trading_date.asc",
        page_size=1000, max_rows=5000,
    )

    # Filter sane rows
    rows = [r for r in rows if r.get("total_rev_cr") and 1.0 <= r["total_rev_cr"] <= 50.0
            and r["trading_date"] >= min_date]
    print(f"Loaded {len(rows)} valid daily revenue rows")
    if len(rows) < 30:
        print("Insufficient data for robust calibration.")
        return 1

    # Reclassify day_type from current event calendar
    enriched = []
    for r in rows:
        d = datetime.strptime(r["trading_date"], "%Y-%m-%d")
        if not is_trading_day(d.date()):
            continue
        dt_label = get_day_type(d)
        enriched.append({
            "date": r["trading_date"],
            "rev": r["total_rev_cr"],
            "day_type": dt_label,
            "month": r["trading_date"][:7],
        })

    # Month-normalize: divide each day's revenue by its month's mean (LOW only,
    # to avoid contamination from event days when computing the trend).
    monthly_low_mean = {}
    by_month_low = defaultdict(list)
    for r in enriched:
        if r["day_type"] == "LOW":
            by_month_low[r["month"]].append(r["rev"])
    for m, revs in by_month_low.items():
        if len(revs) >= 5:
            monthly_low_mean[m] = sum(revs) / len(revs)

    # Apply normalization (skip months with insufficient LOW data)
    norm_rows = []
    for r in enriched:
        if r["month"] in monthly_low_mean and monthly_low_mean[r["month"]] > 0:
            r["rev_norm"] = r["rev"] / monthly_low_mean[r["month"]]
            norm_rows.append(r)

    print(f"Months with sufficient LOW baseline: {len(monthly_low_mean)}")
    print(f"Days normalized: {len(norm_rows)}")
    print()

    # Compute mean and CI per day_type
    by_dt = defaultdict(list)
    for r in norm_rows:
        by_dt[r["day_type"]].append(r["rev_norm"])

    print(f"{'Day type':<10} {'N':>4} {'Mean':>8} {'StDev':>8} {'95% CI':<20} {'Significant?'}")
    print("-" * 72)

    proposed = {}
    for dt in ("LOW", "MEDIUM", "HIGH", "EXPIRY"):
        xs = by_dt.get(dt, [])
        if not xs:
            print(f"{dt:<10} {0:>4} (no data)")
            continue
        n = len(xs)
        mean = sum(xs) / n
        if n > 1:
            sd = statistics.stdev(xs)
            tc = t_critical(n)
            margin = tc * sd / math.sqrt(n)
            ci_lo, ci_hi = mean - margin, mean + margin
            # Significant if CI doesn't cross 1.0 (relative to LOW baseline)
            if dt == "LOW":
                sig = "(baseline)"
            else:
                sig = "YES" if (ci_lo > 1.0 or ci_hi < 1.0) else "no"
        else:
            sd = 0.0
            ci_lo, ci_hi = mean, mean
            sig = "n=1"

        print(f"{dt:<10} {n:>4} {mean:>7.4f} {sd:>7.4f}  [{ci_lo:>5.3f}, {ci_hi:>5.3f}]   {sig}")
        proposed[dt] = round(mean, 3)

    # Write CSV
    log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
    os.makedirs(log_dir, exist_ok=True)
    csv_path = os.path.join(log_dir, "day_type_calibration.csv")
    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["date", "rev", "day_type", "month", "rev_norm"])
        w.writeheader()
        for r in norm_rows:
            w.writerow(r)
    print(f"\nDetail CSV: {csv_path}")

    # Proposed DAY_MULTIPLIER
    print()
    print("Proposed DAY_MULTIPLIER (relative to LOW=1.0):")
    base = proposed.get("LOW", 1.0)
    for dt in ("LOW", "MEDIUM", "HIGH", "EXPIRY"):
        if dt in proposed:
            mult = proposed[dt] / base
            print(f"  {dt}: {mult:.3f}")

    # Bias check on small samples
    for dt in ("MEDIUM", "HIGH", "EXPIRY"):
        n = len(by_dt.get(dt, []))
        if n < 10:
            print(f"  WARN: {dt} sample size n={n} — wide CI, treat with caution")

    return 0


if __name__ == "__main__":
    sys.exit(main())
