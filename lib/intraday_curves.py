"""
Intraday Volume Curve Analysis — Dynamic curves from mcx_snapshots.

Derives actual bucket weights from historical snapshot data and compares
against the static INTRADAY_BUCKETS model. Used by exchange_dashboard?view=intraday_curve.
"""
from datetime import date, timedelta
from collections import defaultdict

from lib.mcx_config import (
    INTRADAY_BUCKETS, SESSION_START, SESSION_END, SESSION_TOTAL,
    supabase_read_all, now_ist, get_day_type, calc_revenue,
)

# Bucket boundaries as elapsed minutes from session start
_BUCKET_EDGES = [b[0] - SESSION_START for b in INTRADAY_BUCKETS] + [SESSION_TOTAL]
_BUCKET_LABELS = [
    "09:00–10:30", "10:30–12:30", "12:30–15:00", "15:00–17:00",
    "17:00–19:30", "19:30–22:00", "22:00–23:30",
]
_STATIC_WEIGHTS = [b[2] for b in INTRADAY_BUCKETS]

# ─── helpers ──────────────────────────────────────────────────────────────

def _interpolate_volume(snapshots, target_elapsed):
    """Interpolate cumulative total volume (fut_notl + opt_prem) at target_elapsed.
    snapshots must be sorted by elapsed_min ascending."""
    if not snapshots:
        return 0.0
    # Volume metric: fut_notl_cr + opt_prem_cr (revenue-equivalent)
    def _vol(s):
        return (s.get("fut_notl_cr") or 0) + (s.get("opt_prem_cr") or 0)

    if target_elapsed <= 0:
        return 0.0
    if target_elapsed <= snapshots[0]["elapsed_min"]:
        v0 = _vol(snapshots[0])
        em0 = snapshots[0]["elapsed_min"]
        return v0 * (target_elapsed / em0) if em0 > 0 else 0.0
    if target_elapsed >= snapshots[-1]["elapsed_min"]:
        return _vol(snapshots[-1])
    # Linear interpolation between two bounding snapshots
    for i in range(len(snapshots) - 1):
        s1, s2 = snapshots[i], snapshots[i + 1]
        if s1["elapsed_min"] <= target_elapsed <= s2["elapsed_min"]:
            span = s2["elapsed_min"] - s1["elapsed_min"]
            if span == 0:
                return _vol(s1)
            frac = (target_elapsed - s1["elapsed_min"]) / span
            return _vol(s1) + frac * (_vol(s2) - _vol(s1))
    return _vol(snapshots[-1])


def _derive_bucket_weights(snapshots):
    """Derive 7 bucket weights from a single day's sorted snapshots.
    Returns list of 7 floats summing to ~1.0, or None if insufficient data."""
    if len(snapshots) < 4:
        return None

    total_vol = _interpolate_volume(snapshots, SESSION_TOTAL)
    if total_vol <= 0:
        return None

    weights = []
    prev_cum = 0.0
    for i in range(len(_BUCKET_EDGES) - 1):
        edge_end = _BUCKET_EDGES[i + 1]
        cum = _interpolate_volume(snapshots, edge_end)
        bucket_vol = max(0, cum - prev_cum)
        weights.append(bucket_vol / total_vol)
        prev_cum = cum

    return weights


def _percentiles(values, pcts):
    """Compute percentiles from a list of floats."""
    if not values:
        return {p: 0 for p in pcts}
    s = sorted(values)
    n = len(s)
    result = {}
    for p in pcts:
        idx = p / 100 * (n - 1)
        lo = int(idx)
        hi = min(lo + 1, n - 1)
        frac = idx - lo
        result[p] = s[lo] * (1 - frac) + s[hi] * frac
    return result


# ─── main generator ───────────────────────────────────────────────────────

def generate_intraday_curves(days=30, include_today=True):
    """Build dynamic intraday curve data from mcx_snapshots."""
    ist = now_ist()
    today_str = ist.strftime("%Y-%m-%d")
    start_date = (ist - timedelta(days=days + 10)).strftime("%Y-%m-%d")

    # Fetch snapshots
    snapshots = supabase_read_all(
        "mcx_snapshots",
        f"?select=trading_date,elapsed_min,fut_notl_cr,opt_prem_cr"
        f"&trading_date=gte.{start_date}"
        f"&order=trading_date.asc,elapsed_min.asc",
        max_rows=5000,
    )

    # Group by trading_date
    by_date = defaultdict(list)
    for s in snapshots:
        em = s.get("elapsed_min")
        if em is not None:
            by_date[s["trading_date"]].append(s)

    # Separate today from historical
    today_snaps = by_date.pop(today_str, [])
    hist_dates = sorted(by_date.keys(), reverse=True)[:days]

    # ── Historical bucket weights per day ──
    daily_curves = []
    all_weights = [[] for _ in range(7)]  # 7 buckets

    for dt_str in hist_dates:
        snaps = by_date[dt_str]
        if len(snaps) < 4:
            continue
        w = _derive_bucket_weights(snaps)
        if w is None:
            continue

        total_vol = _interpolate_volume(snaps, SESSION_TOTAL)
        daily_curves.append({
            "date": dt_str,
            "total_volume_cr": round(total_vol, 1),
            "buckets": [round(x, 4) for x in w],
        })
        for i in range(7):
            all_weights[i].append(w[i])

    n_days = len(daily_curves)

    # ── Rolling averages ──
    avg_weights = []
    std_weights = []
    for i in range(7):
        vals = all_weights[i]
        if vals:
            m = sum(vals) / len(vals)
            var = sum((v - m) ** 2 for v in vals) / len(vals)
            avg_weights.append(round(m, 4))
            std_weights.append(round(var ** 0.5, 4))
        else:
            avg_weights.append(_STATIC_WEIGHTS[i])
            std_weights.append(0)

    evening_pct = round(sum(avg_weights[4:]) * 100, 1) if n_days > 0 else 67.0

    # ── Percentiles (per bucket) ──
    pct_keys = [10, 25, 50, 75, 90]
    percentiles = {}
    for p in pct_keys:
        percentiles[f"p{p}"] = [
            round(_percentiles(all_weights[i], [p])[p], 4)
            for i in range(7)
        ]

    # ── Today's developing curve ──
    today_data = None
    if include_today and today_snaps:
        elapsed_now = max(s["elapsed_min"] for s in today_snaps)
        # Cumulative curve (every snapshot as a point)
        cum_curve = []
        total_so_far = _interpolate_volume(today_snaps, elapsed_now)
        for s in today_snaps:
            vol = (s.get("fut_notl_cr") or 0) + (s.get("opt_prem_cr") or 0)
            cum_curve.append({
                "elapsed_min": s["elapsed_min"],
                "volume_cr": round(vol, 1),
            })

        # Partial bucket weights (only for completed buckets)
        partial_buckets = []
        for i in range(len(_BUCKET_EDGES) - 1):
            edge_start = _BUCKET_EDGES[i]
            edge_end = _BUCKET_EDGES[i + 1]
            if total_so_far > 0 and elapsed_now >= edge_end:
                cum_end = _interpolate_volume(today_snaps, edge_end)
                cum_start = _interpolate_volume(today_snaps, edge_start)
                w = (cum_end - cum_start) / total_so_far if total_so_far > 0 else 0
                partial_buckets.append({
                    "label": _BUCKET_LABELS[i],
                    "weight": round(w, 4),
                    "complete": True,
                })
            elif elapsed_now > edge_start:
                # Partially complete bucket
                cum_now = _interpolate_volume(today_snaps, elapsed_now)
                cum_start = _interpolate_volume(today_snaps, edge_start)
                partial_w = (cum_now - cum_start) / total_so_far if total_so_far > 0 else 0
                partial_buckets.append({
                    "label": _BUCKET_LABELS[i],
                    "weight": round(partial_w, 4),
                    "complete": False,
                    "bucket_pct": round((elapsed_now - edge_start) / (edge_end - edge_start) * 100, 0),
                })
            else:
                partial_buckets.append({
                    "label": _BUCKET_LABELS[i],
                    "weight": None,
                    "complete": False,
                })

        today_data = {
            "elapsed_min": elapsed_now,
            "total_volume_cr": round(total_so_far, 1),
            "cumulative_curve": cum_curve,
            "partial_buckets": partial_buckets,
        }

    # ── Divergence analysis (static vs actual) ──
    divergences = []
    for i in range(7):
        static_w = _STATIC_WEIGHTS[i]
        actual_w = avg_weights[i]
        diff = actual_w - static_w
        divergences.append({
            "label": _BUCKET_LABELS[i],
            "static": static_w,
            "actual_avg": actual_w,
            "diff": round(diff, 4),
            "diff_pct": round(diff / static_w * 100, 1) if static_w > 0 else 0,
        })

    return {
        "success": True,
        "as_of": ist.strftime("%Y-%m-%d %H:%M IST"),
        "static_model": {
            "buckets": [
                {"label": _BUCKET_LABELS[i], "weight": _STATIC_WEIGHTS[i],
                 "start_min": INTRADAY_BUCKETS[i][0], "end_min": INTRADAY_BUCKETS[i][1]}
                for i in range(7)
            ],
        },
        "rolling_average": {
            "days_used": n_days,
            "buckets": [
                {"label": _BUCKET_LABELS[i], "weight": avg_weights[i], "std": std_weights[i]}
                for i in range(7)
            ],
            "evening_pct": evening_pct,
        },
        "today": today_data,
        "percentiles": percentiles,
        "divergences": divergences,
        "historical_curves": daily_curves[:7],  # last 7 days full detail
    }
