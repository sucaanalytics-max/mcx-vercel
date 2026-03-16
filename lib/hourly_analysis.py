"""
Hourly Predictor Accuracy Analysis — Retrospective accuracy at different hours.

For each historical day, reconstructs what the predictor would have said at
different hours using intraday snapshots + project_full_day(), and compares
against actual EOD revenue and next-day returns.

Used by analytics?section=hourly_accuracy.
"""
import math
from datetime import date, timedelta
from collections import defaultdict

from lib.mcx_config import (
    INTRADAY_BUCKETS, SESSION_START, SESSION_TOTAL,
    supabase_read_all, supabase_read, now_ist,
    get_intraday_weight, project_full_day, calc_revenue, get_day_type,
)

# Target hours: elapsed minutes from session start (09:00 IST)
TARGET_HOURS = [
    (120,  "11:00"),   # 2h into session
    (180,  "12:00"),
    (240,  "13:00"),
    (360,  "15:00"),
    (480,  "17:00"),   # evening session starts
    (600,  "19:00"),
    (720,  "21:00"),   # mid-NYMEX
    (810,  "22:30"),
    (870,  "23:30"),   # session end
]


def _f(v):
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _interpolate_snapshot(snapshots, target_elapsed):
    """Find interpolated (fut_notl_cr, opt_prem_cr) at target elapsed minutes."""
    if not snapshots:
        return None, None
    if target_elapsed <= 0:
        return 0.0, 0.0

    def _get(s, key):
        return s.get(key) or 0

    # Before first snapshot
    if target_elapsed <= snapshots[0]["elapsed_min"]:
        em0 = snapshots[0]["elapsed_min"]
        if em0 <= 0:
            return _get(snapshots[0], "fut_notl_cr"), _get(snapshots[0], "opt_prem_cr")
        ratio = target_elapsed / em0
        return _get(snapshots[0], "fut_notl_cr") * ratio, _get(snapshots[0], "opt_prem_cr") * ratio

    # After last snapshot
    if target_elapsed >= snapshots[-1]["elapsed_min"]:
        return _get(snapshots[-1], "fut_notl_cr"), _get(snapshots[-1], "opt_prem_cr")

    # Interpolate between two bounding snapshots
    for i in range(len(snapshots) - 1):
        s1, s2 = snapshots[i], snapshots[i + 1]
        if s1["elapsed_min"] <= target_elapsed <= s2["elapsed_min"]:
            span = s2["elapsed_min"] - s1["elapsed_min"]
            if span == 0:
                return _get(s1, "fut_notl_cr"), _get(s1, "opt_prem_cr")
            frac = (target_elapsed - s1["elapsed_min"]) / span
            fut = _get(s1, "fut_notl_cr") + frac * (_get(s2, "fut_notl_cr") - _get(s1, "fut_notl_cr"))
            opt = _get(s1, "opt_prem_cr") + frac * (_get(s2, "opt_prem_cr") - _get(s1, "opt_prem_cr"))
            return fut, opt

    return _get(snapshots[-1], "fut_notl_cr"), _get(snapshots[-1], "opt_prem_cr")


def _pearson(xs, ys):
    """Pearson correlation (None-safe)."""
    pairs = [(x, y) for x, y in zip(xs, ys) if x is not None and y is not None]
    n = len(pairs)
    if n < 10:
        return None
    mx = sum(p[0] for p in pairs) / n
    my = sum(p[1] for p in pairs) / n
    num = sum((p[0] - mx) * (p[1] - my) for p in pairs)
    dx = math.sqrt(sum((p[0] - mx) ** 2 for p in pairs))
    dy = math.sqrt(sum((p[1] - my) ** 2 for p in pairs))
    if dx == 0 or dy == 0:
        return None
    return round(num / (dx * dy), 4)


def _zscore(value, window):
    """Compute z-score of value within a window of values."""
    if len(window) < 10:
        return 0
    m = sum(window) / len(window)
    var = sum((v - m) ** 2 for v in window) / len(window)
    sd = var ** 0.5
    return (value - m) / sd if sd > 0 else 0


def generate_hourly_accuracy(lookback_days=90):
    """Analyze predictor accuracy at different hours of the trading day."""
    ist = now_ist()
    start_date = (ist - timedelta(days=lookback_days + 15)).strftime("%Y-%m-%d")

    # ── Fetch data ──
    snapshots = supabase_read_all(
        "mcx_snapshots",
        f"?select=trading_date,elapsed_min,fut_notl_cr,opt_prem_cr"
        f"&trading_date=gte.{start_date}"
        f"&order=trading_date.asc,elapsed_min.asc",
        max_rows=8000,
    )

    daily_revenue = supabase_read_all(
        "mcx_daily_revenue",
        f"?select=trading_date,total_rev_cr,fut_notl_cr,opt_prem_cr"
        f"&trading_date=gte.{start_date}"
        f"&order=trading_date.asc",
        max_rows=500,
    )

    model_signals = supabase_read_all(
        "mcx_model_signals",
        f"?select=trading_date,ensemble_score,ensemble_signal,ecm_spread_zscore,"
        f"mf_revenue_z,mf_turnover_z,mf_composite_z,position_score"
        f"&trading_date=gte.{start_date}"
        f"&order=trading_date.asc",
        max_rows=500,
    )

    share_prices = supabase_read_all(
        "mcx_share_price",
        f"?select=trading_date,close"
        f"&trading_date=gte.{start_date}"
        f"&order=trading_date.asc",
        max_rows=500,
    )

    # ── Build lookups ──
    by_date_snaps = defaultdict(list)
    for s in snapshots:
        if s.get("elapsed_min") is not None:
            by_date_snaps[s["trading_date"]].append(s)

    rev_map = {}
    rev_list = []  # ordered list of (date_str, total_rev)
    for r in daily_revenue:
        total = _f(r.get("total_rev_cr"))
        if total and total > 0:
            rev_map[r["trading_date"]] = {
                "total_rev_cr": total,
                "fut_notl_cr": _f(r.get("fut_notl_cr")) or 0,
                "opt_prem_cr": _f(r.get("opt_prem_cr")) or 0,
            }
            rev_list.append((r["trading_date"], total))

    signal_map = {}
    for s in model_signals:
        signal_map[s["trading_date"]] = s

    price_map = {}
    price_dates = []
    for p in share_prices:
        c = _f(p.get("close"))
        if c and c > 0:
            price_map[p["trading_date"]] = c
            price_dates.append(p["trading_date"])

    # ── Compute forward returns ──
    fwd_1d = {}
    fwd_5d = {}
    for i, dt in enumerate(price_dates):
        if i + 1 < len(price_dates):
            fwd_1d[dt] = (price_map[price_dates[i + 1]] - price_map[dt]) / price_map[dt]
        if i + 5 < len(price_dates):
            fwd_5d[dt] = (price_map[price_dates[i + 5]] - price_map[dt]) / price_map[dt]

    # ── Per-hour analysis ──
    hourly_results = {elapsed: [] for elapsed, _ in TARGET_HOURS}

    sorted_dates = sorted(by_date_snaps.keys())
    for dt_str in sorted_dates:
        snaps = by_date_snaps[dt_str]
        if len(snaps) < 4:
            continue
        rev_data = rev_map.get(dt_str)
        if not rev_data:
            continue
        eod_signal = signal_map.get(dt_str)

        actual_rev = rev_data["total_rev_cr"]
        max_elapsed = max(s["elapsed_min"] for s in snaps)

        for target_elapsed, label in TARGET_HOURS:
            if target_elapsed > max_elapsed + 30:
                continue  # No snapshot near this hour

            # Interpolate snapshot at target hour
            fut_at_hour, opt_at_hour = _interpolate_snapshot(snaps, target_elapsed)
            if fut_at_hour is None:
                continue

            # Project full day from this partial observation
            try:
                dt_obj = date.fromisoformat(dt_str)
                from datetime import datetime as dt_cls
                day_type = get_day_type(dt_cls(dt_obj.year, dt_obj.month, dt_obj.day))
            except Exception:
                day_type = "LOW"

            proj_fut, proj_opt, conf = project_full_day(fut_at_hour, opt_at_hour, target_elapsed, day_type)
            _, _, _, proj_rev = calc_revenue(proj_fut, proj_opt)

            # Revenue error
            rev_error_pct = (proj_rev - actual_rev) / actual_rev * 100 if actual_rev > 0 else 0

            obs = {
                "date": dt_str,
                "projected_rev": round(proj_rev, 4),
                "actual_rev": round(actual_rev, 4),
                "rev_error_pct": round(rev_error_pct, 2),
            }

            # Signal reconstruction: substitute projected revenue into ensemble
            if eod_signal:
                eod_ens = _f(eod_signal.get("ensemble_score"))
                ecm_z = _f(eod_signal.get("ecm_spread_zscore")) or 0
                turn_z = _f(eod_signal.get("mf_turnover_z")) or 0

                # Recompute rev_z with projected revenue
                # Build revenue window (last 60 days before this date)
                dt_idx = next((i for i, (d, _) in enumerate(rev_list) if d == dt_str), None)
                if dt_idx is not None and dt_idx >= 30:
                    rev_window = [r for _, r in rev_list[max(0, dt_idx - 59):dt_idx]]
                    proj_rev_z = _zscore(proj_rev, rev_window + [proj_rev])

                    # Reconstruct ensemble
                    mf_comp = proj_rev_z * (3 / 7) + turn_z * (4 / 7)
                    proj_ens = (-ecm_z * 0.30) + (mf_comp * 0.70)

                    obs["proj_ensemble"] = round(proj_ens, 4)
                    obs["eod_ensemble"] = round(eod_ens, 4) if eod_ens else None

                    # Signal direction match
                    if eod_ens is not None:
                        obs["signal_match"] = (
                            (proj_ens > 0 and eod_ens > 0) or
                            (proj_ens < 0 and eod_ens < 0) or
                            (abs(proj_ens) < 0.1 and abs(eod_ens) < 0.1)
                        )

                    # Forward accuracy
                    ret_1d = fwd_1d.get(dt_str)
                    ret_5d = fwd_5d.get(dt_str)
                    if ret_1d is not None:
                        obs["fwd_1d_hit"] = (proj_ens > 0 and ret_1d > 0) or (proj_ens < 0 and ret_1d < 0)
                        obs["fwd_1d_ret"] = round(ret_1d, 6)
                    if ret_5d is not None:
                        obs["fwd_5d_hit"] = (proj_ens > 0 and ret_5d > 0) or (proj_ens < 0 and ret_5d < 0)
                        obs["fwd_5d_ret"] = round(ret_5d, 6)

            hourly_results[target_elapsed].append(obs)

    # ── Aggregate metrics per hour ──
    revenue_accuracy = []
    signal_stability = []
    forward_accuracy = []

    for target_elapsed, label in TARGET_HOURS:
        obs_list = hourly_results[target_elapsed]
        n = len(obs_list)
        if n < 3:
            continue

        # Revenue accuracy
        errors = [o["rev_error_pct"] for o in obs_list]
        abs_errors = [abs(e) for e in errors]
        mae = sum(abs_errors) / n
        mean_err = sum(errors) / n
        rmse = (sum(e ** 2 for e in errors) / n) ** 0.5
        sorted_abs = sorted(abs_errors)
        p90_err = sorted_abs[int(0.9 * (n - 1))] if n > 1 else sorted_abs[0]

        revenue_accuracy.append({
            "elapsed_min": target_elapsed,
            "label": label,
            "mae_pct": round(mae, 2),
            "mean_error_pct": round(mean_err, 2),
            "rmse_pct": round(rmse, 2),
            "p90_error_pct": round(p90_err, 2),
            "n": n,
        })

        # Signal stability (match with EOD)
        matches = [o for o in obs_list if o.get("signal_match") is not None]
        if matches:
            match_rate = sum(1 for o in matches if o["signal_match"]) / len(matches)
            # Ensemble score MAE vs EOD
            ens_diffs = [
                abs(o["proj_ensemble"] - o["eod_ensemble"])
                for o in matches
                if o.get("proj_ensemble") is not None and o.get("eod_ensemble") is not None
            ]
            ens_mae = sum(ens_diffs) / len(ens_diffs) if ens_diffs else None
        else:
            match_rate = None
            ens_mae = None

        signal_stability.append({
            "elapsed_min": target_elapsed,
            "label": label,
            "signal_match_rate": round(match_rate, 3) if match_rate is not None else None,
            "ensemble_score_mae": round(ens_mae, 4) if ens_mae is not None else None,
            "n": len(matches),
        })

        # Forward accuracy
        fwd1_hits = [o for o in obs_list if o.get("fwd_1d_hit") is not None]
        fwd5_hits = [o for o in obs_list if o.get("fwd_5d_hit") is not None]
        hit_1d = sum(1 for o in fwd1_hits if o["fwd_1d_hit"]) / len(fwd1_hits) if fwd1_hits else None
        hit_5d = sum(1 for o in fwd5_hits if o["fwd_5d_hit"]) / len(fwd5_hits) if fwd5_hits else None

        # IC: correlation of projected ensemble with forward returns
        proj_ens_vals = [o.get("proj_ensemble") for o in obs_list]
        fwd5_rets = [o.get("fwd_5d_ret") for o in obs_list]
        ic_5d = _pearson(proj_ens_vals, fwd5_rets)

        forward_accuracy.append({
            "elapsed_min": target_elapsed,
            "label": label,
            "hit_rate_1d": round(hit_1d, 3) if hit_1d is not None else None,
            "hit_rate_5d": round(hit_5d, 3) if hit_5d is not None else None,
            "ic_5d": ic_5d,
            "n_1d": len(fwd1_hits),
            "n_5d": len(fwd5_hits),
        })

    # ── Convergence point ──
    convergence = {}
    for ra in revenue_accuracy:
        if ra["mae_pct"] < 5 and "revenue_5pct" not in convergence:
            convergence["revenue_5pct"] = {"elapsed_min": ra["elapsed_min"], "label": ra["label"]}
    for ss in signal_stability:
        mr = ss.get("signal_match_rate")
        if mr is not None and mr >= 0.90 and "signal_90pct" not in convergence:
            convergence["signal_90pct"] = {"elapsed_min": ss["elapsed_min"], "label": ss["label"]}

    # ── Volume curve bias (compare projection basis) ──
    curve_bias = []
    for ra in revenue_accuracy:
        curve_bias.append({
            "label": ra["label"],
            "mean_bias_pct": ra["mean_error_pct"],
            "direction": "over" if ra["mean_error_pct"] > 0 else "under",
        })

    return {
        "success": True,
        "as_of": ist.strftime("%Y-%m-%d %H:%M IST"),
        "revenue_accuracy": revenue_accuracy,
        "signal_stability": signal_stability,
        "forward_accuracy": forward_accuracy,
        "convergence": convergence,
        "curve_bias": curve_bias,
        "data_quality": {
            "days_with_snapshots": len([d for d in sorted_dates if len(by_date_snaps[d]) >= 4]),
            "days_with_revenue": len(rev_map),
            "days_with_signals": len(signal_map),
            "lookback_days": lookback_days,
        },
    }
