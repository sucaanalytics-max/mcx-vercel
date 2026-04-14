"""
lib/oi_participants_dashboard — MCX OI Participant Category Analytics

Returns current participant distribution, hedger-vs-speculator ratios,
cross-commodity comparison, net positioning, historical trends with
Overall (F+O) aggregation, rolling averages, and WoW/MoM/QoQ/YoY growth.

Data source: mcx_oi_participants (populated by cron_oi_participants from MCX XLSX).

Served via /api/commodity_dashboard?view=oi_participants
"""
from collections import defaultdict

try:
    from lib.mcx_config import supabase_read_all, now_ist
except ImportError:
    from lib.mcx_config import supabase_read_all, now_ist


# Category column prefixes for iteration
CATEGORIES = [
    ("fpo",     "FPOs/Farmers"),
    ("vcp",     "VCPs/Hedgers"),
    ("prop",    "Proprietary Traders"),
    ("dfi",     "DFI"),
    ("foreign", "Foreign Participants"),
    ("others",  "Others"),
]

# Hedger categories vs speculator categories
HEDGER_CATS  = {"vcp"}
SPECULATOR_CATS = {"prop", "others"}

# Growth lookback windows (trading days)
GROWTH_WINDOWS = {"wow": 5, "mom": 22, "qoq": 63, "yoy": 252}

# Commodity renames — merge old names into current names for continuous series
COMMODITY_RENAMES = {
    "COTTONCNDY": "COTTON",
}


def _safe(val):
    """Return 0 for suppressed (-1) or None values in arithmetic."""
    if val is None or val == -1:
        return 0
    return val


def _rolling_avg(data, window):
    """Compute rolling average, returning None where insufficient data."""
    result = []
    for i in range(len(data)):
        if data[i] is None:
            result.append(None)
            continue
        start = max(0, i - window + 1)
        vals = [v for v in data[start:i + 1] if v is not None]
        result.append(round(sum(vals) / len(vals), 1) if vals else None)
    return result


def _growth_pct(current, previous):
    """Compute growth %, return None if data missing."""
    if current is None or previous is None or previous == 0:
        return None
    return round((current - previous) / previous * 100, 1)


def generate_oi_participants_dashboard():
    """Fetch OI participant data and compute full analytics payload."""

    rows = supabase_read_all(
        "mcx_oi_participants",
        "?select=report_date,commodity,instrument,total_participation,unit,"
        "fpo_long,fpo_short,vcp_long,vcp_short,prop_long,prop_short,"
        "dfi_long,dfi_short,foreign_long,foreign_short,"
        "others_long,others_short"
        "&order=report_date.asc",
        max_rows=50000,
    )

    if not rows:
        return {"success": False, "error": "No OI participant data yet. Run collection first."}

    # ── Apply commodity renames for continuous series ──
    for r in rows:
        r["commodity"] = COMMODITY_RENAMES.get(r["commodity"], r["commodity"])

    # ── Group by date ──
    by_date = defaultdict(list)
    for r in rows:
        by_date[r["report_date"]].append(r)

    all_dates = sorted(by_date.keys())
    latest_date = all_dates[-1]
    latest_rows = by_date[latest_date]

    # ── Build keyed lookup ──
    keyed = {}
    for r in rows:
        key = (r["report_date"], r["commodity"], r["instrument"])
        keyed[key] = r

    # ── Get unique commodity+instrument pairs from latest ──
    pairs = [(r["commodity"], r["instrument"]) for r in latest_rows]
    unique_commodities = sorted(set(c for c, _ in pairs))

    # ── 1. Current Participant Snapshot ──
    participants = []
    for r in latest_rows:
        participants.append({
            "commodity": r["commodity"],
            "instrument": r["instrument"],
            "total_participation": r["total_participation"],
            "unit": r["unit"],
            "fpo_long": r["fpo_long"], "fpo_short": r["fpo_short"],
            "vcp_long": r["vcp_long"], "vcp_short": r["vcp_short"],
            "prop_long": r["prop_long"], "prop_short": r["prop_short"],
            "dfi_long": r["dfi_long"], "dfi_short": r["dfi_short"],
            "foreign_long": r["foreign_long"], "foreign_short": r["foreign_short"],
            "others_long": r["others_long"], "others_short": r["others_short"],
        })
    participants.sort(key=lambda x: -(x.get("total_participation") or 0))

    # ── 2. Hedger vs Speculator Ratios ──
    hedger_speculator = []
    for r in latest_rows:
        h_long = sum(_safe(r.get(f"{c}_long")) for c in HEDGER_CATS)
        h_short = sum(_safe(r.get(f"{c}_short")) for c in HEDGER_CATS)
        s_long = sum(_safe(r.get(f"{c}_long")) for c in SPECULATOR_CATS)
        s_short = sum(_safe(r.get(f"{c}_short")) for c in SPECULATOR_CATS)
        total = _safe(r.get("total_participation"))
        hedger_total = h_long + h_short
        speculator_total = s_long + s_short

        hedger_speculator.append({
            "commodity": r["commodity"],
            "instrument": r["instrument"],
            "total": total,
            "hedger_long": h_long, "hedger_short": h_short,
            "hedger_total": hedger_total,
            "speculator_long": s_long, "speculator_short": s_short,
            "speculator_total": speculator_total,
            "hedger_pct": round(hedger_total / total * 100, 1) if total > 0 else 0,
            "speculator_pct": round(speculator_total / total * 100, 1) if total > 0 else 0,
            "net_hedger": h_long - h_short,
            "net_speculator": s_long - s_short,
        })
    hedger_speculator.sort(key=lambda x: -x["total"])

    # ── 3. Cross-Commodity Comparison ──
    cross_commodity = []
    for r in latest_rows:
        total = _safe(r.get("total_participation"))
        cat_breakdown = {}
        for prefix, label in CATEGORIES:
            l = _safe(r.get(f"{prefix}_long"))
            s = _safe(r.get(f"{prefix}_short"))
            cat_breakdown[prefix] = l + s
        dominant_cat = max(cat_breakdown, key=cat_breakdown.get) if total > 0 else None
        dominant_label = dict(CATEGORIES).get(dominant_cat, "")

        cross_commodity.append({
            "commodity": r["commodity"],
            "instrument": r["instrument"],
            "total": total,
            "dominant_category": dominant_label,
            "dominant_pct": round(cat_breakdown.get(dominant_cat, 0) / total * 100, 1) if total > 0 else 0,
            **{f"{p}_total": cat_breakdown[p] for p, _ in CATEGORIES},
        })
    cross_commodity.sort(key=lambda x: -x["total"])

    # ── 4. Trend Data (ALL dates, with Overall and exchange-wide) ──
    trend = {"dates": all_dates}

    # 4a. Per commodity × instrument trends (with per-category breakdowns)
    for commodity, instrument in pairs:
        series_key = f"{commodity}_{instrument}"
        totals = []
        cat_arrays = {}
        for prefix, _ in CATEGORIES:
            cat_arrays[f"{prefix}_long"] = []
            cat_arrays[f"{prefix}_short"] = []

        for dt in all_dates:
            r = keyed.get((dt, commodity, instrument))
            if r:
                totals.append(_safe(r.get("total_participation")))
                for prefix, _ in CATEGORIES:
                    cat_arrays[f"{prefix}_long"].append(_safe(r.get(f"{prefix}_long")))
                    cat_arrays[f"{prefix}_short"].append(_safe(r.get(f"{prefix}_short")))
            else:
                totals.append(None)
                for prefix, _ in CATEGORIES:
                    cat_arrays[f"{prefix}_long"].append(None)
                    cat_arrays[f"{prefix}_short"].append(None)

        trend[series_key] = {
            "total": totals,
            "ma7": _rolling_avg(totals, 7),
            "ma30": _rolling_avg(totals, 30),
            **cat_arrays,
        }

    # 4b. Overall (Futures + Options) per commodity
    for commodity in unique_commodities:
        overall_key = f"{commodity}_Overall"
        totals = []
        for dt in all_dates:
            f_row = keyed.get((dt, commodity, "Futures"))
            o_row = keyed.get((dt, commodity, "Options"))
            f_val = _safe(f_row.get("total_participation")) if f_row else 0
            o_val = _safe(o_row.get("total_participation")) if o_row else 0
            if f_row or o_row:
                totals.append(f_val + o_val)
            else:
                totals.append(None)
        trend[overall_key] = {
            "total": totals,
            "ma7": _rolling_avg(totals, 7),
            "ma30": _rolling_avg(totals, 30),
        }

    # 4c. Exchange-wide totals (ALL commodities)
    for inst_filter in ["Futures", "Options", "Overall"]:
        agg_key = f"ALL_{inst_filter}"
        totals = []
        for dt in all_dates:
            day_rows = by_date.get(dt, [])
            if not day_rows:
                totals.append(None)
                continue
            if inst_filter == "Overall":
                total = sum(_safe(r.get("total_participation")) for r in day_rows)
            else:
                total = sum(_safe(r.get("total_participation"))
                            for r in day_rows if r["instrument"] == inst_filter)
            totals.append(total if total > 0 else None)
        trend[agg_key] = {
            "total": totals,
            "ma7": _rolling_avg(totals, 7),
            "ma30": _rolling_avg(totals, 30),
        }

    # ── 5. Net Positioning (hedger vs speculator net, all dates) ──
    net_positioning = {"dates": all_dates}
    for commodity, instrument in pairs:
        series_key = f"{commodity}_{instrument}"
        h_net, s_net = [], []
        for dt in all_dates:
            r = keyed.get((dt, commodity, instrument))
            if r:
                hl = sum(_safe(r.get(f"{c}_long")) for c in HEDGER_CATS)
                hs = sum(_safe(r.get(f"{c}_short")) for c in HEDGER_CATS)
                sl = sum(_safe(r.get(f"{c}_long")) for c in SPECULATOR_CATS)
                ss = sum(_safe(r.get(f"{c}_short")) for c in SPECULATOR_CATS)
                h_net.append(hl - hs)
                s_net.append(sl - ss)
            else:
                h_net.append(None)
                s_net.append(None)
        net_positioning[series_key] = {"hedger_net": h_net, "speculator_net": s_net}

    # ── 6. Growth Metrics (WoW/MoM/QoQ/YoY) ──
    def _compute_growth(totals_array):
        """Compute growth vs N trading days ago from a totals array."""
        if not totals_array:
            return {}
        current = totals_array[-1]
        result = {"current": current}
        for label, window in GROWTH_WINDOWS.items():
            idx = len(totals_array) - 1 - window
            prev = totals_array[idx] if 0 <= idx < len(totals_array) else None
            result[f"{label}_pct"] = _growth_pct(current, prev)
            result[f"{label}_prev"] = prev
        return result

    # Per commodity growth
    growth = []
    for commodity, instrument in pairs:
        series = trend.get(f"{commodity}_{instrument}", {})
        totals = series.get("total", [])
        g = _compute_growth(totals)
        g["commodity"] = commodity
        g["instrument"] = instrument
        growth.append(g)

    # Overall per commodity
    for commodity in unique_commodities:
        series = trend.get(f"{commodity}_Overall", {})
        totals = series.get("total", [])
        g = _compute_growth(totals)
        g["commodity"] = commodity
        g["instrument"] = "Overall"
        growth.append(g)

    # Exchange-wide growth
    growth_overall = {}
    for inst_filter in ["Futures", "Options", "Overall"]:
        series = trend.get(f"ALL_{inst_filter}", {})
        totals = series.get("total", [])
        growth_overall[inst_filter] = _compute_growth(totals)

    # ── 7. Monthly growth series for bar chart ──
    # Aggregate total participation per month for exchange-wide
    monthly_growth = {"months": [], "Futures": [], "Options": [], "Overall": []}
    month_buckets = defaultdict(lambda: {"Futures": 0, "Options": 0, "count_f": 0, "count_o": 0})
    for dt in all_dates:
        month_key = dt[:7]  # "YYYY-MM"
        day_rows = by_date.get(dt, [])
        for r in day_rows:
            inst = r["instrument"]
            val = _safe(r.get("total_participation"))
            if inst == "Futures":
                month_buckets[month_key]["Futures"] += val
                month_buckets[month_key]["count_f"] += 1
            elif inst == "Options":
                month_buckets[month_key]["Options"] += val
                month_buckets[month_key]["count_o"] += 1

    sorted_months = sorted(month_buckets.keys())
    for m in sorted_months:
        b = month_buckets[m]
        avg_f = round(b["Futures"] / b["count_f"]) if b["count_f"] > 0 else 0
        avg_o = round(b["Options"] / b["count_o"]) if b["count_o"] > 0 else 0
        monthly_growth["months"].append(m)
        monthly_growth["Futures"].append(avg_f)
        monthly_growth["Options"].append(avg_o)
        monthly_growth["Overall"].append(avg_f + avg_o)

    # Compute MoM % change for each month
    for inst in ["Futures", "Options", "Overall"]:
        pcts = []
        vals = monthly_growth[inst]
        for i in range(len(vals)):
            if i == 0 or vals[i - 1] == 0:
                pcts.append(None)
            else:
                pcts.append(round((vals[i] - vals[i - 1]) / vals[i - 1] * 100, 1))
        monthly_growth[f"{inst}_mom_pct"] = pcts

    return {
        "success": True,
        "as_of": latest_date,
        "snapshot_dates": len(all_dates),
        "participants": participants,
        "hedger_speculator": hedger_speculator,
        "cross_commodity": cross_commodity,
        "trend": trend,
        "net_positioning": net_positioning,
        "growth": growth,
        "growth_overall": growth_overall,
        "monthly_growth": monthly_growth,
        "commodities": sorted(set(f"{c}_{i}" for c, i in pairs)),
        "unique_commodities": unique_commodities,
    }
