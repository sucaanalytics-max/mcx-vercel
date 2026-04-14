"""
lib/oi_participants_dashboard — MCX OI Participant Category Analytics

Returns current participant distribution, hedger-vs-speculator ratios,
cross-commodity comparison, net positioning, and historical trends.

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
HEDGER_CATS  = {"vcp"}           # VCPs/Hedgers
SPECULATOR_CATS = {"prop", "others"}  # Proprietary Traders + Others


def _safe(val):
    """Return 0 for suppressed (-1) or None values in arithmetic."""
    if val is None or val == -1:
        return 0
    return val


def generate_oi_participants_dashboard():
    """Fetch OI participant data and compute full analytics payload."""

    rows = supabase_read_all(
        "mcx_oi_participants",
        "?select=report_date,commodity,instrument,total_participation,unit,"
        "fpo_long,fpo_short,vcp_long,vcp_short,prop_long,prop_short,"
        "dfi_long,dfi_short,foreign_long,foreign_short,"
        "others_long,others_short"
        "&order=report_date.asc",
        max_rows=20000,
    )

    if not rows:
        return {"success": False, "error": "No OI participant data yet. Run collection first."}

    # ── Group by date ──
    by_date = defaultdict(list)
    for r in rows:
        by_date[r["report_date"]].append(r)

    all_dates = sorted(by_date.keys())
    latest_date = all_dates[-1]
    latest_rows = by_date[latest_date]

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
            "hedger_long": h_long,
            "hedger_short": h_short,
            "hedger_total": hedger_total,
            "speculator_long": s_long,
            "speculator_short": s_short,
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

    # ── 4. Trend Data (last 60 dates) ──
    trend_dates = all_dates[-60:]
    trend = {"dates": trend_dates}

    # Build keyed lookup
    keyed = {}
    for r in rows:
        key = (r["report_date"], r["commodity"], r["instrument"])
        keyed[key] = r

    # Get unique commodity+instrument pairs from latest
    pairs = [(r["commodity"], r["instrument"]) for r in latest_rows]

    for commodity, instrument in pairs:
        series_key = f"{commodity}_{instrument}"
        series = {"total": []}
        for prefix, _ in CATEGORIES:
            series[f"{prefix}_long"] = []
            series[f"{prefix}_short"] = []
            series[f"{prefix}_net"] = []

        for dt in trend_dates:
            r = keyed.get((dt, commodity, instrument))
            if r:
                series["total"].append(_safe(r.get("total_participation")))
                for prefix, _ in CATEGORIES:
                    l = _safe(r.get(f"{prefix}_long"))
                    s = _safe(r.get(f"{prefix}_short"))
                    series[f"{prefix}_long"].append(l)
                    series[f"{prefix}_short"].append(s)
                    series[f"{prefix}_net"].append(l - s)
            else:
                series["total"].append(None)
                for prefix, _ in CATEGORIES:
                    series[f"{prefix}_long"].append(None)
                    series[f"{prefix}_short"].append(None)
                    series[f"{prefix}_net"].append(None)

        trend[series_key] = series

    # ── 5. Net Positioning (hedger vs speculator net, time series) ──
    net_positioning = {"dates": trend_dates}
    for commodity, instrument in pairs:
        series_key = f"{commodity}_{instrument}"
        h_net = []
        s_net = []
        for dt in trend_dates:
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
        net_positioning[series_key] = {
            "hedger_net": h_net,
            "speculator_net": s_net,
        }

    return {
        "success": True,
        "as_of": latest_date,
        "snapshot_dates": len(all_dates),
        "participants": participants,
        "hedger_speculator": hedger_speculator,
        "cross_commodity": cross_commodity,
        "trend": trend,
        "net_positioning": net_positioning,
        "commodities": sorted(set(f"{c}_{i}" for c, i in pairs)),
    }
