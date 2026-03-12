"""
lib/margin_dashboard — MCX Margin Requirements Dashboard logic

Returns current margins, margin change history, and time series
for charting margin fluctuations per commodity.

Data source: mcx_margin_daily (populated by cron_margins from Sharekhan SPAN file).

Served via /api/commodity_dashboard?view=margins
"""
try:
    from lib.mcx_config import supabase_read_all, now_ist
except ImportError:
    from lib.mcx_config import supabase_read_all, now_ist


# Normalize mini/micro variants to parent commodity for display grouping
MARGIN_COMMODITY_MAP = {
    "CRUDEOILM": "CRUDEOIL", "NATGASMINI": "NATURALGAS",
    "GOLDM": "GOLD", "GOLDGUINEA": "GOLD", "GOLDPETAL": "GOLD", "GOLDTEN": "GOLD",
    "SILVERM": "SILVER", "SILVERMIC": "SILVER",
    "LEADMINI": "LEAD", "ZINCMINI": "ZINC", "ALUMINI": "ALUMINIUM",
}

# Commodities we care about (main contracts only, skip index/mini variants)
MAIN_INSTRUMENTS = {"FUTCOM"}


def generate_margin_dashboard():
    """Fetch margin data and compute dashboard payload."""
    ist = now_ist()

    rows = supabase_read_all(
        "mcx_margin_daily",
        "?select=snapshot_date,symbol,instrument,expiry,"
        "initial_margin_pct,tender_margin_pct,total_margin_pct,"
        "additional_long_pct,additional_short_pct,"
        "special_long_pct,special_short_pct,"
        "elm_long_pct,elm_short_pct,delivery_margin_pct"
        "&order=snapshot_date.asc",
        max_rows=10000,
    )

    if not rows:
        return {"success": False, "error": "No margin data yet. Run margin collection first."}

    # ── Group by (snapshot_date, symbol) — take FUTCOM only, pick nearest expiry ──
    # For each date+symbol, keep one representative row (nearest expiry)
    keyed = {}  # (date, symbol) → row
    for r in rows:
        inst = (r.get("instrument") or "").strip()
        if inst not in MAIN_INSTRUMENTS:
            continue

        sym = r["symbol"].strip()
        # Skip mini variants — we want parent contract margins
        if sym in MARGIN_COMMODITY_MAP:
            continue

        dt = r["snapshot_date"]
        key = (dt, sym)

        # Keep one row per (date, symbol) — first occurrence (nearest expiry since ordered)
        if key not in keyed:
            keyed[key] = r

    if not keyed:
        return {"success": False, "error": "No FUTCOM margin rows found."}

    # ── Get all dates and symbols ──
    all_dates = sorted(set(k[0] for k in keyed))
    all_symbols = sorted(set(k[1] for k in keyed))
    latest_date = all_dates[-1]

    # ── Current margins (latest snapshot) ──
    current_margins = []
    for sym in all_symbols:
        r = keyed.get((latest_date, sym))
        if not r:
            continue

        # Find previous snapshot for this symbol to detect change
        prev_total = None
        last_change_date = None
        for dt in reversed(all_dates[:-1]):
            prev = keyed.get((dt, sym))
            if prev and prev.get("total_margin_pct") is not None:
                if prev_total is None:
                    prev_total = prev["total_margin_pct"]
                if prev["total_margin_pct"] != r.get("total_margin_pct"):
                    last_change_date = dt
                    break

        total = r.get("total_margin_pct")
        change = round(total - prev_total, 2) if total is not None and prev_total is not None else None

        current_margins.append({
            "symbol": sym,
            "instrument": (r.get("instrument") or "").strip(),
            "initial_margin_pct": r.get("initial_margin_pct"),
            "tender_margin_pct": r.get("tender_margin_pct"),
            "total_margin_pct": total,
            "additional_long_pct": r.get("additional_long_pct"),
            "additional_short_pct": r.get("additional_short_pct"),
            "special_long_pct": r.get("special_long_pct"),
            "special_short_pct": r.get("special_short_pct"),
            "elm_long_pct": r.get("elm_long_pct"),
            "elm_short_pct": r.get("elm_short_pct"),
            "delivery_margin_pct": r.get("delivery_margin_pct"),
            "prev_total_margin_pct": prev_total,
            "change_pct": change,
            "last_change_date": last_change_date,
        })

    # Sort by total margin descending (highest margin = most volatile)
    current_margins.sort(key=lambda x: -(x.get("total_margin_pct") or 0))

    # ── Margin history (all dates × symbols) for chart ──
    margin_history = {"dates": all_dates}
    for sym in all_symbols:
        margin_history[sym] = []
        for dt in all_dates:
            r = keyed.get((dt, sym))
            margin_history[sym].append(r.get("total_margin_pct") if r else None)

    # ── Margin changes (detect day-over-day changes) ──
    margin_changes = []
    for i in range(1, len(all_dates)):
        dt = all_dates[i]
        prev_dt = all_dates[i - 1]
        for sym in all_symbols:
            cur = keyed.get((dt, sym))
            prev = keyed.get((prev_dt, sym))
            if not cur or not prev:
                continue
            cur_total = cur.get("total_margin_pct")
            prev_total = prev.get("total_margin_pct")
            if cur_total is not None and prev_total is not None and cur_total != prev_total:
                margin_changes.append({
                    "date": dt,
                    "symbol": sym,
                    "old_total": prev_total,
                    "new_total": cur_total,
                    "change": round(cur_total - prev_total, 2),
                    "direction": "up" if cur_total > prev_total else "down",
                })

    # Most recent changes first
    margin_changes.sort(key=lambda x: x["date"], reverse=True)

    return {
        "success": True,
        "as_of": latest_date,
        "snapshot_dates": len(all_dates),
        "current_margins": current_margins,
        "margin_history": margin_history,
        "margin_changes": margin_changes[:50],  # Last 50 changes
        "commodities": all_symbols,
    }
