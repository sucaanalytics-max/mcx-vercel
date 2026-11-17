"""
/api/commodities — commodity signal analytics and iCOMDEX index levels.

  ?view=signals&range=…   sector rotation, commodity momentum, today's lineup
  ?view=icomdex&range=…   iCOMDEX sub-index series + latest table

The old default "prices" view (WTI / Henry Hub / USD-INR from
mcx_commodity_prices) had no caller in the frontend and was removed; a request
without a known view gets a 400.
"""
from datetime import timedelta
from urllib.parse import urlparse, parse_qs

from lib.api_handler import JSONHandler
from lib.mcx_config import (
    now_ist, COMMODITY_MAP, supabase_read_all, safe_float as _f,
    BOUNDED_RANGE_DAYS as RANGE_DAYS, DEFAULT_RANGE,
)

# Legacy fragmented commodity symbols whose data is now consolidated into the
# parent (per lib/mcx_config.COMMODITY_MAP). Old signal rows for these may
# still exist in mcx_commodity_signals until the cleanup SQL has been run; we
# defensively skip them at the API layer so the UI only ever sees parents.
LEGACY_CHILD_SYMBOLS = frozenset(COMMODITY_MAP.keys())


# ═══════════════════════════════════════════════════════════════════════════
#  Commodity Signal Analytics (merged from commodity_analytics.py)
#  Called via ?view=signals
# ═══════════════════════════════════════════════════════════════════════════

def _fetch_all(table, select, limit=20000, where=""):
    """Rows of `table` ascending by trading_date, newest kept if over `limit`
    (every caller filters to a recent window, so the cap is only a backstop).
    The order is total (paginated_order appends the table's unique key)."""
    return supabase_read_all(table, f"?select={select}{where}&order=trading_date.asc",
                             max_rows=limit, newest_first=True)


# RANGE_DAYS: lib/mcx_config.BOUNDED_RANGE_DAYS (no Max: ?range=Max serves the 60D default).


def generate_commodity_analytics(range_key=DEFAULT_RANGE):
    if range_key not in RANGE_DAYS:
        range_key = DEFAULT_RANGE
    window = RANGE_DAYS[range_key]
    ist_now = now_ist()

    # Only the recent window is needed (today's lineup, range-sized rotation/momentum,
    # prev-day movers). Filtering by date avoids the old bug where an ascending
    # row-cap returned the OLDEST rows and reported a years-stale "today".
    cal_days = int(window * 1.55) + 25   # trading→calendar buffer (was fixed 150)
    cutoff = (ist_now - timedelta(days=cal_days)).strftime("%Y-%m-%d")
    signals = _fetch_all(
        "mcx_commodity_signals",
        "trading_date,commodity,commodity_head,"
        "total_turnover_cr,total_oi_value_cr,total_volume_lots,"
        "turnover_zscore,oi_zscore,volume_zscore,"
        "composite_z,commodity_signal,weight_of_turnover",
        where=f"&trading_date=gte.{cutoff}",
    )

    # Drop any legacy fragmented child rows (SILVERM, GOLDM, ...) that pre-date
    # the consolidation fix — their data is now reflected in the parent row.
    signals = [s for s in signals if s["commodity"] not in LEGACY_CHILD_SYMBOLS]
    # Drop OI-only stub rows (from the removed refresh_dhan_oi.py, which ran ahead of bhav_refresh).
    # Without turnover/volume the lineup is meaningless and "latest_date" gets
    # polluted by partial in-progress sessions.
    signals = [s for s in signals if _f(s.get("total_turnover_cr")) and _f(s.get("total_turnover_cr")) > 0]

    if not signals:
        return {"success": False, "error": "No commodity signals available. Run cron_commodity_signals first."}

    # ── 1. Today's Commodity Lineup ──
    latest_date = max(s["trading_date"] for s in signals)
    today_rows = [s for s in signals if s["trading_date"] == latest_date]
    today_rows.sort(key=lambda x: _f(x.get("total_turnover_cr")) or 0, reverse=True)

    exchange_turnover = sum(_f(s.get("total_turnover_cr")) or 0 for s in today_rows)

    today = {
        "date": latest_date,
        "exchange_turnover_cr": round(exchange_turnover, 2),
        "commodities": [],
    }
    for s in today_rows:
        today["commodities"].append({
            "commodity": s["commodity"],
            "head": s["commodity_head"],
            "turnover_cr": round(_f(s.get("total_turnover_cr")) or 0, 2),
            "weight": round(_f(s.get("weight_of_turnover")) or 0, 4),
            "signal": s.get("commodity_signal"),
            "composite_z": _f(s.get("composite_z")),
            "turnover_z": _f(s.get("turnover_zscore")),
            "oi_z": _f(s.get("oi_zscore")),
            "volume_z": _f(s.get("volume_zscore")),
        })

    # ── 2. Sector Rotation ──
    dates = sorted(set(s["trading_date"] for s in signals))
    dates = dates[-window:]

    sector_rotation = []
    for dt in dates:
        day_rows = [s for s in signals if s["trading_date"] == dt]
        total_to = sum(_f(s.get("total_turnover_cr")) or 0 for s in day_rows)
        if total_to <= 0:
            continue
        heads = {}
        for s in day_rows:
            head = s["commodity_head"]
            if head not in heads:
                heads[head] = 0
            heads[head] += _f(s.get("total_turnover_cr")) or 0
        entry = {"date": dt}
        for head, to in heads.items():
            key = head.lower().replace(" ", "_") + "_pct"
            entry[key] = round(to / total_to * 100, 1)
        sector_rotation.append(entry)

    # ── 3. Commodity Momentum ──
    commodity_names = sorted(set(s["commodity"] for s in today_rows))
    commodity_momentum = []
    for c in commodity_names:
        c_rows = [s for s in signals if s["commodity"] == c]
        c_rows.sort(key=lambda x: x["trading_date"])
        recent = c_rows[-window:]
        composites = [_f(r.get("composite_z")) for r in recent]
        valid = [z for z in composites if z is not None]
        if len(valid) < 10:
            continue
        avg_z = sum(valid) / len(valid)
        positive_days = sum(1 for z in valid if z > 0)
        trend = positive_days / len(valid) if valid else 0
        latest_z = composites[-1] if composites[-1] is not None else 0
        commodity_momentum.append({
            "commodity": c,
            "head": recent[-1]["commodity_head"] if recent else "",
            "avg_composite_z": round(avg_z, 3),
            "positive_day_pct": round(trend * 100, 1),
            "latest_z": latest_z,
            "signal": recent[-1].get("commodity_signal") if recent else "NO_DATA",
            "days": len(valid),
        })
    commodity_momentum.sort(key=lambda x: x["avg_composite_z"], reverse=True)

    # ── 4. Top Movers ──
    prev_date = dates[-2] if len(dates) >= 2 else None
    top_movers = []
    if prev_date:
        prev_map = {s["commodity"]: _f(s.get("composite_z"))
                    for s in signals if s["trading_date"] == prev_date}
        for s in today_rows:
            c = s["commodity"]
            curr_z = _f(s.get("composite_z"))
            prev_z = prev_map.get(c)
            if curr_z is not None and prev_z is not None:
                delta = round(curr_z - prev_z, 3)
                top_movers.append({
                    "commodity": c, "head": s["commodity_head"],
                    "prev_z": prev_z, "curr_z": curr_z,
                    "delta_z": delta, "signal": s.get("commodity_signal"),
                })
        top_movers.sort(key=lambda x: abs(x["delta_z"]), reverse=True)

    return {
        "success": True,
        "as_of": ist_now.strftime("%Y-%m-%d %H:%M IST"),
        "today": today,
        "sector_rotation": sector_rotation,
        "commodity_momentum": commodity_momentum,
        "top_movers": top_movers[:10],
        "data_quality": {
            "signal_rows": len(signals),
            "commodities_today": len(today_rows),
            "rotation_days": len(sector_rotation),
            "latest_date": latest_date,
            "range": range_key,
        },
    }


# ═══════════════════════════════════════════════════════════════════════════
#  iCOMDEX Index Levels (BULLDEX & siblings) — ?view=icomdex
#  Source: mcx_icomdex_daily, populated locally by scripts/icomdex_refresh.py.
#  The BULLDEX futures contract is dormant (zero volume since Jun 2026); the
#  index VALUE is the live series, published T+1 by MCX.
# ═══════════════════════════════════════════════════════════════════════════

# Display order: headline sector indices first (BULLDEX is the one users ask
# for), then single-commodity indices alphabetically.
ICOMDEX_ORDER = ["MCXBULLDEX", "MCXMETLDEX", "MCXENRGDEX", "MCXCOMPDEX"]


def generate_icomdex(range_key=DEFAULT_RANGE):
    if range_key not in RANGE_DAYS:
        range_key = DEFAULT_RANGE
    window = RANGE_DAYS[range_key]
    ist_now = now_ist()

    cal_days = int(window * 1.55) + 25   # trading→calendar buffer (as in signals view)
    cutoff = (ist_now - timedelta(days=cal_days)).strftime("%Y-%m-%d")
    try:
        rows = _fetch_all(
            "mcx_icomdex_daily",
            "trading_date,index_code,index_name,close,change_pct",
            where=f"&trading_date=gte.{cutoff}",
        )
    except Exception:
        rows = None   # table missing (PGRST205) or transient DB error
    if not rows:
        return {"success": False,
                "error": "No iCOMDEX data yet. Create the table "
                         "(scripts/sql/create_icomdex_table.sql), then run "
                         "scripts/icomdex_refresh.py locally."}

    by_code = {}
    for r in rows:
        by_code.setdefault(r["index_code"], []).append(r)   # already date-asc

    codes = sorted(by_code.keys(),
                   key=lambda c: (ICOMDEX_ORDER.index(c) if c in ICOMDEX_ORDER else 99, c))

    # Union of trading dates, trimmed to the requested window
    dates = sorted(set(r["trading_date"] for r in rows))[-window:]
    date_idx = {d: i for i, d in enumerate(dates)}

    series = {"dates": dates}
    latest = []
    for code in codes:
        c_rows = [r for r in by_code[code] if r["trading_date"] in date_idx]
        col = [None] * len(dates)
        for r in c_rows:
            col[date_idx[r["trading_date"]]] = _f(r.get("close"))
        series[code] = col
        if not c_rows:
            continue
        last = c_rows[-1]
        first_close = next((_f(r.get("close")) for r in c_rows if _f(r.get("close"))), None)
        last_close = _f(last.get("close"))
        range_chg = (round((last_close / first_close - 1) * 100, 2)
                     if first_close and last_close else None)
        latest.append({
            "code": code,
            "name": last.get("index_name") or code,
            "date": last["trading_date"],
            "close": last_close,
            "change_pct": _f(last.get("change_pct")),
            "range_change_pct": range_chg,
        })

    return {
        "success": True,
        "as_of": ist_now.strftime("%Y-%m-%d %H:%M IST"),
        "latest": latest,
        "series": series,
        "data_quality": {
            "rows": len(rows),
            "indices": len(codes),
            "latest_date": dates[-1] if dates else None,
            "range": range_key,
        },
    }


class handler(JSONHandler):
    default_cache = "public, max-age=1800"

    def do_GET(self):
        try:
            qs = parse_qs(urlparse(self.path).query)
            view = qs.get("view", [None])[0]
            if view == "signals":
                range_key = qs.get("range", [DEFAULT_RANGE])[0]
                data = generate_commodity_analytics(range_key=range_key)
            elif view == "icomdex":
                range_key = qs.get("range", [DEFAULT_RANGE])[0]
                data = generate_icomdex(range_key=range_key)
            else:
                self.send_json({"success": False, "error": "Unknown view: use ?view=signals or ?view=icomdex"}, 400)
                return
            self.send_json(data)
        except Exception as e:
            self.send_json({"success": False, "error": str(e)[:200]}, 500)
