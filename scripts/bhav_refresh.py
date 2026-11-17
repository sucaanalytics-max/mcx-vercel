#!/usr/bin/env python3
"""
MCX Daily Revenue Refresh — scheduled task
Fetches MCX daily revenue data and upserts to Supabase.
Run daily after 7:30 PM IST (market close + buffer).

Priority chain:
  1. relay EOD data (direct PremiumValue from live API) — authoritative
  2. MCX Historical Detailed Report (GetHistoricalDataDetails) — exact premium

Uses curl_cffi with Chrome TLS impersonation to bypass Akamai bot detection.

Usage:
  python3 scripts/bhav_refresh.py              # refresh today + missing last 5 days
  python3 scripts/bhav_refresh.py 2026-02-20   # refresh specific date
  python3 scripts/bhav_refresh.py --backfill 30 # backfill last 30 days
  python3 scripts/bhav_refresh.py --commodity 2026-11-10          # breakdown only
  python3 scripts/bhav_refresh.py --commodity-recent 4 --retry-until 06:55
"""
import sys, os, time
from curl_cffi import requests as cfreq  # noqa: F401  (fail fast without curl_cffi; sessions: lib.curl_session)
from datetime import datetime, timedelta

# Add parent to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lib.mcx_config import (  # noqa: E402
    get_day_type, FUTURES_RATE, OPTIONS_RATE, NONTX_DAILY,
)
# One calendar for every job (lib/calendar.py): a local copy would silently
# miss the 2027 dates when they are added there.
from lib.calendar import (  # noqa: E402,F401  (is_trading_day / MCX_HOLIDAYS: module API used by tests)
    session_end, now_ist, is_trading_day, has_session, MCX_HOLIDAYS, MCX_SPECIAL_SESSIONS,
)

# ── Config ──────────────────────────────────────────────────────────────────
# Supabase access goes through lib.supabase (keys, headers, batching); fee
# rates come from lib.mcx_config.
MCX_TIMEOUT = 30
MCX_MAX_RETRIES = 2

from lib.curl_session import new_session  # noqa: E402
from lib.supabase import (  # noqa: E402
    supabase_read, supabase_read_all, supabase_upsert, upsert_batches,
)


def check_relay_eod(date_iso):
    """Check if relay has already captured EOD data for this date.
    Returns the row dict if found (source='mcx_relay_eod'), else None."""
    try:
        rows = supabase_read("mcx_daily_revenue",
                             f"?trading_date=eq.{date_iso}&source=eq.mcx_relay_eod&limit=1")
        return rows[0] if rows else None
    except Exception:
        return None


# Shared curl_cffi session for Chrome TLS impersonation (bypasses Akamai)
_hist_session = None

def _get_hist_session(force_new=False):
    global _hist_session
    if _hist_session is None or force_new:
        _hist_session = new_session("https://www.mcxindia.com/market-data/historical-data",
                                    timeout=MCX_TIMEOUT)
    return _hist_session


# MCX renamed the JSON keys when it moved the endpoint (Jun-2026). Accept both
# spellings so a rollback on their side doesn't break us again.
_FIELD_ALIASES = {
    "InstrumentName": ("Instrumentname", "InstrumentName"),
    "Symbol":         ("Commodity", "Symbol"),
    "CommodityHead":  ("Segment", "CommodityHead"),
    "TradedContract": ("TradedContract", "NoOfContract"),
    "Volume":         ("Quantity", "Volume"),
}


def _field(row, logical, default=None):
    """Read a logical field from a raw MCX row under either key spelling."""
    for key in _FIELD_ALIASES.get(logical, (logical,)):
        if key in row and row[key] not in (None, ""):
            return row[key]
    return default


def _fetch_mcx_raw(date_iso):
    """Fetch raw contract-level rows from MCX Historical API for one date.
    Returns (raw_rows, None) on success or (None, error_msg) on failure."""
    date_compact = date_iso.replace("-", "")
    params = {
        "culture": "en-US", "GroupBy": "D", "Segment": "ALL",
        "CommodityHead": "ALL", "Commodity": "ALL",
        "Startdate": date_compact, "EndDate": date_compact,
        "InstrumentName": "ALL",
    }
    # MCX migrated to Sitefinity (Jun 2026): the historical report is now a
    # root-scoped GET. The old backpage.aspx POST returns an HTML 404 (HTTP 200)
    # for every date, which silently looked like "no data" for all backfills.
    url = "https://www.mcxindia.com/GetHistoricalDataDetails"

    for attempt in range(MCX_MAX_RETRIES + 1):
      try:
        session = _get_hist_session(force_new=(attempt > 0))
        resp = session.get(url, params=params, headers={
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://www.mcxindia.com/market-data/historical-data",
        }, timeout=MCX_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        rows = (data.get("Data") or {}).get("Data")
        if not rows or len(rows) < 5:
            return None, "no data"
        return rows, None
      except Exception as e:
        if attempt < MCX_MAX_RETRIES:
            print(f"  ⓘ Attempt {attempt+1} failed: {e}, retrying in {3*(attempt+1)}s...")
            time.sleep(3 * (attempt + 1))
            continue
        return None, str(e)


def _aggregate_commodity_rows(date_iso, raw_rows):
    """Aggregate raw contract-level MCX rows into commodity-level summaries.
    Returns list of dicts for mcx_commodity_daily table."""
    groups = {}
    for r in raw_rows:
        symbol = str(_field(r, "Symbol", "")).strip()
        inst = str(_field(r, "InstrumentName", "")).strip()
        chead = str(_field(r, "CommodityHead", "")).strip()
        if not symbol or not inst:
            continue
        key = (symbol, inst)
        if key not in groups:
            groups[key] = {"commodity": symbol, "commodity_head": chead,
                           "instrument_type": inst, "contracts": 0,
                           "volume_lots": 0, "turnover_lacs": 0.0,
                           "premium_turnover_lacs": 0.0, "open_interest": 0,
                           "oi_value_lacs": 0.0}
        g = groups[key]
        g["contracts"] += int(_field(r, "TradedContract", 0) or 0)
        g["volume_lots"] += int(_field(r, "Volume", 0) or 0)
        g["turnover_lacs"] += float(r.get("TotalValue", 0) or 0)
        prem_str = str(r.get("PremiumTurnover", "-")).strip()
        # Futures have no premium. Since ~21 Sep 2026 MCX sometimes copies
        # TotalValue into PremiumTurnover on futures rows (10 Nov 06:31: all 22
        # traded FUTCOM rows, Rs 40,289 Cr = their notional); 610 FUT rows over
        # 25 dates were stored with that value before this guard.
        if inst.upper().startswith("OPT") and prem_str not in ("-", "", "0"):
            try: g["premium_turnover_lacs"] += float(prem_str)
            except ValueError: pass
        g["open_interest"] += int(r.get("OpenInterest", 0) or 0)
        oi_val = r.get("OIValue", 0)
        if oi_val:
            try: g["oi_value_lacs"] += float(oi_val)
            except (ValueError, TypeError): pass

    rows_out = []
    for key, g in groups.items():
        rows_out.append({
            "trading_date": date_iso,
            "commodity": g["commodity"],
            "commodity_head": g["commodity_head"] or None,
            "instrument_type": g["instrument_type"],
            "contracts": g["contracts"],
            "volume_lots": g["volume_lots"],
            "turnover_cr": round(g["turnover_lacs"] / 100, 2),
            "premium_turnover_cr": round(g["premium_turnover_lacs"] / 100, 2)
                if g["premium_turnover_lacs"] > 0 else None,
            "open_interest": g["open_interest"],
            "oi_value_cr": round(g["oi_value_lacs"] / 100, 2),
            "source": "mcx_historical",
        })
    return rows_out


# ── Completeness gate ─────────────────────────────────────────────────────────
# MCX publishes a day's Historical Detailed Report in pieces. Probed for
# 10 Nov 2026 (close 23:30): 23:48 no rows; 00:35 and 02:00 the endpoint
# returned zero rows for EVERY date (9 Nov too); 04:31 all 42 rows and
# contracts, but no option premium; 06:31 complete. The anon key can INSERT
# but not UPDATE mcx_commodity_daily, so a partial write is permanent unless
# SUPABASE_SERVICE_KEY is set. Write only complete breakdowns.
MIN_BREAKDOWN_ROWS = 20
MIN_OPT_PREMIUM_SHARE = 0.9   # traded option rows that must carry premium


def breakdown_completeness(rows):
    """(ok, reason) for aggregated commodity rows from _aggregate_commodity_rows.

    Complete = enough rows, futures turnover present, and (if any options
    traded) nearly every traded option row carries premium."""
    if not rows or len(rows) < MIN_BREAKDOWN_ROWS:
        return False, f"only {len(rows or [])} commodity rows"
    fut = [r for r in rows if str(r["instrument_type"]).upper().startswith("FUT")]
    if sum(r["turnover_cr"] or 0 for r in fut) <= 0:
        return False, "futures turnover missing"
    opt = [r for r in rows if str(r["instrument_type"]).upper().startswith("OPT")
           and (r["contracts"] or 0) > 0]
    if opt:
        with_prem = sum(1 for r in opt if r["premium_turnover_cr"])
        if with_prem < MIN_OPT_PREMIUM_SHARE * len(opt):
            return False, f"options premium missing ({with_prem}/{len(opt)} traded option rows)"
    return True, ""


_OI_FIELDS = ("open_interest", "oi_value_cr")


def _read_existing_oi(dates):
    """{(trading_date, commodity, instrument_type): {open_interest, oi_value_cr}}
    for stored rows on `dates` that carry non-zero OI. Raises on a read error
    (the caller then writes nothing)."""
    out = {}
    for d in sorted(set(dates)):
        for r in supabase_read(
                "mcx_commodity_daily",
                f"?trading_date=eq.{d}&open_interest=gt.0"
                f"&select=trading_date,commodity,instrument_type,open_interest,oi_value_cr"
                f"&order=commodity.asc,instrument_type.asc&limit=1000", timeout=15):
            out[(r["trading_date"], r["commodity"], r["instrument_type"])] = {
                k: r[k] for k in _OI_FIELDS}
    return out


_bhav_session = None


def fill_futures_oi(rows, date_iso, fetch=None):
    """Put MCX Bhavcopy open interest on the futures rows (FUTCOM / FUTIDX) of
    a freshly aggregated breakdown. The historical report has none, so without
    this every new row starts at OI = 0 until backfill_commodity_oi runs.

    fetch(date_iso) -> {(symbol, instrument): {open_interest, oi_value_cr}}
    (default: lib.mcx_bhavcopy over a shared session). Soft: on any error, or
    when MCX has no Bhavcopy for the date yet, the rows are returned unchanged
    and preserve_existing_oi / the nightly backfill cover them."""
    global _bhav_session
    try:
        if fetch is None:
            from lib.mcx_bhavcopy import new_bhavcopy_session, fetch_futures_oi
            if _bhav_session is None:
                _bhav_session = new_bhavcopy_session(timeout=MCX_TIMEOUT)
            fetch = lambda d: fetch_futures_oi(_bhav_session, d)[0]   # noqa: E731
        oi = fetch(date_iso)
    except Exception as e:   # noqa: BLE001
        print(f"  ⓘ {date_iso}: Bhavcopy OI unavailable ({type(e).__name__}); OI left for the nightly backfill")
        return rows
    if not oi:
        return rows
    out = []
    for r in rows:
        new = oi.get((r["commodity"], r["instrument_type"]))
        if new and new["open_interest"]:
            r = {**r, "open_interest": new["open_interest"],
                 "oi_value_cr": new["oi_value_cr"] if new["oi_value_cr"] is not None else 0}
        out.append(r)
    return out


def preserve_existing_oi(rows, existing):
    """MCX's historical report carries no open interest, so freshly aggregated
    rows have open_interest = 0 unless fill_futures_oi found the Bhavcopy. The
    nightly OI backfill (backfill_commodity_oi, MCX Bhavcopy) writes the real
    figure onto those rows afterwards. A later re-write of the
    breakdown (merge-duplicates, which can UPDATE once SUPABASE_SERVICE_KEY is
    set) must not reset it to zero: keep the stored OI wherever the new row has
    none. Returns new row dicts; a row with its own non-zero OI is unchanged."""
    out = []
    for r in rows:
        key = (r["trading_date"], r["commodity"], r["instrument_type"])
        if not r.get("open_interest") and key in existing:
            r = {**r, **existing[key]}
        out.append(r)
    return out


def _upsert_commodity_batch(rows, read_existing_oi=None):
    """Upsert commodity rows to mcx_commodity_daily.

    merge-duplicates needs UPDATE rights. With the anon key the RLS USING
    clause rejects it (42501) and a plain PATCH silently matches zero rows, so
    existing dates can only be corrected with SUPABASE_SERVICE_KEY set.

    Stored non-zero open interest is carried into the payload (see
    preserve_existing_oi). If it can't be read, nothing is written: a write
    without it could zero the day's OI."""
    try:
        existing = (read_existing_oi or _read_existing_oi)(r["trading_date"] for r in rows)
    except Exception as e:
        print(f"  ⚠ commodity upsert skipped: could not read stored OI ({e})")
        return False
    rows = preserve_existing_oi(rows, existing)
    errors = upsert_batches("mcx_commodity_daily", rows, batch=max(len(rows), 1))
    for detail in errors:
        print(f"  ⚠ commodity upsert failed: {detail}")
        if "42501" in detail:
            print("      RLS blocked the write: set SUPABASE_SECRET_KEY "
                  "(worktrees have no .env — run writes from the main checkout).")
    return not errors


def fetch_mcx_historical(date_iso, write_commodity=True):
    """Fetch daily revenue from MCX Historical Detailed Report API.
    Returns revenue dict with exact PremiumTurnover (no proxy), or None on failure.
    Also upserts commodity-level breakdown to mcx_commodity_daily unless
    write_commodity is False (tests and dry runs)."""
    raw_rows, err = _fetch_mcx_raw(date_iso)
    if raw_rows is None:
        if err and err != "no data":
            print(f"  ⓘ Historical API unavailable: {err}")
        return None

    # ── Commodity-level upsert (complete breakdowns only) ──
    commodity_rows = _aggregate_commodity_rows(date_iso, raw_rows)
    if commodity_rows and write_commodity:
        complete, why = breakdown_completeness(commodity_rows)
        if not complete:
            print(f"  ⓘ {date_iso}: commodity breakdown not written — {why}")
        elif _upsert_commodity_batch(fill_futures_oi(commodity_rows, date_iso)):
            print(f"  ✓ {len(commodity_rows)} commodity rows → mcx_commodity_daily")

    # ── Exchange-wide aggregate (existing logic) ──
    fut_notl_lacs = 0.0
    opt_prem_lacs = 0.0
    opt_notl_lacs = 0.0
    n_fut = 0
    n_opt = 0

    for r in raw_rows:
        inst = str(_field(r, "InstrumentName", "")).strip().upper()
        total_val = float(r.get("TotalValue", 0) or 0)
        prem_str = str(r.get("PremiumTurnover", "-")).strip()

        if inst in ("FUTCOM", "FUTIDX"):
            fut_notl_lacs += total_val
            if total_val > 0:
                n_fut += 1
        elif inst in ("OPTFUT", "OPTIDX"):
            opt_notl_lacs += total_val
            if prem_str != "-" and prem_str != "":
                try:
                    opt_prem_lacs += float(prem_str)
                except ValueError:
                    pass
            if total_val > 0:
                n_opt += 1

    if fut_notl_lacs <= 0 and opt_prem_lacs <= 0:
        return None

    fn_cr = fut_notl_lacs / 100
    op_cr = opt_prem_lacs / 100
    fut_rev = 2 * fn_cr * FUTURES_RATE / 1e7
    opt_rev = 2 * op_cr * OPTIONS_RATE / 1e7
    total = fut_rev + opt_rev + NONTX_DAILY

    return {
        "fut_notl_cr": round(fn_cr, 2),
        "opt_prem_cr": round(op_cr, 2),
        "fut_rev_cr": round(fut_rev, 4),
        "opt_rev_cr": round(opt_rev, 4),
        "nontx_rev_cr": NONTX_DAILY,
        "total_rev_cr": round(total, 4),
        "active_futures": n_fut,
        "active_options": n_opt,
    }


def get_existing_dates():
    """Dates (and their source) of the last 200 rows of mcx_daily_revenue."""
    rows = supabase_read("mcx_daily_revenue",
                         "?select=trading_date,source&order=trading_date.desc&limit=200")
    return {r["trading_date"]: r.get("source", "") for r in rows}


def get_commodity_dates(since_iso):
    """Distinct trading_dates that already have a REAL breakdown in
    mcx_commodity_daily on/after since_iso. Paginated (Supabase caps at 1000
    rows/request).

    The turnover_cr > 0 filter matters: the Dhan OI jobs insert OI-only stub
    rows (turnover_cr = 0), often before bhav_refresh runs. Counting those as
    coverage made ensure_commodity_breakdown() skip the date for good (found
    2026-05-27/28)."""
    rows = supabase_read_all(
        "mcx_commodity_daily",
        f"?select=trading_date&trading_date=gte.{since_iso}&turnover_cr=gt.0&order=trading_date.asc",
        max_rows=None, timeout=15)
    dates = {r["trading_date"] for r in rows}
    return dates


# commodity_breakdown() outcomes, and the --commodity CLI exit code for each.
BREAKDOWN_WRITTEN = "written"
BREAKDOWN_PRESENT = "present"            # a real breakdown is already stored
BREAKDOWN_NOT_PUBLISHED = "not_published"  # MCX returned no rows yet
BREAKDOWN_INCOMPLETE = "incomplete"      # rows, but a leg is missing (gate)
BREAKDOWN_FETCH_ERROR = "fetch_error"
BREAKDOWN_WRITE_FAILED = "write_failed"
EXIT_OK, EXIT_ERROR, EXIT_NOT_READY = 0, 1, 3
_EXIT_CODES = {
    BREAKDOWN_WRITTEN: EXIT_OK, BREAKDOWN_PRESENT: EXIT_OK,
    BREAKDOWN_NOT_PUBLISHED: EXIT_NOT_READY, BREAKDOWN_INCOMPLETE: EXIT_NOT_READY,
    BREAKDOWN_FETCH_ERROR: EXIT_ERROR, BREAKDOWN_WRITE_FAILED: EXIT_ERROR,
}


def commodity_breakdown(date_iso, present=None):
    """Write the per-commodity breakdown for date_iso if MCX has a complete one.

    Returns (status, n_rows, detail); status is one of the BREAKDOWN_* values.
    `present` is a set of dates already covered (get_commodity_dates); pass it
    to skip the fetch for those. Idempotent."""
    if present is not None and date_iso in present:
        return BREAKDOWN_PRESENT, 0, ""
    raw_rows, err = _fetch_mcx_raw(date_iso)
    if raw_rows is None:
        if err == "no data":
            return BREAKDOWN_NOT_PUBLISHED, 0, "MCX historical report has no rows yet"
        return BREAKDOWN_FETCH_ERROR, 0, str(err)[:200]
    commodity_rows = _aggregate_commodity_rows(date_iso, raw_rows)
    complete, why = breakdown_completeness(commodity_rows)
    if not complete:
        return BREAKDOWN_INCOMPLETE, len(commodity_rows), why
    if not _upsert_commodity_batch(fill_futures_oi(commodity_rows, date_iso)):
        return BREAKDOWN_WRITE_FAILED, len(commodity_rows), "upsert to mcx_commodity_daily failed"
    contracts = sum(r["contracts"] for r in commodity_rows)
    return BREAKDOWN_WRITTEN, len(commodity_rows), f"{contracts:,} contracts"


def ensure_commodity_breakdown(date_iso, present=None):
    """Guarantee mcx_commodity_daily has the per-commodity breakdown for date_iso.

    The live relay writes only the exchange aggregate (mcx_daily_revenue), never
    the per-commodity rows — so without this, every relay-EOD day is missing from
    mcx_commodity_daily and the commodity dashboard / signals silently lose data.
    Idempotent (merge-duplicates). Returns the number of rows upserted."""
    status, n, detail = commodity_breakdown(date_iso, present)
    if status == BREAKDOWN_WRITTEN:
        print(f"  ✓ {date_iso}: {n} commodity rows → mcx_commodity_daily ({detail})")
        return n
    if status != BREAKDOWN_PRESENT:
        print(f"  ⓘ {date_iso}: commodity breakdown {status}: {detail}")
    return 0


def run_commodity_cli(dates, retry_until=None, retry_every_min=20, now=None, sleep=time.sleep):
    """Write breakdowns for `dates`, retrying not-yet-ready ones every
    retry_every_min until the IST clock time retry_until ("HH:MM", same
    morning). Returns the exit code: 0 all present/written, 3 some still not
    published/incomplete, 1 a fetch or write error on the last pass."""
    now = now or now_ist
    pending = list(dict.fromkeys(dates))
    if not pending:
        print("No dates to check.")
        return EXIT_OK
    deadline = None
    if retry_until:
        hh, mm = (int(x) for x in retry_until.split(":"))
        deadline = hh * 60 + mm
    while True:
        try:
            present = get_commodity_dates(min(pending))
        except Exception as e:
            print(f"  ⚠ coverage read failed ({e}); fetching all pending dates")
            present = set()
        had_error, still = False, []
        for d in pending:
            status, n, detail = commodity_breakdown(d, present)
            print(f"  {d}: {status}" + (f" — {n} rows, {detail}" if n or detail else ""))
            code = _EXIT_CODES[status]
            if code != EXIT_OK:
                still.append(d)
                had_error = had_error or code == EXIT_ERROR
        if not still:
            return EXIT_OK
        t = now()
        cur = t.hour * 60 + t.minute
        if deadline is None or cur + retry_every_min > deadline:
            return EXIT_ERROR if had_error else EXIT_NOT_READY
        print(f"  {len(still)} date(s) not ready — retrying in {retry_every_min} min "
              f"(until {retry_until} IST)")
        sleep(retry_every_min * 60)
        pending = still


def recent_session_dates(days, today=None):
    """Sessions (trading days + special sessions) in the `days` calendar days
    before today, oldest first. Today is excluded: its session isn't over."""
    today = today or now_ist().date()
    out = []
    for i in range(days, 0, -1):
        d = today - timedelta(days=i)
        if has_session(d):
            out.append(d.strftime("%Y-%m-%d"))
    return out


def upsert_rows(rows):
    """Upsert revenue rows to mcx_daily_revenue (merge on trading_date); returns
    the written rows. Raises on an HTTP error."""
    return supabase_upsert("mcx_daily_revenue", rows, on_conflict="trading_date", timeout=15)


def fetch_and_compute(date_iso, commodity_present=None):
    """Fetch daily revenue for one date using the priority chain:
    1. relay EOD (already in Supabase) — skip revenue if found
    2. MCX Historical Detailed Report — exact premium, no proxy

    Regardless of the revenue source, ensures the per-commodity breakdown exists
    in mcx_commodity_daily (the relay never writes it)."""

    # Priority 1: Check if relay already captured authoritative EOD revenue
    relay = check_relay_eod(date_iso)
    if relay:
        # Relay wrote the exchange aggregate but NOT the per-commodity rows —
        # backfill those so mcx_commodity_daily stays whole (root-cause fix).
        ensure_commodity_breakdown(date_iso, commodity_present)
        print(f"  ✓ {date_iso}: relay EOD found ({relay['total_rev_cr']} Cr) — revenue skipped")
        return None  # Revenue already in Supabase with authoritative data

    # Priority 2: MCX Historical Detailed Report (exact PremiumTurnover).
    # fetch_mcx_historical() also upserts the commodity breakdown internally.
    hist = fetch_mcx_historical(date_iso)
    if hist:
        # A ~1-hour special session books well under Rs 1 Cr (2024 Muhurat: 0.4964).
        rev = hist["total_rev_cr"]
        too_low = rev <= 0 if date_iso in MCX_SPECIAL_SESSIONS else rev < 1.0
        if too_low or rev > 50.0:
            print(f"  ⚠ {date_iso}: historical API revenue {hist['total_rev_cr']} out of range")
        else:
            dt = datetime.strptime(date_iso, "%Y-%m-%d")
            return {
                "trading_date": date_iso,
                "day_type": get_day_type(dt),
                "source": "mcx_historical",
                "data_source": "mcx_historical_detailed",
                "is_actual": True,
                **hist,
            }

    print(f"  ✗ {date_iso}: no source available (relay EOD + historical API both missed)")
    return None


def refresh(lookback_days=5, force_dates=None):
    """Main refresh: find missing dates and fill them.

    A date is a target if it's missing from mcx_daily_revenue (needs revenue) OR
    from mcx_commodity_daily (needs the per-commodity breakdown). The latter is
    what relay-EOD days lack."""
    existing = get_existing_dates()
    print(f"mcx_daily_revenue has {len(existing)} dates (latest: {max(existing) if existing else 'none'})")

    if force_dates:
        targets = force_dates
        since_iso = min(force_dates)
    else:
        today = now_ist().date()
        since_iso = (today - timedelta(days=lookback_days + 5)).strftime("%Y-%m-%d")

    commodity_present = get_commodity_dates(since_iso)
    print(f"mcx_commodity_daily covers {len(commodity_present)} dates since {since_iso}")

    if not force_dates:
        now = now_ist()
        today = now.date()
        # Today's figures are partial until the session closes; api/quarterly.py
        # would count a partial row as a finished day, so wait until 20 min after.
        today_closed = now.hour * 60 + now.minute >= session_end(today) + 20
        targets = []
        for i in range(lookback_days):
            d = today - timedelta(days=i)
            iso = d.strftime("%Y-%m-%d")
            if i == 0 and not today_closed:
                continue
            if has_session(d) and (iso not in existing or iso not in commodity_present):
                targets.append(iso)

    if not targets:
        print("All dates up to date — nothing to refresh.")
        return

    print(f"Dates to fetch: {sorted(targets)}")
    rows = []
    for date_iso in sorted(targets):
        print(f"Fetching {date_iso}...")
        row = fetch_and_compute(date_iso, commodity_present)
        if row:
            rows.append(row)
            print(f"  ✓ {date_iso}: {row['total_rev_cr']} Cr "
                  f"({row['active_futures']}F/{row['active_options']}O) [{row['source']}]")

    if rows:
        print(f"\nUpserting {len(rows)} rows...")
        result = upsert_rows(rows)
        print(f"Done — {len(result)} rows upserted.")
    else:
        print("No new data fetched.")


def _opt(name, default=None):
    if name in sys.argv:
        i = sys.argv.index(name)
        if i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return default


if __name__ == "__main__":
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        if arg in ("--commodity", "--commodity-recent"):
            # Per-commodity breakdown only (mcx_commodity_daily), complete
            # reports only. Exit 0 = written/present, 3 = not ready yet,
            # 1 = fetch/write error.
            #   --commodity 2026-11-10 [--retry-until 06:55 --retry-every 20]
            #   --commodity-recent 4   [--retry-until 06:55 --retry-every 20]
            val = sys.argv[2] if len(sys.argv) > 2 else ""
            dates = ([val] if arg == "--commodity"
                     else recent_session_dates(int(val or 4)))
            sys.exit(run_commodity_cli(dates, retry_until=_opt("--retry-until"),
                                       retry_every_min=int(_opt("--retry-every", 20))))
        if arg == "--backfill":
            days = int(sys.argv[2]) if len(sys.argv) > 2 else 30
            refresh(lookback_days=days)
        else:
            # Specific date(s)
            refresh(force_dates=[arg])
    else:
        refresh(lookback_days=5)
