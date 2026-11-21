#!/usr/bin/env python3
"""
MCX Live Relay — scheduled task
Fetches MCX MarketWatch data locally (bypasses cloud IP blocks),
computes intraday revenue projection, and upserts snapshot to Supabase.
The Vercel /api/refresh GET endpoint reads from Supabase to serve the frontend.

Runs every 15 minutes during MCX trading hours (09:00 to 23:30 IST, or 23:55
while the US is on standard time; see lib.mcx_config.session_end). On part-day
sessions it runs only the hours that trade: 17:00 to the close when the morning
is closed, 09:00–17:00 when the evening is (lib.mcx_config.session_shape).
Designed to be called by a cron/scheduled task starting at 8:00 AM IST.

Usage:
  python3 scripts/mcx_relay.py          # single snapshot, push to Supabase
  python3 scripts/mcx_relay.py --loop   # loop every 15 min until session ends
  python3 scripts/mcx_relay.py --contract-eod  # per-contract EOD file now (after the close)
"""
import sys, os, time, urllib.request, urllib.error, csv, re, tempfile

# Force UTF-8 output on Windows (cp1252 can't handle Unicode symbols)
if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
from curl_cffi import requests as cfreq  # noqa: F401  (fail fast without curl_cffi; sessions: lib.curl_session)
from datetime import datetime, timedelta, timezone
from collections import defaultdict
import concurrent.futures as _cf
import threading

# Hard wall-clock guard for curl_cffi calls — libcurl's own timeout has been
# observed to hang for hours when DNS or a half-open socket stalls. Abandoned
# threads leak (libcurl can't be cancelled cleanly), so callers must drop the
# stale session and rebuild before retrying.
_NET_POOL = _cf.ThreadPoolExecutor(max_workers=4, thread_name_prefix="mcx-net")


def _hard_call(fn, *args, hard_timeout=25, **kwargs):
    fut = _NET_POOL.submit(fn, *args, **kwargs)
    try:
        return fut.result(timeout=hard_timeout)
    except _cf.TimeoutError:
        raise TimeoutError(f"network call exceeded {hard_timeout}s wall-clock")

# Add parent to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ── Import shared config from single source of truth ────────────────────────
from lib.mcx_config import (
    FUTURES_RATE, OPTIONS_RATE, NONTX_DAILY, SESSION_START, fmt_ist_minutes,
    session_shape, session_clock, SESSION_FULL, SESSION_CLOSED,
    DAY_MULTIPLIER, is_trading_day,
    now_ist, get_day_type, project_full_day, calc_revenue, calc_uncertainty,
    check_regime_drift, supabase_upsert, SUPABASE_WRITE_ROLE,
    calendar_status, supabase_read,
)

from lib.curl_session import new_session
from lib.cron_margins import (
    _download_xls, _parse_xls,
    sb_upsert as margin_upsert,
)
try:
    from lib.cron_oi_participants import (
        _build_url as oi_build_url,
        _parse_participants as oi_parse_participants,
        sb_upsert as oi_upsert,
    )
    _HAS_OI = True
except ImportError:
    _HAS_OI = False

LOOP_INTERVAL = 900  # 15 minutes in seconds
# Minutes after the close to take the final snapshot and EOD capture. The loop
# shortens its sleep to land here, so the EOD row is taken at the real close
# (23:30 or 23:55) instead of up to 15 min later, and, in winter, before
# midnight rolls the date over.
EOD_DELAY_MIN = 2
# If the snapshot after the close keeps failing, retry for up to
# EOD_FALLBACK_MIN, then capture EOD directly. Never later than LATEST_EOD_MIN:
# after a 23:55 close there are only minutes left before midnight.
EOD_FALLBACK_MIN = 15
LATEST_EOD_MIN = 23 * 60 + 57
RELAY_ID = os.environ.get("MCX_RELAY_ID", "default")

# ── MCX API (curl_cffi with Chrome TLS impersonation) ───────────────────────
_mw_session = None
_hist_session = None
# Newest target first; older curl_cffi builds (e.g. 0.13 under the system
# python that daily_verify may run on) reject chrome142 with ImpersonateError.


def _new_session(warm_url):
    """curl_cffi session warmed on warm_url (lib.curl_session), the warm-up
    under the relay's hard wall-clock guard."""
    return new_session(warm_url, get=lambda s, u, t: _hard_call(s.get, u, timeout=20, hard_timeout=25))


def _get_mw_session(force_new=False):
    """Shared curl_cffi session for MarketWatch API (bypasses Akamai)."""
    global _mw_session
    if _mw_session is None or force_new:
        _mw_session = _new_session("https://www.mcxindia.com/market-data/market-watch")
    return _mw_session


def _get_hist_session(force_new=False):
    """Shared curl_cffi session for Historical API (bypasses Akamai)."""
    global _hist_session
    if _hist_session is None or force_new:
        _hist_session = _new_session("https://www.mcxindia.com/market-data/historical-data")
    return _hist_session


def _reset_net_state():
    """Recover from leaked/hung network threads. A `_hard_call` timeout abandons
    its worker thread (libcurl can't be cancelled), permanently consuming a slot
    in the 4-worker pool; enough leaks and every later call blocks. This rebuilds
    the pool (stuck threads die on their own) and forces fresh curl sessions."""
    global _NET_POOL
    old = _NET_POOL
    _NET_POOL = _cf.ThreadPoolExecutor(max_workers=4, thread_name_prefix="mcx-net")
    try:
        old.shutdown(wait=False, cancel_futures=True)
    except Exception:
        pass
    for _reset in (_get_mw_session, _get_hist_session):
        try:
            _reset(force_new=True)
        except Exception:
            pass


def fetch_market_watch(session=None, timeout=18):
    """Call MCX GetMarketWatch API via curl_cffi. Auto-retries with fresh session on failure."""
    if session is None:
        session = _get_mw_session()
    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/json",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": "https://www.mcxindia.com/market-data/market-watch",
    }
    # 2026-06: MCX migrated to Sitefinity. The old ASP.NET WebMethod
    # (POST backpage.aspx/GetMarketWatch, returning {"d":...}) now 404s.
    # New endpoint is a page-scoped GET returning {"data":{"Data":[...]}}.
    url = "https://www.mcxindia.com/market-data/market-watch/GetMarketWatch?culture=en-US"
    hard = max(timeout + 5, 25)
    try:
        resp = _hard_call(session.get, url, headers=headers,
                          timeout=timeout, hard_timeout=hard)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"  ⚠ MarketWatch call failed ({e}), retrying with fresh session...")
        session = _get_mw_session(force_new=True)
        resp = _hard_call(session.get, url, headers=headers,
                          timeout=timeout, hard_timeout=hard)
        resp.raise_for_status()
        return resp.json()


def extract_notionals(raw_json):
    # New Sitefinity shape: {"data":{"Data":[...]}}; fall back to legacy {"d":...}.
    contracts = (raw_json.get("data") or raw_json.get("d") or {}).get("Data", [])
    futures = [c for c in contracts if c.get("InstrumentName") == "FUTCOM" and c.get("Volume", 0) > 0]
    options = [c for c in contracts if c.get("InstrumentName") == "OPTFUT" and c.get("Volume", 0) > 0]
    fut_notl = sum(c.get("NotionalValue", 0) for c in futures) / 100
    opt_notl = sum(c.get("NotionalValue", 0) for c in options) / 100
    opt_prem = sum(c.get("PremiumValue", 0) for c in options) / 100
    return fut_notl, opt_notl, opt_prem, futures, options



def run_snapshot():
    """Fetch MCX data, compute projection, push to Supabase. Returns True on success."""
    capture = now_ist()
    current_min = capture.hour * 60 + capture.minute
    # elapsed counts from 09:00 (what snapshots store); traded counts from the
    # day's real open, so it differs on a part-day session.
    elapsed, traded, shape = session_clock(capture)
    open_min, close, total = shape["start"], shape["end"], shape["total"]

    # Check if within trading window (with 30-min pre-buffer)
    if (shape["type"] == SESSION_CLOSED
            or current_min < open_min - 30 or current_min > close + 15):
        print(f"  ⏸ {capture.strftime('%H:%M IST')} — outside trading hours "
              f"({_hours(shape)}), skipping")
        return False

    time_pct = traded / total

    # Fetch market data via curl_cffi session
    print(f"  Fetching market data...")
    session = _get_mw_session()
    raw1 = fetch_market_watch(session)

    # Dual call for late session
    raw2 = None
    if time_pct > 0.80:
        try:
            time.sleep(2)
            raw2 = fetch_market_watch(session)
        except Exception:
            pass

    fut_n1, opt_n1, opt_p1, futures, options = extract_notionals(raw1)
    if raw2:
        fut_n2, opt_n2, opt_p2, _, _ = extract_notionals(raw2)
        fut_notl = max(fut_n1, fut_n2)
        opt_notl = max(opt_n1, opt_n2)
        opt_prem = max(opt_p1, opt_p2)
        dual_call = True
    else:
        fut_notl, opt_notl, opt_prem = fut_n1, opt_n1, opt_p1
        dual_call = False

    # Stale data guard: if early in session but notionals are huge, API returned
    # yesterday's cached data. Skip this snapshot.
    if traded <= 15 and fut_notl > 30000:
        print(f"  ⚠ Stale data detected: {fut_notl:.0f} Cr notional at {traded}m traded — skipping")
        return False

    day_type = get_day_type(capture)
    proj_fut, proj_opt, conf = project_full_day(fut_notl, opt_prem, elapsed, day_type,
                                                 trading_date=capture)
    fut_rev, opt_rev, tx_rev, total_rev = calc_revenue(proj_fut, proj_opt)

    unc = calc_uncertainty(time_pct, day_type, dual_call)

    # Per-commodity breakdown
    sym_fut = defaultdict(float)
    sym_opt = defaultdict(float)
    sym_optN = defaultdict(float)
    # MCX now returns Symbol=null; the commodity name is in ProductCode
    # (already clean, e.g. "COPPER"), so no OPT-prefix stripping needed.
    for c in futures:
        sym_fut[c.get("ProductCode") or c.get("Symbol")] += c.get("NotionalValue", 0) / 100
    for c in options:
        base = c.get("ProductCode") or (c.get("Symbol") or "").replace("OPT", "")
        sym_opt[base] += c.get("PremiumValue", 0) / 100
        sym_optN[base] += c.get("NotionalValue", 0) / 100

    top_fut = sorted(
        [{"sym": k, "notl": round(v, 2)} for k, v in sym_fut.items()],
        key=lambda x: -x["notl"]
    )[:12]
    top_opt = sorted(
        [{"sym": k, "prem": round(v, 2), "notl": round(sym_optN[k], 2),
          "ratio": round(v / sym_optN[k] * 100, 3) if sym_optN[k] > 0 else 0}
         for k, v in sym_opt.items()],
        key=lambda x: -x["prem"]
    )[:12]

    snapshot = {
        "trading_date": capture.strftime("%Y-%m-%d"),
        "elapsed_min": elapsed,
        "session_closed": traded >= total,
        "fut_notl_cr": round(fut_notl, 2),
        "opt_notl_cr": round(opt_notl, 2),
        "opt_prem_cr": round(opt_prem, 2),
        "fut_rev_cr": round(fut_rev, 4),
        "opt_rev_cr": round(opt_rev, 4),
        "nontx_rev_cr": NONTX_DAILY,
        "total_rev_cr": round(tx_rev, 4),
        "proj_fut_cr": round(proj_fut, 2),
        "proj_opt_cr": round(proj_opt, 2),
        "proj_total_rev": round(total_rev, 4),
        "uncertainty_pct": round(unc * 100, 2),
        "confidence": conf,
        "day_type": day_type,
        "day_multiplier": DAY_MULTIPLIER[day_type],
        "active_futures": len(futures),
        "active_options": len(options),
        "prem_notl_pct": round(opt_prem / opt_notl * 100, 3) if opt_notl > 0 else 0,
        "dual_call": dual_call,
        "data_source": "mcx_relay_local",
        "top_futures": top_fut,
        "top_options": top_opt,
    }

    print(f"  Pushing to Supabase...")
    # Merge on the table's unique key (mcx_snapshots_trading_date_elapsed_min_key):
    # without on_conflict a second write for the same minute (relay restart, or
    # the /api/refresh POST path) was a 409 that counted as a failed snapshot.
    result = _hard_call(supabase_upsert, "mcx_snapshots", snapshot, hard_timeout=20,
                        on_conflict="trading_date,elapsed_min")
    print(f"  ✓ {capture.strftime('%H:%M IST')} — Rev: ₹{total_rev:.2f} Cr "
          f"(Fut: {fut_notl:.0f} | Opt Prem: {opt_prem:.0f}) "
          f"[{conf}, ±{unc*100:.0f}%] "
          f"({len(futures)}F/{len(options)}O)")
    send_heartbeat("running", last_rev=total_rev, elapsed=elapsed)

    # Regime drift check — run once per session when ≥3 buckets complete (15:00-16:00)
    today_iso = capture.strftime("%Y-%m-%d")
    if (shape["type"] == SESSION_FULL and 360 <= elapsed < 420
            and getattr(run_snapshot, '_regime_done', None) != today_iso):
        run_snapshot._regime_done = today_iso
        try:
            from lib.mcx_config import supabase_read_all
            today_snaps = supabase_read_all(
                "mcx_snapshots",
                f"?select=elapsed_min,fut_notl_cr,opt_prem_cr"
                f"&trading_date=eq.{today_iso}"
                f"&order=elapsed_min.asc",
                max_rows=100,
            )
            alerts = check_regime_drift(today_snaps, trading_date=capture)
            for a in alerts:
                print(f"  ⚠ REGIME SHIFT: {a['bucket']} volume {a['direction']} "
                      f"(today={a['today_weight']:.1%} vs adaptive={a['adaptive_weight']:.1%}, "
                      f"z={a['z_score']:.1f})")
        except Exception as e:
            print(f"  ⚠ Regime check failed: {e}")

    return "closed" if snapshot["session_closed"] else True


def fetch_mcx_historical(date_iso):
    """Fetch daily revenue from MCX Historical Detailed Report API.
    Returns revenue dict with exact PremiumTurnover (no proxy), or None on failure.
    Uses curl_cffi with Chrome TLS impersonation to bypass Akamai bot detection."""
    date_compact = date_iso.replace("-", "")

    params = {
        "culture": "en-US", "GroupBy": "D", "Segment": "ALL",
        "CommodityHead": "ALL", "Commodity": "ALL", "Startdate": date_compact,
        "EndDate": date_compact, "InstrumentName": "ALL",
    }

    # MCX migrated to Sitefinity (Jun 2026): historical report is now a root-scoped
    # GET. The old backpage.aspx POST returns an HTML 404 (HTTP 200) for every date,
    # which silently broke the relay catch-up self-heal for all missed days.
    url = "https://www.mcxindia.com/GetHistoricalDataDetails"

    try:
        session = _get_hist_session()
        resp = _hard_call(session.get, url, params=params, headers={
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://www.mcxindia.com/market-data/historical-data",
        }, timeout=30, hard_timeout=35)
        resp.raise_for_status()
        data = resp.json()

        rows = (data.get("Data") or {}).get("Data")
        if not rows or len(rows) < 5:
            return None

        fut_notl_lacs = 0.0
        opt_prem_lacs = 0.0
        n_fut = n_opt = 0

        for r in rows:
            inst = (r.get("Instrumentname") or r.get("InstrumentName") or "").strip().upper()
            total_val = float(r.get("TotalValue", 0) or 0)
            prem_str = str(r.get("PremiumTurnover", "-")).strip()

            if inst in ("FUTCOM", "FUTIDX"):
                fut_notl_lacs += total_val
                if total_val > 0:
                    n_fut += 1
            elif inst in ("OPTFUT", "OPTIDX"):
                if prem_str not in ("-", ""):
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
    except Exception:
        return None


# The MarketWatch JSON capture_eod() fetched, by session date, so the
# per-contract file (capture_contract_eod) is written from it with no MCX call
# of its own. close_session() pops it.
_EOD_MARKET_WATCH = {}


def capture_eod(date_iso=None):
    """Capture final EOD data and upsert to mcx_daily_revenue as authoritative record.
    Priority: market watch API (direct PremiumValue) > historical detailed report.
    This replaces/supersedes BHAV proxy data for the day.

    date_iso is the session being closed. Pass it explicitly: a 23:55 close
    leaves only minutes before now_ist() rolls over to the next date."""
    t = now_ist()
    if date_iso is None:
        date_iso = t.strftime("%Y-%m-%d")
    session_dt = datetime.strptime(date_iso, "%Y-%m-%d")
    print(f"\n  ── EOD Capture for {date_iso} ──")

    eod_record = None
    source_tag = None

    # Priority 1: Market watch API (direct PremiumValue)
    try:
        session = _get_mw_session()
        raw = fetch_market_watch(session)
        _EOD_MARKET_WATCH[date_iso] = raw
        time.sleep(2)
        raw2 = fetch_market_watch(session)
        _EOD_MARKET_WATCH[date_iso] = raw2

        fut_n1, opt_n1, opt_p1, futures1, options1 = extract_notionals(raw)
        fut_n2, opt_n2, opt_p2, _, _ = extract_notionals(raw2)
        fut_notl = max(fut_n1, fut_n2)
        opt_prem = max(opt_p1, opt_p2)

        fut_rev = 2 * fut_notl * FUTURES_RATE / 1e7
        opt_rev = 2 * opt_prem * OPTIONS_RATE / 1e7
        total_rev = fut_rev + opt_rev + NONTX_DAILY

        if 1.0 <= total_rev <= 50.0:
            eod_record = {
                "trading_date": date_iso,
                "fut_notl_cr": round(fut_notl, 2),
                "opt_prem_cr": round(opt_prem, 2),
                "fut_rev_cr": round(fut_rev, 4),
                "opt_rev_cr": round(opt_rev, 4),
                "nontx_rev_cr": NONTX_DAILY,
                "total_rev_cr": round(total_rev, 4),
                "day_type": get_day_type(session_dt),
                "source": "mcx_relay_eod",
                "is_actual": True,
                "active_futures": len(futures1),
                "active_options": len(options1),
            }
            source_tag = "mcx_relay_eod"
        else:
            print(f"  ⚠ Market watch revenue {total_rev:.4f} out of range")
    except Exception as e:
        print(f"  ⓘ Market watch failed ({e}), trying historical API...")

    # Priority 2: MCX Historical Detailed Report (exact PremiumTurnover)
    if eod_record is None:
        hist = fetch_mcx_historical(date_iso)
        if hist and 1.0 <= hist["total_rev_cr"] <= 50.0:
            eod_record = {
                "trading_date": date_iso,
                "day_type": get_day_type(session_dt),
                "source": "mcx_historical",
                "is_actual": True,
                **hist,
            }
            source_tag = "mcx_historical"
        else:
            print("  ✗ Historical API also unavailable")

    if eod_record is None:
        print(f"  ✗ EOD capture failed for {date_iso} — no source available")
        return False

    # data_source too, so a stale value from another writer never survives
    eod_record.setdefault("data_source", source_tag)
    result = supabase_upsert("mcx_daily_revenue", eod_record)
    tr = eod_record["total_rev_cr"]
    fr = eod_record["fut_rev_cr"]
    opr = eod_record["opt_rev_cr"]
    print(f"  ✓ EOD {date_iso}: ₹{tr:.4f} Cr "
          f"(Fut: {fr:.4f} | Opt: {opr:.4f}) "
          f"→ mcx_daily_revenue [source={source_tag}]")
    return True




def send_heartbeat(status="running", last_rev=None, elapsed=None, error=None):
    """Push heartbeat to Supabase relay_heartbeat table."""
    t = now_ist()
    beat = {
        "relay_id": RELAY_ID,
        "heartbeat_at": t.strftime("%Y-%m-%dT%H:%M:%S+05:30"),
        "trading_date": t.strftime("%Y-%m-%d"),
        "status": status,  # running | eod_done | error | stopped
    }
    if last_rev is not None:
        beat["last_rev_cr"] = round(last_rev, 4)
    if elapsed is not None:
        beat["elapsed_min"] = elapsed
    # Always send last_error: the upsert merges on relay_id and only updates the
    # columns it is given, so a heartbeat without the key left the last failure
    # (e.g. a long-fixed 409) on /api/refresh?health=1 for good. No error now
    # means the field is cleared.
    beat["last_error"] = str(error)[:200] if error else None
    try:
        _hard_call(supabase_upsert, "relay_heartbeat", beat, hard_timeout=15)
    except Exception as e:
        print(f"  ⚠ Heartbeat failed: {e}")


def refresh_margins_local():
    """Download and upsert margin data from Sharekhan (runs locally to bypass cloud IP blocks).
    Sharekhan's XLS "Date" column is an effective-from date, not the publication date,
    so we use the HTTP Last-Modified header (in IST) as the authoritative snapshot_date."""
    print(f"\n  ── Margin Refresh ──")
    try:
        # Probe Last-Modified to pin snapshot_date to when Sharekhan published the file.
        from email.utils import parsedate_to_datetime
        _IST_OFFSET = timezone(timedelta(hours=5, minutes=30))
        pub_date = None
        try:
            req = urllib.request.Request(
                "https://www.sharekhan.com/MediaGalary/Commodity/McxSpan.xls",
                method="HEAD",
                headers={"User-Agent": "Mozilla/5.0"},
            )
            with urllib.request.urlopen(req, timeout=10) as r:
                lm = r.headers.get("Last-Modified")
                if lm:
                    pub_date = parsedate_to_datetime(lm).astimezone(_IST_OFFSET).strftime("%Y-%m-%d")
        except Exception:
            pass
        if pub_date is None:
            pub_date = now_ist().strftime("%Y-%m-%d")

        data = _download_xls(log=[])
        rows = _parse_xls(data)
        if not rows:
            print("  ✗ No margin rows parsed")
            return
        for r in rows:
            r["snapshot_date"] = pub_date

        today_str = now_ist().strftime("%Y-%m-%d")
        if pub_date != today_str:
            print(f"  ⚠ Stale: Sharekhan file published {pub_date} ≠ today {today_str}")
        stats = {}
        errors = margin_upsert("mcx_margin_daily", rows, stats=stats)
        skipped = stats.get("skipped", 0)
        if errors:
            print(f"  ⚠ Upsert errors: {errors[:2]}")
        elif skipped >= len(rows):
            print(f"  · Margins {pub_date}: already stored, nothing written ({len(rows)} rows)")
        else:
            print(f"  ✓ Margins upserted: {pub_date} ({len(rows) - skipped} rows"
                  + (f", {skipped} already stored" if skipped else "") + ")")
    except Exception as e:
        print(f"  ✗ Margin refresh failed: {e}")


def refresh_oi_participants_local():
    """Download and upsert OI participant category data from MCX.
    MCX serves a 404 HTML page with HTTP 200 when the day's report isn't published
    yet, so we sniff the ZIP magic and walk back up to 5 trading days."""
    if not _HAS_OI:
        print("  OI participants module not available, skipping")
        return
    print(f"\n  -- OI Participants Refresh --")
    try:
        session = _new_session("https://www.mcxindia.com/market-operations/trading-survelliance")

        dt = now_ist().date()
        xlsx_bytes = None
        resolved_dt = None
        tried = []
        for _ in range(7):
            if is_trading_day(dt):
                url = oi_build_url(dt)
                tried.append(dt.isoformat())
                resp = _hard_call(session.get, url, timeout=20, hard_timeout=25)
                if resp.status_code == 200 and resp.content[:2] == b"PK":
                    xlsx_bytes = resp.content
                    resolved_dt = dt
                    break
            dt -= timedelta(days=1)

        if xlsx_bytes is None:
            print(f"  ⏸ No OI report available yet (tried {tried})")
            return

        report_date, rows = oi_parse_participants(xlsx_bytes)
        if not rows:
            print(f"  ✗ No OI participant rows parsed for {resolved_dt}")
            return
        stats = {}
        errors = oi_upsert("mcx_oi_participants", rows, stats=stats)
        skipped = stats.get("skipped", 0)
        if errors:
            print(f"  ⚠ Upsert errors: {errors[:2]}")
        elif skipped >= len(rows):
            print(f"  · OI participants {report_date}: already stored, nothing written")
        else:
            print(f"  ✓ OI participants upserted: {report_date} ({len(rows) - skipped} rows)")
    except Exception as e:
        print(f"  ✗ OI participants refresh failed: {e}")


def _watchful_sleep(seconds, slack=600):
    """Sleep, then detect system-sleep gaps. caffeinate -is cannot block
    lid-close sleep on battery: observed Jul 27–30 2026, one frozen-but-alive
    process spanned 4 days, EOD capture never fired, and launchd never
    respawned because the process never exited. If wall clock advanced more
    than `slack` seconds past the requested sleep, the Mac slept — exit(1) so
    launchd KeepAlive relaunches a clean process whose startup catch-up
    (catchup_missing + margin/OI refresh) self-heals the missed window."""
    before = time.time()
    time.sleep(seconds)
    drift = time.time() - before - seconds
    if drift > slack:
        print(f"\n*** SYSTEM SLEEP DETECTED: woke {drift/60:.0f} min late from a "
              f"{seconds//60}-min sleep — exit(1) for launchd to relaunch. ***")
        send_heartbeat("error", error=f"system-sleep watchdog: {drift/60:.0f} min gap")
        sys.exit(1)


# ── Stall watchdog ───────────────────────────────────────────────────────────
# The failure-watchdog (consecutive exceptions) can't catch a SILENT hang: a
# leaked libcurl thread exhausts _NET_POOL and the next _hard_call blocks forever
# with NO exception raised, so the loop freezes mid-session (observed: 08-13 froze
# at 104 min, 08-18 at 17 min — a full day of no snapshots). This independent
# daemon thread hard-exits the process if the loop makes no progress for
# STALL_LIMIT, so launchd relaunches a clean process with a fresh pool.
_last_progress = time.time()
STALL_LIMIT = 1500  # 25 min with no loop progress -> assume frozen -> hard-exit

def _mark_progress():
    global _last_progress
    _last_progress = time.time()

def _start_stall_watchdog():
    def _watch():
        while True:
            time.sleep(60)
            age = time.time() - _last_progress
            if age > STALL_LIMIT:
                # os._exit (not sys.exit): a wedged _hard_call thread can swallow
                # SystemExit; os._exit is unconditional. launchd KeepAlive then
                # relaunches a clean process; startup catch-up self-heals the gap.
                print(f"\n*** STALL WATCHDOG: no loop progress for {age/60:.0f} min "
                      f"(>{STALL_LIMIT//60} min) — network pool likely frozen. "
                      f"os._exit(1) for launchd to relaunch. ***", flush=True)
                os._exit(1)
    threading.Thread(target=_watch, daemon=True, name="stall-watchdog").start()


def _hours(shape):
    """'09:00–23:55 IST' / '17:00–23:55 IST, morning closed'."""
    if shape["type"] == SESSION_CLOSED:
        return "closed"
    note = {"evening_only": ", morning closed", "morning_only": ", evening closed"}.get(shape["type"], "")
    return f"{fmt_ist_minutes(shape['start'])}–{fmt_ist_minutes(shape['end'])} IST{note}"


# ── Per-commodity breakdown after the close ─────────────────────────────────
# capture_eod() writes only the exchange totals. The per-commodity rows come
# from MCX's Historical Detailed Report, which was NOT out at the close when
# probed (10 Nov 2026: empty at 23:48, contracts at 04:31, premium by 06:31),
# so this one attempt usually reports "not ready"; scripts/launchd/
# com.mcx.commodity-eod.plist retries 05:30-06:55 and daily_verify (07:00) is
# the backstop. It runs bhav_refresh in a subprocess so a wedged libcurl call
# can't touch this process's pool, and it is bounded well inside STALL_LIMIT.
COMMODITY_EOD_TIMEOUT = 90


def capture_commodity_breakdown(session_date, timeout=COMMODITY_EOD_TIMEOUT, run=None):
    """One non-fatal attempt at the day's per-commodity breakdown. Never
    raises; returns the bhav_refresh exit code, or None if it didn't finish."""
    import subprocess
    run = run or subprocess.run
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bhav_refresh.py")
    print(f"\n  ── Commodity breakdown for {session_date} ──")
    _mark_progress()
    try:
        res = run([sys.executable, script, "--commodity", session_date],
                  capture_output=True, text=True, timeout=timeout,
                  cwd=os.path.dirname(os.path.dirname(script)))
    except subprocess.TimeoutExpired:
        print(f"  ⓘ breakdown fetch exceeded {timeout}s — left to the 05:30 job / daily_verify")
        return None
    except Exception as e:
        print(f"  ⓘ breakdown fetch failed to run ({type(e).__name__}: {e}) — left to daily_verify")
        return None
    finally:
        _mark_progress()
    for line in (res.stdout or "").strip().splitlines()[-3:]:
        print(f"  {line.strip()}")
    if res.returncode == 0:
        print(f"  ✓ {session_date}: per-commodity breakdown stored")
    elif res.returncode == 3:
        print(f"  ⓘ {session_date}: MCX report not complete yet — the 05:30 job / daily_verify will retry")
    else:
        print(f"  ⓘ {session_date}: breakdown fetch exit {res.returncode}: "
              f"{(res.stderr or '').strip()[-160:]}")
    return res.returncode


# ── Per-contract EOD file ────────────────────────────────────────────────────
# After the close, every FUTCOM contract in MarketWatch (all expiries, not just
# the near month) goes to one CSV per session, read by a separate local
# project. A local file only; nothing goes to Supabase or the dashboard. The
# columns and the file name are that project's contract: change neither.
#
# Built from the MarketWatch JSON capture_eod() already fetched (no extra MCX
# call) and run after every other close-of-session step; any failure is
# logged, never raised.
#
# `close`: MarketWatch has no closing-price field (only Open, High, Low, LTP,
# PreviousClose), so `close` is the LTP at the EOD capture, i.e. the last
# traded price, NOT MCX's official closing price (the Bhavcopy's Close). On
# 20 Nov 2026 they agreed for 8 of 98 traded contracts; median gap 13 bp,
# 90th percentile 53 bp, worst 286 bp. `ltp` repeats it as MarketWatch
# reported it. Price columns are blank where MarketWatch reports 0 (the
# contract did not trade); oi and volume are in lots, as MarketWatch reports
# them. MarketWatch lists every contract with volume or open interest (115 of
# the Bhavcopy's 169 that day; the other 54 had neither).
CONTRACT_EOD_COLUMNS = ("date", "symbol", "instrument", "expiry", "open", "high", "low",
                        "close", "prev_close", "ltp", "oi", "volume", "captured_at")
CONTRACT_EOD_DEFAULT_DIR = "~/tradingone/data/mcx_contract_eod"
_MONTHS = {m: i for i, m in enumerate(
    ("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"), 1)}


def contract_eod_dir():
    """$MCX_CONTRACT_EOD_DIR, else ~/tradingone/data/mcx_contract_eod (read at call time)."""
    return os.environ.get("MCX_CONTRACT_EOD_DIR") or os.path.expanduser(CONTRACT_EOD_DEFAULT_DIR)


def _mw_data(raw):
    """The {"Summary":..., "Data":[...]} block of a MarketWatch payload, or {}."""
    if not isinstance(raw, dict):
        return {}
    data = raw.get("data") or raw.get("d") or {}
    return data if isinstance(data, dict) else {}


def _mw_rows(raw):
    rows = _mw_data(raw).get("Data")
    return [r for r in rows if isinstance(r, dict)] if isinstance(rows, list) else []


def market_watch_as_of(raw):
    """Naive IST datetime the MarketWatch payload is as of: Summary.AsOn
    ("/Date(<epoch ms>)/"), else the latest row LTTValue. None if neither
    (MCX's "/Date(-62135596800000)/" placeholder is year 1, not a date)."""
    summary = _mw_data(raw).get("Summary")
    m = re.match(r"/Date\((-?\d+)", str((summary or {}).get("AsOn") or "")) \
        if isinstance(summary, dict) else None
    if m:
        try:
            t = datetime(1970, 1, 1) + timedelta(milliseconds=int(m.group(1)), hours=5, minutes=30)
            if t.year >= 2000:
                return t
        except (OverflowError, ValueError):
            pass
    stamps = []
    for r in _mw_rows(raw):
        try:
            stamps.append(datetime.strptime(str(r.get("LTTValue") or "").strip(), "%Y-%m-%d %H:%M:%S"))
        except ValueError:
            pass
    return max(stamps) if stamps else None


def _instrument(r):
    return str(r.get("InstrumentName") or r.get("Instrumentname") or "").strip().upper()


def _expiry_iso(s):
    """'30NOV2026' -> '2026-11-30' (locale-free), else None."""
    m = re.fullmatch(r"(\d{1,2})([A-Za-z]{3})(\d{4})", str(s or "").strip())
    if not m or m.group(2).upper() not in _MONTHS:
        return None
    try:
        return datetime(int(m.group(3)), _MONTHS[m.group(2).upper()], int(m.group(1))).strftime("%Y-%m-%d")
    except ValueError:
        return None


def _price(v):
    """A MarketWatch price; "" where it reports 0 or nothing (no trade)."""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return ""
    return f if f > 0 else ""


def _count(v):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return ""


def contract_eod_rows(raw, session_date, captured_at):
    """One dict per FUTCOM contract in a MarketWatch payload, keys in
    CONTRACT_EOD_COLUMNS order, sorted by symbol then expiry. A row without a
    symbol or a readable expiry is dropped; a repeated (symbol, expiry) keeps
    the first."""
    out = {}
    for r in _mw_rows(raw):
        if _instrument(r) != "FUTCOM":
            continue
        sym = str(r.get("ProductCode") or r.get("Symbol") or "").strip()
        exp = _expiry_iso(r.get("ExpiryDate"))
        if not sym or not exp or (sym, exp) in out:
            continue
        ltp = _price(r.get("LTP"))
        out[(sym, exp)] = dict(zip(CONTRACT_EOD_COLUMNS, (
            session_date, sym, "FUTCOM", exp,
            _price(r.get("Open")), _price(r.get("High")), _price(r.get("Low")),
            ltp, _price(r.get("PreviousClose")), ltp,
            _count(r.get("OpenInterest")), _count(r.get("Volume")), captured_at)))
    return [out[k] for k in sorted(out)]


def write_contract_eod(rows, session_date, out_dir):
    """Write rows to <out_dir>/<session_date>.csv atomically: a temp file in
    the same directory, fsync, then os.replace, so a reader never sees a
    half-written file. Returns "written"; "empty" (no rows: nothing touched,
    no directory made); or "kept" (the existing file has more contracts: a
    smaller capture never replaces it). I/O errors are raised after the temp
    file is removed; the existing file is then untouched."""
    if not rows:
        return "empty"
    path = os.path.join(out_dir, f"{session_date}.csv")
    if os.path.exists(path):
        try:
            with open(path, newline="") as f:
                have = max(sum(1 for line in csv.reader(f) if line) - 1, 0)
        except Exception:   # unreadable: not worth keeping over a full capture
            have = 0
        if have > len(rows):
            return "kept"
    os.makedirs(out_dir, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{session_date}.", suffix=".csv.tmp", dir=out_dir)
    try:
        os.fchmod(fd, 0o644)
        with os.fdopen(fd, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=CONTRACT_EOD_COLUMNS)
            w.writeheader()
            w.writerows(rows)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return "written"


def capture_contract_eod(session_date=None, raw=None, out_dir=None):
    """Write the session's per-contract file from `raw`, the MarketWatch JSON
    capture_eod() fetched. Never raises. Returns the file's path when written,
    else None. Not written when there is no payload, or when the payload is
    for a different day than session_date (never file one day's prices under
    another's date). session_date=None takes the payload's own day (manual
    runs). out_dir defaults to contract_eod_dir()."""
    _mark_progress()
    try:
        print(f"\n  ── Contract EOD file for {session_date or 'the MarketWatch day'} ──")
        if raw is None:
            print("  ⓘ no MarketWatch data from the EOD capture — contract file skipped")
            return None
        as_of = market_watch_as_of(raw)
        as_of_day = as_of.strftime("%Y-%m-%d") if as_of else None
        session_date = session_date or as_of_day
        if as_of_day is None or as_of_day != session_date:
            print(f"  ⓘ MarketWatch is as of {as_of or 'an unknown time'}, not {session_date}"
                  " — contract file not written")
            return None
        captured_at = now_ist().strftime("%Y-%m-%dT%H:%M:%S+05:30")
        rows = contract_eod_rows(raw, session_date, captured_at)
        n_futcom = sum(1 for r in _mw_rows(raw) if _instrument(r) == "FUTCOM")
        out_dir = out_dir or contract_eod_dir()
        path = os.path.join(out_dir, f"{session_date}.csv")
        status = write_contract_eod(rows, session_date, out_dir)
        if status == "empty":
            print(f"  ⓘ no usable FUTCOM contracts in MarketWatch ({n_futcom} rows) — nothing written")
            return None
        if status == "kept":
            print(f"  ⓘ {path} already has more contracts than this capture ({len(rows)}) — kept")
            return None
        per_sym = defaultdict(int)
        for r in rows:
            per_sym[r["symbol"]] += 1
        untraded = sum(1 for r in rows if r["ltp"] == "")
        dropped = n_futcom - len(rows)
        print(f"  ✓ {len(rows)} FUTCOM contracts, {len(per_sym)} commodities "
              f"({untraded} untraded" + (f", {dropped} rows dropped" if dropped else "")
              + f", as of {as_of:%H:%M} IST) → {path}")
        print("    " + ", ".join(f"{s} {n}" for s, n in sorted(per_sym.items())))
        return path
    except Exception as e:
        print(f"  ⓘ contract EOD file failed ({type(e).__name__}: {e}) — relay unaffected")
        return None
    finally:
        _mark_progress()


def close_session(session_date, clock_fallback=False):
    """Everything after the close, in priority order. Revenue first and always:
    nothing after it can fail or delay it.

    A failed revenue write (network, the 409 a re-run hits on the unique
    trading_date, or the guard trigger) is logged and reported in the heartbeat
    but no longer aborts the steps after it: before, the exception skipped
    margins / OI / commodity and sent the outer loop into up to 10 restarts that
    retried the same failing write. daily_verify (07:00) and the next start-up's
    catch-up backfill a missing revenue row."""
    try:
        eod_ok = capture_eod(session_date)
    except Exception as e:
        eod_ok = False
        print(f"  ✗ EOD write failed for {session_date}: {e}")
        print("    (daily_verify at 07:00 and the next relay start-up backfill missing revenue)")
        send_heartbeat("error", error=f"eod {session_date}: {e}")
    refresh_margins_local()
    refresh_oi_participants_local()
    if not clock_fallback and eod_ok:
        send_heartbeat("eod_done")
    try:
        capture_commodity_breakdown(session_date)
    except Exception as e:  # belt and braces: the function already never raises
        print(f"  ⓘ commodity breakdown skipped: {e}")
    # Last: the research lab's per-contract file, from the MarketWatch JSON the
    # EOD capture already fetched. Nothing above waits on it.
    try:
        capture_contract_eod(session_date, raw=_EOD_MARKET_WATCH.pop(session_date, None))
    except Exception as e:  # belt and braces: the function already never raises
        print(f"  ⓘ contract EOD file skipped: {e}")


def next_sleep_seconds(t, base_s):
    """Sleep before the next snapshot: base_s, cut short so the loop wakes
    EOD_DELAY_MIN after today's close. Without the cut, a 15-min cadence can
    land the final snapshot anywhere up to 15 min late; at a 23:55 close that
    crosses midnight, and the loop then exits without an EOD capture."""
    wake = (session_shape(t)["end"] + EOD_DELAY_MIN) * 60
    now_s = t.hour * 3600 + t.minute * 60 + t.second
    until_wake = wake - now_s
    if 0 < until_wake < base_s:
        return until_wake
    return base_s


def _report_calendar_status():
    """Put a missing MCX holiday calendar where it is seen: the heartbeat's
    last_error (shown by /api/refresh?health=1), not just stderr. The relay
    keeps running: stopping it would also lose real trading days."""
    st = calendar_status()
    if st["status"] != "OK":
        print(f"  ⚠ Calendar {st['status']}: {st['message']}")
        send_heartbeat("error" if st["status"] == "STALE" else "running",
                       error=f"calendar {st['status']}: {st['message']}")
    return st


def run_loop():
    """Run snapshots every 15 minutes until trading session ends.
    After session close, captures authoritative EOD record for mcx_daily_revenue."""
    print(f"MCX Relay — loop mode (every {LOOP_INTERVAL//60} min)")
    print(f"Trading hours: {_hours(session_shape(now_ist()))}")
    _start_stall_watchdog()
    _report_calendar_status()

    fail_streak = 0
    MAX_CONSEC_FAIL = 4  # back-to-back failures before we exit for launchd to relaunch

    while True:
        _mark_progress()  # reset the stall-watchdog each iteration
        t = now_ist()
        current_min = t.hour * 60 + t.minute
        shape = session_shape(t)
        open_min, close = shape["start"], shape["end"]
        session_date = t.strftime("%Y-%m-%d")

        if not is_trading_day(t.date()):
            print(f"{t.strftime('%H:%M IST')} — Not a trading day. Exiting.")
            break

        if current_min > close + 30 or (0 < current_min < SESSION_START - 60):
            print(f"{t.strftime('%H:%M IST')} — Session ended. Exiting.")
            break

        if current_min < open_min - 30:
            wait = (open_min - 30 - current_min) * 60
            print(f"{t.strftime('%H:%M IST')} — Waiting {wait//60} min until pre-market "
                  f"({_hours(shape)})...")
            _watchful_sleep(min(wait, LOOP_INTERVAL))
            continue

        try:
            result = run_snapshot()
        except Exception as e:
            print(f"  ✗ Error: {e}")
            send_heartbeat("error", error=e)
            fail_streak += 1
            print(f"  ⚠ Consecutive snapshot failures: {fail_streak}/{MAX_CONSEC_FAIL}")
            # Watchdog: a hung-but-alive relay is the worst failure mode — launchd
            # KeepAlive only relaunches on process exit, never on a stalled loop.
            # After repeated failures, exit(1) so launchd respawns a clean process
            # (fresh pool + sessions). This auto-recovers the 2026-06-19 stall.
            if fail_streak >= MAX_CONSEC_FAIL:
                print("  ✗ Too many consecutive failures — exit(1) for launchd to relaunch.")
                send_heartbeat("error", error=f"watchdog exit: {fail_streak} consecutive failures")
                sys.exit(1)
            # Drop the (possibly wedged) pool/sessions before the next attempt.
            _reset_net_state()
            result = False
        else:
            fail_streak = 0

        # Trigger EOD immediately when session closes (no timing race)
        if result == "closed":
            print("\nSession closed. Capturing EOD record...")
            close_session(session_date)
            break

        # Fallback: past the close but the snapshot hasn't reported it (fetches
        # failing, or it ran outside hours). Summer keeps its 15 min of retries;
        # winter captures at 23:57 at the latest.
        t2 = now_ist()
        current_min2 = t2.hour * 60 + t2.minute
        fallback_min = min(close + EOD_FALLBACK_MIN, LATEST_EOD_MIN)
        if t2.strftime("%Y-%m-%d") == session_date and current_min2 >= fallback_min:
            print("\nSession ended (clock fallback). Capturing EOD record...")
            close_session(session_date, clock_fallback=True)
            break

        # After a failure, retry in 2 min (not 15) so a transient stall recovers
        # in minutes and the watchdog can trip quickly, instead of dead cycles.
        sleep_s = next_sleep_seconds(now_ist(), 120 if fail_streak > 0 else LOOP_INTERVAL)
        print(f"  Next snapshot in {sleep_s/60:.0f} min...")
        _watchful_sleep(sleep_s)

    print("MCX Relay finished.")


def catchup_missing(days=7):
    """Check last N days for missing revenue data and fill from Historical API.
    Runs automatically before snapshot to self-heal gaps."""
    t = now_ist()
    filled = 0
    print(f"\n  ── Catch-up: scanning last {days} days for gaps ──")

    for i in range(1, days + 1):  # start at 1 to skip today
        d = (t - timedelta(days=i)).date()
        ds = d.strftime("%Y-%m-%d")

        if not is_trading_day(d):
            continue

        # Check if we already have data for this date
        try:
            if supabase_read("mcx_daily_revenue", f"?trading_date=eq.{ds}&limit=1"):
                continue  # Already have data
        except Exception:
            continue  # Skip on error, don't block main flow

        # Missing — try Historical API
        print(f"    Gap found: {ds} — fetching from Historical API...")
        hist = fetch_mcx_historical(ds)
        if hist and 0.5 <= hist["total_rev_cr"] <= 50.0:
            record = {
                "trading_date": ds,
                "day_type": get_day_type(datetime.combine(d, datetime.min.time())),
                "source": "mcx_historical",
                "is_actual": True,
                **hist,
            }
            try:
                supabase_upsert("mcx_daily_revenue", record)
                print(f"    ✓ {ds}: ₹{hist['total_rev_cr']:.4f} Cr (backfilled)")
                filled += 1
            except Exception as e:
                print(f"    ✗ {ds}: upsert failed — {e}")
        else:
            print(f"    · {ds}: no data from Historical API")

        time.sleep(0.5)

    if filled > 0:
        print(f"  Catch-up done — filled {filled} gaps")
    else:
        print(f"  Catch-up done — no gaps found")
    return filled


if __name__ == "__main__":
    # Flush stdout after every print (critical for Windows log files)
    sys.stdout.reconfigure(line_buffering=True)
    from lib.mcx_config import SUPABASE_WRITE_KEY, supabase_key_kind
    print(f"Supabase writes as: {SUPABASE_WRITE_ROLE} ({supabase_key_kind(SUPABASE_WRITE_KEY)} key)"
          + ("" if SUPABASE_WRITE_ROLE == "service_role" else
             " — set SUPABASE_SECRET_KEY in .env.local; mcx_daily_revenue rejects anon writes once locked"))

    if "--loop" in sys.argv:
        catchup_missing(7)  # Self-heal before starting loop
        refresh_margins_local()  # Catch up margins on startup
        refresh_oi_participants_local()  # Catch up OI participants on startup
        # Outer restart loop: if run_loop crashes (e.g. curl_cffi segfault recovery),
        # wait 60s and retry — keeps the process alive all day
        MAX_RESTARTS = 10
        for attempt in range(1, MAX_RESTARTS + 1):
            try:
                run_loop()
                break  # Clean exit (session ended normally)
            except Exception as e:
                print(f"\n*** RELAY CRASHED (attempt {attempt}/{MAX_RESTARTS}): {e} ***")
                send_heartbeat("error", error=f"crash #{attempt}: {e}")
                if attempt < MAX_RESTARTS:
                    # Reset curl_cffi sessions before retry
                    _get_mw_session(force_new=True)
                    _get_hist_session(force_new=True)
                    print(f"  Restarting in 60s...")
                    time.sleep(60)
                else:
                    print(f"  Max restarts reached. Exiting.")
                    sys.exit(1)
    elif "--contract-eod" in sys.argv:
        # Manual per-contract file from MarketWatch as it is now, filed under
        # the day the data is as of (e.g. after a missed close).
        # MCX_CONTRACT_EOD_DIR picks the folder. Refused while trading: the
        # prices would be intraday, not end of day.
        from lib.mcx_config import is_market_open
        if is_market_open(now_ist()):
            print("Market is open: run --contract-eod after the close.")
            sys.exit(2)
        sys.exit(0 if capture_contract_eod(None, raw=fetch_market_watch()) else 1)
    elif "--catchup" in sys.argv:
        days = 7
        for i, arg in enumerate(sys.argv):
            if arg == "--catchup" and i + 1 < len(sys.argv):
                try:
                    days = int(sys.argv[i + 1])
                except ValueError:
                    pass
        catchup_missing(days)
    else:
        try:
            success = run_snapshot()
            if not success:
                print("No snapshot taken (outside trading hours or fetch failed).")
        except Exception as e:
            print(f"ERROR: {e}")
            sys.exit(1)
