#!/usr/bin/env python3
"""
MCX iCOMDEX Index Levels Refresh — local scheduled task
Fetches daily OHLC of the MCX iCOMDEX indices (BULLDEX, METLDEX, ENRGDEX,
COMPDEX, GOLDEX, SILVDEX, ALUMDEX, COPRDEX) and upserts to mcx_icomdex_daily.

Why: the BULLDEX index *futures* contract has had zero volume since Jun 2026
and MCX's live MarketWatch no longer carries FUTIDX rows at all — the index
VALUE published on /market-data/mcx-icomdex-indices is the only live BULLDEX
series. This endpoint is page-scoped Sitefinity (the site's GetData() JS
helper prefixes location.pathname), history available from 2015-12-31, and
the site's datepicker disallows selecting today — so data lands T+1.

Uses curl_cffi with Chrome TLS impersonation to bypass Akamai bot detection.
Must run under homebrew python (curl_cffi is absent from system Python 3.9).

Usage:
  python3 scripts/icomdex_refresh.py                          # last 7 days (self-healing)
  python3 scripts/icomdex_refresh.py 2026-08-21               # one specific date
  python3 scripts/icomdex_refresh.py --backfill 2015-12-31 today   # chunked range
  python3 scripts/icomdex_refresh.py --dry-run                # fetch+parse, no writes
"""
import sys, os, json, time, urllib.request
from curl_cffi import requests as cfreq
from datetime import datetime, timedelta, timezone, date

# ── Config (self-contained, mirrors bhav_refresh.py) ────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://avqwpebveqetwwzkmtux.supabase.co")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImF2cXdwZWJ2ZXFldHd3emttdHV4Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzE0MDkwMzMsImV4cCI6MjA4Njk4NTAzM30.U_Ug61Fp1NSCesXBkYU7GJGTbuATFtXsz6GTi5948Rw")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
SUPABASE_WRITE_KEY = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY

MCX_TIMEOUT = 30
MCX_MAX_RETRIES = 2
CHROME_IMPERSONATE = os.environ.get("MCX_IMPERSONATE", "chrome142")
CHROME_IMPERSONATE_FALLBACKS = ["chrome136", "chrome131", "chrome"]

PAGE_URL = "https://www.mcxindia.com/market-data/mcx-icomdex-indices"
HISTORY_URL = PAGE_URL + "/GetMCXIComdexIndicesHistoryFilter"
BACKFILL_CHUNK_DAYS = 180
TABLE = "mcx_icomdex_daily"
BLOCKED_MSG = ("blocked_on:\"scripts/sql/create_icomdex_table.sql\" — the "
               "mcx_icomdex_daily table/policies are missing; a human must run "
               "that SQL once in the Supabase SQL editor.")


def now_ist():
    return datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)


# ── MCX fetch ────────────────────────────────────────────────────────────────
_session = None

def _get_session(force_new=False):
    global _session
    if _session is None or force_new:
        last_err = None
        for target in [CHROME_IMPERSONATE, *CHROME_IMPERSONATE_FALLBACKS]:
            try:
                sess = cfreq.Session(impersonate=target)
                sess.get(PAGE_URL, timeout=MCX_TIMEOUT)  # Akamai cookie warm-up
                _session = sess
                return _session
            except Exception as e:
                last_err = e
                if "impersonat" in str(e).lower() or "not supported" in str(e).lower():
                    continue
                raise
        raise last_err
    return _session


def fetch_range(from_d: date, to_d: date):
    """Fetch index history rows for [from_d, to_d]. Returns (rows, err)."""
    params = {
        "instrument_Identifier": 0,  # 0 = ALL indices
        "fromDate": from_d.strftime("%d/%m/%Y"),
        "toDate": to_d.strftime("%d/%m/%Y"),
    }
    for attempt in range(MCX_MAX_RETRIES + 1):
      try:
        session = _get_session(force_new=(attempt > 0))
        resp = session.get(HISTORY_URL, params=params, headers={
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": PAGE_URL,
        }, timeout=MCX_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        # Shape: {"IsSuccess": true, "Data": [ {Instrument_Code, Date, O/H/L/C, Change}, ... ]}
        rows = data.get("Data")
        if rows is None:
            return None, f"unexpected response shape: {str(data)[:120]}"
        return rows, None
      except Exception as e:
        if attempt < MCX_MAX_RETRIES:
            print(f"  ⓘ Attempt {attempt+1} failed: {e}, retrying in {3*(attempt+1)}s...")
            time.sleep(3 * (attempt + 1))
            continue
        return None, str(e)


def parse_rows(raw_rows):
    """Raw endpoint rows → mcx_icomdex_daily records. Skips unparseable rows."""
    out, skipped = [], 0
    for r in raw_rows:
        try:
            code = (r.get("Instrument_Code") or "").strip()
            d = datetime.strptime((r.get("Date") or "").strip(), "%d %b %Y").date()
            close = float(r.get("Close") or 0)
            if not code or close <= 0:   # an index level is never 0
                skipped += 1
                continue
            def _num(v):
                # Pre-2020 history publishes 0.0 for O/H/L (close-only era) —
                # store NULL rather than a fake zero level.
                f = float(v) if v not in (None, "") else 0.0
                return f if f > 0 else None
            out.append({
                "trading_date": d.isoformat(),
                "index_code": code,
                "index_name": (r.get("Instrument_Display_Name") or "").strip() or None,
                "open": _num(r.get("Open")),
                "high": _num(r.get("High")),
                "low": _num(r.get("Low")),
                "close": close,
                "change_pct": float(r.get("Change")) if r.get("Change") not in (None, "") else None,
                "source": "mcx_icomdex_api",
            })
        except (ValueError, TypeError, AttributeError):
            skipped += 1
    if skipped:
        print(f"  ⓘ skipped {skipped} unparseable rows")
    return out


# ── Supabase upsert ──────────────────────────────────────────────────────────

def upsert_batch(rows):
    """Upsert records; PK (trading_date, index_code) makes merge-duplicates safe."""
    for i in range(0, len(rows), 500):
        batch = rows[i:i + 500]
        req = urllib.request.Request(
            f"{SUPABASE_URL}/rest/v1/{TABLE}",
            data=json.dumps(batch).encode(), method="POST", headers={
                "apikey": SUPABASE_WRITE_KEY,
                "Authorization": f"Bearer {SUPABASE_WRITE_KEY}",
                "Content-Type": "application/json",
                "Prefer": "resolution=merge-duplicates",
            })
        try:
            with urllib.request.urlopen(req, timeout=15):
                pass
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", "replace")[:200] if e.fp else ""
            if e.code in (404, 401, 403) or "42501" in body or "PGRST205" in body:
                print(f"  ✗ upsert denied ({e.code}): {body}")
                print(f"  {BLOCKED_MSG}")
            else:
                print(f"  ✗ upsert failed ({e.code}): {body}")
            return False
        except Exception as e:
            print(f"  ✗ upsert failed: {e}")
            return False
    return True


# ── Runner ───────────────────────────────────────────────────────────────────

def refresh(from_d: date, to_d: date, dry_run=False):
    """Fetch + upsert one date window. Returns True on success."""
    print(f"iCOMDEX refresh {from_d} → {to_d}")
    raw, err = fetch_range(from_d, to_d)
    if raw is None:
        print(f"  ✗ fetch failed: {err}")
        return False
    rows = parse_rows(raw)
    if not rows:
        # Legitimate for weekend/holiday-only windows; report and succeed.
        print(f"  ⓘ no index rows in window ({len(raw)} raw)")
        return True
    dates = sorted({r["trading_date"] for r in rows})
    codes = sorted({r["index_code"] for r in rows})
    print(f"  ✓ {len(rows)} rows | {len(dates)} day(s) {dates[0]}..{dates[-1]} | {len(codes)} indices")
    if dry_run:
        print("  (dry-run: skipping upsert)")
        print(json.dumps(rows[:3], indent=2))
        return True
    if upsert_batch(rows):
        print(f"  ✓ upserted → {TABLE}")
        return True
    return False


def main():
    args = [a for a in sys.argv[1:]]
    dry_run = "--dry-run" in args
    args = [a for a in args if a != "--dry-run"]
    today = now_ist().date()

    if args and args[0] == "--backfill":
        if len(args) < 3:
            print("usage: icomdex_refresh.py --backfill FROM_ISO TO_ISO|today")
            return 1
        from_d = date.fromisoformat(args[1])
        to_d = today if args[2] == "today" else date.fromisoformat(args[2])
        ok = True
        chunk_start = from_d
        while chunk_start <= to_d:
            chunk_end = min(chunk_start + timedelta(days=BACKFILL_CHUNK_DAYS - 1), to_d)
            if not refresh(chunk_start, chunk_end, dry_run=dry_run):
                ok = False
            chunk_start = chunk_end + timedelta(days=1)
            if chunk_start <= to_d:
                time.sleep(2)  # be polite between chunks
        return 0 if ok else 1

    if args:  # single specific date
        d = date.fromisoformat(args[0])
        return 0 if refresh(d, d, dry_run=dry_run) else 1

    # Default: trailing week — heals missed days and picks up T+1 publication.
    return 0 if refresh(today - timedelta(days=7), today, dry_run=dry_run) else 1


if __name__ == "__main__":
    sys.exit(main())
