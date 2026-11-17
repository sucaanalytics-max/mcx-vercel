"""
MCX Bhavcopy: the exchange's own daily per-contract report, which carries open
interest (the Historical Detailed Report that bhav_refresh reads has none).

Source of futures OI for mcx_commodity_daily since Nov 2026, replacing Dhan's
stitched history: no login or token (a Dhan PIN+TOTP mint revokes the token
on any other machine using the same account), official exchange totals, and
history back to 2022. Fetched locally only (curl_cffi; Akamai blocks cloud IPs).

Endpoint, read from MCX's own page script (assets/customjs/BhavCopy.js +
data.js GetData, which prefixes the page path):
    GET https://www.mcxindia.com/market-data/bhavcopy/GetDateWiseBhavCopy
        ?InstrumentName=FUTCOM&fromDate=DD/MM/YYYY
The date MUST be DD/MM/YYYY: any other format is not an error, MCX silently
returns the LATEST day instead. fetch_day() therefore checks every row's Date.

Per row (17 Nov 2026): Symbol (space-padded), InstrumentName, ExpiryDate,
OptionType, StrikePrice, Open/High/Low/Close, PreviousClose, Volume (lots),
Value (Rs lakh), OpenInterest (lots), Date ("MM/DD/YYYY").

Validated 13 Nov 2026 against the Dhan-sourced OI then stored: identical for
every single-contract commodity (GOLD, SILVER, ZINC, LEAD, NICKEL, ...); 1-14%
higher where several contracts are open, because MCX sums every open contract.
"""
from __future__ import annotations

import datetime as dt
import statistics
import time

BHAVCOPY_PAGE = "https://www.mcxindia.com/market-data/bhavcopy"
BHAVCOPY_URL = BHAVCOPY_PAGE + "/GetDateWiseBhavCopy"
FUTURES = ("FUTCOM", "FUTIDX")
_HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": BHAVCOPY_PAGE,
}


class BhavcopyDateMismatch(Exception):
    """MCX answered with a different day than asked (not published yet, a
    holiday, or a date format it ignored)."""


def new_bhavcopy_session(timeout=30):
    from lib.curl_session import new_session
    return new_session(BHAVCOPY_PAGE, timeout=timeout)


def _row_date(r) -> str | None:
    """Row 'Date' ("MM/DD/YYYY") as ISO, or None."""
    s = str(r.get("Date") or "").strip()
    try:
        return dt.datetime.strptime(s, "%m/%d/%Y").date().isoformat()
    except ValueError:
        return None


def fetch_day(session, date_iso: str, instrument: str = "FUTCOM", timeout=60,
              retries=2) -> list[dict]:
    """Raw bhavcopy rows for one date and instrument.

    [] when MCX has no report for the date (holiday, not yet published): MCX
    then returns the latest day, which is detected by the row dates and never
    passed on as the asked date. Raises on a network/HTTP/JSON error after
    `retries` retries."""
    d = dt.date.fromisoformat(date_iso)
    params = {"InstrumentName": instrument, "fromDate": d.strftime("%d/%m/%Y")}
    last = None
    for attempt in range(retries + 1):
        try:
            r = session.get(BHAVCOPY_URL, params=params, headers=_HEADERS, timeout=timeout)
            r.raise_for_status()
            body = r.json()
            rows = body.get("Data") or []
            if not body.get("IsSuccess", True) or not rows:
                return []
            dates = {_row_date(x) for x in rows}
            if dates != {date_iso}:
                return []          # another day (latest) came back: no report for date_iso
            return rows
        except Exception as e:     # noqa: BLE001 — retried, then re-raised
            last = e
            if attempt < retries:
                time.sleep(2 * (attempt + 1))
    raise last


def lot_multipliers(rows) -> dict[str, float]:
    """symbol -> rupees per lot per unit of quoted price, from that day's
    traded contracts: Value (Rs lakh) * 1e5 / (Volume (lots) * Close). Median
    across the symbol's traded contracts (VWAP vs close makes each ~0.1% off).
    Symbols with no traded contract that day are absent."""
    per: dict[str, list[float]] = {}
    for r in rows:
        vol, close, value = r.get("Volume") or 0, r.get("Close") or 0, r.get("Value") or 0
        if vol > 0 and close > 0 and value > 0:
            per.setdefault(str(r.get("Symbol") or "").strip(), []).append(value * 1e5 / (vol * close))
    return {s: statistics.median(v) for s, v in per.items() if s}


def futures_oi(rows, fallback_mult: dict[str, float] | None = None) -> dict[tuple[str, str], dict]:
    """{(symbol, instrument): {"open_interest": lots, "oi_value_cr": Rs Cr}}
    summed over every open futures contract in `rows`.

    oi_value_cr = sum(OI x Close x lot multiplier) / 1e7; the multiplier comes
    from the day's traded contracts, else `fallback_mult` (e.g. an earlier
    day's), else the value is None (OI in lots is still returned)."""
    mult = {**(fallback_mult or {}), **lot_multipliers(rows)}
    out: dict[tuple[str, str], dict] = {}
    for r in rows:
        inst = str(r.get("InstrumentName") or "").strip()
        if inst not in FUTURES:
            continue
        sym = str(r.get("Symbol") or "").strip()
        if not sym:
            continue
        g = out.setdefault((sym, inst), {"open_interest": 0, "_value": 0.0, "_priced": True})
        oi = int(r.get("OpenInterest") or 0)
        g["open_interest"] += oi
        m, close = mult.get(sym), r.get("Close") or 0
        if oi and (m is None or close <= 0):
            g["_priced"] = False
        elif oi:
            g["_value"] += oi * close * m / 1e7
    return {k: {"open_interest": g["open_interest"],
                "oi_value_cr": round(g["_value"], 2) if g["_priced"] else None}
            for k, g in out.items()}


def fetch_futures_oi(session, date_iso: str, fallback_mult=None):
    """(futures OI dict as futures_oi(), lot multipliers) for one date, both
    futures instruments. Empty dict when MCX has no report for the date."""
    rows = []
    for inst in FUTURES:
        rows += fetch_day(session, date_iso, inst)
    return futures_oi(rows, fallback_mult), lot_multipliers(rows)
