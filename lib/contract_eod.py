"""
Per-contract MCX futures end-of-day files: one CSV per session,
${MCX_CONTRACT_EOD_DIR:-~/tradingone/data/mcx_contract_eod}/YYYY-MM-DD.csv,
read by a separate local project. The columns and the file name are that
project's contract: change neither.

Source: MCX's daily Bhavcopy (lib/mcx_bhavcopy.py), the exchange's own
per-contract report. Every FUTCOM contract and expiry it lists goes in, traded
or not. Written by scripts/backfill_contract_eod.py: daily_verify runs it each
morning for the last 10 sessions, and it is run by hand for the history.

Columns:
  date        the session (the report's own Date; rows of any other day are
              never filed under it)
  symbol      commodity code, e.g. GOLDM, CRUDEOIL
  instrument  FUTCOM
  expiry      YYYY-MM-DD
  open/high/low  as reported; blank where the report has 0 (did not trade)
  close       MCX's official Close. MCX publishes one for an untraded
              contract too (equal to PreviousClose in every report checked,
              Nov 2026); volume = 0 marks those rows.
  prev_close  the report's PreviousClose; blank if 0 or missing
  ltp         the report's last traded price; the Bhavcopy has none (its
              sLTT is always null), so blank unless an LTP field appears
  oi, volume  lots
  captured_at IST time the report was fetched, ISO with +05:30

Standard library only, no network: fetching is the caller's.
"""
import csv
import os
import re
import tempfile
from datetime import date, datetime, timedelta

from lib.calendar import has_session

CONTRACT_EOD_COLUMNS = ("date", "symbol", "instrument", "expiry", "open", "high", "low",
                        "close", "prev_close", "ltp", "oi", "volume", "captured_at")
DEFAULT_DIR = "~/tradingone/data/mcx_contract_eod"
_MONTHS = {m: i for i, m in enumerate(
    ("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"), 1)}


def contract_eod_dir():
    """$MCX_CONTRACT_EOD_DIR, else ~/tradingone/data/mcx_contract_eod (read at call time)."""
    return os.environ.get("MCX_CONTRACT_EOD_DIR") or os.path.expanduser(DEFAULT_DIR)


def contract_eod_path(date_iso, out_dir=None):
    return os.path.join(out_dir or contract_eod_dir(), f"{date_iso}.csv")


def expiry_iso(s):
    """'30NOV2026' -> '2026-11-30' (locale-free), else None."""
    m = re.fullmatch(r"(\d{1,2})([A-Za-z]{3})(\d{4})", str(s or "").strip())
    if not m or m.group(2).upper() not in _MONTHS:
        return None
    try:
        return date(int(m.group(3)), _MONTHS[m.group(2).upper()], int(m.group(1))).isoformat()
    except ValueError:
        return None


def _row_date(r):
    """The row's Date ("MM/DD/YYYY") as ISO, else None."""
    try:
        return datetime.strptime(str(r.get("Date") or "").strip(), "%m/%d/%Y").date().isoformat()
    except ValueError:
        return None


def _price(v):
    """A reported price; "" where the report has 0 or nothing."""
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


def bhavcopy_rows(raw_rows, date_iso, captured_at):
    """One dict per FUTCOM contract of `date_iso` in a Bhavcopy report, keys in
    CONTRACT_EOD_COLUMNS order, sorted by symbol then expiry. Dropped: other
    instruments, rows dated another day, rows without a symbol or a readable
    expiry; a repeated (symbol, expiry) keeps the first."""
    out = {}
    for r in raw_rows or []:
        if not isinstance(r, dict):
            continue
        inst = str(r.get("InstrumentName") or r.get("Instrumentname") or "").strip().upper()
        if inst != "FUTCOM" or _row_date(r) != date_iso:
            continue
        sym = str(r.get("Symbol") or r.get("ProductCode") or "").strip()
        exp = expiry_iso(r.get("ExpiryDate"))
        if not sym or not exp or (sym, exp) in out:
            continue
        out[(sym, exp)] = dict(zip(CONTRACT_EOD_COLUMNS, (
            date_iso, sym, "FUTCOM", exp,
            _price(r.get("Open")), _price(r.get("High")), _price(r.get("Low")),
            _price(r.get("Close")), _price(r.get("PreviousClose")), _price(r.get("LTP")),
            _count(r.get("OpenInterest")), _count(r.get("Volume")), captured_at)))
    return [out[k] for k in sorted(out)]


def write_contract_eod(rows, date_iso, out_dir=None):
    """Write rows to <out_dir>/<date_iso>.csv atomically: a temp file in the
    same directory, fsync, then os.replace, so a reader never sees a partial
    file. Returns "written"; "empty" (no rows: nothing touched, no directory
    made); or "kept" (the existing file has more contracts: a smaller capture
    never replaces it). Raises ValueError, writing nothing, if any row is
    dated another day; I/O errors are raised after the temp file is removed,
    leaving any existing file as it was."""
    if not rows:
        return "empty"
    wrong = {r.get("date") for r in rows} - {date_iso}
    if wrong:
        raise ValueError(f"rows dated {sorted(map(str, wrong))} refused for {date_iso}")
    out_dir = out_dir or contract_eod_dir()
    path = contract_eod_path(date_iso, out_dir)
    if os.path.exists(path):
        try:
            with open(path, newline="") as f:
                have = max(sum(1 for line in csv.reader(f) if line) - 1, 0)
        except Exception:   # unreadable: not worth keeping over a full capture
            have = 0
        if have > len(rows):
            return "kept"
    os.makedirs(out_dir, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{date_iso}.", suffix=".csv.tmp", dir=out_dir)
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


def session_dates(start, end, weekends=False):
    """ISO dates from start to end (inclusive), oldest first: days MCX trades
    (lib.calendar.has_session: weekdays minus the known holidays, plus special
    sessions), or every day with weekends=True. Before the years
    lib.calendar's holiday list covers, holidays are included; the Bhavcopy
    has no report for them and the caller counts them as such."""
    d, last, out = date.fromisoformat(start), date.fromisoformat(end), []
    while d <= last:
        if weekends or has_session(d):
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def recent_session_dates(today, n):
    """The last n session dates before `today` (a date), oldest first."""
    out, d = [], today - timedelta(days=1)
    while len(out) < n and d > today - timedelta(days=n * 3 + 30):
        if has_session(d):
            out.append(d.isoformat())
        d -= timedelta(days=1)
    return sorted(out)


def pending_dates(dates, out_dir=None):
    """The dates that have no file yet, in the given order (resume)."""
    return [d for d in dates if not os.path.exists(contract_eod_path(d, out_dir))]
