"""
MCX trading calendar: session hours (seasonal close), holidays, part-day and
special sessions, the IST clock, and fiscal-year / quarter labels.

One copy for api/, lib/ and scripts/ (Phase 3 removed the holiday and
is_trading_day copies in api/quarterly, bhav_refresh and the relay, the
now_ist copies in four scripts, and three sets of FY / quarter helpers).
lib.mcx_config re-exports every public name.
"""
from datetime import date, datetime, timedelta


# ─── SESSION TIMING ──────────────────────────────────────────────────────────
# Non-agri commodities (bullion, energy, base metals — ~all of MCX turnover)
# open at 09:00 IST year-round but close with the US clock:
#   23:30 IST while the US is on daylight time
#   23:55 IST while the US is on standard time
# MCX announces each switch by circular. It takes effect on the first trading
# day after the US change and the old hours run to the Friday before it:
#   MCX/TRD/643/2024  23:55 from 2024-11-04 to 2025-03-07
#   MCX/TRD/071/2025  23:30 from 2025-03-10
#   MCX/TRD/491/2025  23:55 from 2025-11-03 to 2026-03-06
#   MCX/TRD/068/2026  23:30 from 2026-03-09
# The US rule (DST from the 2nd Sunday of March to the 1st Sunday of November)
# reproduces every date above, so it is computed rather than listed. If a
# circular ever departs from it, put the exception in SESSION_END_OVERRIDES.
# Agri contracts close at 17:00 or 21:00 all year; their turnover is too small
# to move the exchange total, so the session model ignores them.
SESSION_START      = 540    # 09:00 IST in minutes since midnight
SESSION_END_US_DST = 1410   # 23:30 IST, US on daylight time (Mar–Nov)
SESSION_END_US_STD = 1435   # 23:55 IST, US on standard time (Nov–Mar)
SESSION_END_OVERRIDES = {}  # {"YYYY-MM-DD": close in minutes since midnight IST}


def _as_date(d=None):
    """Coerce None (today, IST) / date / datetime / 'YYYY-MM-DD' to a date."""
    if d is None:
        return now_ist().date()
    if isinstance(d, str):
        return datetime.strptime(d[:10], "%Y-%m-%d").date()
    if isinstance(d, datetime):
        return d.date()
    return d


def _nth_sunday(year, month, n):
    first = datetime(year, month, 1).date()
    return first + timedelta(days=(6 - first.weekday()) % 7 + 7 * (n - 1))


def us_on_standard_time(d=None) -> bool:
    """True if US clocks are on standard time on trading date d."""
    d = _as_date(d)
    return not (_nth_sunday(d.year, 3, 2) <= d < _nth_sunday(d.year, 11, 1))


def session_end(d=None) -> int:
    """MCX close for trading date d, in minutes since midnight IST."""
    d = _as_date(d)
    override = SESSION_END_OVERRIDES.get(d.strftime("%Y-%m-%d"))
    if override is not None:
        return override
    return SESSION_END_US_STD if us_on_standard_time(d) else SESSION_END_US_DST


def session_total(d=None) -> int:
    """Session length for trading date d in minutes (870 summer, 895 winter)."""
    return session_end(d) - SESSION_START


def fmt_ist_minutes(m: int) -> str:
    """1435 -> '23:55'."""
    return f"{m // 60:02d}:{m % 60:02d}"


# ─── MCX HOLIDAYS — Official calendar (2026 verified Feb 25, 2026) ────────────
# Source: mcxindia.com / 5paisa.com / ICICI Direct
# FULL-DAY = both sessions closed (zero revenue)
# MORNING-ONLY = morning closed, evening session OPEN (~67% revenue)
# EVENING-ONLY = morning open, evening closed (~33% revenue)
# Dates are fully-qualified (YYYY-MM-DD), so each set spans MULTIPLE years.
# ⚠ When MCX publishes a new year's calendar, ADD its dates here AND to
#   _MACRO_EVENTS_BY_YEAR below. _warn_if_calendar_stale() prints a startup
#   warning if the current year has no holiday data, so a year rollover never
#   silently turns a holiday into a normal trading day.
MCX_HOLIDAYS = {
    # ── 2025 tail (within 45-day lookback) ──
    "2025-12-25",  # Christmas (full day)
    # ── 2026 FULL-DAY closures ──
    "2026-01-26",  # Republic Day
    "2026-04-03",  # Good Friday
    "2026-10-02",  # Gandhi Jayanti
    "2026-12-25",  # Christmas
    # ── 2027 FULL-DAY closures: ADD when MCX publishes the 2027 calendar ──
}
# Morning-only closures (evening session trades — partial revenue day)
MCX_MORNING_CLOSE = {
    "2026-03-03",  # Holi (2nd day)
    "2026-03-26",  # Shri Ram Navmi
    "2026-03-31",  # Mahavir Jayanti
    "2026-04-14",  # Ambedkar Jayanti
    "2026-05-01",  # Maharashtra Day
    "2026-05-28",  # Bakri Id
    "2026-06-26",  # Moharram
    "2026-09-14",  # Ganesh Chaturthi
    "2026-10-20",  # Dassera
    "2026-11-10",  # Diwali-Balipratipada
    "2026-11-24",  # Guru Nanak Jayanti
    # ── 2027: ADD when published ──
}
# Evening-only closure
MCX_EVENING_CLOSE = {
    "2025-01-01",  # New Year Day (MCX revenue Rs 0.426 Cr, morning only)
    "2026-01-01",  # New Year Day
    # ── 2027: ADD when published ──
}
# Special sessions on days that are otherwise closed (Muhurat trading on a
# weekend). Capture-only: daily_verify and bhav_refresh backfill the day's row
# from MCX's historical report, api/quarterly.py counts its revenue but keeps it
# out of the run-rate, and is_trading_day() stays False so the day is never
# projected as a full session. MCX's historical report covers these sessions
# (2025-10-21 Muhurat reproduces the stored Rs 1.2612 Cr exactly).
MCX_SPECIAL_SESSIONS = {
    # Past Muhurat sessions fell on weekdays with the morning session closed;
    # listed so they stay out of averages like the 2026 one (MCX's historical
    # report: 2024 Rs 0.4964 Cr, 2025 Rs 1.2612 Cr).
    "2024-11-01": "Muhurat trading (Diwali, Friday; morning closed)",
    "2025-10-21": "Muhurat trading (Diwali, Tuesday; morning closed)",
    "2026-11-08": "Muhurat trading (Diwali, Sunday) — timings not yet notified",
}
# Years whose MCX holiday circular has been entered IN FULL above (full-day,
# morning-closed and evening-closed dates). A year that is missing here is
# "uncovered": its holidays would be treated as normal trading days. Add the
# year here in the same commit that adds its dates. The 2027 circular was not
# published as of 17 Nov 2026 (MCX issued the 2026 one on 15 Dec 2025).
MCX_CALENDAR_YEARS = {2026}
# Days before 1 January from which a missing next-year calendar is reported as
# DUE (MCX normally publishes in mid-December).
CALENDAR_LEAD_DAYS = 21


def calendar_status(today=None) -> dict:
    """Is the MCX holiday calendar loaded for the year we are trading in?

    status: "OK"    — current year covered (and next year too, or not yet due)
            "DUE"   — current year covered, but 1 Jan is within
                      CALENDAR_LEAD_DAYS and next year's calendar is missing
            "STALE" — the current year is not covered: every holiday is being
                      treated as a trading day right now
    Callers surface this loudly (freshness scan, daily_verify exit code, relay
    heartbeat, /api/refresh?health=1) instead of only printing to stderr."""
    if today is None:
        from datetime import timezone   # now_ist() is defined further down
        today = (datetime.now(timezone.utc).replace(tzinfo=None)
                 + timedelta(hours=5, minutes=30)).date()
    elif isinstance(today, datetime):
        today = today.date()
    yr = today.year
    days_to_rollover = (type(today)(yr + 1, 1, 1) - today).days
    if yr not in MCX_CALENDAR_YEARS:
        status, msg = "STALE", (f"MCX holiday calendar for {yr} is not loaded: holidays are "
                                f"being treated as trading days. Add {yr} to MCX_HOLIDAYS / "
                                f"MCX_MORNING_CLOSE / MCX_EVENING_CLOSE and MCX_CALENDAR_YEARS "
                                f"in lib/mcx_config.py.")
    elif yr + 1 not in MCX_CALENDAR_YEARS and days_to_rollover <= CALENDAR_LEAD_DAYS:
        status, msg = "DUE", (f"MCX holiday calendar for {yr + 1} is not loaded and the year "
                              f"rolls over in {days_to_rollover} day(s). Add it from MCX's "
                              f"circular before 1 Jan.")
    else:
        status, msg = "OK", None
    return {"status": status, "year": yr, "covered_years": sorted(MCX_CALENDAR_YEARS),
            "days_to_rollover": days_to_rollover, "message": msg}


# Backward-compat aliases — several modules import the *_2026 names.
MCX_HOLIDAYS_2026 = MCX_HOLIDAYS
MCX_MORNING_CLOSE_2026 = MCX_MORNING_CLOSE
MCX_EVENING_CLOSE_2026 = MCX_EVENING_CLOSE

# ─── SESSION SHAPE (part-day sessions) ───────────────────────────────────────
# On a morning-closed holiday MCX trades only the evening session, 17:00 to the
# day's seasonal close; on an evening-closed one only the morning, 09:00–17:00.
# session_shape() gives the hours actually traded on a date. The close of a
# full or evening-only day is always session_end(d), so the 23:30 / 23:55 rule
# and SESSION_END_OVERRIDES keep applying.
#
# Stored elapsed_min stays "minutes since 09:00, capped at the day's close" on
# every day, because the frontend clock, the curves and the range statistics all
# read it that way. session_clock() also returns traded_min, the minutes the
# market has actually been open, which is what the projection and the share of
# the session gone are based on.
EVENING_SESSION_START = 1020   # 17:00 IST
SESSION_FULL = "full"
SESSION_EVENING_ONLY = "evening_only"   # morning closed
SESSION_MORNING_ONLY = "morning_only"   # evening closed
SESSION_CLOSED = "closed"


def session_shape(d=None) -> dict:
    """Trading hours on date d: {"start", "end", "total", "type"}, in minutes
    since midnight IST. "closed" (total 0) for weekends, full-day holidays and
    special sessions such as Muhurat, which are never projected live."""
    d = _as_date(d)
    ds = d.strftime("%Y-%m-%d")
    if d.weekday() >= 5 or ds in MCX_HOLIDAYS:
        return {"start": SESSION_START, "end": SESSION_START, "total": 0, "type": SESSION_CLOSED}
    if ds in MCX_EVENING_CLOSE:
        start, end, kind = SESSION_START, EVENING_SESSION_START, SESSION_MORNING_ONLY
    elif ds in MCX_MORNING_CLOSE:
        start, end, kind = EVENING_SESSION_START, session_end(d), SESSION_EVENING_ONLY
    else:
        start, end, kind = SESSION_START, session_end(d), SESSION_FULL
    return {"start": start, "end": end, "total": end - start, "type": kind}


# Revenue of a part-day session relative to a normal day, for counting the
# quarter's remaining days. Measured on 2026's part days against the median of
# the ten nearest full days: morning-closed (evening-only) median 0.82, mean
# 0.85, n=10, range 0.62-1.34; evening-closed (morning-only) 0.14, n=1
# (1 Jan 2026). Re-measure as more part days accumulate.
PART_DAY_REVENUE_WEIGHT = {"evening_only": 0.82, "morning_only": 0.15}


def session_weight(d) -> float:
    """Expected revenue on date d as a share of a full session (0 when closed)."""
    shape = session_shape(d)
    if shape["type"] == SESSION_CLOSED:
        return 0.0
    return PART_DAY_REVENUE_WEIGHT.get(shape["type"], 1.0)


def session_clock(t, d=None):
    """(elapsed_min, traded_min, shape) at IST time t on trading date d
    (default: t's date). elapsed_min counts from 09:00, capped at the day's
    close; traded_min counts from the day's real open, capped at its length."""
    shape = session_shape(t if d is None else d)
    now = t.hour * 60 + t.minute
    elapsed = max(0, min(now, shape["end"]) - SESSION_START)
    traded = max(0, min(now, shape["end"]) - shape["start"])
    return elapsed, traded, shape


def traded_minutes(elapsed_min, d) -> int:
    """Minutes traded on date d by a stored elapsed_min (minutes since 09:00)."""
    shape = session_shape(d)
    return max(0, min(SESSION_START + elapsed_min, shape["end"]) - shape["start"])

def now_ist() -> datetime:
    """Return current time in IST."""
    from datetime import timezone
    return datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=5, minutes=30)


def is_trading_day(d) -> bool:
    """Check if date is a weekday and not a full-closure MCX holiday."""
    return d.weekday() < 5 and d.strftime("%Y-%m-%d") not in MCX_HOLIDAYS_2026


def has_session(d) -> bool:
    """True if MCX trades at all on d: a regular trading day or a special session."""
    return is_trading_day(d) or d.strftime("%Y-%m-%d") in MCX_SPECIAL_SESSIONS


def is_market_open(dt=None) -> bool:
    """Check if MCX is currently in trading hours (part-day sessions aware)."""
    if dt is None:
        dt = now_ist()
    mins = dt.hour * 60 + dt.minute
    shape = session_shape(dt)
    return shape["type"] != SESSION_CLOSED and shape["start"] <= mins <= shape["end"]


# ─── Fiscal year and quarter labels (MCX's year runs April to March) ────────

def fy_label(d) -> str:
    """'FY27' for any date from 1 Apr 2026 to 31 Mar 2027."""
    return f"FY{(d.year + 1 if d.month >= 4 else d.year) % 100:02d}"


def fy_start(d) -> date:
    """1 April of the fiscal year containing d."""
    return date(d.year if d.month >= 4 else d.year - 1, 4, 1)


def quarter_start(d) -> date:
    """First day of the fiscal quarter containing d."""
    return date(d.year, (d.month - 1) // 3 * 3 + 1, 1)


def quarter_label(d) -> str:
    """'Q3 FY27' for any date in Oct–Dec 2026 (Q1 = Apr–Jun)."""
    return f"Q{(d.month - 4) % 12 // 3 + 1} {fy_label(d)}"


def prev_quarter(label: str) -> str:
    """'Q1 FY27' -> 'Q4 FY26'; 'Q3 FY27' -> 'Q2 FY27'."""
    q, fy = int(label[1]), int(label[-2:])
    return f"Q4 FY{fy - 1:02d}" if q == 1 else f"Q{q - 1} FY{fy:02d}"


def yoy_quarter(label: str) -> str:
    """Same quarter a year earlier: 'Q3 FY27' -> 'Q3 FY26'."""
    return f"{label[:2]} FY{int(label[-2:]) - 1:02d}"
