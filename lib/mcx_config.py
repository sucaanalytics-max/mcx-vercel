"""
MCX Revenue Model — Shared Configuration (F-02, F-22)
Single source of truth for all fee rates, day classifications, volume curves,
and projection logic. Imported by all API endpoints.
"""
import os, math, time as _time
from datetime import datetime, timedelta
from collections import defaultdict

from lib.supabase import _load_project_env  # noqa: F401  (loads .env.local keys on import)



# ─── FEE SCHEDULE (SEBI Oct 2024 flat rate) ──────────────────────────────────
FUTURES_RATE = 210.0        # ₹ per crore, both sides
OPTIONS_RATE = 4180.0       # ₹ per crore of premium, both sides
NONTX_DAILY  = float(os.environ.get("MCX_NONTX_DAILY", "0.00"))  # removed from daily predictor
TRADING_DAYS = int(os.environ.get("MCX_TRADING_DAYS", "256"))      # FY27 MCX sessions: 261 weekdays − 5 full closures (FY28: 260). Annualises daily revenue for valuation and the Annualized KPI.

# ─── Supabase ────────────────────────────────────────────────────────────────
# The REST client, keys and pagination live in lib/supabase.py. Re-exported
# here because ~40 modules import them from lib.mcx_config.
from lib.supabase import (  # noqa: E402,F401
    SUPABASE_URL, SUPABASE_ANON_KEY, SUPABASE_SERVICE_KEY, SUPABASE_WRITE_KEY,
    SUPABASE_WRITE_ROLE, _LEGACY_ANON_KEY, _is_jwt, supabase_key_kind, supabase_headers,
    _supabase_request, supabase_read, PAGINATION_KEYS, paginated_order,
    supabase_read_all, supabase_upsert, upsert_batches, fetch_all,
)

# ─── Commodity normalization ─────────────────────────────────────────────────
# Groups MCX mini/micro/petal variants under their parent commodity so all
# downstream stats (revenue dashboard, signals z-scores, OI) reflect the
# *complex* rather than fragmenting silver/gold across multiple symbols.
COMMODITY_MAP = {
    "CRUDEOILM": "CRUDEOIL", "NATGASMINI": "NATURALGAS",
    "GOLDM": "GOLD", "GOLDGUINEA": "GOLD", "GOLDPETAL": "GOLD", "GOLDTEN": "GOLD",
    "SILVERM": "SILVER", "SILVERMIC": "SILVER",
    "LEADMINI": "LEAD", "ZINCMINI": "ZINC", "ALUMINI": "ALUMINIUM",
    "ELECDMBL": "NATURALGAS",  # Electric daily bilateral — group under energy
}


def consolidate_commodity(sym: str) -> str:
    """Return the parent commodity for a mini/micro/petal variant (else sym)."""
    return COMMODITY_MAP.get(sym, sym)


# MCX commodity_head taxonomy (used as fallback when upstream data lacks it,
# e.g. live-OI rows from Dhan that don't carry the segment field).
COMMODITY_HEAD = {
    # Bullion
    "SILVER": "BULLION", "SILVERM": "BULLION", "SILVERMIC": "BULLION",
    "GOLD": "BULLION", "GOLDM": "BULLION", "GOLDGUINEA": "BULLION",
    "GOLDPETAL": "BULLION", "GOLDTEN": "BULLION",
    # Energy
    "CRUDEOIL": "ENERGY", "CRUDEOILM": "ENERGY",
    "NATURALGAS": "ENERGY", "NATGASMINI": "ENERGY", "ELECDMBL": "ENERGY",
    # Base metals
    "COPPER": "BASE METALS",
    "ZINC": "BASE METALS", "ZINCMINI": "BASE METALS",
    "ALUMINIUM": "BASE METALS", "ALUMINI": "BASE METALS",
    "LEAD": "BASE METALS", "LEADMINI": "BASE METALS",
    "NICKEL": "BASE METALS",
    # Agri
    "COTTON": "AGRI COMMODITIES", "COTTONOIL": "AGRI COMMODITIES",
    "KAPAS": "AGRI COMMODITIES", "MENTHAOIL": "AGRI COMMODITIES",
    "CARDAMOM": "AGRI COMMODITIES",
    # Index
    "MCXBULLDEX": "INDEX", "MCXMETLDEX": "INDEX",
}


# ─── CORS (F-13: restricted to deployment domains) ───────────────────────────
ALLOWED_ORIGINS = os.environ.get(
    "ALLOWED_ORIGINS",
    "https://mcx-vercel.vercel.app,http://localhost:8765,http://localhost:3000"
).split(",")

# ─── RANGE CHIPS (30D / 60D / Q / 1Y / 2Y / Max) ─────────────────────────────
# Trading days behind each range chip; mirrors RANGE_TRADING_DAYS in
# assets/js/core.js. Max = every row. Endpoints that cannot serve all history
# use BOUNDED_RANGE_DAYS, so ?range=Max falls back to DEFAULT_RANGE there.
RANGE_DAYS = {"30D": 30, "60D": 60, "Q": 63, "1Y": 252, "2Y": 504, "Max": None}
BOUNDED_RANGE_DAYS = {k: v for k, v in RANGE_DAYS.items() if v is not None}
DEFAULT_RANGE = "60D"

# mcx_daily_revenue.source values whose row is the final figure for its day:
# the relay's end-of-day capture and MCX's historical report. A row for today
# counts as a finished session only with one of these and after the close.
FINAL_ROW_SOURCES = {"mcx_relay_eod", "mcx_historical"}

# ─── Calendar ────────────────────────────────────────────────────────────────
# Session hours, holidays, part-day and special sessions, now_ist and the
# FY / quarter labels live in lib/calendar.py; re-exported for existing importers.
from lib.calendar import (  # noqa: E402,F401
    SESSION_START, SESSION_END_US_DST, SESSION_END_US_STD, SESSION_END_OVERRIDES,
    _as_date, _nth_sunday, us_on_standard_time, session_end, session_total, fmt_ist_minutes,
    MCX_HOLIDAYS, MCX_MORNING_CLOSE, MCX_EVENING_CLOSE, MCX_SPECIAL_SESSIONS,
    MCX_CALENDAR_YEARS, CALENDAR_LEAD_DAYS, calendar_status,
    MCX_HOLIDAYS_2026, MCX_MORNING_CLOSE_2026, MCX_EVENING_CLOSE_2026,
    EVENING_SESSION_START, SESSION_FULL, SESSION_EVENING_ONLY, SESSION_MORNING_ONLY,
    SESSION_CLOSED, session_shape, PART_DAY_REVENUE_WEIGHT, session_weight, session_clock,
    traded_minutes, now_ist, is_trading_day, has_session, is_market_open,
    fy_label, fy_start, quarter_label, quarter_start, prev_quarter, yoy_quarter,
)

# ─── INTRADAY VOLUME CURVE (calibrated MCX energy-heavy session) ─────────────
# MCX evening session (17:00–close) = ~67% of daily turnover.
# Weights sum to 1.0 exactly. The last bucket runs from 22:00 to the day's
# close, so it is 90 min in summer and 115 min in winter.
_BUCKETS_BEFORE_LATE = [
    ( 540,  630, 0.06),   # 09:00–10:30  Opening + metals
    ( 630,  750, 0.10),   # 10:30–12:30  Mid-morning
    ( 750,  900, 0.07),   # 12:30–15:00  Post-lunch lull
    ( 900, 1020, 0.10),   # 15:00–17:00  Pre-evening
    (1020, 1170, 0.18),   # 17:00–19:30  Europe open
    (1170, 1320, 0.34),   # 19:30–22:00  ★ PRIME: NYMEX open
]
LATE_BUCKET_START = 1320  # 22:00
_LATE_WEIGHT = 0.15       # 22:00–23:30 in a summer session
# Share of a winter day's turnover traded 23:30–23:55. The static curve gives
# it to the late bucket and scales the rest down so the weights still sum to 1.
# Measured on the six full winter days with relay snapshots (23 Feb – 6 Mar
# 2026): the 23:30 cumulative was 3.3–7.4% (median 4.0%) short of MCX's
# full-day historical report. The adaptive curve replaces this once enough
# 23:55 days have snapshots.
WINTER_EXTENSION_SHARE = 0.04


def intraday_buckets(d=None):
    """[(start_min, end_min, weight), ...] for trading date d. Seven buckets."""
    end = session_end(d)
    if end == SESSION_END_US_DST:
        return _BUCKETS_BEFORE_LATE + [(LATE_BUCKET_START, end, _LATE_WEIGHT)]
    # Winter (or an override): the extension's share goes to the late bucket.
    ext = WINTER_EXTENSION_SHARE if end > SESSION_END_US_DST else 0.0
    scale = 1.0 - ext
    return ([(s, e, w * scale) for s, e, w in _BUCKETS_BEFORE_LATE]
            + [(LATE_BUCKET_START, end, _LATE_WEIGHT * scale + ext)])


def bucket_labels(d=None):
    """['09:00–10:30', ..., '22:00–23:55'] for trading date d."""
    return [f"{fmt_ist_minutes(s)}–{fmt_ist_minutes(e)}" for s, e, _ in intraday_buckets(d)]

# ─── DAY-TYPE CLASSIFICATION (F-21: algorithmic + manual overrides) ──────────
# Volume multipliers — calibrated by research/calibrate_day_types.py against 400
# trading days (2024-10-01 → 2026-04-24), month-normalized to LOW baseline:
#   LOW    n=371  mean 1.000 (baseline)
#   MEDIUM n=14   mean 1.016 ± 0.089 (95% CI [0.93, 1.10] — not significant)
#   HIGH   n=10   mean 1.206 ± 0.295 (95% CI [0.91, 1.50] — high variance)
#   EXPIRY n=4    mean 1.012 ± 0.154 (sample too small, hold at LOW)
# Adopted values trim toward observed point estimates while remaining conservative
# given the wide CIs on small samples. Re-run calibrate_day_types.py periodically.
DAY_MULTIPLIER = {"HIGH": 1.15, "MEDIUM": 1.02, "EXPIRY": 1.00, "LOW": 1.00}
DAY_DESCRIPTION = {
    "HIGH":   "High-event — major macro (RBI/FOMC/Budget/GDP). Observed +21% vs baseline (n=10), held conservatively at +15%.",
    "MEDIUM": "Medium — CrudeOil T-1 · NatGas expiry · CPI · NFP. Observed +1.6% (n=14, not significant); +2% applied.",
    "EXPIRY": "CrudeOil expiry day — observed neutral vs baseline (n=4); held at 1.0x.",
    "LOW":    "No scheduled catalyst — baseline (n=371).",
}

# ─── ALGORITHMIC EXPIRY CALENDAR (F-21) ──────────────────────────────────────
def _get_mcx_crude_expiry(year: int, month: int) -> datetime:
    """MCX CrudeOil expiry: 19th of each month, or previous trading day if holiday/weekend."""
    d = datetime(year, month, 19)
    ds = d.strftime("%Y-%m-%d")
    while d.weekday() >= 5 or ds in MCX_HOLIDAYS_2026:
        d -= timedelta(days=1)
        ds = d.strftime("%Y-%m-%d")
    return d

def _get_trading_day_before(d: datetime, n: int = 1) -> datetime:
    """Get the nth trading day before date d."""
    count = 0
    cur = d - timedelta(days=1)
    while count < n:
        ds = cur.strftime("%Y-%m-%d")
        if cur.weekday() < 5 and ds not in MCX_HOLIDAYS_2026:
            count += 1
            if count == n:
                return cur
        cur -= timedelta(days=1)
    return cur

# Macro / scheduled-event dates that can't be derived algorithmically (central-bank
# decisions, data releases, NatGas expiry). Listed per year — add a new year's entry
# when its schedule is published. Years absent here get no macro HIGH/MEDIUM tags
# (those days fall back to LOW), but algorithmic CrudeOil expiry still applies.
_MACRO_EVENTS_BY_YEAR = {
    2026: {
        # NatGas expiry (~20th-26th, approximate: 4th Friday or nearby)
        "natgas": ["2026-01-23", "2026-02-20", "2026-03-20", "2026-04-24",
                   "2026-05-22", "2026-06-26", "2026-07-24", "2026-08-21",
                   "2026-09-25", "2026-10-23", "2026-11-20", "2026-12-24"],
        "fomc":   ["2026-01-29", "2026-03-18", "2026-05-06", "2026-06-17",
                   "2026-07-29", "2026-09-16", "2026-10-28", "2026-12-09"],
        "rbi":    ["2026-02-06", "2026-04-09", "2026-06-05", "2026-08-07",
                   "2026-10-09", "2026-12-04"],
        "budget": ["2026-02-02"],
        "gdp":    ["2026-02-27", "2026-05-29", "2026-08-31", "2026-11-30"],
        "cpi":    ["2026-01-13", "2026-02-12", "2026-03-12", "2026-04-13",
                   "2026-05-12", "2026-06-12", "2026-07-14", "2026-08-12",
                   "2026-09-14", "2026-10-12", "2026-11-12", "2026-12-14"],
        "nfp":    ["2026-01-09", "2026-03-06", "2026-04-02", "2026-04-30",
                   "2026-07-10", "2026-09-04", "2026-10-01", "2026-11-06"],
        # ── 2027: add a `2027: {...}` block when the schedule is published ──
    },
}

def _build_event_calendar(year: int) -> tuple:
    """
    Algorithmically generate HIGH/MEDIUM/EXPIRY events for a given year.
    CrudeOil expiry is computed for any year; macro events come from
    _MACRO_EVENTS_BY_YEAR (empty for years not yet catalogued).
    Returns (high_set, medium_set, expiry_set) of date strings.
    """
    high = set()
    medium = set()
    expiry = set()

    for month in range(1, 13):
        # CrudeOil expiry cycle (algorithmic — valid for any year)
        exp_date = _get_mcx_crude_expiry(year, month)
        expiry.add(exp_date.strftime("%Y-%m-%d"))
        medium.add(_get_trading_day_before(exp_date, 1).strftime("%Y-%m-%d"))
        high.add(_get_trading_day_before(exp_date, 2).strftime("%Y-%m-%d"))

    macro = _MACRO_EVENTS_BY_YEAR.get(year, {})
    for d in macro.get("natgas", []):
        medium.add(d)
    for d in macro.get("fomc", []):
        high.add(d)
    for d in macro.get("rbi", []):
        high.add(d)
    for d in macro.get("budget", []):
        high.add(d)
    for d in macro.get("gdp", []):
        high.add(d)
    for d in macro.get("cpi", []):
        if d not in high:
            medium.add(d)
    for d in macro.get("nfp", []):
        if d not in high:
            medium.add(d)

    # HIGH wins over MEDIUM; EXPIRY only if not already HIGH/MEDIUM.
    medium -= high
    expiry -= high
    expiry -= medium
    return high, medium, expiry


def _current_ist_year() -> int:
    from datetime import timezone
    return (datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=5, minutes=30)).year


def _warn_if_calendar_stale() -> None:
    """Warn (at import) if the running year has no holiday data, so a year
    rollover doesn't silently turn holidays into normal trading days. This is
    only the stderr echo; calendar_status() is what the freshness scan,
    daily_verify, the relay heartbeat and the health endpoint act on."""
    st = calendar_status()
    if st["status"] != "OK":
        import sys as _sys
        print(f"⚠ MCX calendar {st['status']}: {st['message']} "
              f"Also add _MACRO_EVENTS_BY_YEAR[{st['year'] + (st['status'] == 'DUE')}].",
              file=_sys.stderr)


# Build event sets for 2026 (always) plus the current and next year, so the
# classifier keeps working across a year rollover. 2026 values are unchanged.
_HIGH_EVENTS, _MEDIUM_EVENTS, _EXPIRY_EVENTS = set(), set(), set()
for _yr in sorted({2026, _current_ist_year(), _current_ist_year() + 1}):
    _h, _m, _e = _build_event_calendar(_yr)
    _HIGH_EVENTS |= _h
    _MEDIUM_EVENTS |= _m
    _EXPIRY_EVENTS |= _e
_warn_if_calendar_stale()


def get_day_type(dt: datetime) -> str:
    """Classify a trading day into HIGH/MEDIUM/EXPIRY/LOW."""
    date_str = dt.strftime("%Y-%m-%d")
    if date_str in _HIGH_EVENTS:
        return "HIGH"
    if date_str in _MEDIUM_EVENTS:
        return "MEDIUM"
    if date_str in _EXPIRY_EVENTS:
        return "EXPIRY"
    return "LOW"


# ─── SHARED FUNCTIONS ────────────────────────────────────────────────────────

# Shape of the opening bucket (09:00-10:30): the share of its volume traded by
# fraction x of the bucket is x ** OPEN_BUCKET_ALPHA, not x. The open is a
# burst: on 54 days (Jun-Nov 2026) with a snapshot by 09:15, the median share
# of the bucket's volume done at 10 / 25 / 40 / 55 / 70 min was 0.25 / 0.46 /
# 0.62 / 0.75 / 0.88 for futures and 0.28 / 0.52 / 0.66 / 0.77 / 0.88 for
# options premium, against 0.11 / 0.28 / 0.44 / 0.61 / 0.78 if it traded
# evenly; x ** 0.55 fits both. Interpolating linearly put the curve's share at
# 10 min at less than half the real one, so the first hour's projections ran
# about 2x high (walk-forward median error +65% under 60 min, Jun-Nov 2026).
# The other buckets interpolate linearly: from 10% elapsed the error is
# centred (median within +-3%).
OPEN_BUCKET_ALPHA = 0.55


def _bucket_progress(i, x):
    """Share of bucket i's volume traded by fraction x (0-1) of its span."""
    return x ** OPEN_BUCKET_ALPHA if i == 0 else x


def get_intraday_weight(elapsed_minutes: int, trading_date=None) -> float:
    """Return cumulative volume fraction completed at elapsed_minutes into session."""
    e = min(elapsed_minutes, session_total(trading_date))
    cumulative = 0.0
    for i, (start, end, share) in enumerate(intraday_buckets(trading_date)):
        s  = start - SESSION_START
        en = end   - SESSION_START
        if e >= en:
            cumulative += share
        elif e > s:
            cumulative += share * _bucket_progress(i, (e - s) / (en - s))
        else:
            break
    return min(cumulative, 1.0)


def get_intraday_weight_dynamic(elapsed_minutes: int, bucket_weights: list = None,
                                trading_date=None) -> float:
    """Return cumulative volume fraction using custom bucket weights.
    If bucket_weights is None, falls back to the static intraday_buckets().
    bucket_weights should be a list of 7 floats summing to ~1.0."""
    if bucket_weights is None:
        return get_intraday_weight(elapsed_minutes, trading_date)
    e = min(elapsed_minutes, session_total(trading_date))
    cumulative = 0.0
    for i, (start, end, _) in enumerate(intraday_buckets(trading_date)):
        w = bucket_weights[i] if i < len(bucket_weights) else 0
        s = start - SESSION_START
        en = end - SESSION_START
        if e >= en:
            cumulative += w
        elif e > s:
            cumulative += w * _bucket_progress(i, (e - s) / (en - s))
        else:
            break
    return min(cumulative, 1.0)


# ─── ADAPTIVE CURVE (EWMA) ──────────────────────────────────────────────────
# VWAP-style exponentially weighted moving average of intraday bucket weights.
# Halflife=10 trading days — adapts to regime shifts in ~5 days.
_adaptive_cache = {}   # keyed by session close (minutes since midnight IST)
ADAPTIVE_HALFLIFE = 10   # trading days
ADAPTIVE_MIN_DAYS = 3    # minimum clean days (MCX API data quality limits availability)
ADAPTIVE_TTL = 900       # cache for 15 min (aligned with relay cycle)


def _rolling_median_outlier(vols):
    """Detect non-monotonic outliers using rolling median of 3.
    Returns True if any value < 0.4 × rolling_median (window=3, centered)."""
    if len(vols) < 4:
        return False
    for j in range(1, len(vols) - 1):
        window = sorted([vols[j-1], vols[j], vols[j+1]])
        med = window[1]
        if med > 0 and vols[j] < 0.4 * med:
            return True
    return False


def _bucket_volumes(snaps, vol_fn=None, trading_date=None):
    """Compute (vol, completion) per bucket. Used for both Phase 2 (EWMA) and
    Phase 4 (regime drift). Only fully-completed buckets contribute meaningful
    shares — partial-bucket data can't be normalized without knowing the day's
    full total."""
    if vol_fn is None:
        def vol_fn(s):
            return (s.get("fut_notl_cr") or 0) + (s.get("opt_prem_cr") or 0)
    last_elapsed = max(s["elapsed_min"] for s in snaps)
    out = []
    prev_cum = 0.0
    for start, end, _share in intraday_buckets(trading_date):
        bucket_start = max(0, start - SESSION_START)
        bucket_end = end - SESSION_START
        bucket_width = bucket_end - bucket_start
        if last_elapsed <= bucket_start:
            out.append((0.0, 0.0))
            continue
        clip_end = min(bucket_end, last_elapsed)
        cum = _interpolate_vol_at(snaps, clip_end, vol_fn)
        bucket_vol = max(0.0, cum - prev_cum)
        completion = (clip_end - bucket_start) / bucket_width if bucket_width > 0 else 0.0
        out.append((bucket_vol, max(0.0, min(1.0, completion))))
        prev_cum = cum
    return out


def _interpolate_vol_at(snaps, target_elapsed, vol_fn=None):
    """Linear interpolation of cumulative volume at target_elapsed."""
    if vol_fn is None:
        def vol_fn(s):
            return (s.get("fut_notl_cr") or 0) + (s.get("opt_prem_cr") or 0)
    if not snaps or target_elapsed <= 0:
        return 0.0
    if target_elapsed <= snaps[0]["elapsed_min"]:
        return vol_fn(snaps[0]) * (target_elapsed / max(1, snaps[0]["elapsed_min"]))
    if target_elapsed >= snaps[-1]["elapsed_min"]:
        return vol_fn(snaps[-1])
    for i in range(1, len(snaps)):
        a, b = snaps[i - 1], snaps[i]
        if a["elapsed_min"] <= target_elapsed <= b["elapsed_min"]:
            span = b["elapsed_min"] - a["elapsed_min"]
            if span <= 0:
                return vol_fn(b)
            frac = (target_elapsed - a["elapsed_min"]) / span
            return vol_fn(a) + (vol_fn(b) - vol_fn(a)) * frac
    return vol_fn(snaps[-1])


# A day trains the intraday curves only if its last snapshot reached that
# day's own close (within ADAPTIVE_CLOSE_TOLERANCE_MIN). A partial day's shares
# can't be normalized without the full-day total: the volume after its last
# snapshot is missing, so the late bucket reads low and the early ones high.
# The old fixed floor (800 elapsed min) let in winter days whose relay stopped
# at 23:30, 870 of 895 min: 2-16 Nov 2026, when the relay Mac still ran code
# from before the 23:55 close. Those days lack 23:30-23:55 (~2.6% of the day),
# which put the winter late bucket at ~10.6% instead of ~13% and ran live
# winter projections ~2.6% low. The tolerance must stay under 25 min, the gap
# between the two closes, or a 23:30 cut-off would pass for a 23:55 day.
ADAPTIVE_CLOSE_TOLERANCE_MIN = 10


def reached_close(last_elapsed, trading_date) -> bool:
    """True if a day whose last snapshot is at last_elapsed minutes covers its
    whole session (23:30 or 23:55 close), so it can train the curves."""
    return last_elapsed >= session_total(trading_date) - ADAPTIVE_CLOSE_TOLERANCE_MIN


ADAPTIVE_LOOKBACK_DAYS = 60   # calendar days of snapshots the adaptive curve is fitted on


def trim_stale_open(snaps, vol_fn=None):
    """Drop stale opening snapshots: the MCX API can carry the prior day's
    cumulative into the first poll(s) of a session, so the first snapshot is
    larger than the next. A snapshot in the first 30 minutes whose successor is
    under half its volume is dropped (repeatedly), keeping at least 4.
    `snaps` must be sorted by elapsed_min; vol_fn defaults to fut + opt."""
    if vol_fn is None:
        def vol_fn(s):
            return (s.get("fut_notl_cr") or 0) + (s.get("opt_prem_cr") or 0)
    while len(snaps) > 4:
        v0, v1 = vol_fn(snaps[0]), vol_fn(snaps[1])
        if v0 > 0 and v1 < v0 * 0.5 and snaps[0]["elapsed_min"] < 30:
            snaps = snaps[1:]
        else:
            break
    return snaps


# The snapshots every adaptive curve is fitted on: one read per lookback window
# for ADAPTIVE_TTL, shared by the combined, futures and options curves of both
# seasons (six identical ~2,400-row reads per cold request before Phase 4;
# rows before today never change intraday). Keyed by the reader too, so a test
# that swaps supabase_read_all never sees another's rows.
_adaptive_rows_cache = {}


def _adaptive_snapshots(start_date, today_str):
    key = (start_date, today_str, supabase_read_all)
    hit = _adaptive_rows_cache.get(key)
    if hit and (_time.time() - hit["ts"]) < ADAPTIVE_TTL:
        return hit["rows"]
    rows = supabase_read_all(
        "mcx_snapshots",
        f"?select=trading_date,elapsed_min,fut_notl_cr,opt_prem_cr"
        f"&trading_date=gte.{start_date}&trading_date=lt.{today_str}"
        f"&order=trading_date.asc,elapsed_min.asc",
        max_rows=5000,
        newest_first=True,   # a cap drops the oldest days, never the latest
    )
    _adaptive_rows_cache.clear()          # one window at a time
    _adaptive_rows_cache[key] = {"rows": rows, "ts": _time.time()}
    return rows


def _compute_adaptive_weights(vol_fn=None, close_min=None):
    """Generic adaptive bucket-weight EWMA over recent (full-session) days.

    Returns list of 7 floats (sums to 1.0) or None if insufficient data.
    Caller can pass a vol_fn to compute Fut-only or Opt-only curves.

    Only days whose session closed at close_min (default: today's close) are
    used. A 23:30 day and a 23:55 day give the late bucket different shares, so
    mixing them would bias the curve for the first weeks after each switch;
    until ADAPTIVE_MIN_DAYS same-season days exist this returns None and the
    caller falls back to the static curve for that season.
    """
    if close_min is None:
        close_min = session_end()
    try:
        ist = now_ist()
        start_date = (ist - timedelta(days=ADAPTIVE_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
        today_str = ist.strftime("%Y-%m-%d")

        snapshots = _adaptive_snapshots(start_date, today_str)
        if not snapshots:
            return None

        by_date = defaultdict(list)
        for s in snapshots:
            if (s.get("elapsed_min") is not None and session_end(s["trading_date"]) == close_min
                    and session_shape(s["trading_date"])["type"] == SESSION_FULL):
                by_date[s["trading_date"]].append(s)

        if vol_fn is None:
            def vol_fn(s):
                return (s.get("fut_notl_cr") or 0) + (s.get("opt_prem_cr") or 0)

        lam = math.log(2) / ADAPTIVE_HALFLIFE
        weighted_buckets = [0.0] * 7
        total_weight = 0.0
        dates_used = 0

        sorted_dates = sorted(by_date.keys(), reverse=True)
        for age, dt_str in enumerate(sorted_dates):
            snaps = sorted(by_date[dt_str], key=lambda s: s["elapsed_min"])
            if len(snaps) < 4:
                continue
            # Only full sessions contribute to bucket-share EWMA
            if not reached_close(snaps[-1]["elapsed_min"], dt_str):
                continue
            snaps = trim_stale_open(snaps, vol_fn)
            # Rolling-median outlier detection — replaces the arbitrary 30% threshold
            vols = [vol_fn(s) for s in snaps]
            if _rolling_median_outlier(vols):
                continue

            buckets = _bucket_volumes(snaps, vol_fn, dt_str)
            day_total = sum(v for v, _ in buckets)
            if day_total <= 0:
                continue
            day_shares = [v / day_total for v, _ in buckets]
            if max(day_shares) > 0.50:
                continue

            decay = math.exp(-lam * age)
            for b in range(7):
                weighted_buckets[b] += decay * day_shares[b]
            total_weight += decay
            dates_used += 1

        if dates_used < ADAPTIVE_MIN_DAYS or total_weight <= 0:
            return None
        result = [b / total_weight for b in weighted_buckets]
        return result
    except Exception:
        return None


def _other_season_close(close):
    """The other standard close (23:30 <-> 23:55), or None for an override close."""
    return {SESSION_END_US_DST: SESSION_END_US_STD,
            SESSION_END_US_STD: SESSION_END_US_DST}.get(close)


def season_transfer(weights, from_close, to_close):
    """Map a fitted 7-bucket curve from one season's close to the other's.

    The first six buckets cover the same clock times in both seasons; only the
    late bucket (22:00 to the close) differs, by 23:30-23:55, which carries
    WINTER_EXTENSION_SHARE of a winter day (the static curve's rule). Summer
    -> winter scales the six down and gives the extension to the late bucket;
    winter -> summer reverses it. Used for the first days after each switch,
    before ADAPTIVE_MIN_DAYS same-season days exist. Walk-forward on 2-17 Nov
    2026 (10-11 days, curve-only projection), median / MAE at 75% elapsed:
    static winter curve +11.5% / 12.6, summer curve moved to winter hours
    +1.4% / 8.2; at 50%: -6.4% / 9.3 vs -4.4% / 9.4."""
    if not weights or from_close == to_close:
        return weights
    ext = WINTER_EXTENSION_SHARE
    if from_close == SESSION_END_US_DST and to_close == SESSION_END_US_STD:
        return [w * (1 - ext) for w in weights[:-1]] + [weights[-1] * (1 - ext) + ext]
    if from_close == SESSION_END_US_STD and to_close == SESSION_END_US_DST:
        late = max(0.0, weights[-1] - ext)
        out = list(weights[:-1]) + [late]
        tot = sum(out)
        return [w / tot for w in out] if tot > 0 else None
    return None


def get_adaptive_bucket_weights(trading_date=None):
    """Return EWMA-weighted bucket weights from recent snapshots (combined Fut+Opt)
    for trading_date's season. Returns list of 7 floats summing to ~1.0, or None
    (falls back to static)."""
    close = session_end(trading_date)
    hit = _adaptive_cache.get(close)
    if hit and (_time.time() - hit["ts"]) < ADAPTIVE_TTL:
        return hit["weights"]
    result = _compute_adaptive_weights(vol_fn=None, close_min=close)
    other = _other_season_close(close)
    if result is None and other:
        result = season_transfer(_compute_adaptive_weights(vol_fn=None, close_min=other),
                                 other, close)
    _adaptive_cache[close] = {"weights": result, "ts": _time.time()}
    return result


# Cache for split (Fut, Opt) curves — Phase 3. Keyed by session close.
_adaptive_split_cache = {}


def get_adaptive_bucket_weights_split(trading_date=None):
    """Return (fut_weights, opt_weights) — separate EWMA curves for Fut and Opt.
    Each is a list of 7 floats summing to ~1.0, or None if insufficient data."""
    close = session_end(trading_date)
    hit = _adaptive_split_cache.get(close)
    if hit and (_time.time() - hit["ts"]) < ADAPTIVE_TTL:
        return hit["weights"]
    fut_w = _compute_adaptive_weights(vol_fn=lambda s: s.get("fut_notl_cr") or 0, close_min=close)
    opt_w = _compute_adaptive_weights(vol_fn=lambda s: s.get("opt_prem_cr") or 0, close_min=close)
    result = (fut_w, opt_w) if (fut_w and opt_w) else None
    other = _other_season_close(close)
    if result is None and other:
        fut_o = _compute_adaptive_weights(vol_fn=lambda s: s.get("fut_notl_cr") or 0, close_min=other)
        opt_o = _compute_adaptive_weights(vol_fn=lambda s: s.get("opt_prem_cr") or 0, close_min=other)
        if fut_o and opt_o:
            fut_t, opt_t = season_transfer(fut_o, other, close), season_transfer(opt_o, other, close)
            result = (fut_t, opt_t) if (fut_t and opt_t) else None
    _adaptive_split_cache[close] = {"weights": result, "ts": _time.time()}
    return result


def _interpolate_vol(snapshots, target_elapsed):
    """Interpolate cumulative volume (fut_notl + opt_prem) at target elapsed."""
    if not snapshots or target_elapsed <= 0:
        return 0.0
    def _vol(s):
        return (s.get("fut_notl_cr") or 0) + (s.get("opt_prem_cr") or 0)
    if target_elapsed <= snapshots[0]["elapsed_min"]:
        v0 = _vol(snapshots[0])
        em0 = snapshots[0]["elapsed_min"]
        return v0 * (target_elapsed / em0) if em0 > 0 else 0.0
    if target_elapsed >= snapshots[-1]["elapsed_min"]:
        return _vol(snapshots[-1])
    for i in range(len(snapshots) - 1):
        s1, s2 = snapshots[i], snapshots[i + 1]
        if s1["elapsed_min"] <= target_elapsed <= s2["elapsed_min"]:
            span = s2["elapsed_min"] - s1["elapsed_min"]
            if span == 0:
                return _vol(s1)
            frac = (target_elapsed - s1["elapsed_min"]) / span
            return _vol(s1) + frac * (_vol(s2) - _vol(s1))
    return _vol(snapshots[-1])


def _multiplier_from_curve(time_pct, hist_wt, day_type):
    """Projection multiplier at time_pct of the session: 1 / the curve's
    cumulative share, times the day-type prior faded by elapsed time.

    Until Nov 2026 this blended toward linear extrapolation (1 / time_pct,
    weight time_pct ** 1.5) and scaled the curve by a regime-drift ratio. The
    split curves are accurate (Sep-Oct 2026, 50% elapsed: futures 0.425 vs
    0.427 actual, options 0.246 vs 0.242), but options trade late, so the
    linear term pulled projections ~19% low at mid-session. The drift ratio
    compared today's partial-day bucket shares with full-day shares, so it
    sat above 1 almost every day and roughly cancelled the blend in summer;
    in winter, with no same-season curve, both were off and live projections
    ran ~20% low. Walk-forward MAE, new vs old (25% / 50% / 75% elapsed):
    35 summer days 16.1 / 12.2 / 6.4 vs 20.2 / 12.2 / 6.2; 10 winter days
    (with season_transfer) 15.5 / 9.4 / 8.2 vs 19.5 / 17.4 / 8.6.
    """
    if time_pct <= 0:
        return 1.0
    mult = (1.0 / hist_wt) if hist_wt > 0 else (1.0 / time_pct)
    raw_day_mult = DAY_MULTIPLIER.get(day_type, 1.0)
    effective_day_mult = 1.0 + (raw_day_mult - 1.0) * (1.0 - time_pct)
    return mult * effective_day_mult


# ─── EARLY-SESSION PRIOR ────────────────────────────────────────────────────
# Early in the day the realized volume is a few percent of the day and its
# share is noisy (at 10 min the futures share of the day ran 1.8-4.9%, p10-p90),
# so the curve projection alone swings +-30%. project_full_day blends it with
# the mean of the last PRIOR_DAYS full sessions (times the day-type multiplier):
#   weight on the prior = k / (curve share + k),  k = PRIOR_STRENGTH * (1 - t) ** PRIOR_FADE_POWER
# so the prior dominates in the first minutes and fades as volume comes in
# (curve share 0.10 at ~90 min: prior weight ~0.26; at 50% elapsed ~0.05;
# at 75% ~0.02). Walk-forward, 100 days Jun-Nov 2026, with the opening shape
# above, MAE % without -> with the prior at 5 / 10 / 25 / 50 / 75% elapsed:
# 21.6 / 21.6 / 17.2 / 12.4 / 6.1 -> 15.2 / 15.7 / 14.9 / 11.9 / 6.1; under
# 60 min 23.6 -> 15.7. The grid (N 5-20, k 0.03-0.06, fade power 1-3) was flat
# within ~2 points; 20 days, k 0.04, linear fade sit mid-grid.
#
# Heavy mornings revert. When the curve projects above the prior, k grows
# with how far above:  k *= 1 + PRIOR_HEAVY_GAIN * ln(curve / prior).
# A busy open (a price shock, an event) front-loads the day, so the curve's
# usual share understates how much has already traded. Walk-forward, 117 days
# Jun-19 Nov 2026, 8-35% elapsed, median error by curve / prior ratio, before
# -> after: 1.0-1.3x +0.6 -> -1.2%; 1.3-1.6x +8.5 -> +2.4%; 1.6-2x +29 -> +4%;
# >2x +65 -> +16% (4 days). MAE % at 10 / 17 / 25 / 50 / 75% elapsed, all
# days: 15.8 / 15.4 / 14.9 / 11.8 / 6.1 -> 13.7 / 13.0 / 12.7 / 11.3 / 6.1,
# better in each of Jun-Aug, Sep-Oct and Nov; the 1-sigma band now covers
# 72-78% before 40% elapsed (was 68-74%). Light days (curve below the prior)
# keep the plain blend: shrinking them harder raised their error. Gain 4 / 8 / 12 gave
# 13.3 / 12.7 / 12.4 MAE at 25%; 12 still improved but only 4 days sit above
# 2x, so 8. 20 Nov 2026 (gold -4%, silver -5.6%) read Rs 38.5 Cr at 11:30
# with the plain blend, Rs 21.6 Cr with this.
PRIOR_DAYS = 20
PRIOR_MIN_DAYS = 5
PRIOR_STRENGTH = 0.04
PRIOR_FADE_POWER = 1
PRIOR_HEAVY_GAIN = 8
PRIOR_HISTORY_DAYS = 400   # calendar days of mcx_daily_revenue read (one read, cached)

_eod_history_cache = {}


def _eod_history():
    """{trading_date: (fut_notl_cr, opt_prem_cr)} of final full-session rows,
    one read of mcx_daily_revenue per ADAPTIVE_TTL."""
    start = (now_ist() - timedelta(days=PRIOR_HISTORY_DAYS)).strftime("%Y-%m-%d")
    hit = _eod_history_cache.get(start)
    if hit and (_time.time() - hit["ts"]) < ADAPTIVE_TTL:
        return hit["rows"]
    rows = supabase_read_all(
        "mcx_daily_revenue",
        f"?select=trading_date,fut_notl_cr,opt_prem_cr,source"
        f"&trading_date=gte.{start}&order=trading_date.asc",
        max_rows=2000,
    )
    out = {}
    for r in rows or []:
        f, o = safe_float(r.get("fut_notl_cr")), safe_float(r.get("opt_prem_cr"))
        if (r.get("source") in FINAL_ROW_SOURCES and f and o and f > 0 and o > 0
                and session_shape(r["trading_date"])["type"] == SESSION_FULL):
            out[r["trading_date"]] = (f, o)
    _eod_history_cache.clear()
    _eod_history_cache[start] = {"rows": out, "ts": _time.time()}
    return out


def eod_prior(trading_date=None):
    """(fut_notl_cr, opt_prem_cr): mean of the last PRIOR_DAYS full sessions
    before trading_date, or None (fewer than PRIOR_MIN_DAYS, or the read failed)."""
    d = (_as_date(trading_date) if trading_date is not None else now_ist().date()).isoformat()
    try:
        hist = _eod_history()
    except Exception:
        return None
    prev = [hist[k] for k in sorted(hist) if k < d][-PRIOR_DAYS:]
    if len(prev) < PRIOR_MIN_DAYS:
        return None
    return (sum(f for f, _ in prev) / len(prev), sum(o for _, o in prev) / len(prev))


def _blend_with_prior(curve_proj, prior, hist_wt, time_pct, day_type):
    """Shrink a curve projection toward the trailing-mean prior; the prior's
    weight is k / (hist_wt + k) with k fading to 0 at the close, and larger
    the further the curve runs above the prior (PRIOR_HEAVY_GAIN)."""
    if not prior or prior <= 0:
        return curve_proj
    k = PRIOR_STRENGTH * (1.0 - time_pct) ** PRIOR_FADE_POWER
    if k <= 0:
        return curve_proj
    target = prior * DAY_MULTIPLIER.get(day_type, 1.0)
    if curve_proj > target:
        k *= 1.0 + PRIOR_HEAVY_GAIN * math.log(curve_proj / target)
    w = k / (max(hist_wt, 0.0) + k)
    return (1.0 - w) * curve_proj + w * target


def project_full_day(realized_fut, realized_opt, elapsed_min, day_type="LOW",
                     trading_date=None):
    """Project full-day Fut/Opt notionals using separate adaptive curves.

    Returns (proj_fut, proj_opt, confidence_label).

    trading_date (optional, default today IST) picks the session length and
    curve: 870 min to 23:30 in summer, 895 min to 23:55 in winter.

    elapsed_min counts from 09:00 on every day. On a part-day session (see
    session_shape) the intraday buckets describe a day that isn't happening, so
    the projection is time-only: realized / share of the day's real hours gone,
    with the same fading day-type prior.
    """
    shape = session_shape(trading_date)
    if shape["type"] in (SESSION_EVENING_ONLY, SESSION_MORNING_ONLY):
        traded = traded_minutes(elapsed_min, trading_date)
        if traded <= 0:
            return realized_fut, realized_opt, "LOW"
        if traded >= shape["total"]:
            return realized_fut, realized_opt, "CERTAIN"
        time_pct = traded / shape["total"]
        raw_day_mult = DAY_MULTIPLIER.get(day_type, 1.0)
        mult = (1.0 / time_pct) * (1.0 + (raw_day_mult - 1.0) * (1.0 - time_pct))
        conf_label = ("HIGH" if time_pct > 0.70
                      else "MEDIUM" if time_pct > 0.35
                      else "LOW")
        return realized_fut * mult, realized_opt * mult, conf_label

    total = session_total(trading_date)
    if elapsed_min <= 0:
        return realized_fut, realized_opt, "LOW"
    if elapsed_min >= total:
        return realized_fut, realized_opt, "CERTAIN"

    time_pct = elapsed_min / total

    # Try split Fut/Opt curves first; fall back to combined or static.
    split = get_adaptive_bucket_weights_split(trading_date)
    combined = get_adaptive_bucket_weights(trading_date)
    fut_wts = split[0] if split else combined
    opt_wts = split[1] if split else combined

    fut_hist_wt = get_intraday_weight_dynamic(elapsed_min, fut_wts, trading_date)
    opt_hist_wt = get_intraday_weight_dynamic(elapsed_min, opt_wts, trading_date)

    mult_fut = _multiplier_from_curve(time_pct, fut_hist_wt, day_type)
    mult_opt = _multiplier_from_curve(time_pct, opt_hist_wt, day_type)
    proj_fut, proj_opt = realized_fut * mult_fut, realized_opt * mult_opt

    prior = eod_prior(trading_date)
    if prior:
        proj_fut = _blend_with_prior(proj_fut, prior[0], fut_hist_wt, time_pct, day_type)
        proj_opt = _blend_with_prior(proj_opt, prior[1], opt_hist_wt, time_pct, day_type)
        # never below what has already traded
        proj_fut, proj_opt = max(proj_fut, realized_fut), max(proj_opt, realized_opt)

    conf_label = ("HIGH" if time_pct > 0.70
                  else "MEDIUM" if time_pct > 0.35
                  else "LOW")
    return proj_fut, proj_opt, conf_label


def check_regime_drift(today_snapshots, threshold_z=2.0, trading_date=None):
    """Compare today's developing volume shape against the adaptive baseline.
    Returns list of bucket alerts where z-score > threshold, or empty list.

    Diagnostic only (the relay logs it); it no longer scales the projection.
    Each completed bucket's share of today's volume so far is compared with
    the curve's share of the same partial day (its weight / the curve's
    cumulative share at max_elapsed), not with its share of a full day."""
    if not today_snapshots or len(today_snapshots) < 3:
        return []
    adaptive_wts = get_adaptive_bucket_weights(trading_date)
    if not adaptive_wts:
        return []
    # Derive today's completed bucket weights
    max_elapsed = max(s["elapsed_min"] for s in today_snapshots)
    total_vol = _interpolate_vol(today_snapshots, max_elapsed)
    cum_curve = get_intraday_weight_dynamic(max_elapsed, adaptive_wts, trading_date)
    if total_vol <= 0 or cum_curve <= 0:
        return []
    alerts = []
    buckets = intraday_buckets(trading_date)
    labels = bucket_labels(trading_date)
    prev_cum = 0.0
    for i in range(len(buckets)):
        edge_end = buckets[i][1] - SESSION_START
        completed = max_elapsed >= edge_end
        if not completed:
            break  # bucket not yet complete (Phase 4 only acts on completed buckets)
        cum = _interpolate_vol(today_snapshots, edge_end)
        today_w = max(0, cum - prev_cum) / total_vol
        prev_cum = cum
        adapt_w = adaptive_wts[i] / cum_curve
        # Use 15% of adaptive weight as approximate σ (robust estimate)
        sigma = max(adapt_w * 0.15, 0.005)
        z = abs(today_w - adapt_w) / sigma
        if z > threshold_z:
            ratio = (today_w / adapt_w) if adapt_w > 0 else 1.0
            alerts.append({
                "bucket": labels[i],
                "today_weight": round(today_w, 4),
                "adaptive_weight": round(adapt_w, 4),
                "z_score": round(z, 1),
                "direction": "higher" if today_w > adapt_w else "lower",
                "ratio": ratio,            # today/baseline; <1 means slow, >1 fast
                "completed": completed,
            })
    return alerts


def safe_float(v):
    """Convert a value to float, returning None on failure. Shared across all modules."""
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def pearson(xs, ys):
    """Pearson correlation for two lists (skipping None pairs). Shared across all modules."""
    pairs = [(x, y) for x, y in zip(xs, ys) if x is not None and y is not None]
    n = len(pairs)
    if n < 10:
        return None
    mx = sum(p[0] for p in pairs) / n
    my = sum(p[1] for p in pairs) / n
    num = sum((p[0] - mx) * (p[1] - my) for p in pairs)
    dx = math.sqrt(sum((p[0] - mx) ** 2 for p in pairs))
    dy = math.sqrt(sum((p[1] - my) ** 2 for p in pairs))
    if dx == 0 or dy == 0:
        return None
    return round(num / (dx * dy), 4)


def calc_revenue(fut_notl_cr, opt_prem_cr):
    """Compute revenue breakdown from notional/premium volumes."""
    fut_rev = 2 * fut_notl_cr * FUTURES_RATE / 1e7
    opt_rev = 2 * opt_prem_cr * OPTIONS_RATE / 1e7
    tx_rev  = fut_rev + opt_rev
    return fut_rev, opt_rev, tx_rev, tx_rev + NONTX_DAILY


def calc_uncertainty(time_pct, day_type, dual_call=False):
    """Combined 1-sigma uncertainty band, calibrated to backtest residuals.

    Tuned 2026-04-27 against 30-day backtest of post-rebuild model:
      observed 1-sigma at t=0.25 ~18%, at t=0.50 ~13%, at t=0.75 ~8%.
    Components (sum of squares): base + intraday + day-type + snapshot risk.
    """
    base_unc     = 0.04 + 0.16 * (1 - time_pct)        # 20%->4% across session
    intraday_unc = (1 - time_pct) * 0.06               # shape risk fades
    _day_unc_full = {"HIGH": 0.06, "MEDIUM": 0.05, "EXPIRY": 0.05, "LOW": 0.04}[day_type]
    day_unc       = _day_unc_full * (1 - time_pct)
    snapshot_unc  = 0.03 if dual_call else 0.04
    return math.sqrt(base_unc**2 + intraday_unc**2 + day_unc**2 + snapshot_unc**2)


def make_cors_headers(origin: str = "") -> dict:
    """Generate CORS headers, restricting to allowed origins (F-13)."""
    if origin in ALLOWED_ORIGINS:
        return {"Access-Control-Allow-Origin": origin}
    # This project's own Vercel previews and local dev only. Matching on the parsed
    # host — a substring test also admitted evil.vercel.app / evil-localhost.com.
    if origin and _is_dev_or_preview_origin(origin):
        return {"Access-Control-Allow-Origin": origin}
    return {"Access-Control-Allow-Origin": ALLOWED_ORIGINS[0] if ALLOWED_ORIGINS else "*"}


def _is_dev_or_preview_origin(origin: str) -> bool:
    from urllib.parse import urlparse
    try:
        u = urlparse(origin)
        host = (u.hostname or "").lower()
    except ValueError:
        return False
    if host in ("localhost", "127.0.0.1"):
        return u.scheme in ("http", "https")
    return u.scheme == "https" and host.startswith("mcx-vercel") and host.endswith(".vercel.app")


def int_param(qs: dict, name: str, default: int, lo: int, hi: int) -> int:
    """Integer query param clamped to [lo, hi]; garbage falls back to the default
    instead of a 500 (?days=abc used to surface a Python error)."""
    try:
        v = int(qs.get(name, [default])[0])
    except (TypeError, ValueError):
        v = default
    return max(lo, min(hi, v))


def cache_header(result, ok_value: str) -> str:
    """Cache-Control for a JSON payload: never let the CDN or browser hold a
    {success: false} body — a transient Supabase error would otherwise be served
    for the whole max-age / stale-while-revalidate window."""
    if isinstance(result, dict) and result.get("success") is False:
        return "no-store"
    return ok_value


# ─── EPS-PATH VALUATION MODEL (Model A) ────────────────────────────────────
# Economic chain: 45DMA F&O Rev → Annualized → (+Non-F&O) → PAT → EPS → Fair Value
PAT_MARGIN           = float(os.environ.get("MCX_PAT_MARGIN", "0.55"))        # PAT ÷ total income: FY26 54.8%, Q1 FY27 55.0%
NON_FO_REV_ANNUAL_CR = float(os.environ.get("MCX_NON_FO_REV", "374.5"))      # ₹ Cr/yr outside F&O fees, last four reported quarters (Q2 FY26–Q1 FY27):
                                                                              # non-F&O operating revenue 230.3 (reported revenue − daily F&O sums) + other income 144.2.
                                                                              # Refresh each quarter. Replaces an undocumented 527.
DILUTED_SHARES_CR    = float(os.environ.get("MCX_DILUTED_SHARES", "25.451"))  # 254.51M diluted shares
PE_MEAN_DEFAULT      = float(os.environ.get("MCX_PE_MEAN", "34.79"))          # Dynamic PE median (trailing 252 obs)
PE_SD_DEFAULT        = float(os.environ.get("MCX_PE_SD", "3.49"))             # Scaled MAD (robust PE dispersion)
