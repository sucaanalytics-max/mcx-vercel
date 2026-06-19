"""
MCX Revenue Model — Shared Configuration (F-02, F-22)
Single source of truth for all fee rates, day classifications, volume curves,
and projection logic. Imported by all API endpoints.
"""
import os, math, time as _time
from datetime import datetime, timedelta
from collections import defaultdict

# ─── FEE SCHEDULE (SEBI Oct 2024 flat rate) ──────────────────────────────────
FUTURES_RATE = 210.0        # ₹ per crore, both sides
OPTIONS_RATE = 4180.0       # ₹ per crore of premium, both sides
NONTX_DAILY  = float(os.environ.get("MCX_NONTX_DAILY", "0.00"))  # removed from daily predictor
TRADING_DAYS = int(os.environ.get("MCX_TRADING_DAYS", "252"))      # F-04: MCX actual calendar (Excel uses 254)

# ─── Alpha Vantage (F-07: from env var, not hardcoded) ───────────────────────
AV_KEY = os.environ.get("ALPHA_VANTAGE_KEY", "")

# ─── Supabase (F-08: data relay) ─────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://avqwpebveqetwwzkmtux.supabase.co")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImF2cXdwZWJ2ZXFldHd3emttdHV4Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzE0MDkwMzMsImV4cCI6MjA4Njk4NTAzM30.U_Ug61Fp1NSCesXBkYU7GJGTbuATFtXsz6GTi5948Rw")
# Service-role key for server-side WRITES (crons/relay). Bypasses RLS, so tables
# can keep anon read-only policies. Falls back to the anon key when unset, which
# only works on tables whose RLS permits anon INSERT/UPDATE (see
# scripts/sql/enable_signal_table_writes.sql). Reads always use the anon key.
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
SUPABASE_WRITE_KEY = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY

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

# ─── SESSION TIMING ──────────────────────────────────────────────────────────
SESSION_START = 540    # 09:00 IST in minutes since midnight
SESSION_END   = 1410   # 23:30 IST
SESSION_TOTAL = SESSION_END - SESSION_START   # 870 min

# ─── INTRADAY VOLUME CURVE (calibrated MCX energy-heavy session) ─────────────
# MCX evening session (17:00–23:30) = ~67% of daily turnover.
# Weights sum to 1.0 exactly.
INTRADAY_BUCKETS = [
    ( 540,  630, 0.06),   # 09:00–10:30  Opening + metals
    ( 630,  750, 0.10),   # 10:30–12:30  Mid-morning
    ( 750,  900, 0.07),   # 12:30–15:00  Post-lunch lull
    ( 900, 1020, 0.10),   # 15:00–17:00  Pre-evening
    (1020, 1170, 0.18),   # 17:00–19:30  Europe open
    (1170, 1320, 0.34),   # 19:30–22:00  ★ PRIME: NYMEX open
    (1320, 1410, 0.15),   # 22:00–23:30  Late session
]

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
    "2026-01-01",  # New Year Day
    # ── 2027: ADD when published ──
}
# Backward-compat aliases — several modules import the *_2026 names.
MCX_HOLIDAYS_2026 = MCX_HOLIDAYS
MCX_MORNING_CLOSE_2026 = MCX_MORNING_CLOSE
MCX_EVENING_CLOSE_2026 = MCX_EVENING_CLOSE

# ─── DAY-TYPE CLASSIFICATION (F-21: algorithmic + manual overrides) ──────────
# Volume multipliers — calibrated by scripts/calibrate_day_types.py against 400
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
    rollover doesn't silently turn holidays into normal trading days."""
    yr = _current_ist_year()
    if not any(h.startswith(f"{yr}-") for h in MCX_HOLIDAYS):
        import sys as _sys
        print(f"⚠ MCX calendar: no holiday data for {yr} in mcx_config.py — holidays "
              f"will be treated as trading days until added. Add {yr} to MCX_HOLIDAYS / "
              f"MCX_MORNING_CLOSE / MCX_EVENING_CLOSE and _MACRO_EVENTS_BY_YEAR[{yr}].",
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

def get_intraday_weight(elapsed_minutes: int) -> float:
    """Return cumulative volume fraction completed at elapsed_minutes into session."""
    e = min(elapsed_minutes, SESSION_TOTAL)
    cumulative = 0.0
    for start, end, share in INTRADAY_BUCKETS:
        s  = start - SESSION_START
        en = end   - SESSION_START
        if e >= en:
            cumulative += share
        elif e > s:
            cumulative += share * (e - s) / (en - s)
        else:
            break
    return min(cumulative, 1.0)


def get_intraday_weight_dynamic(elapsed_minutes: int, bucket_weights: list = None) -> float:
    """Return cumulative volume fraction using custom bucket weights.
    If bucket_weights is None, falls back to static INTRADAY_BUCKETS.
    bucket_weights should be a list of 7 floats summing to ~1.0."""
    if bucket_weights is None:
        return get_intraday_weight(elapsed_minutes)
    e = min(elapsed_minutes, SESSION_TOTAL)
    cumulative = 0.0
    for i, (start, end, _) in enumerate(INTRADAY_BUCKETS):
        w = bucket_weights[i] if i < len(bucket_weights) else 0
        s = start - SESSION_START
        en = end - SESSION_START
        if e >= en:
            cumulative += w
        elif e > s:
            cumulative += w * (e - s) / (en - s)
        else:
            break
    return min(cumulative, 1.0)


# ─── ADAPTIVE CURVE (EWMA) ──────────────────────────────────────────────────
# VWAP-style exponentially weighted moving average of intraday bucket weights.
# Halflife=10 trading days — adapts to regime shifts in ~5 days.
_adaptive_cache = {"weights": None, "ts": 0}
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


def _bucket_volumes(snaps, vol_fn=None):
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
    for start, end, _share in INTRADAY_BUCKETS:
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


# A day must complete (last snapshot >= ADAPTIVE_MIN_ELAPSED) to contribute
# bucket shares to the EWMA. Partial-day shares can't be normalized without
# knowing the full-day total, so they bias the curve upward in early buckets.
ADAPTIVE_MIN_ELAPSED = 800


def _compute_adaptive_weights(vol_fn=None):
    """Generic adaptive bucket-weight EWMA over recent (full-session) days.

    Returns list of 7 floats (sums to 1.0) or None if insufficient data.
    Caller can pass a vol_fn to compute Fut-only or Opt-only curves.
    """
    try:
        ist = now_ist()
        start_date = (ist - timedelta(days=60)).strftime("%Y-%m-%d")
        today_str = ist.strftime("%Y-%m-%d")

        snapshots = supabase_read_all(
            "mcx_snapshots",
            f"?select=trading_date,elapsed_min,fut_notl_cr,opt_prem_cr"
            f"&trading_date=gte.{start_date}&trading_date=lt.{today_str}"
            f"&order=trading_date.asc,elapsed_min.asc",
            max_rows=5000,
        )
        if not snapshots:
            return None

        by_date = defaultdict(list)
        for s in snapshots:
            if s.get("elapsed_min") is not None:
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
            if snaps[-1]["elapsed_min"] < ADAPTIVE_MIN_ELAPSED:
                continue
            # Trim stale opening snapshot (MCX API can carry prior-day cumulative)
            while len(snaps) > 4:
                v0, v1 = vol_fn(snaps[0]), vol_fn(snaps[1])
                if v0 > 0 and v1 < v0 * 0.5 and snaps[0]["elapsed_min"] < 30:
                    snaps = snaps[1:]
                else:
                    break
            # Rolling-median outlier detection — replaces the arbitrary 30% threshold
            vols = [vol_fn(s) for s in snaps]
            if _rolling_median_outlier(vols):
                continue

            buckets = _bucket_volumes(snaps, vol_fn)
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


def get_adaptive_bucket_weights():
    """Return EWMA-weighted bucket weights from recent snapshots (combined Fut+Opt).
    Returns list of 7 floats summing to ~1.0, or None (falls back to static)."""
    if _adaptive_cache["ts"] > 0 and (_time.time() - _adaptive_cache["ts"]) < ADAPTIVE_TTL:
        return _adaptive_cache["weights"]
    result = _compute_adaptive_weights(vol_fn=None)
    _adaptive_cache["weights"] = result
    _adaptive_cache["ts"] = _time.time()
    return result


# Cache for split (Fut, Opt) curves — Phase 3
_adaptive_split_cache = {"weights": None, "ts": 0}


def get_adaptive_bucket_weights_split():
    """Return (fut_weights, opt_weights) — separate EWMA curves for Fut and Opt.
    Each is a list of 7 floats summing to ~1.0, or None if insufficient data."""
    if _adaptive_split_cache["ts"] > 0 and (_time.time() - _adaptive_split_cache["ts"]) < ADAPTIVE_TTL:
        return _adaptive_split_cache["weights"]
    fut_w = _compute_adaptive_weights(vol_fn=lambda s: s.get("fut_notl_cr") or 0)
    opt_w = _compute_adaptive_weights(vol_fn=lambda s: s.get("opt_prem_cr") or 0)
    result = (fut_w, opt_w) if (fut_w and opt_w) else None
    _adaptive_split_cache["weights"] = result
    _adaptive_split_cache["ts"] = _time.time()
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


def _multiplier_from_curve(time_pct, hist_wt, day_type, regime_drift_factor=1.0):
    """Compute the projection multiplier given time elapsed and historical weight.

    Lean on the volume curve (mult_b = 1/hist_wt) and only blend toward linear
    extrapolation (mult_a = 1/time_pct) once the session is mostly complete.
    Confidence in the curve grows with elapsed time. Fades the day-type prior.
    Optionally scales mult_b by a regime-drift factor (Phase 4).
    """
    if time_pct <= 0:
        return 1.0
    mult_a = 1.0 / time_pct
    mult_b = (1.0 / hist_wt) if hist_wt > 0 else mult_a
    # Phase 4: regime drift scales the curve multiplier; capped at +/-25%
    # to allow real corrections on systematically back/front-loaded days.
    drift = max(0.75, min(1.25, regime_drift_factor))
    mult_b *= drift

    # Blend: trust the curve early in session, blend toward linear late.
    # Earlier formula (sqrt(hist_wt)) over-weighted mult_a in the first half
    # which assumes uniform pace and produced the systematic under-projection.
    blend = time_pct ** 1.5  # 0.18 at t=0.25, 0.35 at t=0.5, 0.65 at t=0.75
    mult_c = blend * mult_a + (1 - blend) * mult_b

    raw_day_mult = DAY_MULTIPLIER.get(day_type, 1.0)
    effective_day_mult = 1.0 + (raw_day_mult - 1.0) * (1.0 - time_pct)
    return mult_c * effective_day_mult


def project_full_day(realized_fut, realized_opt, elapsed_min, day_type="LOW",
                     today_snapshots=None):
    """Project full-day Fut/Opt notionals using separate adaptive curves.

    Returns (proj_fut, proj_opt, confidence_label).

    today_snapshots (optional): list of today's mcx_snapshots rows, used by
    check_regime_drift() to detect bucket-level divergence and scale the
    volume-curve multiplier in real time. Falls back to static behavior if
    not provided or if drift detection has insufficient data.
    """
    if elapsed_min <= 0:
        return realized_fut, realized_opt, "LOW"
    if elapsed_min >= SESSION_TOTAL:
        return realized_fut, realized_opt, "CERTAIN"

    time_pct = elapsed_min / SESSION_TOTAL

    # Try split Fut/Opt curves first; fall back to combined or static.
    split = get_adaptive_bucket_weights_split()
    combined = get_adaptive_bucket_weights()
    fut_wts = split[0] if split else combined
    opt_wts = split[1] if split else combined

    fut_hist_wt = get_intraday_weight_dynamic(elapsed_min, fut_wts)
    opt_hist_wt = get_intraday_weight_dynamic(elapsed_min, opt_wts)

    # Phase 4: regime-drift correction. If today's actual bucket shape
    # diverges from the baseline by >2σ, scale the curve multiplier
    # toward today's pace. Capped at ±20% in _multiplier_from_curve().
    fut_drift = opt_drift = 1.0
    if today_snapshots and len(today_snapshots) >= 3:
        try:
            drift_alerts = check_regime_drift(today_snapshots, threshold_z=2.0)
            if drift_alerts:
                # Average drift ratio across flagged buckets — completed buckets only
                ratios = [a.get("ratio") for a in drift_alerts if a.get("completed")]
                if ratios:
                    avg_ratio = sum(ratios) / len(ratios)
                    fut_drift = opt_drift = avg_ratio
        except Exception:
            pass

    mult_fut = _multiplier_from_curve(time_pct, fut_hist_wt, day_type, fut_drift)
    mult_opt = _multiplier_from_curve(time_pct, opt_hist_wt, day_type, opt_drift)

    conf_label = ("HIGH" if time_pct > 0.70
                  else "MEDIUM" if time_pct > 0.35
                  else "LOW")
    return realized_fut * mult_fut, realized_opt * mult_opt, conf_label


def check_regime_drift(today_snapshots, threshold_z=2.0):
    """Compare today's developing volume shape against adaptive baseline.
    Returns list of bucket alerts where z-score > threshold, or empty list."""
    if not today_snapshots or len(today_snapshots) < 3:
        return []
    adaptive_wts = get_adaptive_bucket_weights()
    if not adaptive_wts:
        return []
    # Derive today's completed bucket weights
    max_elapsed = max(s["elapsed_min"] for s in today_snapshots)
    total_vol = _interpolate_vol(today_snapshots, max_elapsed)
    if total_vol <= 0:
        return []
    alerts = []
    _LABELS = ["09:00-10:30", "10:30-12:30", "12:30-15:00", "15:00-17:00",
               "17:00-19:30", "19:30-22:00", "22:00-23:30"]
    prev_cum = 0.0
    for i in range(len(INTRADAY_BUCKETS)):
        edge_end = INTRADAY_BUCKETS[i][1] - SESSION_START
        completed = max_elapsed >= edge_end
        if not completed:
            break  # bucket not yet complete (Phase 4 only acts on completed buckets)
        cum = _interpolate_vol(today_snapshots, edge_end)
        today_w = max(0, cum - prev_cum) / total_vol
        prev_cum = cum
        adapt_w = adaptive_wts[i]
        # Use 15% of adaptive weight as approximate σ (robust estimate)
        sigma = max(adapt_w * 0.15, 0.005)
        z = abs(today_w - adapt_w) / sigma
        if z > threshold_z:
            ratio = (today_w / adapt_w) if adapt_w > 0 else 1.0
            alerts.append({
                "bucket": _LABELS[i],
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


def now_ist() -> datetime:
    """Return current time in IST."""
    from datetime import timezone
    return datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=5, minutes=30)


def is_trading_day(d) -> bool:
    """Check if date is a weekday and not a full-closure MCX holiday."""
    return d.weekday() < 5 and d.strftime("%Y-%m-%d") not in MCX_HOLIDAYS_2026


def is_market_open(dt=None) -> bool:
    """Check if MCX is currently in trading hours."""
    if dt is None:
        dt = now_ist()
    mins = dt.hour * 60 + dt.minute
    return dt.weekday() < 5 and 540 <= mins <= 1410


def make_cors_headers(origin: str = "") -> dict:
    """Generate CORS headers, restricting to allowed origins (F-13)."""
    if origin in ALLOWED_ORIGINS:
        return {"Access-Control-Allow-Origin": origin}
    # Fallback for Vercel preview deployments
    if origin and (".vercel.app" in origin or "localhost" in origin):
        return {"Access-Control-Allow-Origin": origin}
    return {"Access-Control-Allow-Origin": ALLOWED_ORIGINS[0] if ALLOWED_ORIGINS else "*"}


# ─── SUPABASE HELPERS ────────────────────────────────────────────────────────

def _supabase_request(method, table, data=None, params="", timeout=10):
    """Make a direct REST request to Supabase (no SDK needed)."""
    import urllib.request, urllib.error, json
    url = f"{SUPABASE_URL}/rest/v1/{table}{params}"
    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }
    body = json.dumps(data).encode("utf-8") if data else None
    req = urllib.request.Request(url, data=body, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8") if e.fp else ""
        raise Exception(f"Supabase {e.code}: {error_body[:200]}")


def supabase_read(table, params="", timeout=10):
    """Read from Supabase table."""
    return _supabase_request("GET", table, params=params, timeout=timeout)


def supabase_read_all(table, params="", page_size=1000, max_rows=10000, timeout=10):
    """Paginated read — fetches all rows beyond the 1000-row default limit.

    Uses offset-based pagination (same pattern as existing cron jobs).
    Stops when: fewer than page_size rows returned OR max_rows reached.
    """
    all_rows = []
    offset = 0
    separator = "&" if "?" in params else "?"
    while True:
        page = supabase_read(
            table,
            f"{params}{separator}limit={page_size}&offset={offset}",
            timeout=timeout,
        )
        all_rows.extend(page)
        if len(page) < page_size or len(all_rows) >= max_rows:
            break
        offset += page_size
    return all_rows[:max_rows]


def supabase_upsert(table, data, timeout=10):
    """Upsert data into Supabase table (uses the service-role write key when set)."""
    import urllib.request, urllib.error, json
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {
        "apikey": SUPABASE_WRITE_KEY,
        "Authorization": f"Bearer {SUPABASE_WRITE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=representation",
    }
    body = json.dumps(data).encode("utf-8") if data else None
    req = urllib.request.Request(url, data=body, method="POST", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8") if e.fp else ""
        raise Exception(f"Supabase upsert {e.code}: {error_body[:200]}")


# ─── EPS-PATH VALUATION MODEL (Model A) ────────────────────────────────────
# Economic chain: 45DMA F&O Rev → Annualized → (+Non-F&O) → PAT → EPS → Fair Value
PAT_MARGIN           = float(os.environ.get("MCX_PAT_MARGIN", "0.55"))        # 55% PAT margin (Excel Triangulation)
NON_FO_REV_ANNUAL_CR = float(os.environ.get("MCX_NON_FO_REV", "527.0"))      # FY27 non-F&O revenue (₹ Cr/year)
DILUTED_SHARES_CR    = float(os.environ.get("MCX_DILUTED_SHARES", "25.451"))  # 254.51M diluted shares
PE_MEAN_DEFAULT      = float(os.environ.get("MCX_PE_MEAN", "34.79"))          # Dynamic PE median (trailing 252 obs)
PE_SD_DEFAULT        = float(os.environ.get("MCX_PE_SD", "3.49"))             # Scaled MAD (robust PE dispersion)
