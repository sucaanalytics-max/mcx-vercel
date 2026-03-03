"""
MCX Revenue Model — Shared Configuration (F-02, F-22)
Single source of truth for all fee rates, day classifications, volume curves,
and projection logic. Imported by all API endpoints.
"""
import os, math
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
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "")

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

# ─── MCX HOLIDAYS 2026 — Official calendar (verified Feb 25, 2026) ───────────
# Source: mcxindia.com / 5paisa.com / ICICI Direct
# FULL-DAY = both sessions closed (zero revenue)
# MORNING-ONLY = morning closed, evening session OPEN (~67% revenue)
# EVENING-ONLY = morning open, evening closed (~33% revenue)
MCX_HOLIDAYS_2026 = {
    # ── 2025 tail (within 45-day lookback) ──
    "2025-12-25",  # Christmas (full day)
    # ── 2026 FULL-DAY closures ──
    "2026-01-26",  # Republic Day
    "2026-04-03",  # Good Friday
    "2026-10-02",  # Gandhi Jayanti
    "2026-12-25",  # Christmas
}
# Morning-only closures (evening session trades — partial revenue day)
MCX_MORNING_CLOSE_2026 = {
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
}
# Evening-only closure
MCX_EVENING_CLOSE_2026 = {
    "2026-01-01",  # New Year Day
}

# ─── DAY-TYPE CLASSIFICATION (F-21: algorithmic + manual overrides) ──────────
# Volume multipliers — recalibrated from 45 bhav copies (Dec 22 – Feb 24, 2026):
#   Recalibrated from 361 trading days (Oct 2024 – Feb 2026, Exchanges Dashboard).
#   Month-normalized analysis shows day-type effect is weak (all within ±5% of 1.0).
#   Conservative multipliers retained for genuine macro-event texture:
#   HIGH: major macro events (FOMC/Budget/GDP/RBI) — modest +15% uplift
#   MEDIUM: pre-expiry (T-1), NatGas expiry, CPI/NFP — small +5%
#   EXPIRY: CrudeOil expiry day — baseline
#   LOW: no scheduled catalyst — flat (no significant deviation from mean)
DAY_MULTIPLIER = {"HIGH": 1.15, "MEDIUM": 1.05, "EXPIRY": 1.00, "LOW": 1.00}
DAY_DESCRIPTION = {
    "HIGH":   "High-event — major macro (RBI/FOMC/Budget/GDP). 361d calibration: +15% vs baseline.",
    "MEDIUM": "Medium — CrudeOil T-1 · NatGas expiry · CPI · NFP. 361d calibration: +5% vs baseline.",
    "EXPIRY": "CrudeOil expiry day — positions settling; baseline.",
    "LOW":    "No scheduled catalyst. 361d calibration: flat vs baseline.",
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

def _build_event_calendar(year: int = 2026) -> tuple:
    """
    Algorithmically generate HIGH/MEDIUM/EXPIRY events for a given year.
    Returns (high_set, medium_set, expiry_set) of date strings.
    """
    high = set()
    medium = set()
    expiry = set()

    for month in range(1, 13):
        # CrudeOil expiry cycle
        exp_date = _get_mcx_crude_expiry(year, month)
        expiry.add(exp_date.strftime("%Y-%m-%d"))
        t1 = _get_trading_day_before(exp_date, 1)
        t2 = _get_trading_day_before(exp_date, 2)
        medium.add(t1.strftime("%Y-%m-%d"))
        high.add(t2.strftime("%Y-%m-%d"))

    # NatGas expiry (~20th-26th, approximate: 4th Friday or nearby)
    natgas_expiries = [
        "2026-01-23", "2026-02-20", "2026-03-20", "2026-04-24",
        "2026-05-22", "2026-06-26", "2026-07-24", "2026-08-21",
        "2026-09-25", "2026-10-23", "2026-11-20", "2026-12-24",
    ]
    for d in natgas_expiries:
        medium.add(d)

    # FOMC decisions (known 2026 schedule)
    fomc = ["2026-01-29", "2026-03-18", "2026-05-06", "2026-06-17",
            "2026-07-29", "2026-09-16", "2026-10-28", "2026-12-09"]
    for d in fomc:
        high.add(d)

    # RBI MPC (bimonthly, shifted from weekends)
    rbi = ["2026-02-06", "2026-04-09", "2026-06-05", "2026-08-07",
           "2026-10-09", "2026-12-04"]
    for d in rbi:
        high.add(d)

    # Union Budget
    high.add("2026-02-02")

    # India GDP flash
    gdp = ["2026-02-27", "2026-05-29", "2026-08-31", "2026-11-30"]
    for d in gdp:
        high.add(d)

    # India CPI
    cpi = ["2026-01-13", "2026-02-12", "2026-03-12", "2026-04-13",
           "2026-05-12", "2026-06-12", "2026-07-14", "2026-08-12",
           "2026-09-14", "2026-10-12", "2026-11-12", "2026-12-14"]
    for d in cpi:
        if d not in high:
            medium.add(d)

    # US NFP
    nfp = ["2026-01-09", "2026-03-06", "2026-04-02", "2026-04-30",
           "2026-07-10", "2026-09-04", "2026-10-01", "2026-11-06"]
    for d in nfp:
        if d not in high:
            medium.add(d)

    # Remove any MEDIUM that's also in HIGH (HIGH wins)
    medium -= high
    # Remove any EXPIRY that's also in HIGH or MEDIUM
    expiry -= high
    expiry -= medium

    return high, medium, expiry

_HIGH_EVENTS, _MEDIUM_EVENTS, _EXPIRY_EVENTS = _build_event_calendar(2026)


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


def project_full_day(realized_fut, realized_opt, elapsed_min, day_type="LOW"):
    """Apply hybrid projection with day-type fading prior."""
    if elapsed_min <= 0:
        return realized_fut, realized_opt, "LOW"
    if elapsed_min >= SESSION_TOTAL:
        return realized_fut, realized_opt, "CERTAIN"

    time_pct = elapsed_min / SESSION_TOTAL
    hist_wt  = get_intraday_weight(elapsed_min)

    mult_a = 1.0 / time_pct if time_pct > 0 else 1.0
    mult_b = 1.0 / hist_wt  if hist_wt  > 0 else 1.0
    confidence = time_pct ** 0.5
    mult_c = confidence * mult_a + (1 - confidence) * mult_b

    # Day-type prior fades as session progresses
    raw_day_mult = DAY_MULTIPLIER.get(day_type, 1.0)
    effective_day_mult = 1.0 + (raw_day_mult - 1.0) * (1.0 - time_pct)
    mult_c *= effective_day_mult

    conf_label = ("HIGH" if time_pct > 0.70
                  else "MEDIUM" if time_pct > 0.35
                  else "LOW")
    return realized_fut * mult_c, realized_opt * mult_c, conf_label


def calc_revenue(fut_notl_cr, opt_prem_cr):
    """Compute revenue breakdown from notional/premium volumes."""
    fut_rev = 2 * fut_notl_cr * FUTURES_RATE / 1e7
    opt_rev = 2 * opt_prem_cr * OPTIONS_RATE / 1e7
    tx_rev  = fut_rev + opt_rev
    return fut_rev, opt_rev, tx_rev, tx_rev + NONTX_DAILY


def calc_uncertainty(time_pct, day_type, dual_call=False):
    """Compute combined uncertainty with proper component decomposition."""
    base_unc     = 0.015 + 0.105 * (1 - time_pct)
    intraday_unc = (1 - time_pct) * 0.08
    _day_unc_full = {"HIGH": 0.08, "MEDIUM": 0.07, "EXPIRY": 0.05, "LOW": 0.07}[day_type]
    day_unc       = _day_unc_full * (1 - time_pct)
    snapshot_unc  = 0.02 if dual_call else 0.04
    return math.sqrt(base_unc**2 + intraday_unc**2 + day_unc**2 + snapshot_unc**2)


def now_ist() -> datetime:
    """Return current time in IST."""
    from datetime import timezone
    return datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=5, minutes=30)


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


def supabase_upsert(table, data, timeout=10):
    """Upsert data into Supabase table."""
    import urllib.request, urllib.error, json
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
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
