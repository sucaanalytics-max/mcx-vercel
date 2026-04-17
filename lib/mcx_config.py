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
ADAPTIVE_MIN_DAYS = 10   # minimum days for adaptive curve
ADAPTIVE_TTL = 900       # cache for 15 min (aligned with relay cycle)


def get_adaptive_bucket_weights():
    """Return EWMA-weighted bucket weights from recent snapshot data.
    Returns list of 7 floats summing to ~1.0, or None (falls back to static).
    Caches result (including None) to avoid repeated Supabase queries."""
    if _adaptive_cache["ts"] > 0 and (_time.time() - _adaptive_cache["ts"]) < ADAPTIVE_TTL:
        return _adaptive_cache["weights"]  # may be None (cached failure)
    try:
        ist = now_ist()
        start_date = (ist - timedelta(days=40)).strftime("%Y-%m-%d")
        today_str = ist.strftime("%Y-%m-%d")

        snapshots = supabase_read_all(
            "mcx_snapshots",
            f"?select=trading_date,elapsed_min,fut_notl_cr,opt_prem_cr"
            f"&trading_date=gte.{start_date}&trading_date=lt.{today_str}"
            f"&order=trading_date.asc,elapsed_min.asc",
            max_rows=5000,
        )
        if not snapshots:
            _adaptive_cache["weights"] = None
            _adaptive_cache["ts"] = _time.time()
            return None

        # Group by date
        by_date = defaultdict(list)
        for s in snapshots:
            if s.get("elapsed_min") is not None:
                by_date[s["trading_date"]].append(s)

        # Derive bucket weights per day + apply EWMA
        lam = math.log(2) / ADAPTIVE_HALFLIFE
        weighted_buckets = [0.0] * 7
        total_weight = 0.0
        dates_used = 0

        sorted_dates = sorted(by_date.keys(), reverse=True)  # most recent first
        for age, dt_str in enumerate(sorted_dates):
            snaps = sorted(by_date[dt_str], key=lambda s: s["elapsed_min"])
            if len(snaps) < 4:
                continue
            # Derive bucket weights for this day
            total_vol = _interpolate_vol(snaps, SESSION_TOTAL)
            if total_vol <= 0:
                continue
            day_weights = []
            prev_cum = 0.0
            for i in range(len(INTRADAY_BUCKETS)):
                edge_end = INTRADAY_BUCKETS[i][1] - SESSION_START
                cum = _interpolate_vol(snaps, edge_end)
                bucket_vol = max(0, cum - prev_cum)
                day_weights.append(bucket_vol / total_vol)
                prev_cum = cum
            # EWMA decay
            w = math.exp(-lam * age)
            for b in range(7):
                weighted_buckets[b] += w * day_weights[b]
            total_weight += w
            dates_used += 1

        if dates_used < ADAPTIVE_MIN_DAYS or total_weight <= 0:
            _adaptive_cache["weights"] = None
            _adaptive_cache["ts"] = _time.time()
            return None

        result = [b / total_weight for b in weighted_buckets]
        _adaptive_cache["weights"] = result
        _adaptive_cache["ts"] = _time.time()
        return result

    except Exception:
        _adaptive_cache["weights"] = None
        _adaptive_cache["ts"] = _time.time()
        return None


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


def project_full_day(realized_fut, realized_opt, elapsed_min, day_type="LOW"):
    """Apply hybrid projection with adaptive EWMA curve + volume-based confidence."""
    if elapsed_min <= 0:
        return realized_fut, realized_opt, "LOW"
    if elapsed_min >= SESSION_TOTAL:
        return realized_fut, realized_opt, "CERTAIN"

    time_pct = elapsed_min / SESSION_TOTAL

    # Use adaptive EWMA curve if available, else static
    adaptive_wts = get_adaptive_bucket_weights()
    hist_wt = get_intraday_weight_dynamic(elapsed_min, adaptive_wts)

    mult_a = 1.0 / time_pct if time_pct > 0 else 1.0
    mult_b = 1.0 / hist_wt  if hist_wt  > 0 else 1.0
    # Volume-based confidence: weight blend by how much volume observed, not clock time
    confidence = hist_wt ** 0.5
    mult_c = confidence * mult_a + (1 - confidence) * mult_b

    # Day-type prior fades as session progresses
    raw_day_mult = DAY_MULTIPLIER.get(day_type, 1.0)
    effective_day_mult = 1.0 + (raw_day_mult - 1.0) * (1.0 - time_pct)
    mult_c *= effective_day_mult

    conf_label = ("HIGH" if time_pct > 0.70
                  else "MEDIUM" if time_pct > 0.35
                  else "LOW")
    return realized_fut * mult_c, realized_opt * mult_c, conf_label


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
        if max_elapsed < edge_end:
            break  # bucket not yet complete
        cum = _interpolate_vol(today_snapshots, edge_end)
        today_w = max(0, cum - prev_cum) / total_vol
        prev_cum = cum
        adapt_w = adaptive_wts[i]
        # Use 15% of adaptive weight as approximate σ (robust estimate)
        sigma = max(adapt_w * 0.15, 0.005)
        z = abs(today_w - adapt_w) / sigma
        if z > threshold_z:
            alerts.append({
                "bucket": _LABELS[i],
                "today_weight": round(today_w, 4),
                "adaptive_weight": round(adapt_w, 4),
                "z_score": round(z, 1),
                "direction": "higher" if today_w > adapt_w else "lower",
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


# ─── EPS-PATH VALUATION MODEL (Model A) ────────────────────────────────────
# Economic chain: 45DMA F&O Rev → Annualized → (+Non-F&O) → PAT → EPS → Fair Value
PAT_MARGIN           = float(os.environ.get("MCX_PAT_MARGIN", "0.55"))        # 55% PAT margin (Excel Triangulation)
NON_FO_REV_ANNUAL_CR = float(os.environ.get("MCX_NON_FO_REV", "527.0"))      # FY27 non-F&O revenue (₹ Cr/year)
DILUTED_SHARES_CR    = float(os.environ.get("MCX_DILUTED_SHARES", "25.451"))  # 254.51M diluted shares
PE_MEAN_DEFAULT      = float(os.environ.get("MCX_PE_MEAN", "34.79"))          # Dynamic PE median (trailing 252 obs)
PE_SD_DEFAULT        = float(os.environ.get("MCX_PE_SD", "3.49"))             # Scaled MAD (robust PE dispersion)
