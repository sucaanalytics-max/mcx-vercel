"""
Supabase REST client: keys, auth headers, reads, paginated reads, upserts.

The one copy for api/, lib/ and scripts/ (Phase 3 removed ~15 hand-written
sb_get / sb_upsert / fetch_all copies). No SDK: urllib only, so Vercel needs
no extra package. lib.mcx_config re-exports every public name.
"""
import os
import json
import urllib.request
import urllib.error
from pathlib import Path


_PROJECT_ENV_KEYS = ("SUPABASE_SERVICE_KEY", "SUPABASE_SECRET_KEY", "SUPABASE_PUBLISHABLE_KEY")


def _load_project_env():
    """SUPABASE_SERVICE_KEY for the scripts that run on the relay Mac, which
    launchd starts with no environment: read from .env.local then .env (both
    gitignored), never overriding a shell var. Only the keys in
    _PROJECT_ENV_KEYS are taken, so nothing else in those files changes how
    this module behaves. No such files exist on Vercel."""
    root = Path(__file__).resolve().parents[1]
    for name in (".env.local", ".env"):
        env = root / name
        if not env.exists():
            continue
        for line in env.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            if k.startswith("export "):
                k = k[len("export "):].strip()
            if k in _PROJECT_ENV_KEYS:
                os.environ.setdefault(k, v.strip().strip('"').strip("'"))


_load_project_env()


# ─── Keys ────────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://avqwpebveqetwwzkmtux.supabase.co")
# Supabase API keys. Supabase is retiring the legacy JWT keys (anon /
# service_role) by the end of 2026 in favour of non-JWT keys:
#   SUPABASE_PUBLISHABLE_KEY  sb_publishable_...  replaces anon (reads, RLS applies)
#   SUPABASE_SECRET_KEY       sb_secret_...       replaces service_role (writes)
# Set the new ones in .env.local (relay Mac) and Vercel env; until then the
# legacy names keep working. Send every request's headers through
# supabase_headers(key): the new keys go only in `apikey` (they are not JWTs, so
# an `Authorization: Bearer` copy can be rejected); legacy JWTs keep both.
_LEGACY_ANON_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImF2cXdwZWJ2ZXFldHd3emttdHV4Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzE0MDkwMzMsImV4cCI6MjA4Njk4NTAzM30.U_Ug61Fp1NSCesXBkYU7GJGTbuATFtXsz6GTi5948Rw")
# The read key (name kept for the ~40 modules that import it): publishable first.
SUPABASE_ANON_KEY = (os.environ.get("SUPABASE_PUBLISHABLE_KEY")
                     or os.environ.get("SUPABASE_ANON_KEY") or _LEGACY_ANON_KEY)
# Server-side WRITE key. Bypasses RLS, so tables can keep anon read-only
# policies. Falls back to the read key when unset, which only works while RLS
# still lets anon INSERT/UPDATE (scripts/sql/lock_*.sql removes that).
SUPABASE_SERVICE_KEY = (os.environ.get("SUPABASE_SECRET_KEY")
                        or os.environ.get("SUPABASE_SERVICE_KEY", ""))
SUPABASE_WRITE_KEY = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY
# Logged by the relay at start-up. mcx_daily_revenue accepts writes only from
# service_role once scripts/sql/lock_mcx_daily_revenue.sql has been applied.
SUPABASE_WRITE_ROLE = "service_role" if SUPABASE_SERVICE_KEY else "anon"


def _is_jwt(key: str) -> bool:
    return isinstance(key, str) and key.startswith("eyJ") and key.count(".") == 2


def supabase_key_kind(key: str) -> str:
    """'publishable' | 'secret' | 'legacy-jwt' | 'unknown' (never the key itself)."""
    if not key:
        return "unknown"
    if key.startswith("sb_publishable_"):
        return "publishable"
    if key.startswith("sb_secret_"):
        return "secret"
    return "legacy-jwt" if _is_jwt(key) else "unknown"


def supabase_headers(key: str) -> dict:
    """Auth headers for a Supabase REST call with this key. New sb_ keys go only
    in `apikey`; a legacy JWT key also goes in `Authorization: Bearer`, exactly
    as before, so nothing changes until the new keys are configured."""
    h = {"apikey": key}
    if _is_jwt(key):
        h["Authorization"] = f"Bearer {key}"
    return h

# ─── SUPABASE HELPERS ────────────────────────────────────────────────────────

def _supabase_request(method, table, data=None, params="", timeout=10):
    """Make a direct REST request to Supabase (no SDK needed)."""
    import urllib.request, urllib.error, json
    url = f"{SUPABASE_URL}/rest/v1/{table}{params}"
    headers = {
        **supabase_headers(SUPABASE_ANON_KEY),
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


# Unique key of every table read with offset pagination. PostgREST pages with
# LIMIT/OFFSET, so the ORDER BY must be total: ordering mcx_commodity_daily by
# trading_date alone (42 rows share each date) lets the database return a
# shared-date row on two pages, or on none. paginated_order() appends these
# columns to every order= it is given.
PAGINATION_KEYS = {
    "mcx_snapshots": ("id",),
    "mcx_daily_revenue": ("trading_date",),
    "mcx_commodity_daily": ("trading_date", "commodity", "instrument_type"),
    "mcx_margin_daily": ("id",),
    "mcx_icomdex_daily": ("trading_date", "index_code"),
    "mcx_commodity_signals": ("id",),
    "mcx_model_signals": ("trading_date",),
    "mcx_momentum_signals": ("trading_date",),
    "mcx_valuation": ("trading_date",),
    "mcx_share_price": ("trading_date",),
    "mcx_oi_participants": ("report_date", "commodity", "instrument"),
    "mcx_options_paper": ("id",),
    "mcx_commodity_prices": ("id",),
}
_FLIP = {"asc": "desc", "desc": "asc"}


def _order_terms(order):
    """'a.asc,b' -> [['a', 'asc'], ['b', None]] (nulls modifiers kept as extra parts)."""
    out = []
    for term in filter(None, order.split(",")):
        parts = term.split(".")
        col = parts[0]
        dirn = parts[1] if len(parts) > 1 and parts[1] in _FLIP else None
        rest = [x for x in parts[1:] if x not in _FLIP]
        out.append([col, dirn, rest])
    return out


def paginated_order(table, params="", newest_first=False):
    """params with a total ORDER BY for offset pagination of `table`.

    Appends the table's PAGINATION_KEYS columns not already ordered on (or
    adds order=<keys> when params has none). newest_first flips every term, so
    a row cap keeps the NEWEST rows; the caller reverses the result to get the
    original ascending order back (supabase_read_all does this)."""
    keys = PAGINATION_KEYS.get(table, ())
    q = params[1:] if params.startswith("?") else params
    parts = [x for x in q.split("&") if x] if q else []
    idx = next((i for i, x in enumerate(parts) if x.startswith("order=")), None)
    terms = _order_terms(parts[idx][6:]) if idx is not None else []
    have = {t[0] for t in terms}
    terms += [[k, "asc", []] for k in keys if k not in have]
    if newest_first:
        for t in terms:
            t[1] = _FLIP.get(t[1] or "asc")
    if terms:
        order = "order=" + ",".join(".".join([c] + ([d] if d else []) + r) for c, d, r in terms)
        if idx is None:
            parts.append(order)
        else:
            parts[idx] = order
    return "?" + "&".join(parts) if parts else ""


# Pages fetched at once by supabase_read_all. Supabase's pooler takes far more;
# 8 cuts a 20-page read (margins, the commodity dashboard) from 20 sequential
# round trips to 4 without bursting a free-tier project.
PAGE_WORKERS = 8


def _read_counted(table, params="", timeout=10):
    """supabase_read plus the number of matching rows: (rows, total), total
    None when PostgREST gives none. One GET with Prefer: count=exact, whose
    Content-Range header reads "0-999/21900" ("*/0" when empty)."""
    req = urllib.request.Request(f"{SUPABASE_URL}/rest/v1/{table}{params}", headers={
        **supabase_headers(SUPABASE_ANON_KEY), "Prefer": "count=exact"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            rows = json.loads(resp.read().decode("utf-8"))
            hdrs = getattr(resp, "headers", None)      # no headers (a stub): no count, waves
            rng = (hdrs.get("Content-Range") if hdrs is not None else None) or ""
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8") if e.fp else ""
        raise Exception(f"Supabase {e.code}: {error_body[:200]}")
    tail = rng.rpartition("/")[2]
    return rows, (int(tail) if tail.isdigit() else None)


def _cap_warning(table, params, max_rows):
    """The stderr line for a read that hit max_rows. `params` is the order as
    fetched (paginated_order, after any newest_first flip), so the rows past the
    cap are the far end of its first sort column: the lowest values when it runs
    desc, the highest when asc. (It used to say NEWEST whenever newest_first was
    off, wrong for margin_dashboard, which orders snapshot_date.desc itself and
    so drops the oldest dates.)"""
    q = params[1:] if params.startswith("?") else params
    order = next((x[6:] for x in q.split("&") if x.startswith("order=")), "")
    terms = _order_terms(order)
    if not terms:
        return f"⚠ supabase_read_all({table}): hit max_rows={max_rows}; rows beyond it were dropped"
    col, dirn, _ = terms[0]
    end = "lowest" if dirn == "desc" else "highest"
    timeish = col == "id" or "date" in col or col.endswith("_at")
    which = f"{end} {col}" + ((" (the oldest)" if end == "lowest" else " (the newest)") if timeish else "")
    return f"⚠ supabase_read_all({table}): hit max_rows={max_rows}; rows with the {which} were dropped"


def supabase_read_all(table, params="", page_size=1000, max_rows=10000, timeout=10,
                      newest_first=False, read=None, workers=None):
    """Paginated read — fetches all rows beyond the 1000-row default limit.

    Offset pagination over a total order (paginated_order adds the table's
    unique key). Stops when a page is short or max_rows is reached.

    Pages after the first are fetched concurrently, `workers` at a time
    (default PAGE_WORKERS), and joined in offset order, so the rows and their
    order are exactly what the old one-page-at-a-time loop returned. The first
    page also asks for the row count (Prefer: count=exact), which says how
    many pages to fetch; a custom `read` gives no count, and then pages go in
    waves of `workers` until one is short. Either way, pages after the first
    short one are dropped (the loop never asked for them), a full last page
    still fetches on (rows inserted meanwhile), and any page's error is
    raised. workers=1 is the sequential loop.

    newest_first=True: when the table holds more than max_rows matching rows,
    keep the newest max_rows instead of the oldest. The rows are fetched in the
    reverse of the requested order and returned in the requested order, so
    callers that ask for order=trading_date.asc still get ascending rows.

    PostgREST caps every response at 1000 rows server-side whatever limit= says,
    so page_size above 1000 would end the loop early on a "short" page."""
    first = _read_counted if read is None else None
    read = read or supabase_read
    page_size = min(page_size, 1000)
    workers = max(1, workers or PAGE_WORKERS)
    params = paginated_order(table, params, newest_first)
    separator = "&" if "?" in params else "?"
    # Pages that hold max_rows (the old loop stopped once it had that many).
    max_pages = None if max_rows is None else max(1, -(-max_rows // page_size))

    def query(i):
        return f"{params}{separator}limit={page_size}&offset={i * page_size}"

    def page(i):
        return read(table, query(i), timeout=timeout)

    total = None
    if first:
        rows0, total = first(table, query(0), timeout=timeout)
    else:
        rows0 = page(0)
    pages = [rows0]
    if len(rows0) == page_size and max_pages != 1:
        from concurrent.futures import ThreadPoolExecutor
        # Pages the count says exist (at least one more, as page 0 was full).
        known = max(2, -(-total // page_size)) if total is not None else None
        with ThreadPoolExecutor(max_workers=min(workers, 16)) as pool:
            nxt, done = 1, False
            while not done:
                if known is not None and nxt < known:
                    stop = known                    # every counted page at once
                elif known is not None:
                    stop = nxt + 1                  # past the count: probe one page, as the loop did
                else:
                    stop = nxt + workers            # no count: a wave at a time
                if max_pages is not None:
                    stop = min(stop, max_pages)
                # map() yields in offset order and re-raises a page's error.
                for rows in pool.map(page, range(nxt, stop)):
                    pages.append(rows)
                    if len(rows) < page_size:
                        done = True
                        break
                nxt = stop
                if max_pages is not None and nxt >= max_pages:
                    done = True
    all_rows = [r for p in pages for r in p]
    if max_rows is not None and len(all_rows) >= max_rows and len(pages[-1]) == page_size:
        import sys as _sys
        print(_cap_warning(table, params, max_rows), file=_sys.stderr)
    if max_rows is not None:
        all_rows = all_rows[:max_rows]
    return all_rows[::-1] if newest_first else all_rows


def supabase_upsert(table, data, timeout=10, on_conflict=None):
    """Upsert data into Supabase table (uses the service-role write key when set).

    on_conflict names the unique column(s) to merge on. Without it PostgREST
    merges on the primary key, so rows that carry a surrogate id (e.g.
    mcx_daily_revenue, unique on trading_date) insert and fail on the unique
    column instead of updating."""
    import urllib.request, urllib.error, json
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    if on_conflict:
        url += f"?on_conflict={on_conflict}"
    headers = {
        **supabase_headers(SUPABASE_WRITE_KEY),
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


# ─── Batched upserts that report errors instead of raising ─────────────────

def upsert_batches(table, rows, on_conflict=None, batch=50, timeout=15, stats=None,
                   on_409="error"):
    """POST `rows` in batches with merge-duplicates (write key). Returns a list
    of error strings, one per failed batch ("batch i: HTTP code — body"); an
    empty list means every batch was accepted. Network errors (URLError,
    timeout) still raise.

    on_409="skip": a 409 (a unique-key collision merge-duplicates cannot
    resolve, e.g. a table with no on_conflict target) drops that batch without
    counting it as an error, and adds its size to stats["skipped"]. Every
    accepted row is added to stats["written"]. Used by the margins and OI
    jobs, whose re-runs hit 409 on rows that already exist (Phase 1 H5).

    This is the shape every cron job reports ({"success": not errors, ...}),
    and their handlers answer 500 on any error."""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    if on_conflict:
        url += f"?on_conflict={on_conflict}"
    headers = {**supabase_headers(SUPABASE_WRITE_KEY), "Content-Type": "application/json",
               "Prefer": "resolution=merge-duplicates"}
    errors = []
    for i in range(0, len(rows), batch):
        chunk = rows[i:i + batch]
        req = urllib.request.Request(url, data=json.dumps(chunk).encode(), method="POST",
                                     headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=timeout):
                pass
            if stats is not None:
                stats["written"] = stats.get("written", 0) + len(chunk)
        except urllib.error.HTTPError as e:
            body = e.read().decode()[:200] if e.fp else ""
            if e.code == 409 and on_409 == "skip":
                if stats is not None:
                    stats["skipped"] = stats.get("skipped", 0) + len(chunk)
                continue
            errors.append(f"batch {i}: HTTP {e.code} — {body}")
    return errors


def get_rows(table, params="", timeout=15):
    """GET /rest/v1/<table><params> with the read key and only the auth
    headers. Unlike supabase_read, urllib's errors are raised as-is."""
    req = urllib.request.Request(f"{SUPABASE_URL}/rest/v1/{table}{params}",
                                 headers=supabase_headers(SUPABASE_ANON_KEY))
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def post_rows(table, rows, on_conflict=None, timeout=15, prefer="resolution=merge-duplicates"):
    """One POST (an upsert, by default) of a row or a list of rows with the
    write key. Returns nothing; urllib's HTTPError / URLError are raised as-is,
    for callers that branch on the status (e.g. 409 -> PATCH)."""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    if on_conflict:
        url += f"?on_conflict={on_conflict}"
    req = urllib.request.Request(url, data=json.dumps(rows).encode(), method="POST", headers={
        **supabase_headers(SUPABASE_WRITE_KEY), "Content-Type": "application/json", "Prefer": prefer})
    with urllib.request.urlopen(req, timeout=timeout):
        pass


def patch_rows(table, params, row, timeout=15, prefer="return=minimal"):
    """PATCH the rows matching `params` (e.g. "?trading_date=eq.2026-11-17")
    with the columns in `row`, using the write key. Raises urllib's errors."""
    req = urllib.request.Request(f"{SUPABASE_URL}/rest/v1/{table}{params}",
                                 data=json.dumps(row).encode(), method="PATCH", headers={
        **supabase_headers(SUPABASE_WRITE_KEY), "Content-Type": "application/json", "Prefer": prefer})
    with urllib.request.urlopen(req, timeout=timeout):
        pass


def fetch_all(table, select, order="trading_date", params="", timeout=15):
    """Every row of `table` (no cap), ascending by `order` with the table's
    unique key as tiebreaker. `params` adds filters ("&trading_date=gte.…")."""
    return supabase_read_all(table, f"?select={select}{params}&order={order}.asc",
                             max_rows=None, timeout=timeout)


# ─── The NSE-BSE project (read-only) ─────────────────────────────────────────
# BSE's daily revenue lives in a second Supabase project (codex_bse_daily). Its
# tables have no anon policy, so reading it needs a key with read access set as
# NSEBSE_SUPABASE_KEY (and NSEBSE_SUPABASE_URL). Without both, callers skip BSE.
NSEBSE_SUPABASE_URL = os.environ.get("NSEBSE_SUPABASE_URL", "")
NSEBSE_SUPABASE_KEY = os.environ.get("NSEBSE_SUPABASE_KEY", "")


def nsebse_read_all(table, params="", page_size=1000, max_rows=20000, timeout=15):
    """Every row of a NSE-BSE project table, paged by offset. params must carry a
    total order (e.g. "?select=date,total_rev&order=date.asc"). [] if unconfigured."""
    if not (NSEBSE_SUPABASE_URL and NSEBSE_SUPABASE_KEY):
        return []
    rows, off = [], 0
    sep = "&" if "?" in params else "?"
    while off < max_rows:
        req = urllib.request.Request(
            f"{NSEBSE_SUPABASE_URL}/rest/v1/{table}{params}{sep}offset={off}&limit={page_size}",
            headers=supabase_headers(NSEBSE_SUPABASE_KEY))
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            batch = json.loads(resp.read().decode())
        rows += batch
        if len(batch) < page_size:
            break
        off += page_size
    return rows
