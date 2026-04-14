"""
lib/cron_oi_participants — Daily MCX OI Participant Category Collection

Downloads MCX's "Disclosure of Open Interest and Turnover" XLSX,
parses Part A ("Number of participants in each category"),
and upserts to mcx_oi_participants in Supabase.

Schedule: 20:00 IST weekdays (after market close).
Source: https://www.mcxindia.com/docs/default-source/market-operations/
        trading-survelliance/reports/disclosure-of-open-interest-and-
        turnover-for-various-categories-of-market-participants/
"""
from http.server import BaseHTTPRequestHandler
import json, urllib.request, urllib.error, os, re, time
from io import BytesIO
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs

try:
    from lib.mcx_config import (
        SUPABASE_URL, SUPABASE_ANON_KEY,
        now_ist, is_trading_day, make_cors_headers,
    )
except ImportError:
    from lib.mcx_config import (
        SUPABASE_URL, SUPABASE_ANON_KEY,
        now_ist, is_trading_day, make_cors_headers,
    )

CRON_SECRET = os.environ.get("CRON_SECRET", "")
MAX_RETRIES = 2
RETRY_DELAY = 3

BASE_URL = (
    "https://www.mcxindia.com/docs/default-source/market-operations/"
    "trading-survelliance/reports/disclosure-of-open-interest-and-"
    "turnover-for-various-categories-of-market-participants"
)

# Column mapping from XLSX data rows (0-indexed)
#  0=Commodity, 1=Instrument, 2=TotalParticipation, 3=Unit,
#  4=FPO_Long, 5=FPO_Short, 6=VCP_Long, 7=VCP_Short,
#  8=Prop_Long, 9=Prop_Short, 10=DFI_Long, 11=DFI_Short,
# 12=Foreign_Long, 13=Foreign_Short, 14=Others_Long, 15=Others_Short


def _build_url(dt):
    """Construct download URL from date. Format: april-10-2026."""
    month = dt.strftime("%B").lower()  # e.g. "april"
    day = dt.day                        # e.g. 10 (no zero-pad)
    year = dt.year                      # e.g. 2026
    filename = f"open_interest_and_turnover_for_various_categories_{month}-{day}-{year}.xlsx"
    return f"{BASE_URL}/{filename}"


def _download_oi_xlsx(dt=None, log=None):
    """Download the OI participants XLSX from MCX with retry."""
    if dt is None:
        dt = now_ist().date()
    url = _build_url(dt)
    if log is not None:
        log.append(f"Downloading: {url}")

    last_err = None
    for attempt in range(1, MAX_RETRIES + 2):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*",
                "Referer": "https://www.mcxindia.com/",
            })
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.read()
        except Exception as e:
            last_err = e
            if attempt <= MAX_RETRIES:
                if log is not None:
                    log.append(f"Download attempt {attempt} failed: {e}, retrying in {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)
    raise last_err


def _parse_value(val):
    """Parse a cell value: int, '<10' -> -1 (sentinel), empty/None -> 0."""
    if val is None:
        return 0
    s = str(val).strip()
    if s in ("", "nan", "NaN"):
        return 0
    if "<10" in s:
        return -1
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return 0


def _find_title_row(df):
    """Dynamically find the 'Number of participants' title row."""
    for i in range(0, len(df)):
        cell = str(df.iloc[i, 0]) if df.iloc[i, 0] is not None else ""
        if "number of participants" in cell.lower():
            return i
    return None


def _parse_participants(xlsx_bytes):
    """Parse PART A participant count table.
    Returns (report_date_str, list_of_row_dicts)."""
    import pandas as pd

    df = pd.read_excel(BytesIO(xlsx_bytes), sheet_name="PART A", header=None, engine="openpyxl")

    # Find title row dynamically
    title_idx = _find_title_row(df)
    if title_idx is None:
        raise ValueError("Could not find 'Number of participants in each category' title row")

    # Extract date from title
    title_text = str(df.iloc[title_idx, 0])
    date_match = re.search(r'as on (.+?)$', title_text, re.IGNORECASE)
    if not date_match:
        raise ValueError(f"Could not extract date from title: {title_text}")

    date_str = date_match.group(1).strip()
    report_date = datetime.strptime(date_str, "%B %d, %Y").strftime("%Y-%m-%d")

    # Data rows start 3 rows after title (title, header, sub-header, data)
    data_start = title_idx + 3
    rows = []

    for i in range(data_start, len(df)):
        commodity = df.iloc[i, 0]
        if commodity is None or str(commodity).strip() in ("", "nan"):
            break  # End of data

        instrument = str(df.iloc[i, 1]).strip()
        if instrument not in ("Futures", "Options"):
            continue  # Skip unrecognized instruments (e.g. OPTIDX) but keep parsing

        rows.append({
            "report_date": report_date,
            "commodity": str(commodity).strip(),
            "instrument": instrument,
            "total_participation": _parse_value(df.iloc[i, 2]),
            "unit": str(df.iloc[i, 3]).strip() if df.iloc[i, 3] is not None else None,
            "fpo_long": _parse_value(df.iloc[i, 4]),
            "fpo_short": _parse_value(df.iloc[i, 5]),
            "vcp_long": _parse_value(df.iloc[i, 6]),
            "vcp_short": _parse_value(df.iloc[i, 7]),
            "prop_long": _parse_value(df.iloc[i, 8]),
            "prop_short": _parse_value(df.iloc[i, 9]),
            "dfi_long": _parse_value(df.iloc[i, 10]),
            "dfi_short": _parse_value(df.iloc[i, 11]),
            "foreign_long": _parse_value(df.iloc[i, 12]),
            "foreign_short": _parse_value(df.iloc[i, 13]),
            "others_long": _parse_value(df.iloc[i, 14]),
            "others_short": _parse_value(df.iloc[i, 15]),
            "source": "mcx_xlsx",
        })

    return report_date, rows


def sb_upsert(table, rows):
    """Upsert rows to Supabase in batches. Treats 409 as success."""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }
    errors = []
    for i in range(0, len(rows), 50):
        chunk = rows[i:i + 50]
        body = json.dumps(chunk).encode()
        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                pass
        except urllib.error.HTTPError as e:
            if e.code == 409:
                continue
            err_body = e.read().decode()[:200] if e.fp else ""
            errors.append(f"batch {i}: HTTP {e.code} — {err_body}")
    return errors


def refresh_oi_participants(dt=None):
    """Download, parse, and upsert OI participant data. Returns summary dict."""
    log = []

    if dt is None:
        dt = now_ist().date()

    log.append(f"Refreshing OI participants for {dt}...")

    # Download XLSX
    try:
        data = _download_oi_xlsx(dt, log=log)
        log.append(f"Downloaded {len(data)} bytes")
    except Exception as e:
        return {"success": False, "error": f"Download failed: {e}", "log": log}

    # Parse
    try:
        report_date, rows = _parse_participants(data)
        log.append(f"Parsed {len(rows)} participant rows for {report_date}")
    except Exception as e:
        return {"success": False, "error": f"Parse failed: {e}", "log": log}

    if not rows:
        return {"success": False, "error": "No rows parsed", "log": log}

    # Upsert
    errors = sb_upsert("mcx_oi_participants", rows)
    if errors:
        log.extend([f"Error: {e}" for e in errors])

    commodities = sorted(set(r["commodity"] for r in rows))
    return {
        "success": len(errors) == 0,
        "report_date": report_date,
        "rows_upserted": len(rows),
        "commodities": commodities,
        "log": log,
        "errors": errors,
    }


class handler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        pass

    def _cors(self):
        origin = self.headers.get("Origin", "")
        hdrs = make_cors_headers(origin)
        for k, v in hdrs.items():
            self.send_header(k, v)

    def send_json(self, data, status=200):
        body = json.dumps(data, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def _check_auth(self):
        auth_header = self.headers.get("Authorization", "")
        if auth_header.startswith("Bearer ") and auth_header[7:] == CRON_SECRET:
            return True
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)
        if qs.get("secret", [None])[0] == CRON_SECRET:
            return True
        if not CRON_SECRET:
            return True
        return False

    def do_GET(self):
        if not self._check_auth():
            self.send_json({"success": False, "error": "Unauthorized"}, 401)
            return

        try:
            result = refresh_oi_participants()
            result["as_of"] = now_ist().strftime("%Y-%m-%d %H:%M IST")
            self.send_json(result, 200 if result.get("success") else 500)
        except Exception as e:
            self.send_json({"success": False, "error": str(e)[:200]}, 500)
