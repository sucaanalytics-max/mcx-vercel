"""
lib/cron_margins — Daily MCX margin snapshot collection

Downloads Sharekhan's MCX SPAN margin file (XLS), parses it,
and upserts to mcx_margin_daily in Supabase.

Schedule: 19:55 IST weekdays (after market close).
Source: https://www.sharekhan.com/MediaGalary/Commodity/McxSpan.xls
"""
from http.server import BaseHTTPRequestHandler
import json, urllib.request, urllib.error, os
from io import BytesIO
from urllib.parse import urlparse, parse_qs

try:
    from lib.mcx_config import (
        SUPABASE_URL, SUPABASE_ANON_KEY,
        now_ist, make_cors_headers,
    )
except ImportError:
    from lib.mcx_config import (
        SUPABASE_URL, SUPABASE_ANON_KEY,
        now_ist, make_cors_headers,
    )

CRON_SECRET = os.environ.get("CRON_SECRET", "")
SHAREKHAN_URL = "https://www.sharekhan.com/MediaGalary/Commodity/McxSpan.xls"

# Column mapping from XLS → database
# XLS columns (0-indexed):
#  0=Date, 1=FileID, 2=Instrument, 3=Symbol, 4=Expiry,
#  5=InitialMargin%, 6=TenderMargin%, 7=TotalMargin%,
#  8=AdditionalLong%, 9=AdditionalShort%,
# 10=SpecialLong%, 11=SpecialShort%,
# 12=ELMLong%, 13=ELMShort%, 14=DeliveryMargin%


def _download_xls():
    """Download the Sharekhan MCX SPAN XLS file."""
    req = urllib.request.Request(SHAREKHAN_URL, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    })
    with urllib.request.urlopen(req, timeout=20) as resp:
        return resp.read()


def _parse_xls(data):
    """Parse XLS bytes into list of margin row dicts."""
    import pandas as pd

    df = pd.read_excel(BytesIO(data), engine="xlrd")

    # Normalize column names
    cols = df.columns.tolist()
    if len(cols) < 15:
        raise ValueError(f"Expected 15+ columns, got {len(cols)}: {cols}")

    rows = []
    for _, r in df.iterrows():
        # Extract date from first column
        date_val = r.iloc[0]
        if hasattr(date_val, 'strftime'):
            snapshot_date = date_val.strftime("%Y-%m-%d")
        else:
            snapshot_date = str(date_val).strip()

        symbol = str(r.iloc[3]).strip()
        instrument = str(r.iloc[2]).strip()
        expiry_raw = r.iloc[4]
        if hasattr(expiry_raw, 'strftime'):
            expiry = expiry_raw.strftime("%Y-%m-%d")
        elif expiry_raw and str(expiry_raw).strip() not in ("", "nan"):
            expiry = str(expiry_raw).strip()[:10]  # Take date part only
        else:
            expiry = None

        def _num(val):
            try:
                v = float(val)
                return round(v, 2) if v == v else None  # NaN check
            except (ValueError, TypeError):
                return None

        rows.append({
            "snapshot_date": snapshot_date,
            "symbol": symbol,
            "instrument": instrument,
            "expiry": expiry,
            "initial_margin_pct": _num(r.iloc[5]),
            "tender_margin_pct": _num(r.iloc[6]),
            "total_margin_pct": _num(r.iloc[7]),
            "additional_long_pct": _num(r.iloc[8]),
            "additional_short_pct": _num(r.iloc[9]),
            "special_long_pct": _num(r.iloc[10]),
            "special_short_pct": _num(r.iloc[11]),
            "elm_long_pct": _num(r.iloc[12]),
            "elm_short_pct": _num(r.iloc[13]),
            "delivery_margin_pct": _num(r.iloc[14]),
            "source": "sharekhan_span",
        })

    return rows


def sb_upsert(table, rows):
    """Upsert rows to Supabase in batches."""
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
            err_body = e.read().decode()[:200] if e.fp else ""
            errors.append(f"batch {i}: HTTP {e.code} — {err_body}")
    return errors


def refresh_margins():
    """Download, parse, and upsert margin data. Returns summary dict."""
    log = []

    # Download XLS
    log.append("Downloading Sharekhan MCX SPAN file...")
    try:
        data = _download_xls()
        log.append(f"Downloaded {len(data)} bytes")
    except Exception as e:
        return {"success": False, "error": f"Download failed: {e}", "log": log}

    # Parse
    try:
        rows = _parse_xls(data)
        log.append(f"Parsed {len(rows)} margin rows")
    except Exception as e:
        return {"success": False, "error": f"Parse failed: {e}", "log": log}

    if not rows:
        return {"success": False, "error": "No rows parsed", "log": log}

    snapshot_date = rows[0]["snapshot_date"]
    log.append(f"Snapshot date: {snapshot_date}")

    # Upsert
    errors = sb_upsert("mcx_margin_daily", rows)
    if errors:
        log.extend([f"Error: {e}" for e in errors])

    # Summary
    symbols = sorted(set(r["symbol"] for r in rows))
    avg_total_margin = sum(r["total_margin_pct"] or 0 for r in rows) / len(rows)

    return {
        "success": len(errors) == 0,
        "snapshot_date": snapshot_date,
        "rows_upserted": len(rows),
        "symbols": symbols,
        "avg_total_margin_pct": round(avg_total_margin, 2),
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
            result = refresh_margins()
            result["as_of"] = now_ist().strftime("%Y-%m-%d %H:%M IST")
            self.send_json(result, 200 if result.get("success") else 500)
        except Exception as e:
            self.send_json({"success": False, "error": str(e)[:200]}, 500)
