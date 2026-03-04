"""
/api/mcxprice — MCX CMP (Current Market Price) endpoint.
Primary: indianapi.in  |  Fallback: Yahoo Finance  |  Cache: Supabase
Single-purpose: returns latest MCX Ltd share price for frontend auto-update.
"""
from http.server import BaseHTTPRequestHandler
import json, os, urllib.request, urllib.error
from datetime import datetime, timedelta, timezone

try:
    from api.mcx_config import (
        SUPABASE_URL, SUPABASE_ANON_KEY, make_cors_headers
    )
except ImportError:
    from mcx_config import (
        SUPABASE_URL, SUPABASE_ANON_KEY, make_cors_headers
    )

INDIANAPI_KEY = os.environ.get("INDIANAPI_KEY", "")
INDIANAPI_URL = "https://stock.indianapi.in/stock?name=MCX"
YAHOO_URL = "https://query1.finance.yahoo.com/v8/finance/chart/MCX.NS?range=1d&interval=1d"

CACHE_TTL_MINUTES = 5


def _now_utc():
    return datetime.now(timezone.utc)


def _fetch_indianapi():
    """Fetch MCX price from indianapi.in — returns (price, change_pct, source)."""
    if not INDIANAPI_KEY:
        raise ValueError("INDIANAPI_KEY not set")
    req = urllib.request.Request(INDIANAPI_URL, headers={
        "x-api-key": INDIANAPI_KEY,
        "User-Agent": "mcx-revenue-predictor/1.0",
    })
    with urllib.request.urlopen(req, timeout=8) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    # Parse response: { currentPrice: { BSE: "2448", NSE: "2446.30" }, percentChange: 2.17 }
    price = None
    change_pct = None
    if isinstance(data, dict):
        cp = data.get("currentPrice")
        if isinstance(cp, dict):
            # Prefer NSE price, fallback to BSE
            nse = cp.get("NSE") or cp.get("nse")
            bse = cp.get("BSE") or cp.get("bse")
            raw = nse or bse
            if raw is not None:
                price = float(str(raw).replace(",", ""))
        elif cp is not None:
            price = float(str(cp).replace(",", ""))
        # Change percent
        pct = data.get("percentChange")
        if pct is not None:
            try:
                change_pct = float(pct)
            except (ValueError, TypeError):
                pass
    if price is None:
        raise ValueError(f"Could not parse indianapi response: {json.dumps(data)[:400]}")
    return price, change_pct, "indianapi"


def _fetch_yfinance():
    """Fetch MCX.NS price using yfinance library (already in requirements.txt)."""
    import yfinance as yf
    ticker = yf.Ticker("MCX.NS")
    hist = ticker.history(period="1d")
    if hist.empty:
        raise ValueError("yfinance returned no data for MCX.NS")
    price = float(hist["Close"].iloc[-1])
    prev = float(hist["Open"].iloc[0]) if "Open" in hist.columns else price
    change_pct = round(((price - prev) / prev * 100), 2) if prev > 0 else 0
    return price, change_pct, "yfinance"


def _fetch_yahoo():
    """Fetch MCX.NS price from Yahoo Finance — returns (price, change_pct, source)."""
    req = urllib.request.Request(YAHOO_URL, headers={
        "User-Agent": "Mozilla/5.0",
    })
    with urllib.request.urlopen(req, timeout=8) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    result = data["chart"]["result"][0]
    meta = result["meta"]
    price = meta.get("regularMarketPrice") or meta.get("previousClose")
    if price is None:
        raise ValueError("Yahoo Finance: no price in response")
    prev = meta.get("previousClose") or meta.get("chartPreviousClose")
    change_pct = None
    if prev and prev > 0:
        change_pct = round((float(price) - float(prev)) / float(prev) * 100, 2)
    return float(price), change_pct, "yahoo"


def _read_cache():
    """Read cached price from Supabase. Returns dict or None."""
    if not SUPABASE_ANON_KEY:
        return None
    try:
        url = f"{SUPABASE_URL}/rest/v1/mcx_cmp_cache?select=*&order=fetched_at.desc&limit=1"
        req = urllib.request.Request(url, headers={
            "apikey": SUPABASE_ANON_KEY,
            "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        })
        with urllib.request.urlopen(req, timeout=5) as resp:
            rows = json.loads(resp.read().decode("utf-8"))
        if rows and len(rows) > 0:
            return rows[0]
    except Exception:
        pass
    return None


def _write_cache(price, source, change_pct=None):
    """Write price to Supabase cache (singleton row, upsert)."""
    if not SUPABASE_ANON_KEY:
        return
    try:
        url = f"{SUPABASE_URL}/rest/v1/mcx_cmp_cache"
        row = {
            "id": 1,
            "price": price,
            "source": source,
            "change_pct": change_pct,
            "fetched_at": _now_utc().isoformat(),
        }
        body = json.dumps(row).encode("utf-8")
        req = urllib.request.Request(url, data=body, method="POST", headers={
            "apikey": SUPABASE_ANON_KEY,
            "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates,return=representation",
        })
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass  # cache write failure is non-fatal


def _get_price():
    """Fetch MCX price with cache → indianapi → yahoo fallback chain."""
    # 1. Check Supabase cache
    cached = _read_cache()
    if cached:
        try:
            fetched_str = cached.get("fetched_at", "")
            fetched_at = datetime.fromisoformat(fetched_str.replace("Z", "+00:00"))
            age_min = (_now_utc() - fetched_at).total_seconds() / 60
            if age_min < CACHE_TTL_MINUTES:
                return {
                    "price": cached["price"],
                    "change_pct": cached.get("change_pct"),
                    "source": cached.get("source", "cache"),
                    "cached": True,
                    "age_minutes": round(age_min, 1),
                    "fetched_at": fetched_str,
                }
        except Exception:
            pass

    # 2. Try indianapi.in (primary)
    errors = []
    try:
        price, change_pct, source = _fetch_indianapi()
        _write_cache(price, source, change_pct)
        return {
            "price": price,
            "change_pct": change_pct,
            "source": source,
            "cached": False,
            "fetched_at": _now_utc().isoformat(),
        }
    except Exception as e:
        errors.append(f"indianapi: {e}")

    # 3. Try yfinance library (reliable, already in requirements.txt)
    try:
        price, change_pct, source = _fetch_yfinance()
        _write_cache(price, source, change_pct)
        return {
            "price": price,
            "change_pct": change_pct,
            "source": source,
            "cached": False,
            "fetched_at": _now_utc().isoformat(),
        }
    except Exception as e:
        errors.append(f"yfinance: {e}")

    # 4. Fallback to raw Yahoo Finance HTTP
    try:
        price, change_pct, source = _fetch_yahoo()
        _write_cache(price, source, change_pct)
        return {
            "price": price,
            "change_pct": change_pct,
            "source": source,
            "cached": False,
            "fetched_at": _now_utc().isoformat(),
        }
    except Exception as e:
        errors.append(f"yahoo: {e}")

    # 4. Serve stale cache if available
    if cached:
        return {
            "price": cached["price"],
            "change_pct": cached.get("change_pct"),
            "source": "stale_cache",
            "cached": True,
            "fetched_at": cached.get("fetched_at", ""),
            "warnings": errors,
        }

    # 5. All failed
    return {"error": "All price sources failed", "details": errors}


class handler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        pass

    def _cors(self):
        origin = self.headers.get("Origin", "")
        hdrs = make_cors_headers(origin)
        for k, v in hdrs.items():
            self.send_header(k, v)
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Cache-Control", "public, max-age=300")

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        result = _get_price()
        status = 502 if "error" in result else 200
        body = json.dumps(result).encode("utf-8")
        self.send_response(status)
        self._cors()
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)
