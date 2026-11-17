"""
/api/mcxprice — MCX CMP (Current Market Price) endpoint.
Primary: yfinance (live intraday)  |  Fallback: Yahoo Finance, indianapi.in  |  Cache: CDN, 30 s
Single-purpose: returns latest MCX Ltd share price for frontend auto-update.
"""
from lib.api_handler import JSONHandler
import json, os, urllib.request, urllib.error
from datetime import datetime, timezone

INDIANAPI_KEY = os.environ.get("INDIANAPI_KEY", "")
INDIANAPI_URL = "https://stock.indianapi.in/stock?name=MCX"
YAHOO_URL = "https://query1.finance.yahoo.com/v8/finance/chart/MCX.NS?range=1d&interval=1d"


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
    """Fetch MCX.NS live intraday price using yfinance fast_info."""
    import yfinance as yf
    ticker = yf.Ticker("MCX.NS")
    info = ticker.fast_info
    price = float(info["last_price"])
    prev = float(info["previous_close"])
    if price <= 0:
        raise ValueError("yfinance fast_info returned invalid price")
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


def _get_price():
    """Fetch MCX price: yfinance → yahoo → indianapi fallback chain. The CDN
    holds each answer for 30 s (see the handler), which is the only cache: the
    Supabase cache table this used to read and write, mcx_cmp_cache, never
    existed (reads 404'd, writes failed silently)."""
    # 1. Try yfinance library (primary, reliable)
    errors = []
    try:
        price, change_pct, source = _fetch_yfinance()
        return {
            "price": price,
            "change_pct": change_pct,
            "source": source,
            "cached": False,
            "fetched_at": _now_utc().isoformat(),
            "tried": errors,
        }
    except Exception as e:
        errors.append(f"yfinance: {e}")

    # 2. Try raw Yahoo Finance HTTP (live regularMarketPrice)
    try:
        price, change_pct, source = _fetch_yahoo()
        return {
            "price": price,
            "change_pct": change_pct,
            "source": source,
            "cached": False,
            "fetched_at": _now_utc().isoformat(),
            "tried": errors,
        }
    except Exception as e:
        errors.append(f"yahoo: {e}")

    # 3. Try indianapi.in (returns EOD close, last resort)
    try:
        price, change_pct, source = _fetch_indianapi()
        return {
            "price": price,
            "change_pct": change_pct,
            "source": source,
            "cached": False,
            "fetched_at": _now_utc().isoformat(),
            "tried": errors,
        }
    except Exception as e:
        errors.append(f"indianapi: {e}")

    # 4. All failed
    return {"error": "All price sources failed", "details": errors}


class handler(JSONHandler):

    def do_GET(self):
        result = _get_price()
        status = 502 if "error" in result else 200
        # 30 s CDN cache for a price; an error (502) is never cached.
        self.send_json(result, status, cache="public, max-age=30, s-maxage=30")
