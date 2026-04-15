"""
/api/refresh — MCX live intraday data via GetMarketWatch API.
Fixes: F-02 (shared config), F-07 (no hardcoded keys), F-08 (Supabase relay),
       F-09 (secure cookie), F-10 (conditional dual-call), F-12 (error differentiation),
       F-13 (restricted CORS), F-22 (no duplication)
"""
from http.server import BaseHTTPRequestHandler
import json, math, os, time, urllib.request, urllib.error
from datetime import datetime, timedelta
from collections import defaultdict
try:
    from lib.mcx_config import (
        FUTURES_RATE, OPTIONS_RATE, NONTX_DAILY, TRADING_DAYS,
        SESSION_START, SESSION_END, SESSION_TOTAL, INTRADAY_BUCKETS,
        DAY_MULTIPLIER, DAY_DESCRIPTION,
        get_day_type, get_intraday_weight, project_full_day,
        calc_revenue, calc_uncertainty, now_ist, is_market_open,
        make_cors_headers, SUPABASE_URL, SUPABASE_ANON_KEY,
        supabase_read, supabase_upsert,
    )
except (ImportError, Exception):
    from lib.mcx_config import (  # type: ignore
        FUTURES_RATE, OPTIONS_RATE, NONTX_DAILY, TRADING_DAYS,
        SESSION_START, SESSION_END, SESSION_TOTAL, INTRADAY_BUCKETS,
        DAY_MULTIPLIER, DAY_DESCRIPTION,
        get_day_type, get_intraday_weight, project_full_day,
        calc_revenue, calc_uncertainty, now_ist, is_market_open,
        make_cors_headers, SUPABASE_URL, SUPABASE_ANON_KEY,
        supabase_read, supabase_upsert,
    )

# ─── Inline fallback if import missed supabase helpers ──────────────────────
if not callable(globals().get("supabase_read")):
    _SB_URL = os.environ.get("SUPABASE_URL", "https://avqwpebveqetwwzkmtux.supabase.co")
    _SB_KEY = os.environ.get("SUPABASE_ANON_KEY", "")

    def supabase_read(table, params="", timeout=10):
        url = f"{_SB_URL}/rest/v1/{table}{params}"
        headers = {"apikey": _SB_KEY, "Authorization": f"Bearer {_SB_KEY}", "Content-Type": "application/json", "Prefer": "return=representation"}
        req = urllib.request.Request(url, method="GET", headers=headers)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def supabase_upsert(table, data, timeout=10):
        url = f"{_SB_URL}/rest/v1/{table}"
        headers = {"apikey": _SB_KEY, "Authorization": f"Bearer {_SB_KEY}", "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates,return=representation"}
        body = json.dumps(data).encode("utf-8")
        req = urllib.request.Request(url, data=body, method="POST", headers=headers)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))


def _auto_fetch_cookies(timeout: int = 12):
    """Attempt to obtain fresh MCX session cookie server-side."""
    try:
        import http.cookiejar
        cj = http.cookiejar.CookieJar()
        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
        req = urllib.request.Request(
            "https://www.mcxindia.com/market-data/market-watch",
            headers={
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            },
        )
        opener.open(req, timeout=timeout)
        parts = [f"{c.name}={c.value}" for c in cj]
        cookie_str = "; ".join(parts)
        if "ASP.NET_SessionId" in cookie_str or len(parts) >= 2:
            return cookie_str
        return ""
    except Exception:
        return ""


def _fetch_mcx(cookie: str, timeout: int = 18):
    """Single call to MCX GetMarketWatch. Returns raw dict or raises."""
    req = urllib.request.Request(
        "https://www.mcxindia.com/backpage.aspx/GetMarketWatch",
        data=b"",
        method="POST",
        headers={
            "accept": "application/json, text/javascript, */*; q=0.01",
            "content-type": "application/json",
            "origin": "https://www.mcxindia.com",
            "referer": "https://www.mcxindia.com/market-data/market-watch",
            "x-requested-with": "XMLHttpRequest",
            "cookie": cookie,
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw_bytes = resp.read()
        encoding = resp.info().get("Content-Encoding", "")
        if "gzip" in encoding:
            import gzip
            raw_bytes = gzip.decompress(raw_bytes)
        return json.loads(raw_bytes.decode("utf-8"))


def _extract_notionals(raw_json: dict):
    """Return (fut_notl, opt_notl, opt_prem, futures_list, options_list)."""
    contracts = raw_json.get("d", {}).get("Data", [])
    futures = [c for c in contracts if c.get("InstrumentName") == "FUTCOM" and c.get("Volume", 0) > 0]
    options = [c for c in contracts if c.get("InstrumentName") == "OPTFUT" and c.get("Volume", 0) > 0]
    fut_notl = sum(c.get("NotionalValue", 0) for c in futures) / 100
    opt_notl = sum(c.get("NotionalValue", 0) for c in options) / 100
    opt_prem = sum(c.get("PremiumValue",  0) for c in options) / 100
    return fut_notl, opt_notl, opt_prem, futures, options


def process_market_data(raw_json, capture_time_ist, raw_json2=None):
    """Process one (or two averaged) MCX API snapshots."""
    fut_n1, opt_n1, opt_p1, futures, options = _extract_notionals(raw_json)

    if raw_json2 is not None:
        fut_n2, opt_n2, opt_p2, _, _ = _extract_notionals(raw_json2)
        fut_notl = max(fut_n1, fut_n2)
        opt_notl = max(opt_n1, opt_n2)
        opt_prem = max(opt_p1, opt_p2)
        dual_call = True
    else:
        fut_notl, opt_notl, opt_prem = fut_n1, opt_n1, opt_p1
        dual_call = False

    current_min = capture_time_ist.hour * 60 + capture_time_ist.minute
    elapsed_min = max(0, min(current_min - SESSION_START, SESSION_TOTAL))
    time_pct = elapsed_min / SESSION_TOTAL

    day_type = get_day_type(capture_time_ist)
    raw_day_mult = DAY_MULTIPLIER[day_type]
    day_desc = DAY_DESCRIPTION[day_type]

    proj_fut, proj_opt, conf = project_full_day(fut_notl, opt_prem, elapsed_min, day_type)
    fut_rev, opt_rev, tx_rev, total_rev = calc_revenue(proj_fut, proj_opt)
    _, _, tx_rev_realized, _ = calc_revenue(fut_notl, opt_prem)

    combined_unc = calc_uncertainty(time_pct, day_type, dual_call)
    rev_low = round(total_rev * (1 - combined_unc), 2)
    rev_high = round(total_rev * (1 + combined_unc), 2)

    # Per-commodity breakdown
    sym_fut = defaultdict(float)
    sym_opt = defaultdict(float)
    sym_optN = defaultdict(float)
    for c in futures:
        sym_fut[c["Symbol"]] += c.get("NotionalValue", 0) / 100
    for c in options:
        base = c["Symbol"].replace("OPT", "")
        sym_opt[base] += c.get("PremiumValue", 0) / 100
        sym_optN[base] += c.get("NotionalValue", 0) / 100

    top_fut = sorted(
        [{"sym": k, "notl": round(v, 2)} for k, v in sym_fut.items()],
        key=lambda x: -x["notl"]
    )[:12]
    top_opt = sorted(
        [{"sym": k, "prem": round(v, 2), "notl": round(sym_optN[k], 2),
          "ratio": round(v / sym_optN[k] * 100, 3) if sym_optN[k] > 0 else 0}
         for k, v in sym_opt.items()],
        key=lambda x: -x["prem"]
    )[:12]

    result = {
        "success": True,
        "timestamp": capture_time_ist.strftime("%H:%M IST, %d %b %Y"),
        "elapsed_min": elapsed_min,
        "session_total_min": SESSION_TOTAL,
        "elapsed_pct": round(time_pct * 100, 1),
        "session_closed": elapsed_min >= SESSION_TOTAL,
        "active_futures": len(futures),
        "active_options": len(options),
        "fut_notl_cr": round(fut_notl, 2),
        "opt_notl_cr": round(opt_notl, 2),
        "opt_prem_cr": round(opt_prem, 2),
        "prem_notl_pct": round(opt_prem / opt_notl * 100, 3) if opt_notl > 0 else 0,
        "total_notl_cr": round(fut_notl + opt_notl, 2),
        "proj_fut_cr": round(proj_fut, 2),
        "proj_opt_cr": round(proj_opt, 2),
        "proj_fut_rev": round(fut_rev, 2),
        "proj_opt_rev": round(opt_rev, 2),
        "proj_tx_rev": round(tx_rev, 2),
        "proj_total_rev": round(total_rev, 2),
        "proj_annual": round(total_rev * TRADING_DAYS, 0),
        "rev_low": rev_low,
        "rev_high": rev_high,
        "uncertainty_pct": round(combined_unc * 100, 1),
        "confidence": conf,
        "dual_call": dual_call,
        "day_type": day_type,
        "day_multiplier": raw_day_mult,
        "day_description": day_desc,
        "nontx_rev": NONTX_DAILY,
        "top_futures": top_fut,
        "top_options": top_opt,
    }

    # F-08: Push snapshot to Supabase for relay
    if SUPABASE_ANON_KEY:
        try:
            snapshot = {
                "trading_date": capture_time_ist.strftime("%Y-%m-%d"),
                "elapsed_min": elapsed_min,
                "session_closed": elapsed_min >= SESSION_TOTAL,
                "fut_notl_cr": round(fut_notl, 2),
                "opt_notl_cr": round(opt_notl, 2),
                "opt_prem_cr": round(opt_prem, 2),
                "fut_rev_cr": round(fut_rev, 4),
                "opt_rev_cr": round(opt_rev, 4),
                "nontx_rev_cr": NONTX_DAILY,
                "total_rev_cr": round(total_rev, 4),
                "proj_fut_cr": round(proj_fut, 2),
                "proj_opt_cr": round(proj_opt, 2),
                "proj_total_rev": round(total_rev, 4),
                "uncertainty_pct": round(combined_unc * 100, 2),
                "confidence": conf,
                "day_type": day_type,
                "day_multiplier": raw_day_mult,
                "active_futures": len(futures),
                "active_options": len(options),
                "prem_notl_pct": round(opt_prem / opt_notl * 100, 3) if opt_notl > 0 else 0,
                "dual_call": dual_call,
                "data_source": "mcx_api",
                "top_futures": top_fut,
                "top_options": top_opt,
            }
            supabase_upsert("mcx_snapshots", snapshot)
        except Exception:
            pass  # Don't fail the response if Supabase push fails

    return result


class handler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        pass

    def _cors(self):
        origin = self.headers.get("Origin", "")
        hdrs = make_cors_headers(origin)
        for k, v in hdrs.items():
            self.send_header(k, v)
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def send_json(self, data, status=200):
        body = json.dumps(data, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def _health_check(self):
        """Return relay health: last heartbeat + last snapshot age."""
        from datetime import datetime, timezone
        try:
            t = now_ist()
            today = t.strftime("%Y-%m-%d")

            # Try heartbeat table first
            heartbeat = None
            try:
                hb = supabase_read("relay_heartbeat", "?order=heartbeat_at.desc&limit=1")
                if hb:
                    heartbeat = hb[0]
            except Exception:
                pass  # Table may not exist yet

            # Last snapshot (always available)
            snaps = supabase_read("mcx_snapshots", f"?order=captured_at.desc&limit=1")
            snap = snaps[0] if snaps else None

            snap_age_min = None
            if snap and snap.get("captured_at"):
                cap = datetime.fromisoformat(snap["captured_at"].replace("Z", "+00:00"))
                snap_age_min = round((t.astimezone(timezone.utc) - cap.astimezone(timezone.utc)).total_seconds() / 60, 1)

            is_market_hours = (SESSION_START <= t.hour * 60 + t.minute <= SESSION_END) and is_trading_day(t.date())
            alive = snap_age_min is not None and snap_age_min < 30 if is_market_hours else True

            result = {
                "success": True,
                "healthy": alive,
                "checked_at": t.strftime("%Y-%m-%dT%H:%M:%S+05:30"),
                "market_open": is_market_hours,
                "last_snapshot": {
                    "captured_at": snap.get("captured_at") if snap else None,
                    "trading_date": snap.get("trading_date") if snap else None,
                    "total_rev_cr": snap.get("total_rev_cr") if snap else None,
                    "elapsed_min": snap.get("elapsed_min") if snap else None,
                    "age_min": snap_age_min,
                },
            }
            if heartbeat:
                result["heartbeat"] = heartbeat

            self.send_json(result)
        except Exception as e:
            self.send_json({"success": False, "healthy": False, "error": str(e)[:200]})

    def do_GET(self):
        """F-08: Read latest snapshot from Supabase and enrich for frontend.
        ?health=1 — relay health check (heartbeat + snapshot age)."""
        if not SUPABASE_ANON_KEY:
            self.send_json({"success": False, "error": "Supabase not configured"})
            return

        # ── Health check endpoint ─────────────────────────────────────────
        from urllib.parse import urlparse, parse_qs
        qs = parse_qs(urlparse(self.path).query)
        if "health" in qs:
            return self._health_check()

        try:
            today = now_ist().strftime("%Y-%m-%d")
            rows = supabase_read(
                "mcx_snapshots",
                f"?trading_date=eq.{today}&order=elapsed_min.desc&limit=1"
            )
            if rows:
                row = rows[0]
                result = {
                    "success": True,
                    "source": "supabase_cache",
                    "timestamp": row.get("captured_at", ""),
                    **{k: row[k] for k in row if k not in ("id", "captured_at")},
                }
                # Enrich with computed fields the frontend expects
                elapsed = result.get("elapsed_min") or 0
                elapsed_pct = round(elapsed / SESSION_TOTAL * 100, 1) if SESSION_TOTAL > 0 else 0
                result["elapsed_pct"] = elapsed_pct
                result["session_total_min"] = SESSION_TOTAL

                fut_n = result.get("fut_notl_cr") or 0
                opt_n = result.get("opt_notl_cr") or 0
                opt_p = result.get("opt_prem_cr") or 0
                result["total_notl_cr"] = round(fut_n + opt_n, 2)

                # Project full-day if session not closed
                day_type = result.get("day_type") or get_day_type(now_ist())
                result["day_type"] = day_type
                result["day_description"] = DAY_DESCRIPTION.get(day_type, "")
                result["day_multiplier"] = result.get("day_multiplier") or DAY_MULTIPLIER.get(day_type, 1.0)

                if result.get("proj_fut_cr") is None or result.get("proj_opt_cr") is None:
                    pf, po, conf = project_full_day(fut_n, opt_p, elapsed, day_type)
                    result["proj_fut_cr"] = round(pf, 2)
                    result["proj_opt_cr"] = round(po, 2)
                    _, _, _, total_rev = calc_revenue(pf, po)
                    result["proj_total_rev"] = round(total_rev, 4)
                    result["confidence"] = conf

                time_pct = elapsed / SESSION_TOTAL if SESSION_TOTAL > 0 else 0
                dual = result.get("dual_call", False)
                unc = calc_uncertainty(time_pct, day_type, dual)
                result["uncertainty_pct"] = round(unc * 100, 1)
                proj_total = result.get("proj_total_rev") or result.get("total_rev_cr") or 0
                result["rev_low"] = round(proj_total * (1 - unc), 2)
                result["rev_high"] = round(proj_total * (1 + unc), 2)
                result["trading_days"] = TRADING_DAYS

                self.send_json(result)
            else:
                self.send_json({"success": False, "error": "No data for today yet. Run the local relay script."})
        except Exception as e:
            self.send_json({"success": False, "error": f"Supabase read failed: {str(e)[:200]}"})

    def do_POST(self):
        content_len = int(self.headers.get("Content-Length", 0))
        raw_body = self.rfile.read(content_len)

        cookie = ""
        try:
            payload = json.loads(raw_body)
            cookie = payload.get("cookie", "")
        except Exception:
            pass

        # Auto-cookie fallback
        auto_cookie_used = False
        if not cookie:
            cookie = _auto_fetch_cookies(timeout=12)
            if cookie:
                auto_cookie_used = True
            else:
                self.send_json({
                    "success": False,
                    "error": "Auto-cookie failed (MCX likely blocks this server's IP). Use the local relay script or paste a cookie manually.",
                    "error_type": "ip_blocked"  # F-12: error differentiation
                })
                return

        # F-10: Conditional dual-call — only if session is >80% complete
        try:
            raw1 = _fetch_mcx(cookie, timeout=18)
        except urllib.error.HTTPError as e:
            error_type = "cookie_expired" if e.code in (302, 401, 403) else "server_error"
            self.send_json({
                "success": False,
                "error": f"MCX API error {e.code}",
                "error_type": error_type,
                "hint": "Cookie expired — refresh it" if error_type == "cookie_expired" else "MCX server issue"
            })
            return
        except Exception as e:
            self.send_json({
                "success": False,
                "error": f"MCX unreachable: {str(e)[:100]}",
                "error_type": "network_error"
            })
            return

        # F-10: Only do second call late in session when accuracy matters most
        raw2 = None
        capture = now_ist()
        current_min = capture.hour * 60 + capture.minute
        elapsed = max(0, min(current_min - SESSION_START, SESSION_TOTAL))
        time_pct = elapsed / SESSION_TOTAL if SESSION_TOTAL > 0 else 0

        if time_pct > 0.80:
            try:
                time.sleep(2)
                raw2 = _fetch_mcx(cookie, timeout=15)
            except Exception:
                pass

        try:
            processed = process_market_data(raw1, capture, raw_json2=raw2)
            processed["auto_cookie"] = auto_cookie_used
            self.send_json(processed)
        except Exception as e:
            self.send_json({"success": False, "error": str(e)[:200]})
