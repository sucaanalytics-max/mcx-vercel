"""
/api/cron_valuation — Daily EPS-Path valuation refresh (Vercel Cron)

Runs as a Vercel serverless function on a cron schedule.
Computes today's (and last 30 days') valuations and upserts to mcx_valuation.

Secured by CRON_SECRET — Vercel cron sends this automatically.
Can also be triggered manually via GET with ?secret=<CRON_SECRET>.

Chain: 45DMA F&O Rev → Annualized Rev → PAT (55%) → EPS → Fair Value via P/E bands
"""
from http.server import BaseHTTPRequestHandler
import json, math, urllib.request, urllib.error
from urllib.parse import urlparse, parse_qs

try:
    from api.mcx_config import (
        TRADING_DAYS, PAT_MARGIN, NON_FO_REV_ANNUAL_CR, DILUTED_SHARES_CR,
        PE_MEAN_DEFAULT, PE_SD_DEFAULT,
        SUPABASE_URL, SUPABASE_ANON_KEY,
        now_ist, make_cors_headers,
    )
except ImportError:
    from mcx_config import (
        TRADING_DAYS, PAT_MARGIN, NON_FO_REV_ANNUAL_CR, DILUTED_SHARES_CR,
        PE_MEAN_DEFAULT, PE_SD_DEFAULT,
        SUPABASE_URL, SUPABASE_ANON_KEY,
        now_ist, make_cors_headers,
    )

import os

CRON_SECRET = os.environ.get("CRON_SECRET", "")
MA_WINDOW = 45
PE_LOOKBACK = 252   # trailing trading days for dynamic PE computation (1 year)


# ─── Supabase helpers (self-contained, no external deps) ─────────────────

def sb_get(table, params=""):
    url = f"{SUPABASE_URL}/rest/v1/{table}{params}"
    req = urllib.request.Request(url, headers={
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
    })
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode())


def sb_upsert(table, rows):
    """Batch upsert rows to Supabase (chunks of 50)."""
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


def fetch_all_paginated(table, select, order_col="trading_date"):
    all_rows = []
    offset = 0
    while True:
        params = f"?select={select}&order={order_col}.asc&limit=1000&offset={offset}"
        rows = sb_get(table, params)
        all_rows.extend(rows)
        if len(rows) < 1000:
            break
        offset += 1000
    return all_rows


# ─── Valuation computation (same logic as valuation_refresh.py) ──────────

def compute_valuations(rev_rows, price_rows, pe_mean=None, pe_sd=None):
    price_map = {}
    for r in price_rows:
        if r.get("close"):
            price_map[r["trading_date"]] = float(r["close"])

    rev_list = []
    for r in rev_rows:
        if r.get("total_rev_cr") is not None:
            rev_list.append({"date": r["trading_date"], "rev": float(r["total_rev_cr"])})

    if len(rev_list) < MA_WINDOW:
        return [], f"Only {len(rev_list)} rev days, need {MA_WINDOW}"

    # Pass 1: EPS series for dynamic P/E
    eps_series = []
    for i in range(MA_WINDOW - 1, len(rev_list)):
        window_revs = [rev_list[j]["rev"] for j in range(i - MA_WINDOW + 1, i + 1)]
        ma45 = sum(window_revs) / MA_WINDOW
        annual_total = (ma45 * TRADING_DAYS) + NON_FO_REV_ANNUAL_CR
        pat = annual_total * PAT_MARGIN
        eps = pat / DILUTED_SHARES_CR
        date_str = rev_list[i]["date"]
        price = price_map.get(date_str)
        eps_series.append({
            "date": date_str,
            "daily_rev": rev_list[i]["rev"],
            "ma45": ma45, "annual_total": annual_total,
            "pat": pat, "eps": eps, "price": price,
            "implied_pe": round(price / eps, 2) if (price and eps > 0) else None,
        })

    # Dynamic P/E — trailing window + robust quantile bands
    # Uses trailing PE_LOOKBACK observations to avoid regime-shift contamination
    # (e.g. MCX PE was 10x in 2022, 35x in 2025 — full-history mean is misleading)
    if pe_mean is None or pe_sd is None:
        all_pes = [e["implied_pe"] for e in eps_series if e["implied_pe"] is not None]
        # Use only trailing PE_LOOKBACK observations
        recent_pes = all_pes[-PE_LOOKBACK:] if len(all_pes) > PE_LOOKBACK else all_pes
        if len(recent_pes) >= 30:
            # Robust: median + scaled MAD (resistant to outliers and skew)
            recent_sorted = sorted(recent_pes)
            n = len(recent_sorted)
            pe_median = recent_sorted[n // 2] if n % 2 else (recent_sorted[n // 2 - 1] + recent_sorted[n // 2]) / 2
            abs_devs = sorted(abs(p - pe_median) for p in recent_sorted)
            mad_raw = abs_devs[len(abs_devs) // 2] if len(abs_devs) % 2 else (abs_devs[len(abs_devs) // 2 - 1] + abs_devs[len(abs_devs) // 2]) / 2
            mad_scaled = mad_raw * 1.4826  # scale factor for normal-equivalent SD

            pe_mean = round(pe_median, 2)
            pe_sd = round(mad_scaled, 2) if mad_scaled > 0.5 else round(math.sqrt(sum((p - pe_median) ** 2 for p in recent_pes) / n), 2)
        else:
            pe_mean = PE_MEAN_DEFAULT
            pe_sd = PE_SD_DEFAULT

    pe_bear = max(pe_mean - pe_sd, 5.0)
    pe_bull = pe_mean + pe_sd

    # Pass 2: valuation rows
    valuations = []
    for e in eps_series:
        eps = e["eps"]
        price = e["price"]
        fv_bear = round(eps * pe_bear, 2)
        fv_base = round(eps * pe_mean, 2)
        fv_bull = round(eps * pe_bull, 2)

        signal = "NO_PRICE"
        if price and eps > 0:
            if price < fv_bear:
                signal = "DEEP_VALUE"
            elif price < fv_base * 0.95:
                signal = "UNDERVALUED"
            elif price <= fv_base * 1.05:
                signal = "FAIR"
            elif price <= fv_bull:
                signal = "OVERVALUED"
            else:
                signal = "STRETCHED"

        valuations.append({
            "trading_date": e["date"],
            "daily_rev_cr": round(e["daily_rev"], 2),
            "ma45_rev_cr": round(e["ma45"], 2),
            "annualized_rev_cr": round(e["annual_total"], 2),
            "pat_cr": round(e["pat"], 2),
            "eps": round(eps, 2),
            "close_price": price,
            "implied_pe": e["implied_pe"],
            "fair_value_bear": fv_bear,
            "fair_value_base": fv_base,
            "fair_value_bull": fv_bull,
            "signal": signal,
            "pe_mean_used": round(pe_mean, 2),
            "pe_sd_used": round(pe_sd, 2),
        })

    return valuations, None


# ─── Main refresh logic ──────────────────────────────────────────────────

def run_refresh(mode="recent"):
    """
    mode: "recent" (last 30 days), "latest" (today only), "backfill" (all)
    """
    log = []

    rev_rows = fetch_all_paginated("mcx_daily_revenue", "trading_date,total_rev_cr,source")
    log.append(f"Revenue rows: {len(rev_rows)}")

    price_rows = fetch_all_paginated("mcx_share_price", "trading_date,close")
    log.append(f"Price rows: {len(price_rows)}")

    valuations, err = compute_valuations(rev_rows, price_rows)
    if err:
        return {"success": False, "error": err, "log": log}

    log.append(f"Total valuations computed: {len(valuations)}")

    # Filter by mode
    if mode == "latest":
        valuations = valuations[-1:]
    elif mode == "recent":
        valuations = valuations[-30:]
    # "backfill" = all

    log.append(f"Upserting {len(valuations)} rows ({mode} mode)")
    errors = sb_upsert("mcx_valuation", valuations)
    if errors:
        log.extend([f"Upsert error: {e}" for e in errors])

    latest = valuations[-1] if valuations else {}
    return {
        "success": len(errors) == 0,
        "mode": mode,
        "rows_upserted": len(valuations),
        "latest": {
            "date": latest.get("trading_date"),
            "eps": latest.get("eps"),
            "fair_base": latest.get("fair_value_base"),
            "signal": latest.get("signal"),
            "price": latest.get("close_price"),
            "pe_mean": latest.get("pe_mean_used"),
            "pe_sd": latest.get("pe_sd_used"),
        },
        "log": log,
        "errors": errors,
    }


# ─── Vercel handler ──────────────────────────────────────────────────────

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
        """Verify cron secret — Vercel sends via Authorization header or query param."""
        # Vercel cron sends: Authorization: Bearer <CRON_SECRET>
        auth_header = self.headers.get("Authorization", "")
        if auth_header.startswith("Bearer ") and auth_header[7:] == CRON_SECRET:
            return True
        # Manual trigger via ?secret=...
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)
        if qs.get("secret", [None])[0] == CRON_SECRET:
            return True
        # If no CRON_SECRET configured, allow (dev mode)
        if not CRON_SECRET:
            return True
        return False

    def do_GET(self):
        if not self._check_auth():
            self.send_json({"success": False, "error": "Unauthorized"}, 401)
            return

        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)
        mode = qs.get("mode", ["recent"])[0]
        if mode not in ("recent", "latest", "backfill"):
            mode = "recent"

        try:
            result = run_refresh(mode=mode)
            status = 200 if result.get("success") else 500
            result["as_of"] = now_ist().strftime("%Y-%m-%d %H:%M IST")
            self.send_json(result, status)
        except Exception as e:
            self.send_json({
                "success": False,
                "error": str(e)[:200],
                "as_of": now_ist().strftime("%Y-%m-%d %H:%M IST"),
            }, 500)
