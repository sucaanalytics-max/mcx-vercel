"""
/api/cron_models — Daily refresh for Models B (ECM) + C (Multi-Factor) + Ensemble

Reads mcx_valuation + mcx_daily_revenue + mcx_share_price,
computes rolling 60-day z-scores, and upserts to mcx_model_signals.

Vercel Cron: runs after cron_valuation (which refreshes Model A first).
"""
from http.server import BaseHTTPRequestHandler
import json, math, urllib.request, urllib.error
from urllib.parse import urlparse, parse_qs

try:
    from api.mcx_config import (
        SUPABASE_URL, SUPABASE_ANON_KEY,
        now_ist, make_cors_headers,
    )
except ImportError:
    from mcx_config import (
        SUPABASE_URL, SUPABASE_ANON_KEY,
        now_ist, make_cors_headers,
    )

import os

CRON_SECRET = os.environ.get("CRON_SECRET", "")
ROLLING_WINDOW = 60

# Factor weights for Model C  (Phase 3 optimised — Vol/InvVol dropped per factor decomposition)
# Within MF composite: Rev 30 / TO 40 → normalised to 3:4 ratio
W_REV  = 3 / 7   # ≈ 0.4286
W_TURN = 4 / 7   # ≈ 0.5714
# Ensemble blend: ECM 30 % + MF 70 %  (expands to Rev 30 % + TO 40 % + ECM 30 %)
W_ECM  = 0.30
W_MF   = 0.70


def sb_get(table, params=""):
    url = f"{SUPABASE_URL}/rest/v1/{table}{params}"
    req = urllib.request.Request(url, headers={
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
    })
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode())


def sb_upsert(table, rows):
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


def fetch_all(table, select, order="trading_date"):
    all_rows, offset = [], 0
    while True:
        rows = sb_get(table, f"?select={select}&order={order}.asc&limit=1000&offset={offset}")
        all_rows.extend(rows)
        if len(rows) < 1000:
            break
        offset += 1000
    return all_rows


def _f(v):
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _zscore(values, idx, window):
    """Compute z-score for values[idx] using rolling window."""
    start = max(0, idx - window + 1)
    w = [v for v in values[start:idx + 1] if v is not None]
    if len(w) < 30:
        return None
    mean = sum(w) / len(w)
    var = sum((x - mean) ** 2 for x in w) / len(w)
    sd = math.sqrt(var) if var > 0 else 0
    if sd == 0:
        return None
    return round((values[idx] - mean) / sd, 3)


def compute_signals(mode="recent"):
    log = []

    # Fetch all required data
    val_rows = fetch_all("mcx_valuation",
                         "trading_date,close_price,fair_value_base")
    log.append(f"Valuation rows: {len(val_rows)}")

    rev_rows = fetch_all("mcx_daily_revenue",
                         "trading_date,total_rev_cr,fut_notl_cr,opt_prem_cr")
    log.append(f"Revenue rows: {len(rev_rows)}")

    price_rows = fetch_all("mcx_share_price",
                           "trading_date,close,high,low,volume")
    log.append(f"Price rows: {len(price_rows)}")

    # Build lookups
    rev_map = {}
    for r in rev_rows:
        rev_map[r["trading_date"]] = r

    price_map = {}
    for p in price_rows:
        price_map[p["trading_date"]] = p

    # Build aligned series
    series = []
    for v in val_rows:
        dt = v["trading_date"]
        cp = _f(v.get("close_price"))
        fv = _f(v.get("fair_value_base"))
        r = rev_map.get(dt, {})
        p = price_map.get(dt, {})

        if cp is None or fv is None or fv <= 0:
            continue

        rev = _f(r.get("total_rev_cr"))
        fut_notl = _f(r.get("fut_notl_cr"))
        opt_prem = _f(r.get("opt_prem_cr"))
        turnover = (fut_notl or 0) + (opt_prem or 0) if fut_notl is not None else None
        volume = _f(p.get("volume"))
        close_p = _f(p.get("close"))
        high_p = _f(p.get("high"))
        low_p = _f(p.get("low"))
        ivol = round((high_p - low_p) / close_p * 100, 3) if (close_p and close_p > 0 and high_p and low_p) else None

        spread_pct = round((cp - fv) / fv * 100, 2)

        series.append({
            "date": dt, "price": cp, "fv": fv,
            "spread_pct": spread_pct, "rev": rev,
            "turnover": turnover, "volume": volume, "ivol": ivol,
        })

    log.append(f"Aligned series: {len(series)}")

    # Extract arrays for z-score computation
    spreads = [s["spread_pct"] for s in series]
    revs = [s["rev"] for s in series]
    turns = [s["turnover"] for s in series]
    vols = [s["volume"] for s in series]
    ivols = [s["ivol"] for s in series]

    # Compute signals for each row
    results = []
    for i, s in enumerate(series):
        ecm_z = _zscore(spreads, i, ROLLING_WINDOW)
        rev_z = _zscore(revs, i, ROLLING_WINDOW)
        turn_z = _zscore(turns, i, ROLLING_WINDOW)
        vol_z = _zscore(vols, i, ROLLING_WINDOW)
        # Invert volatility z (high intraday vol = bearish for stock)
        raw_ivol_z = _zscore(ivols, i, ROLLING_WINDOW)
        ivol_z = round(-raw_ivol_z, 3) if raw_ivol_z is not None else None

        # ECM signal
        ecm_signal = "NO_DATA"
        if ecm_z is not None:
            if ecm_z < -1.5:
                ecm_signal = "STRONG_REVERT_UP"
            elif ecm_z < -0.5:
                ecm_signal = "MILD_REVERT_UP"
            elif ecm_z <= 0.5:
                ecm_signal = "NEUTRAL"
            elif ecm_z <= 1.5:
                ecm_signal = "MILD_EXTEND_DOWN"
            else:
                ecm_signal = "STRONG_EXTEND_DOWN"

        # Half-life estimate
        half_life = round(60.0 / (1.0 + abs(ecm_z)), 1) if ecm_z is not None else None

        # MF composite (Rev + Turnover only — Vol/InvVol kept for display)
        mf_composite = None
        mf_signal = "NO_DATA"
        if all(x is not None for x in [rev_z, turn_z]):
            mf_composite = round(rev_z * W_REV + turn_z * W_TURN, 3)
            if mf_composite > 1.5:
                mf_signal = "STRONG_BUY"
            elif mf_composite > 0.5:
                mf_signal = "BUY"
            elif mf_composite > -0.5:
                mf_signal = "NEUTRAL"
            elif mf_composite > -1.5:
                mf_signal = "SELL"
            else:
                mf_signal = "STRONG_SELL"

        # Ensemble
        ens_score = None
        ens_signal = "NO_DATA"
        if ecm_z is not None and mf_composite is not None:
            ens_score = round((-ecm_z * W_ECM) + (mf_composite * W_MF), 3)
            if ens_score > 1.5:
                ens_signal = "STRONG_BUY"
            elif ens_score > 0.5:
                ens_signal = "BUY"
            elif ens_score > -0.5:
                ens_signal = "NEUTRAL"
            elif ens_score > -1.5:
                ens_signal = "SELL"
            else:
                ens_signal = "STRONG_SELL"

        # ── Continuous positioning (3D-2) ────────────────────────────
        # Map ensemble_score → position_score [-1, +1] via clipped linear
        # (replaced tanh(x/2) which compressed 76% of positions below 0.5x)
        pos_score = round(max(-1.0, min(1.0, ens_score / 2.0)), 4) if ens_score is not None else None
        conviction = round(abs(pos_score), 4) if pos_score is not None else None

        results.append({
            "trading_date": s["date"],
            "close_price": s["price"],
            "fair_value_base": s["fv"],
            "ecm_spread": round(s["price"] - s["fv"], 2),
            "ecm_spread_pct": s["spread_pct"],
            "ecm_spread_zscore": ecm_z,
            "ecm_half_life_days": half_life,
            "ecm_signal": ecm_signal,
            "mf_revenue_z": rev_z,
            "mf_turnover_z": turn_z,
            "mf_volume_z": vol_z,
            "mf_volatility_z": ivol_z,
            "mf_composite_z": mf_composite,
            "mf_signal": mf_signal,
            "ensemble_score": ens_score,
            "ensemble_signal": ens_signal,
            "position_score": pos_score,
            "conviction": conviction,
            "signal_momentum": ens_score,
        })

    # ── Compute velocity & smoothed conviction (sequential pass) ────
    for j in range(len(results)):
        cur_ps = results[j].get("position_score")
        prev_ps = results[j - 1].get("position_score") if j > 0 else None

        # 1-day change in position_score
        if cur_ps is not None and prev_ps is not None:
            results[j]["position_velocity"] = round(cur_ps - prev_ps, 4)
        else:
            results[j]["position_velocity"] = None

        # 2-day moving average of conviction
        cur_conv = results[j].get("conviction")
        prev_conv = results[j - 1].get("conviction") if j > 0 else None
        if cur_conv is not None and prev_conv is not None:
            results[j]["conviction_2d_ma"] = round((cur_conv + prev_conv) / 2.0, 4)
        else:
            results[j]["conviction_2d_ma"] = cur_conv  # fallback to current

    # Filter by mode
    if mode == "latest":
        results = results[-1:] if results else []
    elif mode == "recent":
        results = results[-30:] if results else []

    log.append(f"Upserting {len(results)} rows ({mode})")
    errors = sb_upsert("mcx_model_signals", results)
    if errors:
        log.extend([f"Error: {e}" for e in errors])

    latest = results[-1] if results else {}
    return {
        "success": len(errors) == 0,
        "mode": mode,
        "rows_upserted": len(results),
        "latest": {
            "date": latest.get("trading_date"),
            "ecm_signal": latest.get("ecm_signal"),
            "mf_signal": latest.get("mf_signal"),
            "ensemble_signal": latest.get("ensemble_signal"),
            "ensemble_score": latest.get("ensemble_score"),
            "position_score": latest.get("position_score"),
            "conviction": latest.get("conviction"),
        },
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

        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)
        mode = qs.get("mode", ["recent"])[0]
        if mode not in ("recent", "latest", "backfill"):
            mode = "recent"

        try:
            result = compute_signals(mode=mode)
            result["as_of"] = now_ist().strftime("%Y-%m-%d %H:%M IST")
            self.send_json(result, 200 if result.get("success") else 500)
        except Exception as e:
            self.send_json({"success": False, "error": str(e)[:200]}, 500)
