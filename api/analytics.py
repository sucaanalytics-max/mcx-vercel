"""
/api/analytics — Factor Correlation, Rolling IC, Regime Detection, Performance Metrics

Returns:
  - factor_correlation: 4×4 Pearson matrix (ecm_z, rev_z, turn_z, position_score)
  - rolling_ic: Information Coefficient (signal vs forward 5d return)
  - regime: current bull/bear/neutral + volatility regime
  - rolling_metrics: 60-day Sharpe, win rate, profit factor
  - factor_decomposition: today's ensemble score broken into factor contributions

Data: reads mcx_model_signals + mcx_share_price.
"""
from http.server import BaseHTTPRequestHandler
import json, math

try:
    from api.mcx_config import (
        SUPABASE_URL, SUPABASE_ANON_KEY, supabase_read,
        now_ist, make_cors_headers,
    )
except ImportError:
    from mcx_config import (
        SUPABASE_URL, SUPABASE_ANON_KEY, supabase_read,
        now_ist, make_cors_headers,
    )


def _f(v):
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _fetch_all(table, select, limit=2000):
    """Paginated fetch from Supabase."""
    all_rows, offset = [], 0
    while True:
        rows = supabase_read(
            table,
            f"?select={select}&order=trading_date.asc&limit=1000&offset={offset}"
        )
        all_rows.extend(rows)
        if len(rows) < 1000 or len(all_rows) >= limit:
            break
        offset += 1000
    return all_rows


def _pearson(xs, ys):
    """Pearson correlation for two lists (skipping None pairs)."""
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


def generate_analytics():
    ist_now = now_ist()

    # Fetch model signals (all history)
    signals = _fetch_all(
        "mcx_model_signals",
        "trading_date,ecm_spread_zscore,mf_revenue_z,mf_turnover_z,"
        "mf_composite_z,ensemble_score,ensemble_signal,position_score,conviction",
        limit=2000
    )
    if not signals:
        return {"success": False, "error": "No model signals available."}

    # Fetch share prices for forward returns
    prices = _fetch_all(
        "mcx_share_price",
        "trading_date,close",
        limit=2000
    )
    price_map = {}
    price_dates = []
    for p in prices:
        c = _f(p.get("close"))
        if c is not None and c > 0:
            price_map[p["trading_date"]] = c
            price_dates.append(p["trading_date"])

    # ── 1. Factor Correlation Matrix (last 120 days) ──
    recent = signals[-120:]
    ecm_zs = [_f(r.get("ecm_spread_zscore")) for r in recent]
    rev_zs = [_f(r.get("mf_revenue_z")) for r in recent]
    turn_zs = [_f(r.get("mf_turnover_z")) for r in recent]
    pos_scores = [_f(r.get("position_score")) for r in recent]

    factors = [ecm_zs, rev_zs, turn_zs, pos_scores]
    factor_names = ["ecm_z", "rev_z", "turn_z", "position_score"]
    corr_matrix = []
    for i in range(4):
        row = []
        for j in range(4):
            row.append(_pearson(factors[i], factors[j]))
        corr_matrix.append(row)

    # ── 2. Rolling IC (60-day window, forward 5d return) ──
    # Build forward 5d returns for each signal date
    sig_dates = [s["trading_date"] for s in signals]
    fwd_5d = {}
    for dt in sig_dates:
        if dt not in price_map:
            continue
        # Find price 5 trading days ahead
        try:
            idx = price_dates.index(dt)
        except ValueError:
            continue
        if idx + 5 < len(price_dates):
            p0 = price_map[dt]
            p5 = price_map[price_dates[idx + 5]]
            fwd_5d[dt] = (p5 - p0) / p0
        else:
            fwd_5d[dt] = None

    ic_history = []
    ic_window = 60
    for i in range(ic_window, len(signals)):
        window = signals[i - ic_window:i]
        ens_vals = []
        ret_vals = []
        for s in window:
            dt = s["trading_date"]
            ens = _f(s.get("ensemble_score"))
            ret = fwd_5d.get(dt)
            if ens is not None and ret is not None:
                ens_vals.append(ens)
                ret_vals.append(ret)

        if len(ens_vals) >= 20:
            ic = _pearson(ens_vals, ret_vals)
        else:
            ic = None

        ic_history.append({
            "date": signals[i]["trading_date"],
            "ensemble_ic": ic,
        })

    # Keep last 60 IC entries for chart
    ic_history = ic_history[-60:]

    # ── 3. Regime Detection ──
    pos_all = [_f(s.get("position_score")) for s in signals if _f(s.get("position_score")) is not None]
    bull_days = sum(1 for p in pos_all if p > 0.25)
    bear_days = sum(1 for p in pos_all if p < -0.25)
    neutral_days = len(pos_all) - bull_days - bear_days

    # Current regime streak
    current_regime = "NEUTRAL"
    regime_duration = 0
    if pos_all:
        last = pos_all[-1]
        current_regime = "BULL" if last > 0.25 else "BEAR" if last < -0.25 else "NEUTRAL"
        for p in reversed(pos_all):
            r = "BULL" if p > 0.25 else "BEAR" if p < -0.25 else "NEUTRAL"
            if r == current_regime:
                regime_duration += 1
            else:
                break

    # Volatility regime (rolling 20-day return std)
    recent_prices = prices[-25:]
    if len(recent_prices) >= 21:
        rets = []
        for k in range(1, len(recent_prices)):
            c0 = _f(recent_prices[k - 1].get("close"))
            c1 = _f(recent_prices[k].get("close"))
            if c0 and c1 and c0 > 0:
                rets.append((c1 - c0) / c0)
        if rets:
            vol = math.sqrt(sum(r ** 2 for r in rets) / len(rets)) * math.sqrt(252) * 100
        else:
            vol = None
    else:
        vol = None

    vol_regime = "UNKNOWN"
    if vol is not None:
        if vol > 35:
            vol_regime = "HIGH"
        elif vol > 20:
            vol_regime = "MODERATE"
        else:
            vol_regime = "LOW"

    # ── 4. Rolling Performance Metrics (60-day) ──
    rolling_metrics = []
    for i in range(60, len(signals)):
        window = signals[i - 60:i]
        daily_rets = []
        for s in window:
            dt = s["trading_date"]
            ens = _f(s.get("ensemble_score"))
            if ens is None or dt not in price_map:
                continue
            try:
                idx = price_dates.index(dt)
            except ValueError:
                continue
            if idx + 1 < len(price_dates):
                p0 = price_map[dt]
                p1 = price_map[price_dates[idx + 1]]
                daily_ret = (p1 - p0) / p0
                # Signal-weighted return: positive ensemble → long, negative → short
                signal_dir = 1 if ens > 0 else -1 if ens < 0 else 0
                daily_rets.append(daily_ret * signal_dir)

        if len(daily_rets) >= 30:
            mean_r = sum(daily_rets) / len(daily_rets)
            std_r = math.sqrt(sum((r - mean_r) ** 2 for r in daily_rets) / len(daily_rets))
            wins = [r for r in daily_rets if r > 0]
            losses = [r for r in daily_rets if r < 0]
            win_rate = len(wins) / len(daily_rets) if daily_rets else 0
            avg_win = sum(wins) / len(wins) if wins else 0
            avg_loss = sum(losses) / len(losses) if losses else 0
            sharpe = (mean_r / std_r * math.sqrt(252)) if std_r > 0 else 0
            pf = (sum(wins) / abs(sum(losses))) if losses and sum(losses) != 0 else 0

            rolling_metrics.append({
                "date": signals[i]["trading_date"],
                "sharpe_ratio": round(sharpe, 3),
                "win_rate": round(win_rate, 3),
                "avg_win": round(avg_win, 5),
                "avg_loss": round(avg_loss, 5),
                "profit_factor": round(pf, 3),
            })

    rolling_metrics = rolling_metrics[-60:]

    # ── 5. Factor Decomposition (latest day) ──
    latest = signals[-1]
    ecm_z_val = _f(latest.get("ecm_spread_zscore")) or 0
    rev_z_val = _f(latest.get("mf_revenue_z")) or 0
    turn_z_val = _f(latest.get("mf_turnover_z")) or 0
    ens_val = _f(latest.get("ensemble_score")) or 0

    decomposition = {
        "date": latest["trading_date"],
        "ensemble_score": round(ens_val, 3),
        "ecm_contribution": round(-ecm_z_val * 0.30, 3),
        "mf_contribution": round((rev_z_val * 3 / 7 + turn_z_val * 4 / 7) * 0.70, 3),
        "mf_revenue_part": round(rev_z_val * 3 / 7 * 0.70, 3),
        "mf_turnover_part": round(turn_z_val * 4 / 7 * 0.70, 3),
    }

    return {
        "success": True,
        "as_of": ist_now.strftime("%Y-%m-%d %H:%M IST"),
        "factor_correlation": {
            "labels": factor_names,
            "matrix": corr_matrix,
            "window": min(120, len(recent)),
        },
        "rolling_ic": ic_history,
        "regime": {
            "current": current_regime,
            "duration_days": regime_duration,
            "bull_days": bull_days,
            "neutral_days": neutral_days,
            "bear_days": bear_days,
            "total_days": len(pos_all),
            "volatility": {
                "annualized_pct": round(vol, 1) if vol else None,
                "regime": vol_regime,
            },
        },
        "rolling_metrics": rolling_metrics,
        "factor_decomposition": decomposition,
        "data_quality": {
            "signal_rows": len(signals),
            "price_rows": len(prices),
            "ic_points": len(ic_history),
            "metric_points": len(rolling_metrics),
        },
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
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
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

    def do_GET(self):
        try:
            result = generate_analytics()
            self.send_json(result)
        except Exception as e:
            self.send_json({"success": False, "error": str(e)[:200]}, 500)
