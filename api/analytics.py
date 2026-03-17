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

from lib.mcx_config import (
    supabase_read_all, now_ist, make_cors_headers,
    safe_float as _f, pearson as _pearson,
)


def generate_analytics():
    ist_now = now_ist()

    # Fetch model signals (all history)
    signals = supabase_read_all(
        "mcx_model_signals",
        "?select=trading_date,ecm_spread_zscore,mf_revenue_z,mf_turnover_z,"
        "mf_composite_z,ensemble_score,ensemble_signal,position_score,conviction"
        "&order=trading_date.asc",
        max_rows=2000,
    )
    if not signals:
        return {"success": False, "error": "No model signals available."}

    # Fetch share prices for forward returns
    prices = supabase_read_all(
        "mcx_share_price",
        "?select=trading_date,close&order=trading_date.asc",
        max_rows=2000,
    )
    price_map = {}
    price_dates = []
    price_date_idx = {}  # O(1) lookup replacing O(n) list.index()
    for p in prices:
        c = _f(p.get("close"))
        if c is not None and c > 0:
            price_date_idx[p["trading_date"]] = len(price_dates)
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
        if dt not in price_date_idx:
            continue
        idx = price_date_idx[dt]
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
            if ens is None or dt not in price_date_idx:
                continue
            idx = price_date_idx[dt]
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

    # ── 6. HMM-like Regime Detection (3F-1 simplified) ──
    # Pure-Python regime detection using position score + volatility
    # 3 states: BULL (positive drift), BEAR (negative drift), TRANSITION (high vol)
    regime_history = []
    if len(pos_all) >= 20:
        for i in range(20, len(pos_all)):
            window = pos_all[max(0, i - 20):i + 1]
            avg_ps = sum(window) / len(window)
            ps_std = math.sqrt(sum((p - avg_ps) ** 2 for p in window) / len(window))

            if ps_std > 0.3:
                state = "TRANSITION"
                confidence = min(1.0, ps_std / 0.5)
            elif avg_ps > 0.15:
                state = "BULL"
                confidence = min(1.0, avg_ps / 0.5)
            elif avg_ps < -0.15:
                state = "BEAR"
                confidence = min(1.0, abs(avg_ps) / 0.5)
            else:
                state = "NEUTRAL"
                confidence = 1.0 - abs(avg_ps) / 0.15

            # Only keep last 60 entries
            if i >= len(pos_all) - 60:
                sig_idx = len(signals) - len(pos_all) + i
                if 0 <= sig_idx < len(signals):
                    regime_history.append({
                        "date": signals[sig_idx]["trading_date"],
                        "state": state,
                        "confidence": round(confidence, 3),
                        "avg_position": round(avg_ps, 4),
                        "position_vol": round(ps_std, 4),
                    })

    # Current regime state
    hmm_current = regime_history[-1] if regime_history else {
        "state": "UNKNOWN", "confidence": 0, "avg_position": 0, "position_vol": 0
    }

    # ── 7. Factor Weight Sensitivity (3F-2 simplified) ──
    # Test alternative ECM/MF weight blends against forward returns
    weight_sensitivity = []
    if fwd_5d and len(signals) > 60:
        for ecm_w in [0.1, 0.2, 0.3, 0.4, 0.5]:
            mf_w = 1.0 - ecm_w
            # Compute alternative ensemble scores
            alt_scores = []
            fwd_rets = []
            for s in signals:
                dt = s["trading_date"]
                ecm_z = _f(s.get("ecm_spread_zscore"))
                comp_z = _f(s.get("mf_composite_z"))
                ret = fwd_5d.get(dt)
                if ecm_z is not None and comp_z is not None and ret is not None:
                    alt_ens = (-ecm_z * ecm_w) + (comp_z * mf_w)
                    alt_scores.append(alt_ens)
                    fwd_rets.append(ret)

            if len(alt_scores) >= 30:
                ic = _pearson(alt_scores, fwd_rets)
                # Hit rate: sign(signal) matches sign(return)
                hits = sum(1 for a, r in zip(alt_scores, fwd_rets)
                           if (a > 0 and r > 0) or (a < 0 and r < 0))
                hit_rate = hits / len(alt_scores) if alt_scores else 0

                weight_sensitivity.append({
                    "ecm_weight": ecm_w,
                    "mf_weight": round(mf_w, 2),
                    "ic": ic,
                    "hit_rate": round(hit_rate, 3),
                    "is_current": abs(ecm_w - 0.3) < 0.01,
                })

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
        "hmm_regime": {
            "current": hmm_current,
            "history": regime_history,
        },
        "rolling_metrics": rolling_metrics,
        "factor_decomposition": decomposition,
        "weight_sensitivity": weight_sensitivity,
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
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(self.path).query)
            section = qs.get("section", [None])[0]

            if section == "hourly_accuracy":
                from lib.hourly_analysis import generate_hourly_accuracy
                days = int(qs.get("days", ["90"])[0])
                result = generate_hourly_accuracy(lookback_days=min(days, 180))
            else:
                result = generate_analytics()

            self.send_json(result)
        except Exception as e:
            self.send_json({"success": False, "error": str(e)[:200]}, 500)
