"""
/api/backtest — Signal Accuracy, Cumulative PnL, Sharpe, Drawdown

Returns:
  - signal_accuracy: hit rate by forward window (5d, 10d, 20d)
  - signal_attribution: avg return & hit rate per signal bucket
  - cumulative_pnl: signal-following vs buy-and-hold
  - statistics: Sharpe, Sortino, max drawdown, win rate, profit factor
  - monthly_returns: heatmap data
  - drawdown_series: for chart

Data: reads mcx_model_signals + mcx_share_price.
"""
from http.server import BaseHTTPRequestHandler
import json, math
from datetime import timedelta
from urllib.parse import urlparse, parse_qs

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


PERIOD_DAYS = {"all": None, "3y": 1095, "1y": 365, "6m": 183, "3m": 91}


def generate_backtest(period="all"):
    ist_now = now_ist()

    # Compute cutoff date for period filtering
    cutoff = None
    if period in PERIOD_DAYS and PERIOD_DAYS[period] is not None:
        cutoff = (ist_now - timedelta(days=PERIOD_DAYS[period])).strftime("%Y-%m-%d")

    # Fetch all signals
    signals = _fetch_all(
        "mcx_model_signals",
        "trading_date,ensemble_score,ensemble_signal,position_score",
        limit=2000
    )
    # Fetch all prices
    prices = _fetch_all(
        "mcx_share_price",
        "trading_date,close",
        limit=2000
    )

    # Apply period filter
    if cutoff:
        signals = [s for s in signals if s["trading_date"] >= cutoff]
        prices = [p for p in prices if p["trading_date"] >= cutoff]

    if not signals or not prices:
        return {"success": False, "error": "Insufficient data for backtest."}

    # Build price index
    price_map = {}
    price_dates = []
    for p in prices:
        c = _f(p.get("close"))
        if c is not None and c > 0:
            price_map[p["trading_date"]] = c
            price_dates.append(p["trading_date"])

    # Compute forward returns for each signal date
    fwd_returns = {}  # {date: {5: ret, 10: ret, 20: ret}}
    for dt in [s["trading_date"] for s in signals]:
        if dt not in price_map:
            continue
        try:
            idx = price_dates.index(dt)
        except ValueError:
            continue
        p0 = price_map[dt]
        entry = {}
        for days in [5, 10, 20]:
            if idx + days < len(price_dates):
                pf = price_map[price_dates[idx + days]]
                entry[days] = (pf - p0) / p0
        fwd_returns[dt] = entry

    # ── 1. Signal Accuracy by Forward Window ──
    accuracy = {}
    for window in [5, 10, 20]:
        total, hits = 0, 0
        hit_rets, miss_rets = [], []
        for s in signals:
            dt = s["trading_date"]
            sig = s.get("ensemble_signal")
            ret = fwd_returns.get(dt, {}).get(window)
            if ret is None or sig in (None, "NO_DATA", "NEUTRAL"):
                continue
            total += 1
            is_buy = sig in ("BUY", "STRONG_BUY")
            is_sell = sig in ("SELL", "STRONG_SELL")
            hit = (is_buy and ret > 0) or (is_sell and ret < 0)
            if hit:
                hits += 1
                hit_rets.append(ret if is_buy else -ret)
            else:
                miss_rets.append(ret if is_buy else -ret)

        accuracy[f"{window}d"] = {
            "total_signals": total,
            "hits": hits,
            "hit_rate": round(hits / total, 3) if total > 0 else 0,
            "avg_return_on_hit": round(sum(hit_rets) / len(hit_rets), 5) if hit_rets else 0,
            "avg_return_on_miss": round(sum(miss_rets) / len(miss_rets), 5) if miss_rets else 0,
        }

    # ── 2. Signal Attribution by Bucket ──
    buckets = {"STRONG_BUY": [], "BUY": [], "NEUTRAL": [], "SELL": [], "STRONG_SELL": []}
    for s in signals:
        dt = s["trading_date"]
        sig = s.get("ensemble_signal")
        ret5 = fwd_returns.get(dt, {}).get(5)
        if ret5 is not None and sig in buckets:
            buckets[sig].append(ret5)

    attribution = {}
    for sig, rets in buckets.items():
        if not rets:
            attribution[sig] = {"count": 0, "avg_5d_return": 0, "hit_rate": 0, "win_loss_ratio": 0}
            continue
        is_directional = sig in ("BUY", "STRONG_BUY", "SELL", "STRONG_SELL")
        if sig in ("SELL", "STRONG_SELL"):
            # For sell signals, a "hit" is negative return
            hits = sum(1 for r in rets if r < 0)
            wins = [-r for r in rets if r < 0]
            losses = [r for r in rets if r >= 0]
        elif sig in ("BUY", "STRONG_BUY"):
            hits = sum(1 for r in rets if r > 0)
            wins = [r for r in rets if r > 0]
            losses = [-r for r in rets if r <= 0]
        else:
            hits = sum(1 for r in rets if r > 0)
            wins = [r for r in rets if r > 0]
            losses = [-r for r in rets if r <= 0]

        avg_win = sum(wins) / len(wins) if wins else 0
        avg_loss = sum(losses) / len(losses) if losses else 0.0001
        wl = (avg_win / avg_loss) if avg_loss > 0 else 0

        attribution[sig] = {
            "count": len(rets),
            "avg_5d_return": round(sum(rets) / len(rets), 5),
            "hit_rate": round(hits / len(rets), 3),
            "win_loss_ratio": round(wl, 2),
        }

    # ── 3. Cumulative PnL (signal-following vs buy-and-hold) ──
    cum_pnl = []
    signal_cum = 0.0
    bh_cum = 0.0
    first_price = None
    for s in signals:
        dt = s["trading_date"]
        if dt not in price_map:
            continue
        price = price_map[dt]
        if first_price is None:
            first_price = price

        # Buy and hold cumulative return
        bh_cum = (price - first_price) / first_price * 100

        # Signal-following: next-day return × signal direction
        ens = _f(s.get("ensemble_score"))
        try:
            idx = price_dates.index(dt)
        except ValueError:
            cum_pnl.append({"date": dt, "signal_pnl_cum": round(signal_cum, 2), "buy_hold_cum": round(bh_cum, 2)})
            continue

        if idx + 1 < len(price_dates) and ens is not None:
            p0 = price_map[dt]
            p1 = price_map[price_dates[idx + 1]]
            daily_ret = (p1 - p0) / p0 * 100
            # Scale by position score (continuous sizing)
            pos = _f(s.get("position_score")) or 0
            signal_cum += daily_ret * pos

        cum_pnl.append({
            "date": dt,
            "signal_pnl_cum": round(signal_cum, 2),
            "buy_hold_cum": round(bh_cum, 2),
        })

    # Downsample cumulative PnL for chart (every 5th point)
    cum_pnl_chart = cum_pnl[::5]
    if cum_pnl and cum_pnl[-1] not in cum_pnl_chart:
        cum_pnl_chart.append(cum_pnl[-1])

    # ── 4. Overall Statistics ──
    daily_rets = []
    for s in signals:
        dt = s["trading_date"]
        ens = _f(s.get("ensemble_score"))
        pos = _f(s.get("position_score")) or 0
        if ens is None or dt not in price_map:
            continue
        try:
            idx = price_dates.index(dt)
        except ValueError:
            continue
        if idx + 1 < len(price_dates):
            p0 = price_map[dt]
            p1 = price_map[price_dates[idx + 1]]
            daily_rets.append((p1 - p0) / p0 * pos)

    stats = {}
    if daily_rets:
        n = len(daily_rets)
        mean_r = sum(daily_rets) / n
        std_r = math.sqrt(sum((r - mean_r) ** 2 for r in daily_rets) / n) if n > 1 else 0
        wins = [r for r in daily_rets if r > 0]
        losses = [r for r in daily_rets if r < 0]
        downside = [r for r in daily_rets if r < 0]
        downside_std = math.sqrt(sum(r ** 2 for r in downside) / len(downside)) if downside else 0

        # Drawdown series
        cum = 0
        peak = 0
        dd_series = []
        max_dd = 0
        for i, r in enumerate(daily_rets):
            cum += r
            peak = max(peak, cum)
            dd = cum - peak
            max_dd = min(max_dd, dd)
            if i % 5 == 0:  # downsample
                dd_series.append({"idx": i, "drawdown": round(dd * 100, 2)})

        sharpe = (mean_r / std_r * math.sqrt(252)) if std_r > 0 else 0
        sortino = (mean_r / downside_std * math.sqrt(252)) if downside_std > 0 else 0

        stats = {
            "total_trades": n,
            "winning_trades": len(wins),
            "losing_trades": len(losses),
            "win_rate": round(len(wins) / n, 3),
            "profit_factor": round(sum(wins) / abs(sum(losses)), 2) if losses and sum(losses) != 0 else 0,
            "sharpe_ratio": round(sharpe, 2),
            "sortino_ratio": round(sortino, 2),
            "max_drawdown_pct": round(max_dd * 100, 2),
            "avg_daily_return_pct": round(mean_r * 100, 4),
            "std_daily_return_pct": round(std_r * 100, 4),
            "annual_return_pct": round(mean_r * 252 * 100, 1),
            "annual_volatility_pct": round(std_r * math.sqrt(252) * 100, 1),
            "best_day_pct": round(max(daily_rets) * 100, 3),
            "worst_day_pct": round(min(daily_rets) * 100, 3),
        }
    else:
        dd_series = []

    # ── 5. Monthly Returns ──
    monthly = {}
    for s in signals:
        dt = s["trading_date"]
        ens = _f(s.get("ensemble_score"))
        pos = _f(s.get("position_score")) or 0
        if ens is None or dt not in price_map:
            continue
        try:
            idx = price_dates.index(dt)
        except ValueError:
            continue
        if idx + 1 < len(price_dates):
            p0 = price_map[dt]
            p1 = price_map[price_dates[idx + 1]]
            daily_ret = (p1 - p0) / p0 * pos
            month_key = dt[:7]  # "YYYY-MM"
            if month_key not in monthly:
                monthly[month_key] = {"total": 0, "count": 0}
            monthly[month_key]["total"] += daily_ret
            monthly[month_key]["count"] += 1

    monthly_returns = [
        {"month": k, "return_pct": round(v["total"] * 100, 2), "num_signals": v["count"]}
        for k, v in sorted(monthly.items())
    ]

    # ── 6. Kelly Criterion Position Sizing (3F-3) ──
    kelly_sizing = {}
    for sig, info in attribution.items():
        if info["count"] < 10:
            kelly_sizing[sig] = {"kelly_fraction": 0, "quarter_kelly_pct": 0, "edge": "Insufficient data"}
            continue
        p = info["hit_rate"]
        q = 1 - p
        b = info["win_loss_ratio"] if info["win_loss_ratio"] > 0 else 0.001
        if b > 0:
            f_star = (p * b - q) / b
        else:
            f_star = 0
        # Apply 1/4 Kelly for safety
        quarter_kelly = max(0, f_star / 4)
        edge = "Favorable" if f_star > 0 else "Unfavorable"
        kelly_sizing[sig] = {
            "full_kelly_pct": round(f_star * 100, 2),
            "quarter_kelly_pct": round(quarter_kelly * 100, 2),
            "win_prob": round(p, 3),
            "odds_ratio": round(b, 3),
            "edge": edge,
        }

    # ── 7. Drawdown Controls / Circuit Breaker (3F-4) ──
    circuit_breaker = {"level": 0, "position_multiplier": 1.0, "status": "NORMAL"}
    if stats and stats.get("max_drawdown_pct") is not None:
        current_dd = 0
        if dd_series:
            current_dd = dd_series[-1].get("drawdown", 0) if dd_series[-1] else 0
        if current_dd < -10:
            circuit_breaker = {"level": 3, "position_multiplier": 0.0, "status": "LIQUIDATE",
                               "message": "Max drawdown exceeded -10%. All positions closed."}
        elif current_dd < -5:
            circuit_breaker = {"level": 2, "position_multiplier": 0.50, "status": "RESTRICT",
                               "message": "Drawdown > -5%. Position sizes halved."}
        elif current_dd < -2:
            circuit_breaker = {"level": 1, "position_multiplier": 0.75, "status": "CAUTION",
                               "message": "Drawdown > -2%. Position sizes reduced 25%."}
        circuit_breaker["current_drawdown_pct"] = round(current_dd, 2)

    return {
        "success": True,
        "as_of": ist_now.strftime("%Y-%m-%d %H:%M IST"),
        "period": {
            "filter": period,
            "cutoff": cutoff,
            "start": signals[0]["trading_date"] if signals else None,
            "end": signals[-1]["trading_date"] if signals else None,
            "trading_days": len(signals),
        },
        "signal_accuracy": accuracy,
        "signal_attribution": attribution,
        "cumulative_pnl": cum_pnl_chart,
        "statistics": stats,
        "monthly_returns": monthly_returns,
        "drawdown_series": dd_series,
        "kelly_sizing": kelly_sizing,
        "circuit_breaker": circuit_breaker,
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
            qs = parse_qs(urlparse(self.path).query)
            period = qs.get("period", ["all"])[0]
            if period not in PERIOD_DAYS:
                period = "all"
            result = generate_backtest(period)
            self.send_json(result)
        except Exception as e:
            self.send_json({"success": False, "error": str(e)[:200]}, 500)
