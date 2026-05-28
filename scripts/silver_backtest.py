#!/usr/bin/env python3
"""
Silver event-study backtest + Monte Carlo design.

Joins (a) SILVER+SILVERM consolidated daily turnover/volume from mcx_commodity_daily
with (b) Dhan continuous SILVERM front-month price returns, builds a rolling
60-day activity z-score, then sweeps short/long strategies after extreme-activity
events to find the structure with the highest robust win rate.

Outputs JSON for downstream MC and Qwen analysis.

Usage:
  python3 scripts/silver_backtest.py
"""
from __future__ import annotations
import os, sys, json, datetime as dt
from pathlib import Path
import numpy as np
import pandas as pd

# Project root on path so we can import lib.mcx_config
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Load .env (Dhan creds + Supabase keys)
env = ROOT / ".env"
if env.exists():
    for line in env.read_text().splitlines():
        line = line.strip()
        if line and "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1); os.environ.setdefault(k, v)

from lib.mcx_config import supabase_read_all  # noqa: E402
from dhanhq import dhanhq                      # noqa: E402


# ─── Data assembly ─────────────────────────────────────────────────────────

def load_price_series() -> pd.DataFrame:
    """SILVERM continuous front-month daily OHLCV from Dhan (~1500 bars)."""
    d = dhanhq(os.environ["DHAN_CLIENT_ID"], os.environ["DHAN_ACCESS_TOKEN"])
    r = d.historical_daily_data("464151", dhanhq.MCX, "FUTCOM",
                                 "2020-01-01", "2026-05-28").get("data", {})
    if not r.get("close"):
        raise RuntimeError("no price data from Dhan")
    df = pd.DataFrame({
        "date": [dt.datetime.utcfromtimestamp(t).date() for t in r["timestamp"]],
        "open": r["open"], "high": r["high"], "low": r["low"],
        "close": r["close"], "vol": r["volume"],
    })
    # Dhan bar timestamps are at UTC midnight which lands on the *prior* calendar
    # day for IST sessions — shift +1 so dates align with the actual MCX day.
    df["date"] = pd.to_datetime(df["date"]) + pd.Timedelta(days=1)
    df = df.set_index("date").sort_index()
    # de-duplicate (Dhan sometimes emits a closing bar after a tz-shifted one)
    df = df[~df.index.duplicated(keep="last")]
    df["ret"] = np.log(df["close"]).diff()
    return df


def load_activity_series() -> pd.DataFrame:
    """SILVER + SILVERM turnover/volume aggregated per day from mcx_commodity_daily.

    Mini/micro consolidation applied here (anticipates the cron fix).
    """
    rows = supabase_read_all(
        "mcx_commodity_daily",
        "?select=trading_date,commodity,turnover_cr,volume_lots"
        "&commodity=in.(SILVER,SILVERM,SILVERMIC)"
        "&order=trading_date.asc",
        max_rows=200000,
    )
    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("no SILVER* rows in mcx_commodity_daily")
    df["turnover_cr"] = pd.to_numeric(df["turnover_cr"], errors="coerce").fillna(0)
    df["volume_lots"] = pd.to_numeric(df["volume_lots"], errors="coerce").fillna(0)
    g = df.groupby("trading_date").agg(turnover=("turnover_cr", "sum"),
                                       volume=("volume_lots", "sum")).reset_index()
    g["trading_date"] = pd.to_datetime(g["trading_date"])
    return g.set_index("trading_date").sort_index()


def build_signal(df: pd.DataFrame, window: int = 60) -> pd.DataFrame:
    """Rolling z-score of turnover & volume; composite = average."""
    df = df.copy()
    df["to_z"] = (df["turnover"] - df["turnover"].rolling(window, min_periods=30).mean()) \
                  / df["turnover"].rolling(window, min_periods=30).std(ddof=0)
    df["vol_z"] = (df["volume"] - df["volume"].rolling(window, min_periods=30).mean()) \
                   / df["volume"].rolling(window, min_periods=30).std(ddof=0)
    df["comp_z"] = df[["to_z", "vol_z"]].mean(axis=1)
    return df


# ─── Event study: forward returns conditioned on signal events ─────────────

def simulate_trade(prices: pd.Series, entry_idx: int, direction: int,
                   target_pct: float, stop_pct: float, max_days: int,
                   intra_high: pd.Series, intra_low: pd.Series) -> dict:
    """Walk forward from entry_idx+1 (next bar) using intraday high/low to
    detect target/stop touches. direction +1=long, -1=short.

    Returns dict with outcome ('target','stop','timeout'), pnl_pct, days.
    """
    entry = prices.iloc[entry_idx]
    target = entry * (1 + direction * target_pct)
    stop   = entry * (1 - direction * stop_pct)
    end = min(entry_idx + max_days, len(prices) - 1)
    for j in range(entry_idx + 1, end + 1):
        hi, lo = intra_high.iloc[j], intra_low.iloc[j]
        # Pessimistic assumption: if both stop and target are touched on the same
        # bar (a wick that overshoots both), assume stop hits first.
        stop_hit   = (direction == 1 and lo <= stop)   or (direction == -1 and hi >= stop)
        target_hit = (direction == 1 and hi >= target) or (direction == -1 and lo <= target)
        if stop_hit and target_hit:
            return {"outcome": "stop", "pnl_pct": -stop_pct, "days": j - entry_idx}
        if stop_hit:
            return {"outcome": "stop", "pnl_pct": -stop_pct, "days": j - entry_idx}
        if target_hit:
            return {"outcome": "target", "pnl_pct": target_pct, "days": j - entry_idx}
    # Timeout — mark-to-market
    exit_px = prices.iloc[end]
    pnl = direction * (exit_px / entry - 1)
    return {"outcome": "timeout", "pnl_pct": pnl, "days": end - entry_idx}


def run_strategy(joined: pd.DataFrame, *, z_thresh: float, signal_day_dir: str,
                 trade_dir: int, target_pct: float, stop_pct: float,
                 max_days: int, train_end: str | None = None) -> dict:
    """
    z_thresh:        composite_z > z_thresh on signal day
    signal_day_dir:  'down' | 'up' | 'any'  — condition on signal day's price direction
    trade_dir:       +1 long, -1 short  (entered at next bar's open)
    """
    df = joined.dropna(subset=["comp_z", "close", "ret"]).copy()
    if train_end:
        df = df[df.index <= pd.Timestamp(train_end)]
    events = df[df["comp_z"] >= z_thresh].copy()
    if signal_day_dir == "down":
        events = events[events["ret"] < 0]
    elif signal_day_dir == "up":
        events = events[events["ret"] > 0]
    # Need an "exit window" after each event — drop events too close to the end
    last_ok_date = df.index.max() - pd.Timedelta(days=max_days * 2)
    events = events[events.index <= last_ok_date]

    prices = df["close"]; hi = df["high"]; lo = df["low"]
    trades = []
    for ts in events.index:
        i = df.index.get_loc(ts)
        # Skip if not enough forward bars
        if i + max_days >= len(df):
            continue
        t = simulate_trade(prices, i, trade_dir, target_pct, stop_pct, max_days, hi, lo)
        t["entry_date"] = ts.strftime("%Y-%m-%d")
        t["entry_px"] = float(prices.iloc[i])
        t["comp_z"] = float(events.loc[ts, "comp_z"])
        t["signal_day_ret"] = float(events.loc[ts, "ret"])
        trades.append(t)

    if not trades:
        return {"n": 0, "win_rate": None, "trades": []}
    wins = sum(1 for t in trades if t["pnl_pct"] > 0)
    win_rate = wins / len(trades)
    avg_pnl = sum(t["pnl_pct"] for t in trades) / len(trades)
    # Expectancy in R-multiples (uses stop_pct as R)
    expectancy_R = avg_pnl / stop_pct
    return {
        "n": len(trades),
        "win_rate": win_rate,
        "avg_pnl_pct": avg_pnl,
        "expectancy_R": expectancy_R,
        "outcomes": {k: sum(1 for t in trades if t["outcome"] == k)
                     for k in ("target", "stop", "timeout")},
        "trades": trades,
    }


# ─── Parameter sweep ───────────────────────────────────────────────────────

def sweep(joined: pd.DataFrame, train_end: str | None = None) -> list:
    """Grid-search target/stop/horizon for several signal/direction hypotheses."""
    results = []
    for z_thresh in [1.5, 2.0, 2.5]:
        for signal_dir in ["down", "up", "any"]:
            for trade_dir, dir_name in [(-1, "short"), (1, "long")]:
                for max_days in [3, 5, 10]:
                    for target_pct in [0.02, 0.03, 0.05, 0.07]:
                        for stop_pct in [0.02, 0.03, 0.05]:
                            r = run_strategy(joined,
                                z_thresh=z_thresh, signal_day_dir=signal_dir,
                                trade_dir=trade_dir, target_pct=target_pct,
                                stop_pct=stop_pct, max_days=max_days,
                                train_end=train_end)
                            if r["n"] < 15:  # statistical floor
                                continue
                            results.append({
                                "z_thresh": z_thresh, "signal_dir": signal_dir,
                                "trade_dir": dir_name, "max_days": max_days,
                                "target_pct": target_pct, "stop_pct": stop_pct,
                                "n": r["n"], "win_rate": r["win_rate"],
                                "avg_pnl_pct": r["avg_pnl_pct"],
                                "expectancy_R": r["expectancy_R"],
                                "outcomes": r["outcomes"],
                            })
    # Sort by win_rate desc, then expectancy
    results.sort(key=lambda x: (x["win_rate"], x["expectancy_R"]), reverse=True)
    return results


def main():
    print("loading silver price history...")
    px = load_price_series()
    print(f"  {len(px)} bars, {px.index.min().date()} -> {px.index.max().date()}")

    print("loading mcx_commodity_daily SILVER complex...")
    act = load_activity_series()
    print(f"  {len(act)} days, {act.index.min().date()} -> {act.index.max().date()}")

    act = build_signal(act)
    joined = px.join(act, how="inner")
    print(f"joined sample: {len(joined)} bars, "
          f"{joined.index.min().date()} -> {joined.index.max().date()}")
    print(f"  events |comp_z|>=1.5: {(joined['comp_z'].abs()>=1.5).sum()}")
    print(f"  events |comp_z|>=2.0: {(joined['comp_z'].abs()>=2.0).sum()}")

    print("\n=== FULL-SAMPLE SWEEP ===")
    full = sweep(joined)
    print(f"  {len(full)} configurations tested (n>=15)")
    print("\nTop 12 by win rate (full sample):")
    print(f"{'win%':>5} {'n':>4} {'expR':>6}  z>={'':<3} sigDay  dir   {'tgt%':>5} {'stp%':>5} {'days':>4}")
    for r in full[:12]:
        print(f"{r['win_rate']*100:5.1f} {r['n']:4d} {r['expectancy_R']:6.2f}  "
              f">={r['z_thresh']:<4} {r['signal_dir']:<6} {r['trade_dir']:<5} "
              f"{r['target_pct']*100:5.1f} {r['stop_pct']*100:5.1f} {r['max_days']:4d}")

    print("\n=== WALK-FORWARD (train <= 2024-12-31, test > 2024-12-31) ===")
    train = sweep(joined, train_end="2024-12-31")
    # Take top-10 in-sample candidates, re-test on full sample minus the train window
    test_df = joined[joined.index > pd.Timestamp("2024-12-31")]
    print(f"  in-sample top 10 by win rate, then OOS performance:")
    print(f"{'IS win%':>7} {'IS n':>5}  {'OOS win%':>9} {'OOS n':>6}  config")
    oos_results = []
    for r in train[:10]:
        oos = run_strategy(joined,
            z_thresh=r["z_thresh"], signal_day_dir=r["signal_dir"],
            trade_dir=(-1 if r["trade_dir"] == "short" else 1),
            target_pct=r["target_pct"], stop_pct=r["stop_pct"],
            max_days=r["max_days"],
            train_end=None)
        # Filter OOS trades only
        oos_trades = [t for t in oos["trades"] if t["entry_date"] > "2024-12-31"]
        if len(oos_trades) >= 3:
            wins = sum(1 for t in oos_trades if t["pnl_pct"] > 0)
            oos_wr = wins / len(oos_trades)
        else:
            oos_wr = None
        cfg = f"z>={r['z_thresh']} {r['signal_dir']:6s} {r['trade_dir']:5s} tgt{r['target_pct']*100:.1f}% stp{r['stop_pct']*100:.1f}% {r['max_days']}d"
        print(f"{r['win_rate']*100:7.1f} {r['n']:5d}  "
              f"{'  '+f'{oos_wr*100:5.1f}' if oos_wr is not None else '    n/a':>9} "
              f"{len(oos_trades):6d}  {cfg}")
        oos_results.append({"is_win": r["win_rate"], "is_n": r["n"],
                            "oos_win": oos_wr, "oos_n": len(oos_trades),
                            "config": r})

    out = {
        "as_of": dt.datetime.utcnow().isoformat() + "Z",
        "sample": {"start": str(px.index.min().date()),
                   "end": str(px.index.max().date()),
                   "joined_bars": len(joined)},
        "full_top": full[:20],
        "walk_forward": oos_results,
    }
    Path("/tmp/silver_backtest.json").write_text(json.dumps(out, default=str, indent=2))
    print("\nsaved /tmp/silver_backtest.json")


if __name__ == "__main__":
    main()
