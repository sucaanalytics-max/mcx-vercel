#!/usr/bin/env python3
"""
Monte Carlo: P(target before stop) for the SILVERM long-after-down-day setup,
simulated from the CURRENT live price (not the entry), with intraday hi/lo
matching the backtest's touch detection.

Models:
  - GBM baseline (μ=0, σ from 2026 realized)
  - Jump-diffusion (Qwen-parameterised: μ=-15%, σ=85%, λ=12/y, jump N(-2.5%, 4%))
  - Stress (σ=100%, μ=-25%, intensified jumps)

Reports: P(target), P(stop), P(timeout), expectancy in % and R-multiples from
the *current* price, plus sensitivity to vol and drift.

Usage: python3 scripts/silver_mc.py
"""
from __future__ import annotations
import math, json, os, sys
from pathlib import Path
import numpy as np

ENTRY     = 278_200.0   # SILVERM 27-May open (where the trade entered)
SPOT_NOW  = 270_150.0   # live mid-session 27-May (intraday underwater)
TARGET    = ENTRY * 1.02   # 283,764
STOP      = ENTRY * 0.95   # 264,290
HORIZON_D = 9              # remaining trading days
# Empirical SILVERM intraday-range model (fit on last 60d):
#   range_pct = RANGE_A + RANGE_B * |daily log return|
# range_pct is (high-low)/close. The intraday band per side ~= range_pct / 2.
RANGE_A   = 0.0287
RANGE_B   = 0.56
N_PATHS    = 50_000
RNG        = np.random.default_rng(20260527)


def simulate_paths(spot: float, mu_ann: float, sigma_ann: float,
                   *, jump_lambda: float = 0.0, jump_mu: float = 0.0,
                   jump_sigma: float = 0.0, df_t: float | None = None,
                   horizon_d: int = HORIZON_D,
                   range_a: float = RANGE_A, range_b: float = RANGE_B,
                   n_paths: int = N_PATHS, rng=RNG, close_only: bool = False) -> dict:
    """Simulate close-to-close diffusion (with optional Merton jumps or Student-t),
    plus a per-bar intraday high/low band proportional to that bar's vol, then
    walk forward checking target/stop touches. If both touch on the same bar,
    stop hits first (matches backtest pessimism).
    """
    dt = 1 / 252.0
    mu = mu_ann / 100.0
    sigma = sigma_ann / 100.0
    sig_d = sigma * math.sqrt(dt)
    drift = (mu - 0.5 * sigma ** 2) * dt

    # Daily log returns
    if df_t is not None:
        # Student-t scaled to match σ
        scale = sig_d * math.sqrt((df_t - 2) / df_t)
        z = rng.standard_t(df_t, size=(n_paths, horizon_d)) * scale
    else:
        z = rng.standard_normal(size=(n_paths, horizon_d)) * sig_d
    rets = drift + z

    # Merton jump component
    if jump_lambda > 0:
        n_jumps = rng.poisson(jump_lambda * dt, size=(n_paths, horizon_d))
        # cap n_jumps per bar at 3 for sanity (extremely rare anyway at λ*dt)
        n_jumps = np.minimum(n_jumps, 3)
        jump_sum = np.zeros_like(rets)
        for k in range(1, 4):
            mask = (n_jumps == k)
            if mask.any():
                draw = rng.normal(jump_mu / 100.0, jump_sigma / 100.0,
                                  size=(mask.sum(), k))
                jump_sum[mask] = draw.sum(axis=1)
        rets = rets + jump_sum

    # Simulate close path
    log_path = np.cumsum(rets, axis=1) + math.log(spot)
    closes = np.exp(log_path)

    # Empirical intraday hi/lo: range_pct = a + b*|ret|, centered on geo-mean
    # of (open, close). Constrain hi >= max(open,close), lo <= min(open,close).
    opens = np.empty_like(closes)
    opens[:, 0] = spot
    opens[:, 1:] = closes[:, :-1]
    if close_only:
        hi = np.maximum(opens, closes)
        lo = np.minimum(opens, closes)
    else:
        range_pct = range_a + range_b * np.abs(rets)
        midpt = np.sqrt(opens * closes)
        half = np.log1p(range_pct / 2)  # log-space half-range
        hi = np.maximum(midpt * np.exp(half), np.maximum(opens, closes))
        lo = np.minimum(midpt * np.exp(-half), np.minimum(opens, closes))

    # Walk forward, detect touch outcomes
    n_target = 0; n_stop = 0; n_timeout = 0; pnl = np.zeros(n_paths); days = np.zeros(n_paths, dtype=int)
    for p in range(n_paths):
        outcome = None
        for t in range(horizon_d):
            stop_hit   = lo[p, t] <= STOP
            target_hit = hi[p, t] >= TARGET
            if stop_hit and target_hit:
                outcome = "stop"; pnl[p] = (STOP / spot) - 1; days[p] = t + 1; break
            if stop_hit:
                outcome = "stop"; pnl[p] = (STOP / spot) - 1; days[p] = t + 1; break
            if target_hit:
                outcome = "target"; pnl[p] = (TARGET / spot) - 1; days[p] = t + 1; break
        if outcome is None:
            outcome = "timeout"
            pnl[p] = (closes[p, -1] / spot) - 1
            days[p] = horizon_d
        if outcome == "target":  n_target += 1
        elif outcome == "stop":  n_stop += 1
        else:                    n_timeout += 1

    p_target = n_target / n_paths
    p_stop   = n_stop   / n_paths
    p_timeout= n_timeout/ n_paths
    # Win = positive pnl (any positive close outcome counts)
    p_win    = float((pnl > 0).mean())

    return {
        "p_target": p_target, "p_stop": p_stop, "p_timeout": p_timeout,
        "p_win_pnl_pos": p_win,
        "expectancy_pct": float(pnl.mean() * 100),
        "median_pnl_pct": float(np.median(pnl) * 100),
        "p5_pnl_pct": float(np.percentile(pnl, 5) * 100),
        "p95_pnl_pct": float(np.percentile(pnl, 95) * 100),
        "median_days": int(np.median(days)),
    }


def main():
    # Geometry sanity check from current price (no model needed)
    up_pct   = TARGET / SPOT_NOW - 1
    down_pct = SPOT_NOW / STOP - 1
    breakeven = down_pct / (up_pct + down_pct)
    print(f"From SPOT_NOW=₹{SPOT_NOW:,.0f}: target ₹{TARGET:,.0f} (+{up_pct*100:.2f}%), "
          f"stop ₹{STOP:,.0f} (-{down_pct*100:.2f}%)")
    print(f"  R:R from here = {up_pct/down_pct:.2f} : 1")
    print(f"  Breakeven win rate from current price = {breakeven*100:.1f}%")

    # Two vol scenarios: full-2026 realized (78%) and recent 20d (50%).
    # 20d vol is more representative of the immediate forward window.
    scenarios = [
        ("GBM close-only (μ=0,  σ=50)",        dict(mu_ann=0,   sigma_ann=50, close_only=True)),
        ("GBM close-only (μ=0,  σ=78)",        dict(mu_ann=0,   sigma_ann=78, close_only=True)),
        ("GBM intraday   (μ=0,  σ=50)",        dict(mu_ann=0,   sigma_ann=50)),
        ("GBM intraday   (μ=0,  σ=78)",        dict(mu_ann=0,   sigma_ann=78)),
        ("GBM mean-rev   (μ=+15, σ=50)",       dict(mu_ann=15,  sigma_ann=50)),   # backtest implies up-drift after sig
        ("Student-t df=5 (μ=0,  σ=50)",        dict(mu_ann=0,   sigma_ann=50, df_t=5)),
        ("Qwen JD        (μ=-15,σ=85, λ=12)",  dict(mu_ann=-15, sigma_ann=85,
                                                     jump_lambda=12, jump_mu=-2.5, jump_sigma=4.0)),
        ("Stress JD      (μ=-25,σ=100,λ=20)",  dict(mu_ann=-25, sigma_ann=100,
                                                     jump_lambda=20, jump_mu=-3.0, jump_sigma=5.0)),
    ]
    print(f"\n{'scenario':<34} {'P_tgt':>6} {'P_stop':>7} {'P_to':>6} {'EV%':>7} {'p5':>7} {'p95':>7}")
    out = {"setup": {"entry": ENTRY, "spot_now": SPOT_NOW, "target": TARGET,
                     "stop": STOP, "horizon_d": HORIZON_D,
                     "breakeven_from_current_pct": breakeven*100,
                     "rr_from_current": up_pct/down_pct}, "scenarios": []}
    for label, kw in scenarios:
        r = simulate_paths(SPOT_NOW, **kw)
        print(f"{label:<34} {r['p_target']*100:5.1f}% {r['p_stop']*100:6.1f}% "
              f"{r['p_timeout']*100:5.1f}% {r['expectancy_pct']:+6.2f}% "
              f"{r['p5_pnl_pct']:+6.1f}% {r['p95_pnl_pct']:+6.1f}%")
        out["scenarios"].append({"label": label, **kw, **r})

    Path("/tmp/silver_mc.json").write_text(json.dumps(out, default=str, indent=2))
    print("\nsaved /tmp/silver_mc.json")


if __name__ == "__main__":
    main()
