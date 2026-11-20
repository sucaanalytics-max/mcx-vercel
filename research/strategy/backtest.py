"""Event study and futures trade simulator for the ADR band rules.

Timing: a signal dated d (revenue known after d's close) sets the position held
from the next NSE open. Daily P&L of the effective position p:
    r_t = p_{t-1} · (open_t / close_{t-1} − 1)  +  p_t · (close_t / open_t − 1)
i.e. yesterday's position carries the overnight gap and today's (possibly new)
position earns the session. Costs: COST_SIDE per unit of position change, and
ROLL per held unit when the calendar month changes (monthly futures roll).
"""
from __future__ import annotations
import math
from statistics import NormalDist
import numpy as np
import pandas as pd

COST_SIDE = 0.0005          # 0.10% round trip: STT, fees, 5 bp slippage
ROLL = 0.0005               # per monthly roll while a position is held
N = NormalDist()


# ------------------------------------------------------------------ rules → target positions
def rule_band(z: pd.Series, k: float = 2.0, max_hold: int = 60) -> pd.Series:
    """R1: long at z<=-k, short at z>=+k; exit when z crosses 0 or after max_hold days."""
    pos, held, out = 0, 0, []
    armed = {1: True, -1: True}          # a side re-arms only after z leaves its band
    for v in z.values:
        if math.isnan(v):
            out.append(pos); continue
        if v > -k:
            armed[1] = True
        if v < k:
            armed[-1] = True
        if pos != 0:
            held += 1
            if (pos > 0 and v >= 0) or (pos < 0 and v <= 0) or held >= max_hold:
                pos, held = 0, 0
        if pos == 0:
            if v <= -k and armed[1]:
                pos, held, armed[1] = 1, 0, False
            elif v >= k and armed[-1]:
                pos, held, armed[-1] = -1, 0, False
        out.append(pos)
    return pd.Series(out, index=z.index, dtype=float)


def rule_turning(state: pd.Series, max_hold: int = 60) -> pd.Series:
    """R2: +1 on a turning-up state, −1 on turning-down; flat after max_hold days;
    an opposite state flips the position."""
    pos, held, out, last = 0, 0, [], 0
    for v in state.values:
        v = 0 if math.isnan(v) else int(v)
        if pos != 0:
            held += 1
            if held >= max_hold:
                pos, held = 0, 0
        if v != 0 and v != pos and v != last:      # a new episode of the state (not a re-fire)
            pos, held = v, 0
        elif v != 0 and v == -pos:                 # opposite state while held: flip
            pos, held = v, 0
        last = v
        out.append(pos)
    return pd.Series(out, index=state.index, dtype=float)


def rule_trend(ratio: pd.Series) -> pd.Series:
    """R4: long while ADR10/ADR45 > 1, short while < 1."""
    return np.sign(ratio - 1).fillna(0.0)


def rule_hold_after(trigger: pd.Series, days: int) -> pd.Series:
    """Long for `days` sessions after each trigger day (re-triggers extend)."""
    out, left = [], 0
    for v in trigger.fillna(False).values:
        if v:
            left = days
        out.append(1.0 if left > 0 else 0.0)
        left = max(0, left - 1)
    return pd.Series(out, index=trigger.index)


# ------------------------------------------------------------------ simulation
def simulate(target: pd.Series, px: pd.DataFrame, long_only: bool = False) -> pd.DataFrame:
    """target: position decided at each signal date (revenue calendar). Returns daily
    frame on the NSE calendar with pos, gross, cost, net."""
    nse = px.index
    t = target.copy()
    if long_only:
        t = t.clip(lower=0)
    # position effective from the first NSE session strictly after the signal date
    eff = pd.Series(np.nan, index=nse)
    loc = nse.searchsorted(t.index, side="right")
    for i, v in zip(loc, t.values):
        if i < len(nse):
            eff.iloc[i] = v
    eff = eff.ffill().fillna(0.0)
    prev = eff.shift(1).fillna(0.0)
    o, c = px["Open"], px["Close"]
    gross = prev * (o / c.shift(1) - 1).fillna(0) + eff * (c / o - 1)
    month_change = pd.Series(nse.month, index=nse).diff().fillna(0) != 0
    cost = (eff - prev).abs() * COST_SIDE + (prev.abs() * month_change * ROLL)
    return pd.DataFrame({"pos": eff, "gross": gross, "cost": cost, "net": gross - cost})


def trades(sim: pd.DataFrame, px: pd.DataFrame) -> pd.DataFrame:
    """One row per position episode. Entry at the open of the first day held, exit at
    the open of the first day the position changes (last close if still open).
    ret = dir · (exit/entry − 1) − round-trip cost − rolls."""
    pos = sim["pos"]
    change = pos.ne(pos.shift(1).fillna(0.0))
    rows, cur, start = [], 0.0, None
    for d in list(pos.index[change]) + [None]:
        if cur != 0 and start is not None:
            if d is None:
                exit_px, exit_d, still = px["Close"].iloc[-1], pos.index[-1], True
            else:
                exit_px, exit_d, still = px["Open"].loc[d], d, False
            entry_px = px["Open"].loc[start]
            months = len(pd.period_range(start, exit_d, freq="M")) - 1
            ret = cur * (exit_px / entry_px - 1) - 2 * COST_SIDE - months * ROLL
            rows.append({"entry": start, "exit": exit_d, "dir": cur, "ret": ret,
                         "days": int(pos.index.get_loc(exit_d) - pos.index.get_loc(start)), "open": still})
        if d is None:
            break
        cur, start = pos.loc[d], d
    return pd.DataFrame(rows, columns=["entry", "exit", "dir", "ret", "days", "open"])


def metrics(sim: pd.DataFrame, px: pd.DataFrame, start=None, end=None) -> dict:
    s = sim.loc[start:end]
    if s.empty:
        return {}
    r = s["net"]
    eq = (1 + r).cumprod()
    yrs = len(r) / 252
    cagr = eq.iloc[-1] ** (1 / yrs) - 1 if yrs > 0 and eq.iloc[-1] > 0 else float("nan")
    sd = r.std()
    sharpe = r.mean() / sd * math.sqrt(252) if sd > 0 else 0.0
    dd = (eq / eq.cummax() - 1).min()
    tr = trades(s, px.loc[s.index])
    wins = tr[tr.ret > 0].ret.sum() if len(tr) else 0
    losses = -tr[tr.ret < 0].ret.sum() if len(tr) else 0
    return {"cagr": round(cagr * 100, 1), "sharpe": round(sharpe, 2), "max_dd": round(dd * 100, 1),
            "trades": len(tr), "hit": round((tr.ret > 0).mean() * 100) if len(tr) else None,
            "avg_trade": round(tr.ret.mean() * 100, 2) if len(tr) else None,
            "pf": round(wins / losses, 2) if losses > 0 else None,
            "in_mkt": round((s.pos != 0).mean() * 100), "days": len(r),
            "skew": float(r.skew()), "kurt": float(r.kurt()) + 3, "sr_daily": float(r.mean() / sd) if sd > 0 else 0.0}


# ------------------------------------------------------------------ event study
def forward_returns(dates, px: pd.DataFrame, horizons=(5, 10, 20, 60)) -> pd.DataFrame:
    """Entry at the next NSE open after each date; exit at the close h sessions later."""
    nse, rows = px.index, []
    for d in dates:
        i = nse.searchsorted(d, side="right")
        if i >= len(nse):
            continue
        o, r = px["Open"].iloc[i], {"date": d}
        for h in horizons:
            j = i + h - 1
            r[f"r{h}"] = px["Close"].iloc[j] / o - 1 if j < len(nse) else np.nan
        rows.append(r)
    return pd.DataFrame(rows)


def event_stats(events: pd.DataFrame, base: pd.DataFrame, h: int, sign: int = 1) -> dict:
    """Mean forward return of events (signed so + = rule was right) vs all days."""
    col = f"r{h}"
    e = events[col].dropna() * sign
    b = base[col].dropna() * sign
    if len(e) < 2:
        return {"n": len(e), "mean": round(e.mean() * 100, 2) if len(e) else None}
    diff = e.mean() - b.mean()
    t = diff / (e.std(ddof=1) / math.sqrt(len(e))) if e.std(ddof=1) > 0 else float("nan")
    return {"n": len(e), "mean": round(e.mean() * 100, 2), "excess": round(diff * 100, 2),
            "hit": round((e > 0).mean() * 100), "t": round(t, 2)}


# ------------------------------------------------------------------ deflated Sharpe
def deflated_sharpe(sr_daily: float, n_obs: int, skew: float, kurt: float,
                    trial_srs: list[float]) -> float:
    """Probability the true Sharpe > the expected max of N unskilled trials
    (Bailey & López de Prado 2014). Inputs are per-period (daily) Sharpes."""
    trial_srs = [s for s in trial_srs if not math.isnan(s)]
    n = len(trial_srs)
    if n < 2 or n_obs < 3:
        return float("nan")
    var = float(np.var(trial_srs, ddof=1))
    g = 0.5772156649
    sr0 = math.sqrt(var) * ((1 - g) * N.inv_cdf(1 - 1 / n) + g * N.inv_cdf(1 - 1 / (n * math.e)))
    denom = math.sqrt(max(1e-12, 1 - skew * sr_daily + (kurt - 1) / 4 * sr_daily ** 2))
    return float(N.cdf((sr_daily - sr0) * math.sqrt(n_obs - 1) / denom))
