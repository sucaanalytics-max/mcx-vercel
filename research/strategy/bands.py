"""Point-in-time ADR band maths (docs/strategy/2026-11-preregistration.md).

Everything here uses numpy/pandas only, so the tests run under the `local` extra.
The fit for date t uses pairs strictly before t: a signal dated t never sees t's
own price or anything later.
"""
from __future__ import annotations
import math
import numpy as np
import pandas as pd

# Engle-Granger critical values, 2 variables with a constant (MacKinnon 2010, large n).
EG_CRIT = {0.01: -3.90, 0.05: -3.34, 0.10: -3.04}


def adr(rev: pd.Series, w: int) -> pd.Series:
    """Rolling w-day mean of daily revenue (needs a full window)."""
    return rev.rolling(w, min_periods=w).mean()


def expanding_fit(x: pd.Series, y: pd.Series, min_obs: int = 120,
                  window: int | None = None, log: bool = False) -> pd.DataFrame:
    """OLS y = a + b·x fitted on pairs before each date (expanding, or rolling `window`).

    Returns a frame indexed like x/y with a, b, s (residual SD), n, fit (price
    implied at today's x), z = (y - fit)/s in the fitted space. With log=True the
    fit is ln y on ln x and `fit` is converted back to price.
    """
    df = pd.DataFrame({"x": x, "y": y}).dropna()
    if log:
        df = df[(df.x > 0) & (df.y > 0)]
        df = np.log(df)
    X, Y = df.x, df.y
    one = pd.Series(1.0, index=df.index)
    parts = {"n": one, "sx": X, "sy": Y, "sxx": X * X, "sxy": X * Y, "syy": Y * Y}
    if window:
        sums = {k: v.rolling(window, min_periods=min_obs).sum().shift(1) for k, v in parts.items()}
    else:
        sums = {k: v.cumsum().shift(1) for k, v in parts.items()}
    n = sums["n"]
    cxx = sums["sxx"] - sums["sx"] ** 2 / n
    cxy = sums["sxy"] - sums["sx"] * sums["sy"] / n
    cyy = sums["syy"] - sums["sy"] ** 2 / n
    b = cxy / cxx
    a = (sums["sy"] - b * sums["sx"]) / n
    ssr = (cyy - b * cxy).clip(lower=0)
    s = np.sqrt(ssr / (n - 2))
    ok = (n >= min_obs) & (cxx > 0)
    fit_space = a + b * X
    out = pd.DataFrame({"a": a, "b": b, "s": s, "n": n,
                        "fit": np.exp(fit_space) if log else fit_space,
                        "z": (Y - fit_space) / s}).where(ok)
    return out.reindex(x.index)


def full_fit(x: pd.Series, y: pd.Series, log: bool = False) -> dict:
    """In-sample OLS on all pairs (for the cointegration test and sanity checks)."""
    df = pd.DataFrame({"x": x, "y": y}).dropna()
    if log:
        df = np.log(df[(df.x > 0) & (df.y > 0)])
    b, a = np.polyfit(df.x, df.y, 1)
    e = df.y - (a + b * df.x)
    ss_tot = ((df.y - df.y.mean()) ** 2).sum()
    return {"a": float(a), "b": float(b), "s": float(math.sqrt((e ** 2).sum() / (len(df) - 2))),
            "n": len(df), "r_squared": float(1 - (e ** 2).sum() / ss_tot), "resid": e}


def adf_t(e: pd.Series, lags: int = 1) -> float:
    """ADF t-stat on β in Δe_t = α + β e_{t-1} + Σ γ_i Δe_{t-i} (constant, no trend)."""
    e = pd.Series(np.asarray(e, dtype=float)).dropna().reset_index(drop=True)
    de = e.diff()
    cols = {"lag": e.shift(1)}
    for i in range(1, lags + 1):
        cols[f"d{i}"] = de.shift(i)
    X = pd.DataFrame(cols)
    m = X.notna().all(axis=1) & de.notna()
    Xm = np.column_stack([np.ones(m.sum()), X[m].values])
    yv = de[m].values
    beta, *_ = np.linalg.lstsq(Xm, yv, rcond=None)
    resid = yv - Xm @ beta
    sigma2 = resid @ resid / (len(yv) - Xm.shape[1])
    cov = sigma2 * np.linalg.inv(Xm.T @ Xm)
    return float(beta[1] / math.sqrt(cov[1, 1]))


def half_life(e: pd.Series) -> float:
    """OU half-life in observations from Δe_t = α + β e_{t-1}; inf if not mean-reverting."""
    e = pd.Series(np.asarray(e, dtype=float)).dropna()
    lag, de = e.shift(1)[1:], e.diff()[1:]
    beta = np.polyfit(lag, de, 1)[0]
    if beta >= 0 or beta <= -1:
        return float("inf") if beta >= 0 else 0.0
    return float(-math.log(2) / math.log(1 + beta))


def cointegration(x: pd.Series, y: pd.Series, log: bool = False) -> dict:
    f = full_fit(x, y, log)
    t = adf_t(f["resid"], lags=1)
    level = next((p for p in (0.01, 0.05, 0.10) if t < EG_CRIT[p]), None)
    return {"adf_t": round(t, 2), "reject_at": level, "half_life_days": round(half_life(f["resid"]), 1),
            "r_squared": round(f["r_squared"], 3), "b": f["b"], "n": f["n"]}


def episode_starts(mask: pd.Series) -> pd.Series:
    """True on the first day of each run of True (non-overlapping event dates)."""
    m = mask.fillna(False).astype(bool)
    return m & ~m.shift(1, fill_value=False)


def turning_state(z: pd.Series, ratio: pd.Series, band: float = 1.0) -> pd.Series:
    """+1 turning up (ratio>1, z<=-band), -1 turning down (ratio<1, z>=+band), else 0."""
    up = (ratio > 1) & (z <= -band)
    dn = (ratio < 1) & (z >= band)
    return pd.Series(np.where(up, 1, np.where(dn, -1, 0)), index=z.index).where(z.notna() & ratio.notna())


def band_state(z: float, ratio: float) -> str:
    """Label for the tracker row."""
    if z is None or ratio is None or math.isnan(z) or math.isnan(ratio):
        return "NO DATA"
    if ratio > 1 and z <= -1:
        return "TURNING UP"
    if ratio < 1 and z >= 1:
        return "TURNING DOWN"
    if z <= -2:
        return "AT LOWER"
    if z >= 2:
        return "AT UPPER"
    return "IN BAND"
