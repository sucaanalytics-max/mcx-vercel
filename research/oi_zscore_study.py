#!/usr/bin/env python3
"""
Commodity OI z-score study: implements docs/OI_Zscore_Preregistration.md
exactly. Read-only: Supabase (mcx_commodity_daily) + the expiry cache from
research/oi_expiries.py.

  scripts/mcx_uv.sh --extra research python -I research/oi_zscore_study.py [--json out.json]
"""
from __future__ import annotations
import json, math, os, sys
from collections import defaultdict

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "research"))

from lib.mcx_config import COMMODITY_MAP                      # noqa: E402
from lib.supabase import supabase_read_all                    # noqa: E402
import oi_expiries                                            # noqa: E402

WINDOW, MIN_OBS = 60, 30                 # the cron's _zscore
MIN_ROWS, MIN_AVG_TURNOVER = 60, 50      # the cron's filter
TRAIN_END, TEST_START = "2024-12-31", "2025-01-01"
HORIZONS = (5, 10, 20)
BUCKETS = [(0, 2), (3, 5), (6, 10), (11, 15), (16, 10**6)]
OI_VARS = ["OI_lots", "OI_val", "OI_lots_adj", "OI_val_adj"]
MIN_XS = 5                               # complexes needed for a cross-sectional IC


# ── panel ────────────────────────────────────────────────────────────────────
def load_panel() -> pd.DataFrame:
    raw = supabase_read_all(
        "mcx_commodity_daily",
        "?select=trading_date,commodity,instrument_type,volume_lots,turnover_cr,open_interest,oi_value_cr",
        max_rows=None, timeout=30)
    agg = {}
    for r in raw:
        sym = COMMODITY_MAP.get(r["commodity"], r["commodity"])
        k = (r["trading_date"], sym)
        a = agg.setdefault(k, {"date": r["trading_date"], "cx": sym, "turnover": 0.0, "volume": 0.0,
                               "oi_lots": 0.0, "oi_val": 0.0, "oi_missing": False})
        a["turnover"] += float(r.get("turnover_cr") or 0)
        a["volume"] += float(r.get("volume_lots") or 0)
        a["oi_lots"] += float(r.get("open_interest") or 0)
        a["oi_val"] += float(r.get("oi_value_cr") or 0)
        if str(r.get("instrument_type", "")).startswith("FUT") and not r.get("open_interest"):
            a["oi_missing"] = True
    df = pd.DataFrame(agg.values())
    keep = [c for c, g in df.groupby("cx") if len(g) >= MIN_ROWS and g.turnover.mean() >= MIN_AVG_TURNOVER]
    df = df[df.cx.isin(keep)].sort_values(["cx", "date"]).reset_index(drop=True)
    for col in ("oi_lots", "oi_val"):
        df.loc[df.oi_missing | (df[col] <= 0), col] = np.nan
    return df


def rolling_z(s: pd.Series) -> pd.Series:
    """The cron's _zscore: window of the last 60 rows incl. today, >= 30 non-null, population SD."""
    m = s.rolling(WINDOW, min_periods=MIN_OBS).mean()
    sd = s.rolling(WINDOW, min_periods=MIN_OBS).std(ddof=0)
    z = (s - m) / sd
    return z.where(sd > 0)


def dte_series(dates: list[str], expiries: list[str], all_days: list[str]) -> list[float]:
    """Trading days from each date to the nearest expiry on/after it (0 on expiry day)."""
    idx = {d: i for i, d in enumerate(all_days)}
    exp = sorted(expiries)
    out, j = [], 0
    for d in dates:
        while j < len(exp) and exp[j] < d:
            j += 1
        if j >= len(exp):
            out.append(np.nan)
            continue
        e = exp[j]
        if e in idx:
            out.append(idx[e] - idx[d])
        else:   # expiry not a stored session (holiday/after data): count business days
            out.append(float(np.busday_count(d, e)))
    return out


def bucket(x):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    for i, (lo, hi) in enumerate(BUCKETS):
        if lo <= x <= hi:
            return i
    return None


def build(df: pd.DataFrame, expiries: dict) -> pd.DataFrame:
    all_days = sorted(df.date.unique())
    parts = []
    for cx, g in df.groupby("cx"):
        g = g.copy()
        g["to_z"] = rolling_z(g.turnover)
        g["vol_z"] = rolling_z(g.volume)
        g["OI_lots"] = rolling_z(g.oi_lots)
        g["OI_val"] = rolling_z(g.oi_val)
        g["C"] = g[["to_z", "vol_z"]].mean(axis=1, skipna=True)
        g["dte"] = dte_series(list(g.date), expiries.get(cx, []), all_days)
        g["bucket"] = [bucket(x) for x in g.dte]
        base = g.turnover.rolling(WINDOW, min_periods=WINDOW).mean()
        for h in HORIZONS:
            fwd = g.turnover[::-1].rolling(h, min_periods=h).mean()[::-1].shift(-1)
            g[f"fwd_{h}"] = np.log(fwd / base)
        train = g.date <= TRAIN_END
        for v in ("OI_lots", "OI_val"):
            means = g[train].groupby("bucket")[v].mean()
            g[f"{v}_adj"] = g[v] - g.bucket.map(means).fillna(0.0)
        parts.append(g)
    return pd.concat(parts, ignore_index=True)


# ── statistics ───────────────────────────────────────────────────────────────
def xs_ic_by_date(p: pd.DataFrame, x: str, y: str) -> pd.Series:
    out = {}
    for d, g in p[[ "date", x, y]].dropna().groupby("date"):
        if len(g) >= MIN_XS and g[x].nunique() > 1 and g[y].nunique() > 1:
            out[d] = spearmanr(g[x], g[y]).statistic
    return pd.Series(out).sort_index()


def nonoverlap_stats(ic: pd.Series, h: int):
    """Mean IC and t over non-overlapping dates: every h-th date, averaged over the h offsets."""
    if ic.empty:
        return {"ic": None, "t": None, "n_dates": 0}
    means, ts = [], []
    for o in range(h):
        s = ic.iloc[o::h].dropna()
        if len(s) < 5:
            continue
        sd = s.std(ddof=1)
        means.append(s.mean())
        ts.append(s.mean() / (sd / math.sqrt(len(s))) if sd > 0 else float("nan"))
    if not means:
        return {"ic": None, "t": None, "n_dates": len(ic)}
    return {"ic": round(float(np.mean(means)), 4), "t": round(float(np.nanmean(ts)), 2), "n_dates": int(len(ic))}


def ts_ic(p: pd.DataFrame, x: str, y: str):
    vals = {}
    for cx, g in p[["cx", x, y]].dropna().groupby("cx"):
        if len(g) >= 30 and g[x].nunique() > 1:
            vals[cx] = spearmanr(g[x], g[y]).statistic
    v = pd.Series(vals)
    return {"median": round(float(v.median()), 4) if len(v) else None,
            "positive": int((v > 0).sum()), "n": int(len(v))}


def residualize(p: pd.DataFrame, y: str) -> pd.Series:
    """Target minus its per-complex training-period OLS fit on C."""
    res = pd.Series(np.nan, index=p.index)
    for cx, g in p.groupby("cx"):
        tr = g[(g.date <= TRAIN_END)][["C", y]].dropna()
        if len(tr) < 30:
            continue
        b, a = np.polyfit(tr.C, tr[y], 1)
        res.loc[g.index] = g[y] - (a + b * g.C)
    return res


SIG = [(1.5, "STRONG_BUY", 1), (0.5, "BUY", 1), (-0.5, "NEUTRAL", 0), (-1.5, "SELL", -1)]


def side(z):
    if z is None or (isinstance(z, float) and math.isnan(z)):
        return None, None
    for thr, name, s in SIG:
        if z > thr:
            return name, s
    return "STRONG_SELL", -1


def main():
    df = load_panel()
    exp = oi_expiries.load()
    p = build(df, exp)
    periods = {"train": p[p.date <= TRAIN_END], "test": p[p.date >= TEST_START]}
    R = {"complexes": sorted(p.cx.unique()), "rows": int(len(p)),
         "date_range": [p.date.min(), p.date.max()],
         "oi_coverage": {v: round(float(p[v].notna().mean()), 3) for v in ("OI_lots", "OI_val")},
         "stat1": {}, "stat2": {}, "stat3": {}, "stat4": {}, "stat5": {}}

    preds = ["C"] + OI_VARS
    for per, q in periods.items():
        for h in HORIZONS:
            y = f"fwd_{h}"
            for x in preds:
                R["stat1"][f"{per}|h{h}|{x}"] = nonoverlap_stats(xs_ic_by_date(q, x, y), h)
                R["stat2"][f"{per}|h{h}|{x}"] = ts_ic(q, x, y)

    for h in HORIZONS:
        y = f"fwd_{h}"
        p[f"res_{h}"] = residualize(p, y)
    periods = {"train": p[p.date <= TRAIN_END], "test": p[p.date >= TEST_START]}
    for per, q in periods.items():
        for h in HORIZONS:
            for v in OI_VARS:
                R["stat3"][f"{per}|h{h}|{v}"] = nonoverlap_stats(xs_ic_by_date(q, v, f"res_{h}"), h)

    test = periods["test"].copy()
    for v in OI_VARS:
        test[f"C_{v}"] = test[["to_z", "vol_z", v]].mean(axis=1, skipna=True)
    for h in HORIZONS:
        y = f"fwd_{h}"
        base = nonoverlap_stats(xs_ic_by_date(test, "C", y), h)["ic"]
        for v in OI_VARS:
            w = nonoverlap_stats(xs_ic_by_date(test, f"C_{v}", y), h)["ic"]
            R["stat4"][f"h{h}|{v}"] = {"C_OI_ic": w, "C_ic": base,
                                       "diff": round(w - base, 4) if w is not None and base is not None else None}

    for v in OI_VARS:
        t = test[["C", f"C_{v}", "fwd_10"]].dropna()
        old = [side(z) for z in t.C]
        new = [side(z) for z in t[f"C_{v}"]]
        flipped = [i for i in range(len(t)) if old[i][0] != new[i][0]]
        sgn = np.sign(t.fwd_10.values)
        agree_old = sum(1 for i in flipped if old[i][1] and old[i][1] == sgn[i])
        agree_new = sum(1 for i in flipped if new[i][1] and new[i][1] == sgn[i])
        R["stat5"][v] = {"test_days": int(len(t)), "flipped": len(flipped),
                         "flip_share": round(len(flipped) / len(t), 3) if len(t) else None,
                         "flipped_agree_old": agree_old, "flipped_agree_new": agree_new}

    # Decision rule (pre-registered)
    verdict = {}
    for v in OI_VARS:
        te10, tr10 = R["stat3"][f"test|h10|{v}"], R["stat3"][f"train|h10|{v}"]
        a = te10["ic"] is not None and te10["ic"] > 0 and (te10["t"] or 0) >= 2
        b = tr10["ic"] is not None and te10["ic"] is not None and np.sign(tr10["ic"]) == np.sign(te10["ic"])
        c = (R["stat4"]["h10|" + v]["diff"] or -1) >= 0.02
        d = all((R["stat3"][f"test|h{h}|{v}"]["ic"] or 0) >= 0 for h in (5, 20))
        verdict[v] = {"a": bool(a), "b": bool(b), "c": bool(c), "d": bool(d), "pass": bool(a and b and c and d)}
    passing = [v for v in OI_VARS if verdict[v]["pass"]]
    R["decision"] = {"by_variant": verdict,
                     "include_oi": bool(passing),
                     "chosen": max(passing, key=lambda v: R["stat3"][f"test|h10|{v}"]["ic"]) if passing else None}

    out = sys.argv[sys.argv.index("--json") + 1] if "--json" in sys.argv else None
    if out:
        json.dump(R, open(out, "w"), indent=1, default=float)
    print(json.dumps(R, indent=1, default=float))


if __name__ == "__main__":
    main()
