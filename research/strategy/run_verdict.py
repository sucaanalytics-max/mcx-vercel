#!/usr/bin/env python3
"""Run the pre-registered ADR-band test (docs/strategy/2026-11-preregistration.md).

  scripts/mcx_uv.sh python -I research/strategy/run_verdict.py     # writes docs/strategy/results/
"""
from __future__ import annotations
import json, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import numpy as np
import pandas as pd
from research.strategy import data, bands, backtest as bt

OUT = Path(__file__).resolve().parents[2] / "docs" / "strategy" / "results"
HORIZONS = (5, 10, 20, 60)
# name, ticker, eval start, train end, fit-data start (None = all)
STOCKS = [
    ("MCX", "MCX.NS", "2022-07-01", "2025-03-31", None),
    ("MCX_post", "MCX.NS", "2025-05-01", "2025-12-31", "2024-11-01"),   # post fee-change sample
    ("BSE", "BSE.NS", "2024-01-01", "2024-12-31", None),
]
FITS = {"exp": dict(window=None, log=False), "roll250": dict(window=250, log=False),
        "log": dict(window=None, log=True)}


def build(panel: pd.DataFrame, w: int, fit: str, min_obs: int = 120) -> pd.DataFrame:
    df = panel.copy()
    df["adr"] = bands.adr(df.rev, w)
    df["ma10"] = bands.adr(df.rev, 10)
    df["ma15"] = bands.adr(df.rev, 15)
    df["ma45"] = bands.adr(df.rev, 45)
    f = bands.expanding_fit(df.adr, df.close, min_obs=min_obs, **FITS[fit])
    df["fit"], df["z"] = f["fit"], f["z"]
    df["ratio"] = df.ma10 / df.ma45
    df["ratio15"] = df.ma15 / df.ma45
    df["turn"] = bands.turning_state(df.z, df.ratio)
    m20, s20 = df.rev.rolling(20).mean(), df.rev.rolling(20).std()
    df["revz20"] = (df.rev - m20) / s20
    return df


def run_stock(name, ticker, start, train_end, fit_start):
    panel = data.panel(name.split("_")[0])
    if fit_start:
        panel = panel[panel.index >= fit_start]
    px = data.prices(ticker)
    px = px[px.index >= panel.index[0]]
    res = {"coint": {}, "events": {}, "rules": {}, "variants": {}}
    # 1. cointegration on the evaluation sample (level and log, 45-day ADR)
    base_df = build(panel, 45, "exp")
    ev = base_df[base_df.index >= start].dropna(subset=["adr"])
    for lg in (False, True):
        res["coint"]["log" if lg else "level"] = bands.cointegration(ev.adr, ev.close, log=lg)
    # 2. event study on the primary build
    fwd_all = bt.forward_returns(ev.index, px, HORIZONS)
    def evs(mask, sign):
        starts = bands.episode_starts(mask)
        dates = starts[starts].index
        f = bt.forward_returns(dates, px, HORIZONS)
        return {f"h{h}": bt.event_stats(f, fwd_all, h, sign) for h in HORIZONS} | {"dates": [str(d.date()) for d in dates]}
    cross_up = (ev.ratio > 1) & (ev.ratio.shift(1) <= 1)
    cross_dn = (ev.ratio < 1) & (ev.ratio.shift(1) >= 1)
    res["events"] = {
        "z<=-2 (long)": evs(ev.z <= -2, 1), "z>=+2 (short)": evs(ev.z >= 2, -1),
        "z<=-1 (long)": evs(ev.z <= -1, 1), "z>=+1 (short)": evs(ev.z >= 1, -1),
        "turning up (long)": evs(ev.turn == 1, 1), "turning down (short)": evs(ev.turn == -1, -1),
        "10/45 cross up (long)": evs(cross_up, 1), "10/45 cross down (short)": evs(cross_dn, -1),
    }
    # H4: forward returns on all days with ratio>1 vs ratio<1 (overlapping; descriptive)
    fa = fwd_all.set_index("date")
    up = fa[ev.ratio.reindex(fa.index) > 1]
    dn = fa[ev.ratio.reindex(fa.index) < 1]
    res["h4"] = {f"h{h}": {"ratio>1 mean": round(up[f"r{h}"].mean() * 100, 2),
                          "ratio>1 share negative": round((up[f"r{h}"] < 0).mean() * 100),
                          "ratio<1 mean": round(dn[f"r{h}"].mean() * 100, 2), "n_up": len(up), "n_dn": len(dn)}
                 for h in (10, 20)}
    # 3. rules and grid
    periods = {"train": (start, train_end), "test": (pd.Timestamp(train_end) + pd.Timedelta(days=1), None),
               "full": (start, None)}
    def score(target, long_only=False):
        sim = bt.simulate(target[target.index >= start], px[px.index >= start], long_only)
        return sim, {p: bt.metrics(sim, px, a, b) for p, (a, b) in periods.items()}
    trials = {}
    for w in (30, 45, 60):
        for fit in FITS:
            df = build(panel, w, fit)
            for k in (1.5, 2.0, 2.5):
                _, m = score(bt.rule_band(df.z, k))
                trials[f"R1 w{w} {fit} k{k}"] = m
            _, m = score(bt.rule_turning(df.turn))
            trials[f"R2 w{w} {fit}"] = m
    for w in (30, 45, 60):
        df = build(panel, 45, "exp")
        r = bands.adr(df.rev, 10) / bands.adr(df.rev, w) if w != 45 else df.ratio
        _, m = score(bt.rule_trend(r))
        trials[f"R4 10/{w}"] = m
    res["variants"] = trials
    test_srs = [v["test"].get("sr_daily", np.nan) for v in trials.values() if v.get("test")]
    primary = {"R1 band (2 SD)": bt.rule_band(base_df.z, 2.0), "R2 turning": bt.rule_turning(base_df.turn),
               "R4 trend 10/45": bt.rule_trend(base_df.ratio),
               "Buy and hold": pd.Series(1.0, index=base_df.index),
               "Rev z20>0.5 hold 5d (Mar-26 brief)": bt.rule_hold_after(base_df.revz20 > 0.5, 5)}
    for label, tgt in primary.items():
        for lo in (False, True):
            if lo and label in ("Buy and hold", "Rev z20>0.5 hold 5d (Mar-26 brief)"):
                continue
            sim, m = score(tgt, lo)
            t = m["test"]
            if t:
                t["dsr"] = round(bt.deflated_sharpe(t["sr_daily"], t["days"], t["skew"], t["kurt"], test_srs), 2)
            key = label + (" long-only" if lo else "")
            res["rules"][key] = m
            if not lo and label.startswith(("R1", "R2", "R4")):
                tr = bt.trades(sim, px[px.index >= start])
                tr.to_csv(OUT / f"trades_{name}_{label.split()[0]}.csv", index=False)
                (1 + sim.net).cumprod().to_csv(OUT / f"equity_{name}_{label.split()[0]}.csv")
    # 4. today
    last = base_df.dropna(subset=["z"]).iloc[-1]
    wk = base_df.dropna(subset=["z"]).tail(5)
    res["today"] = {"date": str(last.name.date()), "close": round(last.close, 1), "fit": round(last.fit, 1),
                    "z": round(last.z, 2), "z_week_avg": round(wk.z.mean(), 2), "ratio_10_45": round(last.ratio, 3),
                    "adr45": round(last.ma45, 2), "state": bands.band_state(last.z, last.ratio),
                    "sd_rs": round((last.close - last.fit) / last.z, 1) if last.z else None}
    fl = bands.expanding_fit(base_df.adr, base_df.close, min_obs=120).dropna().iloc[-1]
    res["today"]["fit_a"], res["today"]["fit_b"] = round(fl.a, 1), round(fl.b, 1)
    res["today"]["band_minus2"] = round(last.fit - 2 * res["today"]["sd_rs"], 1)
    res["today"]["band_plus2"] = round(last.fit + 2 * res["today"]["sd_rs"], 1)
    # z history for charts
    base_df[["close", "fit", "z", "ratio", "ma45"]].dropna().to_csv(OUT / f"z_{name}.csv")
    return res


def pooled(results):
    """Pool R1/R2 event excess returns across MCX and BSE (20d), per direction."""
    out = {}
    for ev in ("z<=-2 (long)", "z>=+2 (short)", "turning up (long)", "turning down (short)",
               "10/45 cross up (long)", "10/45 cross down (short)"):
        ns, ex, hits = 0, [], 0
        for s in ("MCX", "BSE"):
            e = results[s]["events"][ev]["h20"]
            if e.get("n", 0) >= 1 and e.get("excess") is not None:
                ns += e["n"]; ex.append((e["excess"], e["n"]))
        out[ev] = {"n": ns, "weighted_excess_20d": round(sum(a * n for a, n in ex) / ns, 2) if ns else None}
    return out


def pooled_trades():
    """Pooled trade-level mean and t-stat across MCX and BSE, test period, per rule."""
    out = {}
    for rule in ("R1", "R2", "R4"):
        rets = []
        for name, _, _, train_end, _ in STOCKS:
            if name == "MCX_post":
                continue
            tr = pd.read_csv(OUT / f"trades_{name}_{rule}.csv", parse_dates=["entry"])
            rets += list(tr[tr.entry > train_end].ret)
        r = pd.Series(rets, dtype=float)
        t = r.mean() / (r.std(ddof=1) / np.sqrt(len(r))) if len(r) > 1 and r.std(ddof=1) > 0 else float("nan")
        out[rule] = {"n": len(r), "mean_pct": round(r.mean() * 100, 2), "t": round(t, 2)}
    return out


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    results = {name: run_stock(name, *rest) for name, *rest in STOCKS}
    results["pooled"] = pooled(results)
    results["pooled_trades_test"] = pooled_trades()
    (OUT / "results.json").write_text(json.dumps(results, indent=1, default=str))
    for name, r in results.items():
        if name.startswith("pooled"):
            continue
        print(f"\n===== {name}  today: {r['today']}")
        print("coint:", r["coint"])
        for k, v in r["events"].items():
            print(f"  {k:28s} n={v['h20'].get('n')}  " + "  ".join(
                f"{h}: {v[h].get('mean')}% ex {v[h].get('excess')} t {v[h].get('t')} hit {v[h].get('hit')}" for h in ("h10", "h20", "h60")))
        print("  H4:", r["h4"])
        for k, v in r["rules"].items():
            print(f"  {k:40s} " + " | ".join(
                f"{p}: SR {v[p].get('sharpe')} CAGR {v[p].get('cagr')} DD {v[p].get('max_dd')} n {v[p].get('trades')} hit {v[p].get('hit')} avg {v[p].get('avg_trade')}"
                + (f" DSR {v[p].get('dsr')}" if p == 'test' else "") for p in ("train", "test") if v.get(p)))
    print("\npooled:", results["pooled"])
    print("pooled trades (test):", results["pooled_trades_test"])
    vt = {k: v["test"].get("sharpe") for k, v in results["BSE"]["variants"].items() if v.get("test")}
    print("\nBSE test Sharpe by variant:", vt)
    vt = {k: v["test"].get("sharpe") for k, v in results["MCX"]["variants"].items() if v.get("test")}
    print("\nMCX test Sharpe by variant:", vt)


if __name__ == "__main__":
    main()
