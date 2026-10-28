#!/usr/bin/env python3
"""ADR-trend edge stress test — implements Docs/ADR_Edge_StressTest_Preregistration.md.

Parts: B = walk-forward on the stock, E = feature screen of other MCX data,
C = option pricing from NSE bhavcopy (needs scripts/fetch_nse_fo_bhav.py output).

Run:
  uv run --no-project --with scipy --with yfinance --with pandas --with numpy \
    python -I scripts/adr_stress_test.py B|E|C [BHAV_DIR]
"""
from __future__ import annotations
import sys, json, math, urllib.request
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import numpy as np, pandas as pd, yfinance as yf
from scipy import stats
from lib.mcx_config import SUPABASE_URL, SUPABASE_ANON_KEY

H = {"apikey": SUPABASE_ANON_KEY, "Authorization": "Bearer " + SUPABASE_ANON_KEY}
VARIANTS = [(w, L) for w in (20, 45, 60) for L in (10, 20, 40)]
PRIMARY = (45, 20)
TEST_YEARS = (2023, 2024, 2025, 2026)


def fetch(table, select, order, flt=""):
    rows, off = [], 0
    while True:
        url = f"{SUPABASE_URL}/rest/v1/{table}?select={select}&order={order}{flt}&offset={off}&limit=1000"
        batch = json.loads(urllib.request.urlopen(urllib.request.Request(url, headers=H)).read())
        rows += batch
        if len(batch) < 1000:
            return pd.DataFrame(rows)
        off += 1000


def spearman(x, y, min_n=8):
    m = x.notna() & y.notna()
    if m.sum() < min_n:
        return None
    return float(stats.spearmanr(x[m], y[m])[0])


def ic_nonoverlap(df, sig, ret, h, min_n=8):
    """Mean Spearman IC over the h non-overlapping subsamples; t uses the per-subsample n."""
    vals, ns = [], []
    for off in range(h):
        sub = df.iloc[off::h]
        r = spearman(sub[sig], sub[ret], min_n)
        if r is not None and not math.isnan(r):
            vals.append(r); ns.append(int((sub[sig].notna() & sub[ret].notna()).sum()))
    if not vals:
        return None
    m, n = float(np.mean(vals)), int(np.mean(ns))
    t = m * math.sqrt(max(n - 2, 1)) / math.sqrt(max(1e-9, 1 - m * m))
    return {"ic": round(m, 3), "t": round(t, 2), "n": n}


def revenue():
    rev = fetch("mcx_daily_revenue", "trading_date,total_rev_cr", "trading_date.asc")
    rev["trading_date"] = pd.to_datetime(rev["trading_date"])
    return rev[rev["total_rev_cr"] > 0].set_index("trading_date")["total_rev_cr"].sort_index()


def adr_trend(rev, w, L):
    adr = rev.rolling(w, min_periods=w).mean()
    return np.log(adr / adr.shift(L))


def stock_daily():
    d = yf.download("MCX.NS", start="2021-12-01", interval="1d", auto_adjust=True, progress=False)
    d.columns = [c[0] if isinstance(c, tuple) else c for c in d.columns]
    d.index = pd.to_datetime(d.index)
    return d


def forward_frame(signals: dict, px, horizons=(10, 20)):
    """One row per MCX signal date: entry = next NSE open, exit = close h NSE days later.
    exit_date is kept so walk-forward windows can use only fully-known returns."""
    nse = px.index
    rows = []
    any_sig = next(iter(signals.values()))
    for d in any_sig.index:
        i = nse.searchsorted(d, side="right")  # first NSE day strictly after d
        if i >= len(nse):
            continue
        r = {"date": d, "year": d.year}
        o = px["Open"].iloc[i]
        for h in horizons:
            j = i + h - 1
            r[f"ret_{h}"] = px["Close"].iloc[j] / o - 1 if j < len(nse) else np.nan
            r[f"exit_{h}"] = nse[j] if j < len(nse) else pd.NaT
        for k, s in signals.items():
            r[k] = s.get(d, np.nan)
        rows.append(r)
    return pd.DataFrame(rows)


# ---------------------------------------------------------------- Part B
def part_b():
    rev, px = revenue(), stock_daily()
    sigs = {f"v{w}_{L}": adr_trend(rev, w, L) for w, L in VARIANTS}
    df = forward_frame(sigs, px)
    pk = f"v{PRIMARY[0]}_{PRIMARY[1]}"
    out = {"per_year_primary": {}, "pooled_oos_primary": {}, "variants_pooled_oos_20d": {},
           "walk_forward_selection": {}}
    for y in range(2022, 2027):
        sub = df[df["year"] == y]
        out["per_year_primary"][y] = {h: ic_nonoverlap(sub, pk, f"ret_{h}", h) for h in (10, 20)}
    oos = df[df["year"] >= 2023]
    out["pooled_oos_primary"] = {h: ic_nonoverlap(oos, pk, f"ret_{h}", h) for h in (10, 20)}
    for k in sigs:
        out["variants_pooled_oos_20d"][k] = ic_nonoverlap(oos, k, "ret_20", 20)
    # walk-forward selection: choose by 20d IC on returns fully known before Jan 1 of Y
    picked = []
    for y in TEST_YEARS:
        cut = pd.Timestamp(f"{y}-01-01")
        train = df[df["exit_20"] < cut]
        scores = {k: (ic_nonoverlap(train, k, "ret_20", 20) or {"ic": -9})["ic"] for k in sigs}
        best = max(scores, key=scores.get)
        test = df[df["year"] == y][["date", "ret_10", "ret_20", best]].rename(columns={best: "sel"})
        picked.append(test)
        out["walk_forward_selection"][y] = {"picked": best, "train_ic": scores[best],
                                            "test_ic_20d": ic_nonoverlap(test, "sel", "ret_20", 20)}
    sel = pd.concat(picked).sort_values("date")
    out["walk_forward_selection"]["pooled_oos"] = {h: ic_nonoverlap(sel, "sel", f"ret_{h}", h) for h in (10, 20)}
    pos = sum(1 for v in out["variants_pooled_oos_20d"].values() if v and v["ic"] > 0)
    yrs_pos = sum(1 for y in TEST_YEARS if (out["per_year_primary"][y][20] or {"ic": -1})["ic"] > 0)
    p = out["pooled_oos_primary"][20]
    out["criteria"] = {
        "1_pooled_oos_ic>0_t>=2": bool(p and p["ic"] > 0 and p["t"] >= 2),
        "2_positive_in_>=3_of_4_years": f"{yrs_pos}/4",
        "3_>=6_of_9_variants_positive": f"{pos}/9",
    }
    return out


# ---------------------------------------------------------------- Part E
def part_e():
    rev, px = revenue(), stock_daily()
    base = adr_trend(rev, *PRIMARY)
    feats = {}
    cd = fetch("mcx_commodity_daily", "trading_date,commodity_head,instrument_type,turnover_cr", "trading_date.asc")
    cd["trading_date"] = pd.to_datetime(cd["trading_date"])
    tot = cd.groupby("trading_date")["turnover_cr"].sum()
    opt = cd[cd["instrument_type"].isin(["OPTFUT", "OPTIDX"])].groupby("trading_date")["turnover_cr"].sum().reindex(tot.index).fillna(0)
    bul = cd[cd["commodity_head"] == "BULLION"].groupby("trading_date")["turnover_cr"].sum().reindex(tot.index).fillna(0)
    o20 = opt.rolling(20).mean()
    feats["E1_opt_turnover_trend"] = np.log(o20 / o20.shift(20))
    share = (opt / tot).rolling(20).mean()
    feats["E2_opt_share_shift"] = share - share.shift(20)
    bshare = (bul / tot).rolling(20).mean()
    feats["E3_bullion_share_shift"] = bshare - bshare.shift(20)
    ic_ = fetch("mcx_icomdex_daily", "trading_date,close", "trading_date.asc", "&index_code=eq.MCXCOMPDEX")
    ic_["trading_date"] = pd.to_datetime(ic_["trading_date"])
    c = ic_.set_index("trading_date")["close"].astype(float).sort_index()
    rv = np.log(c).diff().rolling(20).std() * math.sqrt(252)
    feats["E4_comdex_vol_trend"] = np.log(rv / rv.shift(20))
    val = fetch("mcx_valuation", "trading_date,close_price,fair_value_base", "trading_date.asc")
    val["trading_date"] = pd.to_datetime(val["trading_date"])
    val = val.set_index("trading_date")
    feats["E5_valuation_gap"] = val["close_price"].astype(float) / val["fair_value_base"].astype(float) - 1
    ms = fetch("mcx_model_signals", "trading_date,ensemble_score", "trading_date.asc")
    ms["trading_date"] = pd.to_datetime(ms["trading_date"])
    feats["E6_ensemble_score"] = ms.set_index("trading_date")["ensemble_score"].astype(float)
    # Deviation (logged in the pre-registration): older mcxccl rows have no expiry and a different
    # "total" definition, so use initial_margin_pct and drop changes that straddle a source switch.
    mg = fetch("mcx_margin_daily", "snapshot_date,symbol,instrument,expiry,initial_margin_pct,source", "snapshot_date.asc",
               "&symbol=in.(GOLD,SILVER,CRUDEOIL)&instrument=eq.FUTCOM")
    mg["snapshot_date"] = pd.to_datetime(mg["snapshot_date"])
    mg["expiry"] = pd.to_datetime(mg["expiry"], errors="coerce")
    mg = mg[(mg["expiry"].isna() | (mg["expiry"] >= mg["snapshot_date"])) & mg["initial_margin_pct"].notna()]
    near = mg.sort_values("expiry", na_position="first").groupby(["snapshot_date", "symbol"]).first().reset_index()
    lvl = near.groupby("snapshot_date")["initial_margin_pct"].mean().astype(float)
    src = near.groupby("snapshot_date")["source"].agg(lambda s: s.mode().iloc[0])
    e7 = lvl - lvl.shift(20)
    feats["E7_margin_trend"] = e7[src == src.shift(20)]
    op = fetch("mcx_oi_participants", "*", "report_date.asc")
    op["report_date"] = pd.to_datetime(op["report_date"])
    sides = [c for c in op.columns if c.endswith("_long") or c.endswith("_short")]
    op[sides] = op[sides].apply(pd.to_numeric, errors="coerce")
    g = op.groupby("report_date")[sides].sum()
    pshare = ((g["prop_long"] + g["prop_short"]) / g.sum(axis=1)).rolling(20).mean()
    feats["E8_prop_share_shift"] = pshare - pshare.shift(20)

    out = {}
    for k, f in feats.items():
        f = f.dropna().sort_index()
        sig = {"f": f, "base": base.reindex(f.index)}
        df = forward_frame(sig, px, horizons=(20,)).dropna(subset=["f", "ret_20"])
        # partial: rank-residualise the feature on the ADR trend
        m = df["base"].notna()
        rf, rb = df.loc[m, "f"].rank(), df.loc[m, "base"].rank()
        b = np.polyfit(rb, rf, 1)
        df.loc[m, "f_resid"] = rf - np.polyval(b, rb)
        out[k] = {"from": str(df["date"].min().date()), "raw": ic_nonoverlap(df, "f", "ret_20", 20),
                  "partial_vs_adr": ic_nonoverlap(df[m], "f_resid", "ret_20", 20),
                  "corr_with_adr": round(float(stats.spearmanr(df.loc[m, "f"], df.loc[m, "base"])[0]), 2)}
    return out


# ---------------------------------------------------------------- Part C
from lib.options_costs import (FUT_TXN, FUT_STT_SELL, GST, BROKERAGE, DEFAULT_LOT, TICK,
                               leg_fees)
HALF_SPREADS = (0.0, 0.01, 0.025, 0.05, 0.10)
SPLIT_DATE = "2026-01-02"  # MCX 1:5 split; bhavcopy prices switch units on this day


def leg_cost(price, qty_sign, lot, h, opening):
    """Per-share cost of one leg trade. qty_sign +1 buy / -1 sell."""
    slip = max(price * h, TICK) if h > 0 else 0.0
    return slip + leg_fees(price, qty_sign, lot)


def part_c(bhav_dir: Path):
    bh = pd.read_csv(bhav_dir / "mcx_bhav.csv", dtype={"opt": str})
    for col in ("strike", "close", "settle", "contracts", "lot"):
        bh[col] = pd.to_numeric(bh[col], errors="coerce")
    sched = json.loads((bhav_dir / "schedule.json").read_text())
    rev = revenue()
    sig = adr_trend(rev, *PRIMARY)
    sig_dates = sig.dropna().index

    def z_at(d):
        prior = sig[sig.index < d].dropna()
        if len(prior) < 120:
            return None, None
        s = prior.iloc[-1]
        hist = prior.iloc[:-1]
        return float(s), float((s - hist.mean()) / hist.std())

    def quote(day, inst, exp, strike=0, opt="XX"):
        q = bh[(bh["date"] == day) & (bh["inst"] == inst) & (bh["expiry"] == exp)]
        if inst == "OPTSTK":
            q = q[(q["strike"] == strike) & (q["opt"] == opt)]
        if q.empty:
            return None, None
        r = q.iloc[0]
        stale = not (r["contracts"] > 0)
        px = r["settle"] if stale or not (r["close"] > 0) else r["close"]
        return float(px), stale

    trades = []
    for s in sched:
        exp = s["expiry"]
        for sch, entry in (("A", s["entry_A"]), ("B", s["entry_B"])):
            exit_ = s["exit"]
            if entry < SPLIT_DATE <= exit_:
                continue  # entry in pre-split prices, exit in post-split prices
            raw, z = z_at(pd.Timestamp(entry))
            if raw is None:
                continue
            F0, _ = quote(entry, "FUTSTK", exp)
            F1, _ = quote(exit_, "FUTSTK", exp)
            if not F0 or not F1:
                continue
            lots = bh[(bh["date"] == entry) & (bh["expiry"] == exp)]["lot"].dropna()
            lot = float(lots.iloc[0]) if len(lots) else DEFAULT_LOT
            strikes = np.array(sorted(bh[(bh["date"] == entry) & (bh["inst"] == "OPTSTK") & (bh["expiry"] == exp)]["strike"].unique()))
            if len(strikes) < 3:
                continue
            atm = float(strikes[np.abs(strikes - F0).argmin()])
            c_otm = float(strikes[np.abs(strikes - F0 * 1.05).argmin()])
            p_otm = float(strikes[np.abs(strikes - F0 * 0.95).argmin()])
            for direction in (+1, -1):
                # legs: (strike, opt, qty) with qty +1 long / -1 short
                if direction > 0:
                    legs = {"S1": [(atm, "CE", 1)], "S2": [(atm, "CE", 1), (c_otm, "CE", -1)],
                            "S3": [(atm, "PE", -1), (p_otm, "PE", 1)]}
                else:
                    legs = {"S1": [(atm, "PE", 1)], "S2": [(atm, "PE", 1), (p_otm, "PE", -1)],
                            "S3": [(atm, "CE", -1), (c_otm, "CE", 1)]}
                for name, L in legs.items():
                    if len({k for k, _, _ in L}) < len(L):
                        continue  # OTM collapsed onto ATM
                    q0 = [quote(entry, "OPTSTK", exp, k, o) for k, o, _ in L]
                    q1 = [quote(exit_, "OPTSTK", exp, k, o) for k, o, _ in L]
                    if any(p is None for p, _ in q0 + q1):
                        continue
                    stale = any(st for _, st in q0 + q1)
                    gross = sum(qty * (q1[i][0] - q0[i][0]) for i, (_, _, qty) in enumerate(L))
                    net_entry = sum(qty * q0[i][0] for i, (_, _, qty) in enumerate(L))
                    width = abs(L[0][0] - L[1][0]) if len(L) == 2 else None
                    risk = net_entry if name in ("S1", "S2") else (width - (-net_entry))
                    if risk is None or risk <= 0:
                        continue
                    for h in HALF_SPREADS:
                        cost = sum(leg_cost(q0[i][0], qty, lot, h, True) + leg_cost(q1[i][0], -qty, lot, h, False)
                                   for i, (_, _, qty) in enumerate(L))
                        trades.append({"sched": sch, "expiry": exp, "entry": entry, "struct": name, "dir": direction,
                                       "sig": raw, "z": z, "h": h, "ret": (gross - cost) / risk, "stale": stale})
                # S0 futures
                for h in HALF_SPREADS:
                    cost = F0 * FUT_TXN * (1 + GST) + F1 * FUT_TXN * (1 + GST) + (F1 if direction > 0 else F0) * FUT_STT_SELL + 2 * BROKERAGE / lot
                    trades.append({"sched": sch, "expiry": exp, "entry": entry, "struct": "S0", "dir": direction,
                                   "sig": raw, "z": z, "h": h, "ret": (direction * (F1 - F0) - cost) / F0, "stale": False})
    T = pd.DataFrame(trades)
    T.to_csv(bhav_dir / "trades_all.csv", index=False)

    def summarise(x):
        r = x["ret"]
        cum = r.cumsum()
        return {"n": int(len(r)), "stale": int(x["stale"].sum()), "mean": round(r.mean(), 4), "median": round(r.median(), 4),
                "hit": round(float((r > 0).mean()), 3), "t": round(r.mean() / r.std() * math.sqrt(len(r)), 2) if len(r) > 2 and r.std() > 0 else None,
                "sum": round(r.sum(), 3), "worst": round(r.min(), 3), "max_dd": round(float((cum.cummax() - cum).max()), 3)}

    out = {}
    for sch in ("A", "B"):
        for st in ("S0", "S1", "S2", "S3"):
            base_rows = T[(T["sched"] == sch) & (T["struct"] == st)]
            for h in HALF_SPREADS:
                b = base_rows[base_rows["h"] == h]
                r1 = b[np.sign(b["sig"]) == b["dir"]]
                r2 = r1[r1["z"].abs() >= 0.5]
                bench = b[b["dir"] == 1]
                key = f"{sch}|{st}|h={h}"
                out[key] = {"R1": summarise(r1), "R2": summarise(r2), "bench_always_bull": summarise(bench),
                            "R1_bull": summarise(r1[r1["dir"] == 1]), "R1_bear": summarise(r1[r1["dir"] == -1])}
                if h == 0.025:
                    out[key]["R1_by_year"] = r1.groupby(r1["entry"].str[:4])["ret"].mean().round(3).to_dict()
            # breakeven half-spread for R1 and R2 (linear interpolation on the grid)
            for rule in ("R1", "R2"):
                xs, ys = [], []
                for h in HALF_SPREADS:
                    xs.append(h); ys.append(out[f"{sch}|{st}|h={h}"][rule]["mean"])
                be = None
                for i in range(len(xs) - 1):
                    if ys[i] > 0 >= ys[i + 1]:
                        be = xs[i] + (xs[i + 1] - xs[i]) * ys[i] / (ys[i] - ys[i + 1])
                if ys[0] <= 0:
                    be = "negative even at zero spread"
                elif be is None:
                    be = f">{xs[-1]}"
                out[f"{sch}|{st}|breakeven_{rule}"] = be
    return out


if __name__ == "__main__":
    part = sys.argv[1]
    res = {"B": part_b, "E": part_e}.get(part, lambda: part_c(Path(sys.argv[2])))()
    print(json.dumps(res, indent=1, default=str))
