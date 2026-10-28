#!/usr/bin/env python3
"""MCX Ltd results-day options study (H3) — implements Docs/Results_Event_Study_Preregistration.md.

Parts: projection (our EPS vs consensus vs reported, walk-forward), events (realised vs
option-implied results-day move from NSE bhavcopy), live (implied event move from Dhan now).

Run:
  uv run --no-project --with scipy --with yfinance --with lxml --with pandas --with numpy \
      --with "dhanhq<2.1" --with python-dotenv python -I scripts/results_event_study.py OUT_DIR
"""
from __future__ import annotations
import sys, json, math, importlib.util, datetime as dt
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import numpy as np, pandas as pd, yfinance as yf
from scipy.stats import norm
from scipy.optimize import brentq
from lib.mcx_config import DILUTED_SHARES_CR, supabase_read_all
from lib.options_costs import leg_fees
from api.quarterly import QUARTERLY_ACTUALS, Q4_EXPENSE_ADJ_CR, _fit_expense_model, _compute_tax_dep_rate

_spec = importlib.util.spec_from_file_location("bhav", ROOT / "scripts" / "fetch_nse_fo_bhav.py")
bhav = importlib.util.module_from_spec(_spec); _spec.loader.exec_module(bhav)

R, HALF_SPREAD = 0.065, 0.025
NSE_CLOSE = dt.time(15, 30)


# ---------------------------------------------------------------- inputs
def events() -> pd.DataFrame:
    e = yf.Ticker("MCX.NS").get_earnings_dates(limit=40)
    e = e.rename(columns={"EPS Estimate": "cons_eps", "Reported EPS": "rep_eps"})
    e.index = e.index.tz_convert("Asia/Kolkata")
    e = e.reset_index().rename(columns={"Earnings Date": "ann_ist"})
    return e[e["ann_ist"] >= pd.Timestamp("2022-01-01", tz="Asia/Kolkata")].sort_values("ann_ist")


def prices():
    d = yf.download("MCX.NS", start="2021-12-01", interval="1d", auto_adjust=True, progress=False)
    d.columns = [c[0] if isinstance(c, tuple) else c for c in d.columns]
    d.index = pd.to_datetime(d.index).date
    return d


def daily_revenue() -> pd.Series:
    rows = supabase_read_all("mcx_daily_revenue", "?select=trading_date,total_rev_cr&order=trading_date.asc")
    s = pd.DataFrame(rows)
    s["trading_date"] = pd.to_datetime(s["trading_date"]).dt.date
    return s[s["total_rev_cr"] > 0].set_index("trading_date")["total_rev_cr"]


def quarter_of(ann: dt.date) -> tuple[dt.date, dt.date]:
    """The quarter whose results are announced on `ann` = the last quarter ended before it."""
    qe = [dt.date(ann.year - 1, 12, 31), dt.date(ann.year, 3, 31), dt.date(ann.year, 6, 30),
          dt.date(ann.year, 9, 30), dt.date(ann.year, 12, 31)]
    end = max(q for q in qe if q < ann)
    return dt.date(end.year, end.month - 2, 1), end


# ---------------------------------------------------------------- projection
def project(rev_sum: float, q_num: int, prior: list[dict]) -> dict:
    alpha, beta, _ = _fit_expense_model(prior)
    tax = _compute_tax_dep_rate(prior)
    exp = max(alpha + beta * rev_sum + (Q4_EXPENSE_ADJ_CR if q_num == 4 else 0), 80)
    pat = (rev_sum - exp) * (1 - tax)
    return {"rev_sum": round(rev_sum, 1), "exp": round(exp, 1), "pat": round(pat, 1), "eps": round(pat / DILUTED_SHARES_CR, 2)}


def part_projection(ev, rev, px):
    rows = []
    for i, a in enumerate(QUARTERLY_ACTUALS):
        if i < 3:
            continue
        s, e = dt.date.fromisoformat(a["start"]), dt.date.fromisoformat(a["end"])
        p = project(rev[(rev.index >= s) & (rev.index <= e)].sum(), a["q_num"], QUARTERLY_ACTUALS[:i])
        match = ev[[quarter_of(x.date())[1] == e for x in ev["ann_ist"]]]
        if match.empty:
            continue
        m = match.iloc[0]
        pre, react = event_days(m["ann_ist"], px)
        ret = px.loc[react, "Close"] / px.loc[pre, "Close"] - 1
        rows.append({"quarter": a["quarter"], "ours_eps": p["eps"], "cons_eps": m["cons_eps"], "rep_eps": m["rep_eps"],
                     "our_pat_eps": round(a["pat_cr"] / DILUTED_SHARES_CR, 2), "rev_sum": p["rev_sum"],
                     "rev_reported": a["revenue_cr"], "reaction_ret": round(ret, 4), "react_day": str(react)})
    df = pd.DataFrame(rows)
    df["err_ours"] = (df["ours_eps"] / df["rep_eps"] - 1).round(3)
    df["err_cons"] = (df["cons_eps"] / df["rep_eps"] - 1).round(3)
    df["gap"] = (df["ours_eps"] / df["cons_eps"] - 1).round(3)
    df["dir_hit"] = np.sign(df["gap"]) == np.sign(df["reaction_ret"])
    # Q2 FY27: full quarter, model fitted on every reported quarter
    s, e = dt.date(2026, 7, 1), dt.date(2026, 9, 30)
    q2 = project(rev[(rev.index >= s) & (rev.index <= e)].sum(), 2, QUARTERLY_ACTUALS)
    q2["days"] = int(((rev.index >= s) & (rev.index <= e)).sum())
    nxt = ev[ev["rep_eps"].isna()]
    q2["cons_eps"] = float(nxt["cons_eps"].iloc[0]) if len(nxt) else None
    q2["ann_ist"] = str(nxt["ann_ist"].iloc[0]) if len(nxt) else None
    q2["gap"] = round(q2["eps"] / q2["cons_eps"] - 1, 3) if q2["cons_eps"] else None
    summary = {"n": len(df), "mape_ours": round(df["err_ours"].abs().mean(), 3),
               "mape_cons": round(df["err_cons"].abs().mean(), 3),
               "bias_ours": round(df["err_ours"].mean(), 3), "dir_hit_rate": round(df["dir_hit"].mean(), 3)}
    return df, summary, q2


# ---------------------------------------------------------------- events
def event_days(ann: pd.Timestamp, px) -> tuple[dt.date, dt.date]:
    days = sorted(px.index)
    d = ann.date()
    if d in px.index and ann.time() < NSE_CLOSE:  # announced during market hours
        i = days.index(d)
        return days[i - 1], d
    pre = max(x for x in days if x <= d)
    react = min(x for x in days if x > d)
    return pre, react


def b76(F, K, T, sig, call):
    d1 = (math.log(F / K) + 0.5 * sig * sig * T) / (sig * math.sqrt(T)); d2 = d1 - sig * math.sqrt(T)
    df = math.exp(-R * T)
    return df * (F * norm.cdf(d1) - K * norm.cdf(d2)) if call else df * (K * norm.cdf(-d2) - F * norm.cdf(-d1))


def iv(price, F, K, T, call):
    try:
        return brentq(lambda s: b76(F, K, T, s, call) - price, 0.01, 5.0)
    except ValueError:
        return None


def chain_day(out_dir: Path, d: dt.date) -> pd.DataFrame:
    f = out_dir / f"mcx_{d}.csv"
    if not f.exists():
        data = bhav.fetch(d)
        rows = bhav.parse(data, d, "MCX") if data else []
        pd.DataFrame(rows, columns=bhav.COLS).to_csv(f, index=False)
    c = pd.read_csv(f)
    for col in ("strike", "close", "settle", "contracts"):
        c[col] = pd.to_numeric(c[col], errors="coerce")
    return c


def atm_iv(c: pd.DataFrame, expiry: str, d: dt.date, days) -> dict | None:
    fut = c[(c.inst == "FUTSTK") & (c.expiry == expiry)]
    opt = c[(c.inst == "OPTSTK") & (c.expiry == expiry)]
    if fut.empty or opt.empty:
        return None
    F = float(fut.iloc[0]["close"])
    T = max(sum(1 for x in days if d < x <= dt.date.fromisoformat(expiry)), 1) / 252
    K = min(opt.strike.unique(), key=lambda k: abs(k - F))
    out = {"F": F, "K": K, "T": T}
    for side in ("CE", "PE"):
        r = opt[(opt.strike == K) & (opt.opt == side)]
        if r.empty:
            return None
        r = r.iloc[0]
        p = r["close"] if r["contracts"] > 0 and r["close"] > 0 else r["settle"]
        out[side] = float(p); out[f"{side}_stale"] = not (r["contracts"] > 0)
        out[f"iv_{side}"] = iv(float(p), F, K, T, side == "CE")
    if out["iv_CE"] is None or out["iv_PE"] is None:
        return None
    out["iv"] = (out["iv_CE"] + out["iv_PE"]) / 2
    return out


def part_events(ev, px, out_dir: Path):
    days = sorted(px.index)
    rows = []
    for _, m in ev.iterrows():
        if pd.isna(m["rep_eps"]):
            continue
        pre, react = event_days(m["ann_ist"], px)
        realised = abs(px.loc[react, "Close"] / px.loc[pre, "Close"] - 1)
        r = {"ann_ist": str(m["ann_ist"]), "pre": str(pre), "react": str(react), "realised": round(realised, 4),
             "react_ret": round(px.loc[react, "Close"] / px.loc[pre, "Close"] - 1, 4)}
        c0, c1 = chain_day(out_dir, pre), chain_day(out_dir, react)
        exps = sorted(e for e in c0[c0.inst == "OPTSTK"].expiry.unique() if e > str(react))
        if exps and not c1.empty:
            e = exps[0]
            a0, a1 = atm_iv(c0, e, pre, days), atm_iv(c1, e, react, days)
            if a0 and a1:
                var_e = a0["iv"] ** 2 * a0["T"] - a1["iv"] ** 2 * a1["T"]
                r.update(expiry=e, iv_pre=round(a0["iv"], 4), iv_post=round(a1["iv"], 4),
                         iv_ratio=round(a1["iv"] / a0["iv"], 3), stale=a0["CE_stale"] or a0["PE_stale"],
                         implied=round(math.sqrt(2 / math.pi) * math.sqrt(var_e), 4) if var_e > 0 else 0.0)
                # long ATM straddle pre -> reaction, same strike, 2.5% half-spread + costs
                k = a0["K"]
                q1 = c1[(c1.inst == "OPTSTK") & (c1.expiry == e) & (c1.strike == k)]
                if len(q1) == 2:
                    exitp = {row.opt: (row.close if row.contracts > 0 and row.close > 0 else row.settle) for row in q1.itertuples()}
                    buy = sum(a0[s] * (1 + HALF_SPREAD) for s in ("CE", "PE"))
                    sell = sum(exitp[s] * (1 - HALF_SPREAD) for s in ("CE", "PE"))
                    lot = 225
                    cost = sum(leg_fees(a0[s], 1, lot) + leg_fees(exitp[s], -1, lot) for s in ("CE", "PE"))
                    r["straddle_ret"] = round((sell - buy - cost) / buy, 4)
        rows.append(r)
    df = pd.DataFrame(rows)
    ok = df.dropna(subset=["implied"]) if "implied" in df else df.iloc[0:0]
    ok = ok[ok["implied"] > 0]
    summary = {"events": len(df), "with_options": len(ok),
               "median_realised_all": round(df["realised"].median(), 4),
               "median_realised_since_2024": round(df[df["pre"] >= "2024-01-01"]["realised"].median(), 4),
               "median_implied": round(ok["implied"].median(), 4) if len(ok) else None,
               "median_ratio_realised_to_implied": round((ok["realised"] / ok["implied"]).median(), 3) if len(ok) else None,
               "share_realised_gt_implied": round(float((ok["realised"] > ok["implied"]).mean()), 3) if len(ok) else None,
               "median_iv_ratio_post_pre": round(ok["iv_ratio"].median(), 3) if len(ok) else None,
               "straddle_mean_ret": round(df["straddle_ret"].mean(), 4) if "straddle_ret" in df else None,
               "straddle_hit": round(float((df["straddle_ret"] > 0).mean()), 3) if "straddle_ret" in df else None,
               "straddle_n": int(df["straddle_ret"].notna().sum()) if "straddle_ret" in df else 0}
    return df, summary


# ---------------------------------------------------------------- live
def part_live(ann: pd.Timestamp | None, iv_ratio: float | None, px):
    from lib import dhan_client as dc
    c = dc.get_client()
    el = c.expiry_list(31181, "NSE_EQ")
    if el.get("status") != "success":
        return {"error": f"Dhan: {str(el.get('data'))[:120]}"}
    exp = el["data"]["data"][0]
    d = c.option_chain(31181, "NSE_EQ", exp)["data"]["data"]
    spot = d["last_price"]
    k = min((float(x) for x in d["oc"]), key=lambda x: abs(x - spot))
    leg = d["oc"][f"{k:.6f}"]
    ivs = [leg["ce"]["implied_volatility"] / 100, leg["pe"]["implied_volatility"] / 100]
    iv_now = sum(ivs) / 2
    today = dt.date.today()
    exp_d = dt.date.fromisoformat(exp)
    bdays = pd.bdate_range(today, exp_d)
    T_now = max(len(bdays) - 1, 1) / 252
    out = {"expiry": exp, "spot": spot, "atm": k, "iv_ce": ivs[0], "iv_pe": ivs[1], "iv_atm": round(iv_now, 4),
           "straddle_mid": round((leg["ce"]["top_bid_price"] + leg["ce"]["top_ask_price"] + leg["pe"]["top_bid_price"] + leg["pe"]["top_ask_price"]) / 2, 2)}
    if ann is not None and iv_ratio:
        react = (pd.Timestamp(ann.date()) + pd.offsets.BDay(1)).date()
        T_post = max(len(pd.bdate_range(react, exp_d)) - 1, 1) / 252
        var_e = iv_now ** 2 * T_now - (iv_ratio * iv_now) ** 2 * T_post
        out.update(T_now=round(T_now, 4), T_post=round(T_post, 4), iv_post_assumed=round(iv_ratio * iv_now, 4),
                   implied_event_move=round(math.sqrt(2 / math.pi) * math.sqrt(var_e), 4) if var_e > 0 else 0.0,
                   straddle_pct_of_spot=round(out["straddle_mid"] / spot, 4))
    return out


def main():
    out_dir = Path(sys.argv[1]); out_dir.mkdir(parents=True, exist_ok=True)
    ev, px, rev = events(), prices(), daily_revenue()
    proj_df, proj_sum, q2 = part_projection(ev, rev, px)
    ev_df, ev_sum = part_events(ev, px, out_dir)
    nxt = ev[ev["rep_eps"].isna()]
    live = part_live(nxt["ann_ist"].iloc[0] if len(nxt) else None, ev_sum.get("median_iv_ratio_post_pre"), px)
    proj_df.to_csv(out_dir / "projection_history.csv", index=False)
    ev_df.to_csv(out_dir / "event_history.csv", index=False)
    pd.set_option("display.width", 220)
    print(proj_df.to_string(index=False)); print("PROJECTION SUMMARY", json.dumps(proj_sum))
    print("Q2 FY27", json.dumps(q2, default=str))
    print(ev_df.to_string(index=False)); print("EVENT SUMMARY", json.dumps(ev_sum))
    print("LIVE", json.dumps(live, default=str))


if __name__ == "__main__":
    main()
