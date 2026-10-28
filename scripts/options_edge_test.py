"""Intraday vs daily edge test for MCX Ltd (MCX.NS) — options agent horizon decision, 28 Oct 2026.

Intraday: revenue pace at decision time T (cumulative tx revenue vs trailing-20-day
median at the same elapsed minute) -> stock return from the T bar open to the close.
Daily: last night's revenue surprise (vs trailing 20-day mean) -> next NSE session returns.
No look-ahead: snapshot must be captured >= 5 min before T; daily signal enters next session.

Run: uv run --no-project --with scipy --with yfinance --with pandas --with numpy python -I scripts/options_edge_test.py
"""
import sys, json, math, urllib.request
sys.path.insert(0, "/Users/tusk-jvb/Claude Projects/mcx-vercel")
import numpy as np, pandas as pd, yfinance as yf
from scipy import stats
from lib.mcx_config import SUPABASE_URL, SUPABASE_ANON_KEY

H = {"apikey": SUPABASE_ANON_KEY, "Authorization": "Bearer " + SUPABASE_ANON_KEY}

def fetch(table, select, order):
    rows, off = [], 0
    while True:
        url = f"{SUPABASE_URL}/rest/v1/{table}?select={select}&order={order}&offset={off}&limit=1000"
        batch = json.loads(urllib.request.urlopen(urllib.request.Request(url, headers=H)).read())
        rows += batch
        if len(batch) < 1000:
            return pd.DataFrame(rows)
        off += 1000

def ic(x, y):
    m = x.notna() & y.notna()
    x, y = x[m], y[m]
    if len(x) < 20:
        return None
    r, p = stats.spearmanr(x, y)
    return {"n": int(len(x)), "ic": round(r, 3), "p": round(p, 4)}

def hit_and_spread(sig, ret):
    m = sig.notna() & ret.notna()
    s, r = sig[m], ret[m]
    q = s.quantile([1/3, 2/3])
    top, bot = r[s >= q.iloc[1]], r[s <= q.iloc[0]]
    return {"top_tercile_mean_bp": round(top.mean() * 1e4, 1),
            "bottom_tercile_mean_bp": round(bot.mean() * 1e4, 1),
            "spread_bp": round((top.mean() - bot.mean()) * 1e4, 1),
            "hit_rate_sign": round(float((np.sign(s - s.median()) == np.sign(r)).mean()), 3)}

# ---------- stock prices ----------
hourly = yf.download("MCX.NS", period="730d", interval="60m", auto_adjust=False, progress=False)
hourly.columns = [c[0] if isinstance(c, tuple) else c for c in hourly.columns]
hourly.index = hourly.index.tz_convert("Asia/Kolkata")
hourly["date"] = hourly.index.date
daily = yf.download("MCX.NS", start="2021-12-01", interval="1d", auto_adjust=True, progress=False)
daily.columns = [c[0] if isinstance(c, tuple) else c for c in daily.columns]
daily.index = pd.to_datetime(daily.index).date

# ---------- intraday ----------
snap = fetch("mcx_snapshots", "trading_date,elapsed_min,total_rev_cr", "trading_date.asc,elapsed_min.asc")
snap["trading_date"] = pd.to_datetime(snap["trading_date"]).dt.date
snap = snap[snap["total_rev_cr"] > 0]
grid = np.arange(15, 400, 5)

def interp_day(g):
    g = g.sort_values("elapsed_min")
    return pd.Series(np.interp(grid, g["elapsed_min"], g["total_rev_cr"], left=np.nan, right=np.nan), index=grid)

curves = snap.groupby("trading_date").apply(interp_day)  # rows: date, cols: elapsed grid
curves = curves.sort_index()
base = curves.rolling(20, min_periods=15).median().shift(1)  # trailing 20 prior days only
pace = np.log(curves / base)

decisions = {"10:15": 75, "11:15": 135, "12:15": 195, "13:15": 255, "14:15": 315}
intraday_out = {}
for label, el in decisions.items():
    rows = []
    for d, g in hourly.groupby("date"):
        if d not in snap["trading_date"].values or d not in pace.index:
            continue
        ds = snap[snap["trading_date"] == d]
        avail = ds[ds["elapsed_min"] <= el - 5]
        if avail.empty or (el - avail["elapsed_min"].max()) > 45:
            continue  # stale or missing snapshot at decision time
        last_el = int(avail["elapsed_min"].max())
        last_el = grid[grid <= last_el].max()
        sig = pace.loc[d, last_el]
        bar = g[g.index.strftime("%H:%M") == label]
        first = g.iloc[0]
        if bar.empty:
            continue
        entry = bar["Open"].iloc[0]
        rows.append({"date": d, "pace": sig,
                     "morning_ret": entry / first["Open"] - 1,
                     "rest_of_day": g["Close"].iloc[-1] / entry - 1,
                     "next_hour": bar["Close"].iloc[0] / entry - 1})
    df = pd.DataFrame(rows)
    # partial: residualise pace on the stock's own morning move
    m = df[["pace", "morning_ret"]].notna().all(axis=1)
    resid = pd.Series(np.nan, index=df.index)
    if m.sum() > 20:
        b = np.polyfit(df.loc[m, "morning_ret"], df.loc[m, "pace"], 1)
        resid[m] = df.loc[m, "pace"] - np.polyval(b, df.loc[m, "morning_ret"])
    intraday_out[label] = {
        "rest_of_day": ic(df["pace"], df["rest_of_day"]),
        "next_hour": ic(df["pace"], df["next_hour"]),
        "rest_of_day_ex_morning_move": ic(resid, df["rest_of_day"]),
        "pace_vs_morning_move": ic(df["pace"], df["morning_ret"]),
        "terciles_rest_of_day": hit_and_spread(df["pace"], df["rest_of_day"]),
        "date_range": [str(df["date"].min()), str(df["date"].max())],
    }

# ---------- daily ----------
rev = fetch("mcx_daily_revenue", "trading_date,total_rev_cr", "trading_date.asc")
rev["trading_date"] = pd.to_datetime(rev["trading_date"]).dt.date
rev = rev[rev["total_rev_cr"] > 0].set_index("trading_date")["total_rev_cr"].sort_index()
surprise = np.log(rev / rev.rolling(20, min_periods=15).mean().shift(1))
adr45 = rev.rolling(45, min_periods=40).mean()
adr_trend = np.log(adr45 / adr45.shift(20))  # slow: 45-day ADR vs 20 days ago

nse_days = sorted(daily.index)
rows = []
for d, s in surprise.dropna().items():
    nxt = [x for x in nse_days if x > d]
    prev = [x for x in nse_days if x <= d]
    if len(nxt) < 1 or not prev:
        continue
    d0, d1 = prev[-1], nxt[0]
    c0, o1, c1 = daily.loc[d0, "Close"], daily.loc[d1, "Open"], daily.loc[d1, "Close"]
    gap = o1 / c0 - 1
    if abs(gap) > 0.25:
        continue  # corporate-action artefact
    r = {"date": d, "surprise": s, "gap": gap, "open_to_close": c1 / o1 - 1}
    for h in (5, 10, 20):
        r[f"open_to_{h}d"] = daily.loc[nxt[h - 1], "Close"] / o1 - 1 if len(nxt) >= h else np.nan
    r["adr_trend"] = adr_trend.get(d, np.nan)
    rows.append(r)
dd = pd.DataFrame(rows)

def ic_nonoverlap(df, sig, ret, h):
    """Mean IC across the h non-overlapping subsamples (every h-th signal day)."""
    vals, ns = [], []
    for off in range(h):
        sub = df.iloc[off::h]
        r = ic(sub[sig], sub[ret])
        if r: vals.append(r["ic"]); ns.append(r["n"])
    if not vals: return None
    m, n = float(np.mean(vals)), int(np.mean(ns))
    t = m * math.sqrt(n - 2) / math.sqrt(max(1e-9, 1 - m * m))
    return {"n_per_subsample": n, "ic": round(m, 3), "approx_t": round(t, 2),
            "ic_range": [round(min(vals), 3), round(max(vals), 3)]}

def daily_block(df):
    out = {"range": f'{df["date"].min()}..{df["date"].max()}'}
    for sig in ("surprise", "adr_trend"):
        out[f"{sig}->gap(not tradable)"] = ic(df[sig], df["gap"])
        out[f"{sig}->open_to_close"] = ic(df[sig], df["open_to_close"])
        for h in (5, 10, 20):
            out[f"{sig}->open_to_{h}d (non-overlap)"] = ic_nonoverlap(df, sig, f"open_to_{h}d", h)
    out["surprise terciles open_to_close"] = hit_and_spread(df["surprise"], df["open_to_close"])
    return out

snap_start = min(snap["trading_date"])
out = {"intraday": intraday_out,
       "daily_full_history": daily_block(dd),
       "daily_same_window_as_intraday": daily_block(dd[dd["date"] >= snap_start])}
for k, v in out["intraday"].items():
    print("INTRADAY", k, v["date_range"])
    for kk in ("rest_of_day", "next_hour", "rest_of_day_ex_morning_move", "pace_vs_morning_move", "terciles_rest_of_day"):
        print("   ", kk, v[kk])
for blk in ("daily_full_history", "daily_same_window_as_intraday"):
    print(blk)
    for kk, vv in out[blk].items():
        print("   ", kk, vv)
