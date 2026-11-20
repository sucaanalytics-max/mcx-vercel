"""Loaders: one daily frame per stock with revenue and share price.

MCX revenue  : mcx_daily_revenue (this project's Supabase, lib/supabase).
BSE revenue  : codex_bse_daily.total_rev from the NSE-BSE project. That project's
               tables have no anon policy, so the study reads a snapshot taken with
               the Supabase MCP (research/strategy/data/bse_total_rev.txt, gitignored).
Share prices : yfinance daily OHLC, split-adjusted, cached in research/strategy/data/.
"""
from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DATA = Path(__file__).resolve().parent / "data"
sys.path.insert(0, str(ROOT))


def mcx_revenue() -> pd.Series:
    cache = DATA / "mcx_total_rev.csv"
    if cache.exists():
        s = pd.read_csv(cache, index_col=0, parse_dates=True).iloc[:, 0]
    else:
        from lib.supabase import supabase_read_all
        rows = supabase_read_all("mcx_daily_revenue", "select=trading_date,total_rev_cr,day_type&order=trading_date.asc")
        df = pd.DataFrame(rows)
        df = df[~df["day_type"].fillna("").str.lower().str.contains("muhurat")]
        df["trading_date"] = pd.to_datetime(df["trading_date"])
        s = df.set_index("trading_date")["total_rev_cr"].astype(float)
        DATA.mkdir(exist_ok=True)
        s.to_csv(cache)
    return s[s > 0].sort_index().rename("rev")


def bse_revenue() -> pd.Series:
    text = (DATA / "bse_total_rev.txt").read_text().splitlines()
    pairs = [p.split(":") for line in text if not line.startswith("#") for p in line.split()]
    s = pd.Series({pd.Timestamp("20" + d): float(v) for d, v in pairs}).sort_index()
    return s[s > 0].rename("rev")


def prices(ticker: str, start: str = "2021-12-01") -> pd.DataFrame:
    cache = DATA / f"px_{ticker.replace('.', '_')}.csv"
    if cache.exists():
        return pd.read_csv(cache, index_col=0, parse_dates=True)
    import yfinance as yf
    d = yf.download(ticker, start=start, interval="1d", auto_adjust=True, progress=False)
    d.columns = [c[0] if isinstance(c, tuple) else c for c in d.columns]
    d.index = pd.to_datetime(d.index).tz_localize(None)
    d = d[["Open", "High", "Low", "Close", "Volume"]].dropna(subset=["Open", "Close"])
    DATA.mkdir(exist_ok=True)
    d.to_csv(cache)
    return d


def panel(name: str) -> pd.DataFrame:
    """Daily frame on revenue dates: rev, and the last known close (as of that day)."""
    rev = {"MCX": mcx_revenue, "BSE": bse_revenue}[name]()
    px = prices({"MCX": "MCX.NS", "BSE": "BSE.NS"}[name])
    close = px["Close"].reindex(rev.index, method="ffill")   # MCX trades some NSE holidays
    return pd.DataFrame({"rev": rev, "close": close}).dropna()
