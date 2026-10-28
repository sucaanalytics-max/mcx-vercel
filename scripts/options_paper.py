#!/usr/bin/env python3
"""Bull-only paper trading of MCX Ltd options — rules in Docs/Options_Paper_Trading_Rules.md.

Runs once per NSE weekday at ~15:10 IST (launchd: scripts/launchd/com.mcx.options-paper.plist).
Each run logs the near-month bid/ask spread, opens positions on the Schedule B entry day
(10 NSE trading days before expiry) and closes them on the exit day (2 days before).
No orders are ever sent — fills are paper fills at the live Dhan bid/ask.

Usage (needs dhanhq<2.1 and pandas):
  uv run --no-project --with "dhanhq<2.1" --with pandas --with python-dotenv \
      python -I scripts/options_paper.py run|status|dry-run
"""
from __future__ import annotations
import sys, json, math, time, statistics, urllib.request, datetime as dt
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import pandas as pd
from lib import dhan_client as dc
from lib.mcx_config import SUPABASE_URL, SUPABASE_ANON_KEY, now_ist
from lib.options_costs import leg_fees

UNDERLYING_ID, SEG = 31181, "NSE_EQ"   # MCX Ltd equity on Dhan
OUT = ROOT / "data" / "options_paper"
LEDGER, SPREADS, RUNLOG = OUT / "ledger.json", OUT / "spreads.csv", OUT / "runs.log"
VARIANTS = {"v45_20": (45, 20), "v60_40": (60, 40)}
ENTRY_OFFSET, EXIT_OFFSET = 10, 2
FILL_WINDOW = (dt.time(14, 45), dt.time(15, 29))
# NSE weekday closures still ahead in the covered period. Diwali Balipratipada is
# listed as 10 Nov by most sources, 9 Nov by some — either way the Nov-2026 entry
# day is 6 Nov. Add 2027 holidays before the January 2027 series.
NSE_HOLIDAYS = {"2026-11-10", "2026-11-24", "2026-12-25"}
HOLIDAY_COVERAGE_END = dt.date(2026, 12, 31)


# ---------------------------------------------------------------- calendar
def is_trading_day(d: dt.date) -> bool:
    return d.weekday() < 5 and d.isoformat() not in NSE_HOLIDAYS


def shift_trading_days(d: dt.date, n: int) -> dt.date:
    """n-th NSE trading day before d (n > 0)."""
    while n > 0:
        d -= dt.timedelta(days=1)
        if is_trading_day(d):
            n -= 1
    return d


# ---------------------------------------------------------------- data
def revenue() -> pd.Series:
    H = {"apikey": SUPABASE_ANON_KEY, "Authorization": "Bearer " + SUPABASE_ANON_KEY}
    rows, off = [], 0
    while True:
        url = (f"{SUPABASE_URL}/rest/v1/mcx_daily_revenue?select=trading_date,total_rev_cr"
               f"&order=trading_date.asc&offset={off}&limit=1000")
        batch = json.loads(urllib.request.urlopen(urllib.request.Request(url, headers=H)).read())
        rows += batch
        if len(batch) < 1000:
            break
        off += 1000
    s = pd.DataFrame(rows)
    s["trading_date"] = pd.to_datetime(s["trading_date"]).dt.date
    s = s[s["total_rev_cr"] > 0].set_index("trading_date")["total_rev_cr"].sort_index()
    return s


def signals(before: dt.date) -> dict:
    """Each variant's ADR trend on the last MCX day strictly before `before`, plus its expanding z."""
    rev = revenue()
    rev = rev[rev.index < before]
    out = {"last_rev_date": rev.index[-1].isoformat()}
    for name, (w, L) in VARIANTS.items():
        adr = rev.rolling(w, min_periods=w).mean()
        sig = (adr / adr.shift(L)).apply(math.log).dropna()
        s, hist = float(sig.iloc[-1]), sig.iloc[:-1]
        z = float((s - hist.mean()) / hist.std()) if len(hist) >= 120 else None
        out[name] = {"signal": round(s, 5), "z": None if z is None else round(z, 3),
                     "r2": z is not None and abs(z) >= 0.5}
    return out


def chain(client, expiry: str) -> dict:
    time.sleep(3.1)  # Dhan option-chain rate limit: 1 request / 3 s
    d = client.option_chain(UNDERLYING_ID, SEG, expiry)
    if d.get("status") != "success":
        raise RuntimeError(f"option_chain failed: {str(d)[:200]}")
    return d["data"]["data"]


def quotes(oc: dict) -> dict:
    """{(strike, 'CE'|'PE'): {bid, ask, mid, iv, security_id}} for legs with a two-sided quote."""
    q = {}
    for k, v in oc["oc"].items():
        for side in ("ce", "pe"):
            o = v.get(side) or {}
            b, a = o.get("top_bid_price", 0) or 0, o.get("top_ask_price", 0) or 0
            if b > 0 and a > 0:
                q[(float(k), side.upper())] = {"bid": b, "ask": a, "mid": round((a + b) / 2, 4),
                                               "iv": o.get("implied_volatility"), "security_id": o.get("security_id")}
    return q


def lot_size() -> float:
    dc._scrip_master()  # refreshes the on-disk cache
    df = pd.read_csv("/tmp/dhan_scrip_master.csv", low_memory=False,
                     usecols=["EXCH_ID", "UNDERLYING_SYMBOL", "INSTRUMENT", "LOT_SIZE"])
    lots = df[(df.EXCH_ID == "NSE") & (df.UNDERLYING_SYMBOL == "MCX") & (df.INSTRUMENT == "OPTSTK")]["LOT_SIZE"]
    return float(lots.iloc[0])


# ---------------------------------------------------------------- structures
def structures(spot: float, strikes: list[float]) -> dict:
    atm = min(strikes, key=lambda k: abs(k - spot))
    c5 = min(strikes, key=lambda k: abs(k - spot * 1.05))
    p5 = min(strikes, key=lambda k: abs(k - spot * 0.95))
    return {  # (book, direction) -> legs [(strike, type, qty)]
        ("PCS", 1): [(atm, "PE", -1), (p5, "PE", 1)],   # bull put credit spread
        ("CDS", 1): [(atm, "CE", 1), (c5, "CE", -1)],   # bull call debit spread
        ("PCS", -1): [(atm, "CE", -1), (c5, "CE", 1)],  # shadow: bear call credit spread
        ("CDS", -1): [(atm, "PE", 1), (p5, "PE", -1)],  # shadow: bear put debit spread
    }


def fill(q: dict, strike: float, typ: str, qty: int, opening: bool) -> float:
    """Paper fill: buy at ask, sell at bid. On exit the trade side is reversed."""
    buy = (qty > 0) == opening
    return q[(strike, typ)]["ask" if buy else "bid"]


# ---------------------------------------------------------------- ledger
def load() -> list[dict]:
    return json.loads(LEDGER.read_text()) if LEDGER.exists() else []


def save(led: list[dict]):
    OUT.mkdir(parents=True, exist_ok=True)
    LEDGER.write_text(json.dumps(led, indent=1))
    pd.DataFrame([{k: v for k, v in t.items() if k != "legs"} for t in led]).to_csv(OUT / "ledger.csv", index=False)


def log(msg: str, notify: bool = False):
    OUT.mkdir(parents=True, exist_ok=True)
    line = f"{now_ist():%Y-%m-%d %H:%M} {msg}"
    print(line)
    with open(RUNLOG, "a") as f:
        f.write(line + "\n")
    if notify:  # macOS banner, best effort
        import subprocess
        subprocess.run(["osascript", "-e", f"display notification {json.dumps(msg[:200])} with title \"MCX options paper\""],
                       check=False, capture_output=True)


def log_spreads(oc: dict, expiry: str, ts: dt.datetime):
    spot = oc["last_price"]
    buckets = {"atm": (0, .015), "near": (.015, .035), "otm5": (.035, .065), "far": (.065, .10)}
    vals = {b: [] for b in buckets}
    for (k, typ), v in quotes(oc).items():
        m = k / spot - 1
        if not (abs(m) < .015 or (typ == "CE" and m > 0) or (typ == "PE" and m < 0)):
            continue  # in-the-money side: wider, irrelevant to our legs
        for b, (lo, hi) in buckets.items():
            if lo <= abs(m) < hi:
                vals[b].append((v["ask"] - v["bid"]) / 2 / v["mid"] * 100)
    row = {"ts": ts.strftime("%Y-%m-%d %H:%M"), "expiry": expiry, "spot": spot}
    row.update({f"half_pct_{b}": round(statistics.median(x), 3) if x else None for b, x in vals.items()})
    new = not SPREADS.exists()
    pd.DataFrame([row]).to_csv(SPREADS, mode="a", header=new, index=False)


# ---------------------------------------------------------------- actions
def open_positions(led, client, expiry, oc, today, ts, write: bool):
    sig = signals(today)
    if dt.date.fromisoformat(sig["last_rev_date"]) < shift_trading_days(today, 1):
        log(f"signal_stale: last revenue {sig['last_rev_date']} — no entries")
        return
    q, spot, lot = quotes(oc), oc["last_price"], lot_size()
    strikes = sorted({k for k, _ in q})
    for vname in VARIANTS:
        s = sig[vname]
        direction = 1 if s["signal"] > 0 else -1
        for (book, d), legs in structures(spot, strikes).items():
            if d != direction or len({k for k, _, _ in legs}) < 2:
                continue
            t = {"id": f"{expiry}|{vname}|{book}|{'bull' if d > 0 else 'bear'}", "variant": vname, "book": book,
                 "direction": "bull" if d > 0 else "bear", "shadow": d < 0, "expiry": expiry,
                 "entry_date": today.isoformat(), "entry_ts": ts.strftime("%H:%M"), "spot_entry": spot, "lot": lot,
                 "signal": s["signal"], "z": s["z"], "r2": s["r2"], "status": "open", "flags": []}
            if any((k, typ) not in q for k, typ, _ in legs):
                t.update(status="no_quote")
            else:
                t["legs"] = [{"strike": k, "type": typ, "qty": qty, "security_id": q[(k, typ)]["security_id"],
                              "entry_bid": q[(k, typ)]["bid"], "entry_ask": q[(k, typ)]["ask"],
                              "entry_fill": fill(q, k, typ, qty, True), "entry_iv": q[(k, typ)]["iv"]} for k, typ, qty in legs]
                net = sum(l["qty"] * l["entry_fill"] for l in t["legs"])
                width = abs(legs[0][0] - legs[1][0])
                t.update(net_entry=round(net, 2), width=width,
                         risk=round(net if net > 0 else width + net, 2),
                         entry_costs=round(sum(leg_fees(l["entry_fill"], l["qty"], lot) for l in t["legs"]), 4))
            log(f"{'OPEN' if write else 'WOULD OPEN'} {t['id']} signal={s['signal']} z={s['z']} "
                f"legs={[(k, typ, qty) for k, typ, qty in legs]} net={t.get('net_entry')} risk={t.get('risk')} status={t['status']}",
                notify=write and not t["shadow"])
            if write and not any(x["id"] == t["id"] for x in led):
                led.append(t)


def close_positions(led, oc, today, ts, late: bool):
    q = quotes(oc)
    for t in led:
        if t["status"] != "open" or t["expiry"] != oc["_expiry"]:
            continue
        if any((l["strike"], l["type"]) not in q for l in t["legs"]):
            log(f"no_quote on exit for {t['id']} — retry next run")
            continue
        for l in t["legs"]:
            k = (l["strike"], l["type"])
            l.update(exit_bid=q[k]["bid"], exit_ask=q[k]["ask"], exit_fill=fill(q, l["strike"], l["type"], l["qty"], False))
        gross = sum(l["qty"] * (l["exit_fill"] - l["entry_fill"]) for l in t["legs"])
        exit_costs = sum(leg_fees(l["exit_fill"], -l["qty"], t["lot"]) for l in t["legs"])
        pnl = gross - t["entry_costs"] - exit_costs
        t.update(status="closed", exit_date=today.isoformat(), exit_ts=ts.strftime("%H:%M"), spot_exit=oc["last_price"],
                 exit_costs=round(exit_costs, 4), pnl_per_share=round(pnl, 4), pnl_per_lot=round(pnl * t["lot"], 2),
                 ret=round(pnl / t["risk"], 4))
        if late:
            t["flags"].append("late_exit")
        log(f"CLOSE {t['id']} pnl/lot=₹{t['pnl_per_lot']} ret={t['ret']:+.1%}", notify=not t["shadow"])


def main(cmd: str) -> int:
    ts = now_ist()
    today = ts.date()
    if today > HOLIDAY_COVERAGE_END:
        log("NSE holiday list does not cover today — update NSE_HOLIDAYS before trading continues")
        return 2
    if cmd == "run" and not is_trading_day(today):
        log("not an NSE trading day")
        return 0
    client = dc.get_client()
    el = client.expiry_list(UNDERLYING_ID, SEG)
    if el.get("status") != "success":
        raise RuntimeError(f"Dhan expiry_list failed (token expired?): {str(el.get('data'))[:150]}")
    exps = el["data"]["data"]
    expiry = min(e for e in exps if e >= today.isoformat())
    exp_d = dt.date.fromisoformat(expiry)
    entry_d, exit_d = shift_trading_days(exp_d, ENTRY_OFFSET), shift_trading_days(exp_d, EXIT_OFFSET)
    oc = chain(client, expiry)
    oc["_expiry"] = expiry
    led = load()

    if cmd == "status":
        q = quotes(oc)
        print(f"series {expiry}: entry {entry_d} exit {exit_d} | spot {oc['last_price']}")
        print("signals (as of today):", json.dumps(signals(today)))
        for t in led:
            if t["status"] == "open":
                mtm = sum(l["qty"] * (q[(l["strike"], l["type"])]["mid"] - l["entry_fill"]) for l in t["legs"]
                          if (l["strike"], l["type"]) in q)
                print(f"  OPEN {t['id']} mid-MTM/lot ₹{mtm * t['lot']:.0f} ({mtm / t['risk']:+.1%} of risk)")
        closed = [t for t in led if t["status"] == "closed"]
        if closed:
            print(pd.DataFrame(closed)[["id", "ret", "pnl_per_lot"]].to_string(index=False))
        return 0

    if cmd == "dry-run":
        print(f"series {expiry}: entry {entry_d} exit {exit_d} (dry run: pretending today is the entry day)")
        open_positions([], client, expiry, oc, today, ts, write=False)
        return 0

    log_spreads(oc, expiry, ts)
    in_window = FILL_WINDOW[0] <= ts.time() <= FILL_WINDOW[1]
    if today == entry_d:
        if in_window:
            open_positions(led, client, expiry, oc, today, ts, write=True)
        elif not any(t["expiry"] == expiry for t in led):
            led.append({"id": f"{expiry}|missed", "expiry": expiry, "status": "missed", "entry_date": today.isoformat(),
                        "flags": [f"run at {ts:%H:%M} outside fill window"]})
            log(f"missed entry for {expiry}: ran at {ts:%H:%M}")
    elif entry_d < today < exit_d and not any(t["expiry"] == expiry for t in led):
        led.append({"id": f"{expiry}|missed", "expiry": expiry, "status": "missed", "entry_date": entry_d.isoformat(),
                    "flags": ["no run on entry day"]})
        log(f"missed entry for {expiry}: no run on {entry_d}")
    if today >= exit_d and today < exp_d and in_window:
        close_positions(led, oc, today, ts, late=today > exit_d)
    save(led)
    log(f"run ok: series {expiry} entry {entry_d} exit {exit_d}; open={sum(t['status'] == 'open' for t in led)}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "run"))
    except Exception as e:  # e.g. expired Dhan token — make it loud, not silent
        log(f"ERROR {type(e).__name__}: {e}", notify=True)
        raise
