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
from lib.mcx_config import SUPABASE_URL, SUPABASE_ANON_KEY, SUPABASE_WRITE_ROLE, supabase_upsert, now_ist
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
    return row


# ---------------------------------------------------------------- actions
def open_positions(led, client, expiry, oc, today, ts, write: bool, sig: dict | None = None, quiet: bool = False):
    """Builds the Schedule B trades at the live bid/ask; appends them to the ledger when write.
    Returns the trades (the feed shows them as the current recommendation on other days)."""
    sig = sig or signals(today)
    trades = []
    if dt.date.fromisoformat(sig["last_rev_date"]) < shift_trading_days(today, 1):
        if not quiet:
            log(f"signal_stale: last revenue {sig['last_rev_date']} — no entries")
        return trades
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
            if not quiet:
                log(f"{'OPEN' if write else 'WOULD OPEN'} {t['id']} signal={s['signal']} z={s['z']} "
                    f"legs={[(k, typ, qty) for k, typ, qty in legs]} net={t.get('net_entry')} risk={t.get('risk')} status={t['status']}",
                    notify=write and not t["shadow"])
            trades.append(t)
            if write and not any(x["id"] == t["id"] for x in led):
                led.append(t)
    return trades


def close_positions(led, oc, today, ts, late: bool, event: bool = False):
    q = quotes(oc)
    for t in led:
        if t["status"] != "open" or t["expiry"] != oc["_expiry"] or bool(t.get("event")) != event:
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


# ---------------------------------------------------------------- results-day event (H3)
EVENT_CFG = OUT / "event.json"
# Fixed by Docs/Results_Event_Study_Q2FY27.md (pre-registered rules): direction is neutral
# (our-vs-consensus sign hit 3/7 < 60%); realised/implied median 0.87 < 1; IV falls to 0.865×.
H3 = {"direction": "neutral", "median_realised": 0.0389, "ratio_realised_implied": 0.87,
      "iv_post_pre": 0.865, "wing": 0.07}


def trading_days_between(a: dt.date, b: dt.date) -> int:
    """NSE trading days d with a < d <= b."""
    return sum(1 for i in range(1, (b - a).days + 1) if is_trading_day(a + dt.timedelta(days=i)))


def event_days(results_date: dt.date) -> tuple[dt.date, dt.date]:
    """MCX announces after the close: pre-day = results day (or the trading day before), reaction = next."""
    pre = results_date if is_trading_day(results_date) else shift_trading_days(results_date, 1)
    react = results_date + dt.timedelta(days=1)
    while not is_trading_day(react):
        react += dt.timedelta(days=1)
    return pre, react


def event_reading(oc: dict, today: dt.date, react: dt.date) -> dict:
    q, spot = quotes(oc), oc["last_price"]
    exp_d = dt.date.fromisoformat(oc["_expiry"])
    atm = min({k for k, _ in q}, key=lambda k: abs(k - spot))
    iv = (q[(atm, "CE")]["iv"] + q[(atm, "PE")]["iv"]) / 200
    t_now, t_post = trading_days_between(today, exp_d) / 252, trading_days_between(react, exp_d) / 252
    var_e = iv ** 2 * t_now - (H3["iv_post_pre"] * iv) ** 2 * t_post
    implied = math.sqrt(2 / math.pi) * math.sqrt(var_e) if var_e > 0 else 0.0
    if implied > 1.2 * H3["median_realised"] and H3["ratio_realised_implied"] < 1:
        vol = "rich"
    elif implied < 0.8 * H3["median_realised"] and H3["ratio_realised_implied"] > 1:
        vol = "cheap"
    else:
        vol = "fair"
    book = {("neutral", "rich"): "EVT_IFLY", ("neutral", "cheap"): "EVT_STRADDLE"}.get((H3["direction"], vol))
    return {"spot": spot, "atm": atm, "iv_atm": round(iv, 4), "t_now": round(t_now, 4), "t_post": round(t_post, 4),
            "implied_move": round(implied, 4), "threshold_rich": round(1.2 * H3["median_realised"], 4),
            "vol_view": vol, "direction": H3["direction"], "book": book}


def open_event(led, oc, today, ts, results_date: str, react: dt.date, write: bool):
    r = event_reading(oc, today, react)
    q, spot, atm = quotes(oc), r["spot"], r["atm"]
    strikes = sorted({k for k, _ in q})
    rid = f"{oc['_expiry']}|event|{results_date}"
    t = {"id": f"{rid}|{r['book'] or 'none'}", "event": True, "results_date": results_date, "book": r["book"],
         "direction": r["direction"], "shadow": False, "expiry": oc["_expiry"], "entry_date": today.isoformat(),
         "entry_ts": ts.strftime("%H:%M"), "spot_entry": spot, "reading": r, "flags": []}
    if r["book"] is None:
        t["status"] = "no_trade"
    else:
        c7 = min(strikes, key=lambda k: abs(k - spot * (1 + H3["wing"])))
        p7 = min(strikes, key=lambda k: abs(k - spot * (1 - H3["wing"])))
        legs = ([(atm, "CE", -1), (atm, "PE", -1), (c7, "CE", 1), (p7, "PE", 1)] if r["book"] == "EVT_IFLY"
                else [(atm, "CE", 1), (atm, "PE", 1)])
        if any((k, typ) not in q for k, typ, _ in legs):
            t["status"] = "no_quote"
        else:
            lot = lot_size()
            t["lot"] = lot
            t["legs"] = [{"strike": k, "type": typ, "qty": qty, "security_id": q[(k, typ)]["security_id"],
                          "entry_bid": q[(k, typ)]["bid"], "entry_ask": q[(k, typ)]["ask"],
                          "entry_fill": fill(q, k, typ, qty, True), "entry_iv": q[(k, typ)]["iv"]} for k, typ, qty in legs]
            net = sum(l["qty"] * l["entry_fill"] for l in t["legs"])
            wing = max(c7 - atm, atm - p7)
            t.update(status="open", net_entry=round(net, 2),
                     risk=round(wing + net if r["book"] == "EVT_IFLY" else net, 2),
                     entry_costs=round(sum(leg_fees(l["entry_fill"], l["qty"], lot) for l in t["legs"]), 4))
    log(f"{'EVENT' if write else 'EVENT (dry run)'} {t['id']} implied={r['implied_move']:.2%} vs rich>{r['threshold_rich']:.2%} "
        f"view={r['direction']}+{r['vol_view']} status={t['status']} net={t.get('net_entry')} risk={t.get('risk')}",
        notify=write)
    if write and not any(x["id"].startswith(rid) for x in led):
        led.append(t)
    return t


def event_step(led, oc, today, ts, in_window: bool):
    if not EVENT_CFG.exists():
        return
    rd = json.loads(EVENT_CFG.read_text())["results_date"]
    pre, react = event_days(dt.date.fromisoformat(rd))
    if react >= dt.date.fromisoformat(oc["_expiry"]):
        if today == pre:
            log(f"event {rd}: reaction day {react} is not before expiry {oc['_expiry']} — no event trade")
        return
    done = any(t.get("event") and t.get("results_date") == rd for t in led)
    if today == pre and not done:
        if in_window:
            open_event(led, oc, today, ts, rd, react, write=True)
        else:
            led.append({"id": f"{oc['_expiry']}|event|{rd}|missed", "event": True, "results_date": rd,
                        "expiry": oc["_expiry"], "status": "missed", "flags": [f"run at {ts:%H:%M} outside fill window"]})
            log(f"missed event entry for results {rd}: ran at {ts:%H:%M}", notify=True)
    if today >= react and in_window:
        close_positions(led, oc, today, ts, late=today > react, event=True)


# ---------------------------------------------------------------- feed (Signals → Options page)
def leg_view(l: dict) -> dict:
    return {"strike": l["strike"], "type": l["type"], "qty": l["qty"],
            "bid": l.get("entry_bid"), "ask": l.get("entry_ask"), "fill": l.get("entry_fill")}


def trade_view(t: dict) -> dict:
    """The fields the page shows for a recommended or open trade."""
    v = {k: t.get(k) for k in ("id", "variant", "book", "direction", "shadow", "event", "status", "expiry",
                               "entry_date", "exit_date", "signal", "z", "r2", "net_entry", "risk", "lot", "ret", "pnl_per_lot")}
    if t.get("legs"):
        v["legs"] = [leg_view(l) for l in t["legs"]]
        width = t.get("width") or max(abs(a["strike"] - b["strike"]) for a in t["legs"] for b in t["legs"])
        net, risk = t.get("net_entry"), t.get("risk")
        if net is not None and risk is not None:  # credit trades keep the credit; debit spreads keep width − debit
            v["max_gain"] = round(-net if net < 0 else width - net, 2) if t["book"] != "EVT_STRADDLE" else None
            v["risk_per_lot"] = round(risk * (t.get("lot") or 0))
    return v


def marks(led: list[dict], oc: dict) -> list[dict]:
    """Open paper positions marked at the chain's mid."""
    q, out = quotes(oc), []
    for t in led:
        if t.get("status") != "open" or t.get("expiry") != oc["_expiry"]:
            continue
        v = trade_view(t)
        if all((l["strike"], l["type"]) in q for l in t["legs"]):
            m = sum(l["qty"] * (q[(l["strike"], l["type"])]["mid"] - l["entry_fill"]) for l in t["legs"])
            v.update(mtm_per_share=round(m, 2), mtm_per_lot=round(m * t["lot"]), mtm_pct=round(m / t["risk"], 4))
        out.append(v)
    return out


def phase_of(today, entry_d, exit_d, exp_d) -> str:
    if today < entry_d:
        return "before_entry"
    if today == entry_d:
        return "entry_day"
    if today < exit_d:
        return "in_trade"
    if today == exit_d:
        return "exit_day"
    return "after_exit" if today < exp_d else "expired"


def event_view(led, oc, today) -> dict | None:
    if not EVENT_CFG.exists():
        return None
    rd = json.loads(EVENT_CFG.read_text())["results_date"]
    pre, react = event_days(dt.date.fromisoformat(rd))
    v = {"results_date": rd, "entry": pre.isoformat(), "exit": react.isoformat()}
    rec = next((t for t in led if t.get("event") and t.get("results_date") == rd), None)
    if rec:
        v["status"] = rec["status"]
        v["reading"] = rec.get("reading")
        v["trade"] = trade_view(rec) if rec.get("legs") else None
    elif today <= pre and react < dt.date.fromisoformat(oc["_expiry"]):
        v["status"] = "pending"
        v["reading"] = event_reading(oc, today, react)  # today's estimate; the trade re-measures on the pre-day
    return v


def pct(x, dp=1) -> str:
    return f"{x * 100:+.{dp}f}%"


def note_text(ts, spot, sig, phase, expiry, entry_d, exit_d, rec, positions, ev, spreads, closed) -> str:
    day = f"{ts:%a} {ts.day} {ts:%b}"
    lines = [f"{day}, {ts:%H:%M} IST · MCX ₹{spot:,.1f}" + (f" · near-month half-spread {spreads['half_pct_atm']:.2f}%" if spreads and spreads.get("half_pct_atm") else "")]
    vs = [f"{n.replace('_', '/')} {pct(sig[n]['signal'])}" + (f" (z {sig[n]['z']:.2f})" if sig[n]["z"] is not None else "")
          for n in VARIANTS]
    bulls = sum(sig[n]["signal"] > 0 for n in VARIANTS)
    lines.append(f"Revenue trend: {'bullish on both' if bulls == 2 else 'bullish on one' if bulls == 1 else 'not bullish'} ({'; '.join(vs)}).")
    live = [t for t in rec if not t["shadow"] and t.get("net_entry") is not None]
    def describe(t):
        legs = t["legs"]
        s = "/".join(f"{int(l['strike'])}" for l in legs)
        kind = {"PCS": "put credit spread", "CDS": "call debit spread"}.get(t["book"], t["book"])
        money = f"₹{abs(t['net_entry']):.2f} {'credit' if t['net_entry'] < 0 else 'debit'}"
        return f"{kind} {s} for {money}, ₹{t['risk']:.2f} at risk per share (₹{t['risk'] * t['lot']:,.0f} per lot)"
    if phase == "entry_day":
        lines.append("Paper-opened today: " + ("; ".join(describe(t) for t in {x['book']: x for x in live}.values()) if live else "nothing (signals not bullish).")
                     + (" Both variants agree." if bulls == 2 else ""))
    elif phase == "before_entry":
        exp_d = dt.date.fromisoformat(expiry)
        lines.append(f"Next paper entry {entry_d:%a} {entry_d.day} {entry_d:%b} (series expiring {exp_d.day} {exp_d:%b}). "
                     + ("If it were today: " + "; ".join(describe(t) for t in {x['book']: x for x in live}.values()) + "." if live
                        else "If it were today: no trade (signals not bullish)."))
    elif phase in ("in_trade", "exit_day"):
        lines.append(f"Positions in the {expiry} series {'close today' if phase == 'exit_day' else f'close {exit_d:%a} {exit_d.day} {exit_d:%b}'}.")
    else:
        lines.append(f"The {expiry} series is closed; the next entry follows that expiry.")
    real = [p for p in positions if not p.get("shadow") and p.get("mtm_per_lot") is not None]
    if real:
        tot = sum(p["mtm_per_lot"] for p in real)
        lines.append(f"Open paper positions: {len(real)}, marked at mid ₹{tot:+,.0f} in total (1 lot each).")
    if ev:
        r = ev.get("reading") or {}
        rd = dt.date.fromisoformat(ev["results_date"])
        if ev.get("status") == "pending" and r:
            act = "iron butterfly" if r["book"] == "EVT_IFLY" else "long straddle" if r["book"] == "EVT_STRADDLE" else "no trade"
            lines.append(f"Results {rd:%a} {rd.day} {rd:%b}: options imply a {r['implied_move'] * 100:.1f}% move today vs the "
                         f"{r['threshold_rich'] * 100:.2f}% rich threshold → {act} if that still holds at 15:10 on the day "
                         f"({dt.date.fromisoformat(ev['entry']):%a} {dt.date.fromisoformat(ev['entry']).day} {dt.date.fromisoformat(ev['entry']):%b}).")
        elif ev.get("status"):
            lines.append(f"Results {rd:%a} {rd.day} {rd:%b}: event trade {ev['status'].replace('_', ' ')}.")
    done = [t for t in closed if not t.get("shadow") and t.get("ret") is not None]
    lines.append(f"Paper record: {len(done)} closed trade{'s' if len(done) != 1 else ''}"
                 + (f", mean {pct(sum(t['ret'] for t in done) / len(done))} per trade, "
                    f"{sum(t['ret'] > 0 for t in done)}/{len(done)} winners." if done else " yet."))
    return "\n".join(lines)


def feed_row(led, client, oc, today, ts, exps, sig, spreads) -> dict:
    expiry = oc["_expiry"]
    exp_d = dt.date.fromisoformat(expiry)
    entry_d, exit_d = shift_trading_days(exp_d, ENTRY_OFFSET), shift_trading_days(exp_d, EXIT_OFFSET)
    phase = phase_of(today, entry_d, exit_d, exp_d)
    if phase == "entry_day":
        rec = [t for t in led if not t.get("event") and t.get("entry_date") == today.isoformat()]
    elif phase == "before_entry":
        rec = open_positions([], client, expiry, oc, today, ts, write=False, sig=sig, quiet=True)
    else:
        rec = []
    nxt = [e for e in exps if e > expiry]
    next_entry = shift_trading_days(dt.date.fromisoformat(nxt[0]), ENTRY_OFFSET).isoformat() \
        if phase in ("after_exit", "expired") and nxt else None
    positions = marks(led, oc)
    ev = event_view(led, oc, today)
    closed = [t for t in led if t.get("status") == "closed"]
    note = note_text(ts, oc["last_price"], sig, phase, expiry, entry_d, exit_d, rec, positions, ev, spreads, closed)
    return {"trading_date": today.isoformat(), "run_ts": ts.isoformat(), "spot": oc["last_price"],
            "series_expiry": expiry, "entry_date": entry_d.isoformat(), "exit_date": exit_d.isoformat(),
            "signals": sig, "recommendation": {"phase": phase, "next_entry": next_entry, "trades": [trade_view(t) for t in rec]},
            "positions": positions, "event": ev, "spreads": spreads, "note": note}


def publish(led: list[dict], row: dict):
    """Pushes the feed row and the whole ledger to Supabase (needs the service-role key)."""
    if SUPABASE_WRITE_ROLE != "service_role":
        log("feed not published: SUPABASE_SERVICE_KEY is not set in .env")
        return
    supabase_upsert("mcx_options_feed", json.loads(json.dumps(row, default=str)))
    rows = [{"id": t["id"], "variant": t.get("variant"), "book": t.get("book"), "direction": t.get("direction"),
             "shadow": t.get("shadow"), "event": bool(t.get("event")), "expiry": t.get("expiry"),
             "entry_date": t.get("entry_date"), "exit_date": t.get("exit_date"), "status": t.get("status"),
             "signal": t.get("signal"), "z": t.get("z"), "net_entry": t.get("net_entry"), "risk": t.get("risk"),
             "ret": t.get("ret"), "pnl_per_lot": t.get("pnl_per_lot"), "record": t} for t in led]
    if rows:
        supabase_upsert("mcx_options_paper", json.loads(json.dumps(rows, default=str)))
    log(f"feed published: {row['trading_date']} ({len(rows)} ledger rows)")


def main(cmd: str) -> int:
    ts = now_ist()
    today = ts.date()
    if cmd == "set-results-date":
        rd = dt.date.fromisoformat(sys.argv[2])
        pre, react = event_days(rd)
        OUT.mkdir(parents=True, exist_ok=True)
        EVENT_CFG.write_text(json.dumps({"results_date": rd.isoformat(), "set_at": ts.strftime("%Y-%m-%d %H:%M")}))
        log(f"results date set to {rd}: event entry {pre} ~15:10, exit {react} ~15:10")
        return 0
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
        if EVENT_CFG.exists():
            rd = json.loads(EVENT_CFG.read_text())["results_date"]
            pre, react = event_days(dt.date.fromisoformat(rd))
            print(f"results-day event: results {rd}, entry {pre} ~15:10, exit {react} ~15:10")
        else:
            print("results-day event: no date set (python ... options_paper.py set-results-date YYYY-MM-DD)")
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

    if cmd == "event-dry-run":  # the results-day rule as if today were the pre-day
        rd = sys.argv[2] if len(sys.argv) > 2 else json.loads(EVENT_CFG.read_text())["results_date"]
        open_event([], oc, today, ts, rd, event_days(dt.date.fromisoformat(rd))[1], write=False)
        return 0

    sig = signals(today)
    if cmd == "feed-preview":  # the feed row this run would publish; writes nothing
        row = feed_row(led, client, oc, today, ts, exps, sig, None)
        print(row["note"]); print(json.dumps(row, default=str))
        return 0

    spreads = log_spreads(oc, expiry, ts)
    in_window = FILL_WINDOW[0] <= ts.time() <= FILL_WINDOW[1]
    scheduled = [t for t in led if not t.get("event")]
    if today == entry_d:
        if in_window:
            open_positions(led, client, expiry, oc, today, ts, write=True, sig=sig)
        elif not any(t["expiry"] == expiry for t in scheduled):
            led.append({"id": f"{expiry}|missed", "expiry": expiry, "status": "missed", "entry_date": today.isoformat(),
                        "flags": [f"run at {ts:%H:%M} outside fill window"]})
            log(f"missed entry for {expiry}: ran at {ts:%H:%M}")
    elif entry_d < today < exit_d and not any(t["expiry"] == expiry for t in scheduled):
        led.append({"id": f"{expiry}|missed", "expiry": expiry, "status": "missed", "entry_date": entry_d.isoformat(),
                    "flags": ["no run on entry day"]})
        log(f"missed entry for {expiry}: no run on {entry_d}")
    if today >= exit_d and today < exp_d and in_window:
        close_positions(led, oc, today, ts, late=today > exit_d)
    event_step(led, oc, today, ts, in_window)
    save(led)
    log(f"run ok: series {expiry} entry {entry_d} exit {exit_d}; open={sum(t['status'] == 'open' for t in led)}")
    try:  # the feed is a view of the ledger: a failure here must not undo the run
        publish(led, feed_row(led, client, oc, today, ts, exps, sig, spreads))
    except Exception as e:
        log(f"feed not published: {type(e).__name__}: {e}", notify=True)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "run"))
    except Exception as e:  # e.g. expired Dhan token — make it loud, not silent
        log(f"ERROR {type(e).__name__}: {e}", notify=True)
        raise
