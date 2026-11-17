#!/usr/bin/env python3
"""
Futures open interest from MCX's Bhavcopy (lib/mcx_bhavcopy.py), which replaced
Dhan as the OI source in Nov 2026:

  1. fetch_day returns [] when MCX answers with another day (its silent
     fallback for a holiday / unpublished date / wrong date format).
  2. futures_oi sums every open futures contract; OI value uses the day's lot
     multiplier, else a carried-forward one, else None.
  3. backfill_commodity_oi.build_payload writes only onto stored futures rows,
     skips empty dates, reports failed ones, keeps a stored value it can't price.
  4. bhav_refresh.fill_futures_oi fills futures rows only and is soft on errors.

Pure: no network, no Supabase.
"""
import os, sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

FAILED = []


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}" + (f"  ({detail})" if detail and not cond else ""))
    if not cond:
        FAILED.append(name)


def row(sym, inst, oi, close=100.0, vol=0, value=0.0, date="11/13/2026", expiry="30NOV2026"):
    return {"Symbol": sym.ljust(12), "InstrumentName": inst, "OpenInterest": oi, "Close": close,
            "Volume": vol, "Value": value, "Date": date, "ExpiryDate": expiry}


class _Resp:
    def __init__(self, body): self.body = body
    def raise_for_status(self): pass
    def json(self): return self.body


class _Session:
    def __init__(self, rows): self.rows, self.params = rows, None
    def get(self, url, params=None, headers=None, timeout=None):
        self.params = params
        return _Resp({"IsSuccess": True, "Data": self.rows})


def test_fetch_day():
    print("\n[1] fetch_day guards MCX's latest-day fallback")
    from lib.mcx_bhavcopy import fetch_day
    s = _Session([row("GOLD", "FUTCOM", 10, date="11/16/2026")])
    check("other day returned -> []", fetch_day(s, "2026-11-13") == [])
    check("asks with DD/MM/YYYY", s.params["fromDate"] == "13/11/2026", str(s.params))
    s = _Session([row("GOLD", "FUTCOM", 10)])
    check("matching day -> rows", len(fetch_day(s, "2026-11-13")) == 1)
    check("no rows -> []", fetch_day(_Session([]), "2026-11-13") == [])


def test_futures_oi():
    print("\n[2] futures_oi")
    from lib.mcx_bhavcopy import futures_oi
    # GOLD: 1 kg lot quoted per 10 g -> multiplier 100. Traded: 10 lots at 1.3 lakh/10g
    # = Rs 1300 lakh value. Second contract untraded, OI 5.
    rows = [row("GOLD", "FUTCOM", 20, close=130000, vol=10, value=1300.0),
            row("GOLD", "FUTCOM", 5, close=131000, expiry="31DEC2026"),
            row("GOLD", "OPTFUT", 999),                     # options are not futures OI
            row("ZINC", "FUTCOM", 7, close=300)]            # no trade, no multiplier
    oi = futures_oi(rows)
    g = oi[("GOLD", "FUTCOM")]
    check("sums every futures contract", g["open_interest"] == 25, str(g))
    want = round((20 * 130000 * 100 + 5 * 131000 * 100) / 1e7, 2)
    check("OI value from the day's lot multiplier", abs(g["oi_value_cr"] - want) < 0.01, f"{g} vs {want}")
    check("options excluded", ("GOLD", "OPTFUT") not in oi)
    check("unpriced symbol: OI kept, value None", oi[("ZINC", "FUTCOM")] == {"open_interest": 7, "oi_value_cr": None})
    z = futures_oi(rows, fallback_mult={"ZINC": 5000.0})[("ZINC", "FUTCOM")]
    check("carried-forward multiplier prices it", z["oi_value_cr"] == round(7 * 300 * 5000 / 1e7, 2), str(z))


def test_build_payload():
    print("\n[3] backfill_commodity_oi.build_payload")
    import backfill_commodity_oi as b
    have = {
        "2026-11-13": {("GOLD", "FUTCOM"): {"open_interest": 19558, "oi_value_cr": 268.49},
                       ("ZINC", "FUTCOM"): {"open_interest": 2647, "oi_value_cr": 0.11}},
        "2026-11-16": {("GOLD", "FUTCOM"): {"open_interest": 0, "oi_value_cr": 0}},
        "2026-11-17": {("GOLD", "FUTCOM"): {"open_interest": 0, "oi_value_cr": 0}},
        "2026-11-18": {("GOLD", "FUTCOM"): {"open_interest": 0, "oi_value_cr": 0}},
    }
    seen_mult = []

    def fetch(d, mult):
        seen_mult.append(dict(mult))
        if d == "2026-11-13":
            return ({("GOLD", "FUTCOM"): {"open_interest": 19563, "oi_value_cr": 26876.0},
                     ("ZINC", "FUTCOM"): {"open_interest": 2647, "oi_value_cr": None},
                     ("NEWSYM", "FUTCOM"): {"open_interest": 5, "oi_value_cr": 1.0}}, {"GOLD": 100.0})
        if d == "2026-11-16":
            return {("GOLD", "FUTCOM"): {"open_interest": 19390, "oi_value_cr": 26262.27}}, {}
        if d == "2026-11-17":
            return {}, {}                                   # not published yet
        raise RuntimeError("timeout")
    payload, changes, failed, empty = b.build_payload(have, fetch, sleep=lambda s: None)
    keys = {(p["trading_date"], p["commodity"]) for p in payload}
    check("writes stored rows only (no NEWSYM stub)", keys == {("2026-11-13", "GOLD"), ("2026-11-13", "ZINC"),
                                                               ("2026-11-16", "GOLD")}, str(keys))
    zinc = next(p for p in payload if p["commodity"] == "ZINC")
    check("unpriced value keeps the stored one", zinc["oi_value_cr"] == 0.11, str(zinc))
    check("unpublished date skipped", empty == ["2026-11-17"], str(empty))
    check("failed date reported", failed == ["2026-11-18"], str(failed))
    check("multipliers carry forward", seen_mult[1] == {"GOLD": 100.0}, str(seen_mult))
    gold16 = next(c for c in changes if c["trading_date"] == "2026-11-16")
    check("change log has old and new", gold16["old_open_interest"] == 0 and gold16["open_interest"] == 19390)
    check("only OI columns written", set(payload[0]) == {"trading_date", "commodity", "instrument_type",
                                                          "open_interest", "oi_value_cr"}, str(payload[0]))


def test_fill_futures_oi():
    print("\n[4] bhav_refresh.fill_futures_oi")
    import bhav_refresh as br
    rows = [{"trading_date": "2026-11-16", "commodity": "GOLD", "instrument_type": "FUTCOM",
             "open_interest": 0, "oi_value_cr": 0.0},
            {"trading_date": "2026-11-16", "commodity": "GOLD", "instrument_type": "OPTFUT",
             "open_interest": 0, "oi_value_cr": 0.0}]
    oi = {("GOLD", "FUTCOM"): {"open_interest": 19390, "oi_value_cr": 26262.27},
          ("GOLD", "OPTFUT"): {"open_interest": 5, "oi_value_cr": 1.0}}
    out = br.fill_futures_oi(rows, "2026-11-16", fetch=lambda d: oi)
    check("futures row filled", out[0]["open_interest"] == 19390 and out[0]["oi_value_cr"] == 26262.27, str(out[0]))
    check("input rows not mutated", rows[0]["open_interest"] == 0)

    def boom(d):
        raise RuntimeError("akamai")
    check("error -> rows unchanged", br.fill_futures_oi(rows, "2026-11-16", fetch=boom) == rows)
    check("no Bhavcopy yet -> rows unchanged", br.fill_futures_oi(rows, "2026-11-16", fetch=lambda d: {}) == rows)


if __name__ == "__main__":
    print("=" * 72)
    print("MCX Bhavcopy OI tests")
    print("=" * 72)
    test_fetch_day()
    test_futures_oi()
    test_build_payload()
    test_fill_futures_oi()
    print("\n" + "=" * 72)
    if FAILED:
        print(f"{len(FAILED)} FAILED: {', '.join(FAILED)}")
        sys.exit(1)
    print("All tests passed.")
