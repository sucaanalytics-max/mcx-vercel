#!/usr/bin/env python3
"""Fetch NSE F&O bhavcopies and keep only one symbol's stock options/futures rows.

Pass 1 downloads the first trading day of each month to learn the monthly expiries;
pass 2 downloads the entry/exit days of the ADR stress-test schedules
(Docs/ADR_Edge_StressTest_Preregistration.md §C). Raw zips are deleted after parsing.

Usage:
  python3 -I scripts/fetch_nse_fo_bhav.py OUT_DIR [SYMBOL]
Writes OUT_DIR/<symbol>_bhav.csv (normalised rows) and OUT_DIR/schedule.json.
"""
from __future__ import annotations
import csv, io, json, sys, time, zipfile, datetime as dt
import urllib.request
from pathlib import Path

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124 Safari/537.36"
UDIFF_START = dt.date(2024, 7, 8)
COLS = ["date", "inst", "expiry", "strike", "opt", "close", "settle", "contracts", "oi", "lot", "undl"]


def urls(d: dt.date) -> list[str]:
    new = f"https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_{d:%Y%m%d}_F_0000.csv.zip"
    mon = d.strftime("%b").upper()
    old = f"https://nsearchives.nseindia.com/content/historical/DERIVATIVES/{d.year}/{mon}/fo{d:%d}{mon}{d.year}bhav.csv.zip"
    return [new, old] if d >= UDIFF_START else [old, new]


def fetch(d: dt.date) -> bytes | None:
    for u in urls(d):
        try:
            req = urllib.request.Request(u, headers={"User-Agent": UA, "Referer": "https://www.nseindia.com/"})
            data = urllib.request.urlopen(req, timeout=30).read()
            if data[:2] == b"PK":
                return data
        except Exception:
            pass
        time.sleep(0.7)
    return None


def parse(data: bytes, d: dt.date, symbol: str) -> list[dict]:
    z = zipfile.ZipFile(io.BytesIO(data))
    text = z.read(z.namelist()[0]).decode("utf-8", "replace")
    rd = csv.DictReader(io.StringIO(text))
    out = []
    for r in rd:
        r = {k.strip(): (v or "").strip() for k, v in r.items() if k}
        if "TckrSymb" in r:  # UDiFF
            if r["TckrSymb"] != symbol or r.get("FinInstrmTp") not in ("STO", "STF"):
                continue
            out.append({"date": d.isoformat(), "inst": "OPTSTK" if r["FinInstrmTp"] == "STO" else "FUTSTK",
                        "expiry": r["XpryDt"], "strike": r.get("StrkPric") or "0", "opt": r.get("OptnTp") or "XX",
                        "close": r["ClsPric"], "settle": r["SttlmPric"], "contracts": r.get("TtlTradgVol", "0"),
                        "oi": r.get("OpnIntrst", ""), "lot": r.get("NewBrdLotQty", ""), "undl": r.get("UndrlygPric", "")})
        elif r.get("SYMBOL") == symbol and r.get("INSTRUMENT") in ("OPTSTK", "FUTSTK"):
            exp = dt.datetime.strptime(r["EXPIRY_DT"], "%d-%b-%Y").date().isoformat()
            out.append({"date": d.isoformat(), "inst": r["INSTRUMENT"], "expiry": exp, "strike": r["STRIKE_PR"],
                        "opt": r["OPTION_TYP"], "close": r["CLOSE"], "settle": r["SETTLE_PR"],
                        "contracts": r["CONTRACTS"], "oi": r["OPEN_INT"], "lot": "", "undl": ""})
    return out


def main() -> int:
    out_dir = Path(sys.argv[1]); out_dir.mkdir(parents=True, exist_ok=True)
    symbol = sys.argv[2] if len(sys.argv) > 2 else "MCX"
    days = [dt.date.fromisoformat(x) for x in json.loads((out_dir / "nse_days.json").read_text())]
    store = out_dir / f"{symbol.lower()}_bhav.csv"
    have = set()
    if store.exists():
        have = {r["date"] for r in csv.DictReader(open(store))}
    missing = []

    def grab(d: dt.date):
        if d.isoformat() in have:
            return
        data = fetch(d)
        if data is None:
            missing.append(d.isoformat()); return
        rows = parse(data, d, symbol)
        new_file = not store.exists()
        with open(store, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=COLS)
            if new_file: w.writeheader()
            w.writerows(rows)
        have.add(d.isoformat())
        print(f"{d} rows={len(rows)}", flush=True)
        time.sleep(0.7)

    # pass 1: first trading day of each month -> expiries
    firsts = {}
    for d in days:
        firsts.setdefault((d.year, d.month), d)
    for d in sorted(firsts.values()):
        grab(d)
    rows = list(csv.DictReader(open(store)))
    expiries = sorted({r["expiry"] for r in rows if r["inst"] == "OPTSTK"})
    dset = [d.isoformat() for d in days]
    sched = []
    for prev, exp in zip(expiries, expiries[1:]):
        if exp not in dset or prev not in dset:
            continue
        i_prev, i_exp = dset.index(prev), dset.index(exp)
        if i_exp - i_prev < 12:
            continue  # not consecutive monthly expiries (weekly/odd listing)
        sched.append({"expiry": exp, "prev_expiry": prev, "entry_A": dset[i_prev + 1],
                      "entry_B": dset[i_exp - 10], "exit": dset[i_exp - 2]})
    (out_dir / "schedule.json").write_text(json.dumps(sched, indent=1))
    # pass 2: entry/exit days
    for s in sched:
        for k in ("entry_A", "entry_B", "exit"):
            grab(dt.date.fromisoformat(s[k]))
    print(f"DONE series={len(sched)} missing={missing}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
