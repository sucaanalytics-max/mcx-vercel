#!/usr/bin/env python3
"""
Futures expiry dates per MCX symbol, for the OI z-score study
(docs/OI_Zscore_Preregistration.md). Reads MCX's Bhavcopy (lib/mcx_bhavcopy)
on the first trading day of each month, 2022-01 → now. Every live contract is
listed on each date, so monthly sampling sees every expiry. Read-only; cached at
~/.cache/mcx-vercel/futures_expiries.json.

  scripts/mcx_uv.sh --extra research python -I research/oi_expiries.py [--refresh]
"""
from __future__ import annotations
import datetime as dt, json, os, sys, time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

CACHE = os.path.expanduser("~/.cache/mcx-vercel/futures_expiries.json")


def _parse_expiry(s: str) -> str | None:
    s = (s or "").strip().upper()
    for fmt in ("%d%b%Y", "%d-%b-%Y", "%d/%m/%Y"):
        try:
            return dt.datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            pass
    return None


def load(refresh=False) -> dict[str, list[str]]:
    """symbol -> sorted list of futures expiry dates (ISO)."""
    if os.path.exists(CACHE) and not refresh:
        return json.load(open(CACHE))
    from lib.mcx_bhavcopy import new_bhavcopy_session, fetch_day
    s = new_bhavcopy_session()
    out: dict[str, set] = {}
    d = dt.date(2022, 1, 1)
    today = dt.date.today()
    while d <= today:
        # first date in the month that has a Bhavcopy (try up to 8 days)
        for k in range(8):
            day = d + dt.timedelta(days=k)
            rows = []
            for inst in ("FUTCOM", "FUTIDX"):
                rows += fetch_day(s, day.isoformat(), inst)
            if rows:
                for r in rows:
                    e = _parse_expiry(r.get("ExpiryDate"))
                    if e:
                        out.setdefault(str(r.get("Symbol") or "").strip(), set()).add(e)
                print(f"{day}: {len(rows)} futures rows", flush=True)
                break
            time.sleep(0.3)
        d = (d.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
        time.sleep(0.5)
    res = {k: sorted(v) for k, v in out.items() if k}
    os.makedirs(os.path.dirname(CACHE), exist_ok=True)
    json.dump(res, open(CACHE, "w"), indent=0)
    return res


if __name__ == "__main__":
    e = load(refresh="--refresh" in sys.argv)
    print(f"{len(e)} symbols; e.g. GOLD {e.get('GOLD', [])[:4]} … CRUDEOIL {e.get('CRUDEOIL', [])[:4]} …")
