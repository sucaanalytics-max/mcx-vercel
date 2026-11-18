#!/usr/bin/env python3
"""
Commodity signals: the OI z-score is computed on futures OI in Rs value, not
lots (docs/OI_Zscore_Results.md), and OI stays out of the composite, so
composite_z and the signal do not depend on OI at all. Pure: the Supabase read
and write are stubbed.
"""
import math, os, sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import lib.cron_commodity_signals as ccs   # noqa: E402

FAILED = []


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}" + (f"  ({detail})" if detail and not cond else ""))
    if not cond:
        FAILED.append(name)


def rows(n=90, lots=None, value=None):
    """One FUTCOM complex (ZINC) over n days; turnover/volume vary, OI per callbacks."""
    out = []
    for i in range(n):
        d = f"2026-{1 + i // 28:02d}-{1 + i % 28:02d}"
        out.append({"trading_date": d, "commodity": "ZINC", "commodity_head": "BASE METALS",
                    "instrument_type": "FUTCOM", "contracts": 100 + i, "volume_lots": 1000 + 7 * (i % 11),
                    "turnover_cr": 500 + 13 * (i % 17), "premium_turnover_cr": 0,
                    "open_interest": lots(i), "oi_value_cr": value(i)})
    return out


def run(data):
    written = []
    real_fetch, real_up = ccs.fetch_all, ccs.upsert_batches
    ccs.fetch_all = lambda table, select, **k: data
    ccs.upsert_batches = lambda table, rs, **k: (written.extend(rs), [])[1]
    try:
        ccs.compute_commodity_signals(mode="backfill")
    finally:
        ccs.fetch_all, ccs.upsert_batches = real_fetch, real_up
    return {r["trading_date"]: r for r in written}


print("[1] OI z follows Rs value, not lots")
check("basis is value", ccs.OI_Z_BASIS == "value")
# lots constant (z undefined: SD 0) but value varying -> OI z present
a = run(rows(lots=lambda i: 1000, value=lambda i: 500 + 9 * (i % 13)))
last = a[max(a)]
check("value varies, lots flat -> OI z computed", last["oi_zscore"] is not None, str(last["oi_zscore"]))
# value constant but lots varying -> OI z undefined
b = run(rows(lots=lambda i: 1000 + 37 * (i % 13), value=lambda i: 500))
check("lots vary, value flat -> no OI z", b[max(b)]["oi_zscore"] is None, str(b[max(b)]["oi_zscore"]))
# missing value (0) is missing, like missing lots
c = run(rows(lots=lambda i: 1000, value=lambda i: 0 if i == 89 else 500 + 9 * (i % 13)))
check("zero value on the day -> no OI z that day", c[max(c)]["oi_zscore"] is None)

print("[2] composite and signal do not depend on OI")
check("OI stays out of the composite", ccs.OI_IN_COMPOSITE is False)
same = all(a[d]["composite_z"] == b[d]["composite_z"] and a[d]["commodity_signal"] == b[d]["commodity_signal"]
           for d in a)
check("composite_z and signal identical whatever the OI", same)
check("composite present", last["composite_z"] is not None and not math.isnan(last["composite_z"]))

print()
if FAILED:
    print(f"{len(FAILED)} FAILED: {', '.join(FAILED)}")
    sys.exit(1)
print("All tests passed.")
