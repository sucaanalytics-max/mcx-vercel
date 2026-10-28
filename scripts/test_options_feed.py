#!/usr/bin/env python3
"""Offline tests for the options feed view in api/models.py: the paper record summary and the
trade rows the Signals → Options page reads. No network.
Run:  python3 scripts/test_options_feed.py"""
import importlib.util
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
spec = importlib.util.spec_from_file_location("models", os.path.join(ROOT, "api", "models.py"))
md = importlib.util.module_from_spec(spec)
spec.loader.exec_module(md)

failed = 0


def check(name, got, want):
    global failed
    ok = got == want
    print(("PASS " if ok else "FAIL ") + name + ("" if ok else f": got {got!r}, want {want!r}"))
    if not ok:
        failed += 1


trades = [
    {"variant": "v45_20", "book": "PCS", "shadow": False, "status": "closed", "ret": 0.30, "pnl_per_lot": 6000},
    {"variant": "v45_20", "book": "PCS", "shadow": False, "status": "closed", "ret": -0.10, "pnl_per_lot": -2000},
    {"variant": "v45_20", "book": "PCS", "shadow": False, "status": "open", "ret": None},
    {"variant": "v45_20", "book": "PCS", "shadow": True, "status": "closed", "ret": -0.50, "pnl_per_lot": -9000},
    {"variant": None, "book": "EVT_IFLY", "shadow": False, "event": True, "status": "closed", "ret": 0.12, "pnl_per_lot": 2400},
    {"id": "2026-11-23|missed", "status": "missed"},
]
s = md.summarise_trades(trades)
check("open, missed and unclosed trades are left out", sum(r["n"] for r in s), 4)
pcs = next(r for r in s if r["book"] == "PCS" and not r["shadow"])
check("mean return per trade", pcs["mean_ret"], 0.1)
check("hit rate", pcs["hit"], 0.5)
check("P&L per lot is summed", pcs["pnl_per_lot"], 4000)
check("worst and best", (pcs["worst"], pcs["best"]), (-0.1, 0.3))
check("shadow trades kept apart", [r["shadow"] for r in s if r["book"] == "PCS"], [False, True])
check("event trades grouped under 'event'", [r["variant"] for r in s if r["book"] == "EVT_IFLY"], ["event"])

row = md._trade_row({"id": "x", "status": "closed", "ret": 0.2, "record": {
    "legs": [{"strike": 3150.0, "type": "PE", "qty": -1, "entry_fill": 117.15, "exit_fill": 60.0, "security_id": 1}],
    "flags": ["late_exit"]}})
check("trade row keeps legs without broker ids", row["legs"], [{"strike": 3150.0, "type": "PE", "qty": -1, "fill": 117.15, "exit": 60.0}])
check("trade row keeps flags", row["flags"], ["late_exit"])
check("backtest references are Schedule B, signal > 0", (md.OPTIONS_BACKTEST["PCS"]["mean"], md.OPTIONS_BACKTEST["CDS"]["mean"]), (0.22, 0.32))

print(f"\n{failed} FAILED" if failed else "\nAll options feed tests passed.")
sys.exit(1 if failed else 0)
