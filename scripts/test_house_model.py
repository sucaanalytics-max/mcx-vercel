#!/usr/bin/env python3
"""Offline tests for lib/house_model.py (the Tusk house valuation). No network.
Run:  python3 scripts/test_house_model.py"""
import math
import os
import sys
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lib import house_model as hm

failed = 0


def check(name, got, want):
    global failed
    ok = got == want
    print(("PASS " if ok else "FAIL ") + name + ("" if ok else f": got {got!r}, want {want!r}"))
    if not ok:
        failed += 1


# The design canvas's snapshot: ADR 11.87, price 3,271.30 on 28 Sep 2026
v = hm.house_view(11.87, 3271.3, date(2026, 9, 28), 25.451, [])
check("FY27E EPS (256 sessions, non-F&O 211.06 x 1.2, 60% margin)", v["eps27"], 77.61)
check("FY28E EPS (ADR and non-F&O +20%, 260 sessions)", v["eps28"], 94.47)
check("pro-rata years to 31 Mar 2027", round(v["years_to_target"], 3), 0.504)
check("target price at 48x and 52x FY28E", v["target"], {"48": 4535.0, "52": 4913.0})
check("ADR leg discounted at 18%", v["adr_leg"], {"48": 4172.0, "52": 4519.0})
check("analyst leg: each target discounted from its own target date", v["analyst_leg"], 2947.0)
check("street: average FY28E EPS and P/E", (v["street"]["eps28"], v["street"]["pe"]), (78.22, 43.0))
check("without a regression, the blend averages the two remaining legs", v["blend"]["48"], round((4171.67 + 2946.96) / 2))

# Regression: exact line recovered, band grows away from the mean
pairs = [(x / 10, 500 + 200 * (x / 10) + (5 if x % 2 else -5)) for x in range(60, 160)]
fit = hm.fit_regression(pairs)
check("OLS slope and intercept", (round(fit["b"], 1), round(fit["a"], 0)), (200.0, 500.0))
y_mid, band_mid = hm.predict(fit, fit["mean_x"])
y_far, band_far = hm.predict(fit, 20)
check("prediction band widens away from the data's centre", band_far > band_mid, True)
check("too few points: no fit", hm.fit_regression(pairs[:10]), None)

# With the regression, the blend is the equal-weight average of three legs
v2 = hm.house_view(11.87, 3271.3, date(2026, 9, 28), 25.451, pairs)
reg = v2["regression"]["value"]
check("house blend within one rupee of the three-leg mean",
      abs(v2["blend"]["48"] - (4171.67 + (500 + 200 * 11.87) + 2946.96) / 3) < 1.5, True)

# Discounting: no discount on the target date, and none after it
v3 = hm.house_view(11.87, 3271.3, date(2027, 3, 31), 25.451, [])
check("on the target date the ADR leg is the target", v3["adr_leg"], v3["target"])
v4 = hm.house_view(11.87, 3271.3, date(2027, 7, 1), 25.451, [])
check("after the target date, no negative discounting", v4["discount"], 1.0)
check("analyst targets past their date are flagged", all(a["expired"] for a in v4["analysts"] if a["target_date"] < "2027-07-01"), True)

# Sensitivity: the 20% row is the house case
row20 = [r for r in v["sensitivity"] if r["growth"] == 0.2][0]
check("sensitivity at 20% matches the house FY28E EPS", round(row20["eps28"], 2), v["eps28"])
check("lower growth, lower value", [r["adr_leg_48"] for r in v["sensitivity"]] == sorted(r["adr_leg_48"] for r in v["sensitivity"]), True)

print(f"\n{failed} FAILED" if failed else "\nAll house model tests passed.")
sys.exit(1 if failed else 0)
