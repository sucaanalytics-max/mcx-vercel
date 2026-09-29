#!/usr/bin/env python3
"""Offline tests for lib/house_model.py (the Tusk house valuation). No network.
Run:  python3 scripts/test_house_model.py"""
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


SH = 25.451
R = lambda d: {k: round(v) for k, v in d.items()}

# The Tusk sheet, exactly: Rs 15.00 x 258 days, fixed 18% then 9%
sheet = hm.house_calc({**hm.default_inputs(), "days_fy28": 258}, SH, date(2026, 9, 29))
check("operating revenue 15 x 258", round(sheet["op_rev_cr"]), 3870)
check("other operating revenue 211.06 x 1.2 x 1.15", round(sheet["non_fo_cr"]), 291)
check("other income 127.05 x 1.2 x 1.15", round(sheet["other_income_cr"]), 175)
check("total revenue", round(sheet["total_rev_cr"]), 4337)
check("PAT at 57% of total revenue", round(sheet["pat_cr"]), 2472)
check("EPS", round(sheet["eps"]), 97)
check("FY28 target at 42 / 48 / 54x", R(sheet["fy28"]), {"bear": 4079, "base": 4662, "bull": 5245})
check("FY27 target, discounted 18%", R(sheet["fy27"]), {"bear": 3457, "base": 3951, "bull": 4445})
check("today, discounted a further 9% (the sheet shows 3,624 for 3,624.5)",
      {k: round(v, 1) for k, v in sheet["today"].items()}, {"bear": 3171.5, "base": 3624.5, "bull": 4077.6})

# The house defaults: 260 sessions (the user's choice on 29 Sep)
v = hm.house_view(11.99, 3263.5, date(2026, 9, 29), SH, [])
check("house inputs", (v["inputs"]["adr_fy28"], v["inputs"]["days_fy28"], v["inputs"]["pe"], v["inputs"]["method"]),
      (15.0, 260, {"bear": 42, "base": 48, "bull": 54}, "fixed"))
check("FY28 EPS at 260 sessions", v["fy28"]["eps"], 97.79)
check("today at 260 sessions", v["today"], {"bear": 3193.0, "base": 3650.0, "bull": 4106.0})
check("no blend: the house view is the model alone", "blend" in v, False)

# Pro-rata second step: 18% x days left to 31 Mar 2027 / 365
check("183 days left on 29 Sep 2026 gives 9.02%", (v["days_to_fy27_end"], v["prorata_pct"]), (183, 9.02))
late = hm.house_calc({**hm.default_inputs(), "method": "prorata"}, SH, date(2027, 3, 1))
check("a month before year end the step is 18% x 30/365", round(late["disc_today"] * 100, 2), 1.48)
after = hm.house_calc({**hm.default_inputs(), "method": "prorata"}, SH, date(2027, 5, 1))
check("after 31 Mar 2027 the pro-rata step is zero", after["disc_today"], 0.0)
check("the fixed step ignores the date", hm.house_calc(hm.default_inputs(), SH, date(2027, 5, 1))["disc_today"], 0.09)

# Cross-checks: regression recovered, brokers discounted from their own dates
pairs = [(x / 10, 500 + 200 * (x / 10) + (5 if x % 2 else -5)) for x in range(60, 160)]
fit = hm.fit_regression(pairs)
check("OLS slope and intercept", (round(fit["b"], 1), round(fit["a"], 0)), (200.0, 500.0))
check("prediction band widens away from the data's centre", hm.predict(fit, 20)[1] > hm.predict(fit, fit["mean_x"])[1], True)
check("too few points: no fit", hm.fit_regression(pairs[:10]), None)
v2 = hm.house_view(11.87, 3271.3, date(2026, 9, 28), SH, pairs)
check("regression reported as a cross-check", round(v2["regression"]["value"]), round(500 + 200 * 11.87))
check("analyst leg: each target discounted from its own target date", v2["analyst_leg"], 2947.0)
check("street: average FY28E EPS and P/E", (v2["street"]["eps28"], v2["street"]["pe"]), (78.22, 43.0))
check("Scenarios still gets the session counts", v2["assumptions"]["days"], {"FY27": 256, "FY28": 260})

print(f"\n{failed} FAILED" if failed else "\nAll house model tests passed.")
sys.exit(1 if failed else 0)
