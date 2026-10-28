"""
Tusk house valuation model (shown beside the data-driven view on Fair value).

Follows the Tusk sheet "Based on latest trend - FY2028" (user, 29 Sep 2026):
- FY28 revenue = revenue per day x trading days, plus non-F&O operating revenue and other
  income, each grown 20% on FY26 and a further 15% on FY27.
- PAT = 57% of that total revenue; EPS = PAT / diluted shares.
- Target price for FY28 = P/E x FY28 EPS, at 42x / 48x / 54x (bear / base / bull).
- Discounted 18% to FY27, then a further 9% to today. The page can switch the second step
  to pro-rata: 18% x the days left to 31 Mar 2027 / 365.
- The regression of price on the 45-day ADR and the broker targets are reported as
  cross-checks; they are not blended into the house view.

Revenue per day uses the calendar count of 260 FY28 sessions (the user's decision; the sheet
shows 258). Every constant below is a house input, editable on the page, not a measured figure.
"""
from datetime import date, timedelta
import math

HOUSE_ADR_FY28 = 15.00              # FY28 revenue per day, Rs Cr (house input)
HOUSE_DAYS_FY28 = 260               # FY28 MCX sessions: 262 weekdays - 26 Jan 2028 - Muhurat-only 29 Oct 2027
HOUSE_DAYS = {"FY27": 256, "FY28": HOUSE_DAYS_FY28}   # FY27: 261 weekdays - 5 closures (used by Scenarios)
NON_FO_FY26_CR = 211.06             # FY26 non-F&O operating revenue: reported 2,302 - F&O daily sum 2,091
OTHER_INCOME_FY26_CR = 127.05       # FY26 other income (reported)
GROWTH_FY27 = 0.20                  # non-F&O and other income, FY26 -> FY27
GROWTH_FY28 = 0.15                  # non-F&O and other income, FY27 -> FY28
HOUSE_MARGIN = 0.57                 # PAT / total revenue including other income (the sheet's arithmetic)
HOUSE_PE = {"bear": 42, "base": 48, "bull": 54}
DISCOUNT_FY28_TO_FY27 = 0.18        # one year at the 18% hurdle
DISCOUNT_FY27_TO_TODAY = 0.09       # the sheet's fixed second step (half a year at 18%)
FY27_END = date(2027, 3, 31)
REGRESSION_START = date(2024, 11, 1)
HURDLE = 0.18                       # used to discount broker targets from their own target dates

# Broker targets (Tusk workbook, sheet 'MCX Analyst'). Update when new reports land.
# (broker, report date, rating, 12-month target Rs, FY27E EPS, FY28E EPS, P/E used on FY28E)
ANALYSTS = [
    ("Haitong", date(2026, 5, 12), "Outperform", 3500, 76.6, 91.9, 40),
    ("HDFC Securities", date(2026, 3, 24), "Buy", 2950, 61.4, 72.0, 43),
    ("IIFL Capital", date(2026, 5, 12), "Buy", 3500, 69.5, 82.0, 45),
    ("ICICI Securities", date(2026, 5, 12), "Hold", 3150, 71.7, 79.8, 40),
    ("Motilal Oswal", date(2026, 5, 11), "Neutral", 2850, 65.5, 71.3, 40),
    ("UBS", date(2026, 5, 29), "Neutral", 3600, 61.1, 72.3, 50),
]


def years(a, b):
    """Year fraction from a to b, actual/365."""
    return (b - a).days / 365


def fit_regression(pairs):
    """OLS price = a + b * adr. pairs: [(adr, price)]. Returns a dict or None."""
    n = len(pairs)
    if n < 20:
        return None
    xs, ys = [p[0] for p in pairs], [p[1] for p in pairs]
    mx, my = sum(xs) / n, sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    if sxx <= 0:
        return None
    b = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sxx
    a = my - b * mx
    resid = [y - (a + b * x) for x, y in zip(xs, ys)]
    s = math.sqrt(sum(r * r for r in resid) / (n - 2))
    ss_tot = sum((y - my) ** 2 for y in ys)
    return {"a": a, "b": b, "s": s, "n": n, "mean_x": mx, "sxx": sxx,
            "r_squared": 1 - sum(r * r for r in resid) / ss_tot if ss_tot else None}


def predict(fit, x):
    """Fitted price at ADR x and the 95% prediction band's half-width."""
    y = fit["a"] + fit["b"] * x
    half = 1.96 * fit["s"] * math.sqrt(1 + 1 / fit["n"] + (x - fit["mean_x"]) ** 2 / fit["sxx"])
    return y, half


def default_inputs():
    """The house inputs, as the page's input panel starts."""
    return {"adr_fy28": HOUSE_ADR_FY28, "days_fy28": HOUSE_DAYS_FY28,
            "non_fo_fy26": NON_FO_FY26_CR, "other_income_fy26": OTHER_INCOME_FY26_CR,
            "growth_fy27": GROWTH_FY27, "growth_fy28": GROWTH_FY28, "margin": HOUSE_MARGIN,
            "pe": dict(HOUSE_PE), "disc_fy28": DISCOUNT_FY28_TO_FY27, "disc_today": DISCOUNT_FY27_TO_TODAY,
            "method": "fixed"}


def second_step(inp, val_date):
    """The FY27 -> today discount: the fixed rate, or pro-rata to 31 Mar 2027 (simple, actual/365)."""
    days_left = max((FY27_END - val_date).days, 0)
    prorata = inp["disc_fy28"] * days_left / 365
    return (prorata if inp["method"] == "prorata" else inp["disc_today"]), days_left, prorata


def house_calc(inp, shares, val_date):
    """The sheet, line by line. Mirrors MCX.valueModel.houseCalc in assets/js/value.js."""
    grow = (1 + inp["growth_fy27"]) * (1 + inp["growth_fy28"])
    op = inp["adr_fy28"] * inp["days_fy28"]
    non_fo = inp["non_fo_fy26"] * grow
    other = inp["other_income_fy26"] * grow
    total = op + non_fo + other
    pat = total * inp["margin"]
    eps = pat / shares
    step2, days_left, prorata = second_step(inp, val_date)
    fy28 = {k: pe * eps for k, pe in inp["pe"].items()}
    fy27 = {k: v / (1 + inp["disc_fy28"]) for k, v in fy28.items()}
    today = {k: v / (1 + step2) for k, v in fy27.items()}
    return {"op_rev_cr": op, "non_fo_cr": non_fo, "other_income_cr": other, "total_rev_cr": total,
            "pat_cr": pat, "eps": eps, "fy28": fy28, "fy27": fy27, "today": today,
            "disc_today": step2, "days_to_fy27_end": days_left, "prorata": prorata}


def house_view(adr, price, val_date, shares, reg_pairs):
    """The house model at its default inputs, plus the two cross-checks.
    adr: today's 45-day ADR (context only; the house case uses its own FY28 input)."""
    inp = default_inputs()
    c = house_calc(inp, shares, val_date)
    pro = house_calc({**inp, "method": "prorata"}, shares, val_date)

    fit = fit_regression(reg_pairs)
    reg = None
    if fit:
        y, half = predict(fit, adr)
        reg = {"value": y, "band": half, "low": y - half, "high": y + half, "a": fit["a"], "b": fit["b"],
               "n": fit["n"], "r_squared": fit["r_squared"], "since": REGRESSION_START.isoformat()}

    analysts = []
    for broker, rep, rating, tgt, e27, e28, pe in ANALYSTS:
        tdate = rep + timedelta(days=365)
        ta = years(val_date, tdate)
        analysts.append({"broker": broker, "report_date": rep.isoformat(), "rating": rating, "target": tgt,
                         "target_date": tdate.isoformat(), "years": round(max(ta, 0.0), 3),
                         "present_value": tgt / (1 + HURDLE) ** max(ta, 0.0), "expired": ta < 0,
                         "eps27": e27, "eps28": e28, "pe": pe})
    analyst_leg = sum(x["present_value"] for x in analysts) / len(analysts)

    r = lambda v, dp=2: None if v is None else round(v, dp)
    rk = lambda d, dp=0: {k: r(v, dp) for k, v in d.items()}
    street_eps28 = sum(x["eps28"] for x in analysts) / len(analysts)
    street_pe = sum(x["pe"] for x in analysts) / len(analysts)
    return {
        "inputs": inp,
        "assumptions": {"days": HOUSE_DAYS, "fy27_end": FY27_END.isoformat(), "hurdle": HURDLE,
                        "margin_base": "total revenue including other income"},
        "valuation_date": val_date.isoformat(), "adr_cr": r(adr), "price": r(price), "shares_cr": shares,
        "fy28": {"op_rev_cr": r(c["op_rev_cr"], 1), "non_fo_cr": r(c["non_fo_cr"], 1), "other_income_cr": r(c["other_income_cr"], 1),
                 "total_rev_cr": r(c["total_rev_cr"], 1), "pat_cr": r(c["pat_cr"], 1), "eps": r(c["eps"])},
        "target_fy28": rk(c["fy28"]), "target_fy27": rk(c["fy27"]), "today": rk(c["today"]),
        "today_prorata": rk(pro["today"]), "prorata_pct": r(pro["prorata"] * 100),
        "days_to_fy27_end": c["days_to_fy27_end"],
        "regression": None if not reg else {k: (r(v, 4) if k in ("a", "b", "r_squared") else r(v, 0) if isinstance(v, float) else v) for k, v in reg.items()},
        "analysts": [{**x, "present_value": r(x["present_value"], 0)} for x in analysts],
        "analyst_leg": r(analyst_leg, 0),
        "street": {"eps28": r(street_eps28), "pe": r(street_pe, 1), "brokers": len(analysts)},
    }
