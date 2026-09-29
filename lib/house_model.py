"""
Tusk house valuation model (shown beside the data-driven view on Fair value).

Follows the Tusk workbook (20260121_Exchanges_Dashboard.xlsx) with the corrections the
user decided on 29 Sep 2026:
- 48-52x is a FORWARD multiple on FY28E EPS, giving a target price at 31 Mar 2027.
- That target is discounted to today at the 18% hurdle, pro rata (actual days / 365).
- The regression of price on the 45-day ADR stays in the blend, refitted live.
- Analyst targets are each discounted from their own target date (report + 12 months).
- The blend is the equal-weight average of the ADR model, the regression and the analysts.

Every constant below is a house assumption or an input table, not a measured figure.
"""
from datetime import date, timedelta
import math

HOUSE_PE = (48, 52)                 # forward P/E on FY28E EPS
HURDLE = 0.18                       # required annual return, used to discount to today
HOUSE_MARGIN = 0.60                 # PAT / total revenue
HOUSE_DAYS = {"FY27": 256, "FY28": 260}   # MCX sessions: FY27 261 weekdays - 5 closures; FY28 262 - 26 Jan 2028 - Muhurat-only 29 Oct 2027
HOUSE_FY28_GROWTH = 0.20            # ADR and non-F&O revenue growth into FY28
NON_FO_FY26_CR = 211.06             # FY26 non-F&O operating revenue: reported 2,302 - F&O daily sum 2,091
NON_FO_FY27_GROWTH = 0.20           # non-F&O FY27 = FY26 x 1.2
TARGET_DATE = date(2027, 3, 31)     # the forward target is struck at FY27's end
REGRESSION_START = date(2024, 11, 1)
SENSITIVITY = (0.0, 0.10, 0.20)

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
    """Year fraction from a to b, actual/365 (YEARFRAC basis 3)."""
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


def eps(adr, days, non_fo, shares, margin=HOUSE_MARGIN):
    return (adr * days + non_fo) * margin / shares


def house_view(adr, price, val_date, shares, reg_pairs):
    """The house model at today's 45-day ADR. reg_pairs: [(adr, price)] since REGRESSION_START."""
    nf27 = NON_FO_FY26_CR * (1 + NON_FO_FY27_GROWTH)
    nf28 = nf27 * (1 + HOUSE_FY28_GROWTH)
    eps27 = eps(adr, HOUSE_DAYS["FY27"], nf27, shares)
    eps28 = eps(adr * (1 + HOUSE_FY28_GROWTH), HOUSE_DAYS["FY28"], nf28, shares)
    t = max(years(val_date, TARGET_DATE), 0.0)
    disc = (1 + HURDLE) ** t
    target = {str(pe): pe * eps28 for pe in HOUSE_PE}
    adr_leg = {str(pe): pe * eps28 / disc for pe in HOUSE_PE}

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

    legs = lambda pe: [adr_leg[str(pe)], analyst_leg] + ([reg["value"]] if reg else [])
    blend = {str(pe): sum(legs(pe)) / len(legs(pe)) for pe in HOUSE_PE}

    sens = []
    for g in SENSITIVITY:
        e = eps(adr * (1 + g), HOUSE_DAYS["FY28"], nf27 * (1 + g), shares)
        row = {"growth": g, "eps28": e}
        for pe in HOUSE_PE:
            leg = pe * e / disc
            row[f"adr_leg_{pe}"] = leg
            parts = [leg, analyst_leg] + ([reg["value"]] if reg else [])
            row[f"blend_{pe}"] = sum(parts) / len(parts)
        sens.append(row)

    street_eps28 = sum(x["eps28"] for x in analysts) / len(analysts)
    street_pe = sum(x["pe"] for x in analysts) / len(analysts)
    r = lambda v, dp=2: None if v is None else round(v, dp)
    return {
        "assumptions": {"pe": list(HOUSE_PE), "hurdle": HURDLE, "margin": HOUSE_MARGIN, "days": HOUSE_DAYS,
                        "fy28_growth": HOUSE_FY28_GROWTH, "non_fo_fy26_cr": NON_FO_FY26_CR,
                        "non_fo_fy27_growth": NON_FO_FY27_GROWTH, "target_date": TARGET_DATE.isoformat()},
        "valuation_date": val_date.isoformat(),
        "adr_cr": r(adr), "price": r(price), "shares_cr": shares,
        "non_fo_fy27_cr": r(nf27), "non_fo_fy28_cr": r(nf28),
        "eps27": r(eps27), "eps28": r(eps28),
        "years_to_target": round(t, 4), "discount": round(disc, 4),
        "target": {k: r(v, 0) for k, v in target.items()},
        "adr_leg": {k: r(v, 0) for k, v in adr_leg.items()},
        "regression": None if not reg else {k: (r(v, 4) if k in ("a", "b", "r_squared") else r(v, 0) if isinstance(v, float) else v) for k, v in reg.items()},
        "analysts": [{**x, "present_value": r(x["present_value"], 0)} for x in analysts],
        "analyst_leg": r(analyst_leg, 0),
        "blend": {k: r(v, 0) for k, v in blend.items()},
        "sensitivity": [{k: (r(v, 2) if k != "growth" else v) for k, v in row.items()} for row in sens],
        "street": {"eps28": r(street_eps28), "pe": r(street_pe, 1), "brokers": len(analysts)},
    }
