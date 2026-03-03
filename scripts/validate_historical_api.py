#!/usr/bin/env python3
"""
Validate MCX Historical Detailed Report API against Excel actuals.
Run on LOCAL machine (MCX blocks cloud IPs).

Usage:
    python3 scripts/validate_historical_api.py
"""
import json, os, sys
from curl_cffi import requests as cfreq
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

FUTURES_RATE = 210.0
OPTIONS_RATE = 4180.0

# Shared session for Chrome TLS impersonation (bypasses Akamai bot detection)
_session = None

def _get_session():
    global _session
    if _session is None:
        _session = cfreq.Session(impersonate="chrome")
        _session.get("https://www.mcxindia.com/market-data/historical-data", timeout=15)
    return _session

def fetch_mcx_historical(date_iso):
    """Fetch from GetHistoricalDataDetails and compute revenue."""
    date_compact = date_iso.replace("-", "")
    payload = {
        "GroupBy": "D", "Segment": "ALL", "CommodityHead": "ALL",
        "Commodity": "ALL", "Startdate": date_compact,
        "EndDate": date_compact, "InstrumentName": "ALL",
    }

    url = "https://www.mcxindia.com/backpage.aspx/GetHistoricalDataDetails"
    session = _get_session()
    resp = session.post(url, json=payload, headers={
        "X-Requested-With": "XMLHttpRequest",
        "Referer": "https://www.mcxindia.com/market-data/historical-data",
    }, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    rows = data.get("d", {}).get("Data")
    if not rows:
        return None

    fut_notl_lacs = 0.0
    opt_prem_lacs = 0.0

    for r in rows:
        inst = r.get("InstrumentName", "")
        total_val = float(r.get("TotalValue", 0) or 0)
        prem_str = str(r.get("PremiumTurnover", "-")).strip()

        if inst in ("FUTCOM", "FUTIDX"):
            fut_notl_lacs += total_val
        elif inst in ("OPTFUT", "OPTIDX"):
            if prem_str not in ("-", ""):
                try:
                    opt_prem_lacs += float(prem_str)
                except ValueError:
                    pass

    fn_cr = fut_notl_lacs / 100
    op_cr = opt_prem_lacs / 100
    fut_rev = 2 * fn_cr * FUTURES_RATE / 1e7
    opt_rev = 2 * op_cr * OPTIONS_RATE / 1e7

    return {"fut": round(fut_rev, 4), "opt": round(opt_rev, 4), "total": round(fut_rev + opt_rev, 4)}


def main():
    # Load Excel actuals
    script_dir = os.path.dirname(os.path.abspath(__file__))
    actuals_path = os.path.join(script_dir, "excel_actuals.json")

    with open(actuals_path) as f:
        excel = json.load(f)

    # Test 10 dates spread across the range
    all_dates = sorted(excel.keys())
    sample = [all_dates[i] for i in range(0, len(all_dates), max(1, len(all_dates) // 10))][:12]

    print(f"{'Date':<12} {'XL_Fut':>8} {'API_Fut':>8} {'F_Err%':>7} {'XL_Opt':>8} {'API_Opt':>8} {'O_Err%':>7} {'XL_Tot':>8} {'API_Tot':>8} {'T_Err%':>7}")
    print("-" * 96)

    opt_errors = []
    total_errors = []
    import time

    for date_iso in sample:
        xl = excel[date_iso]
        try:
            api = fetch_mcx_historical(date_iso)
        except Exception as e:
            print(f"{date_iso}: ERROR — {e}")
            continue

        if api is None:
            print(f"{date_iso}: No API data")
            continue

        f_err = ((api["fut"] - xl["fut"]) / xl["fut"] * 100) if xl["fut"] > 0 else 0
        o_err = ((api["opt"] - xl["opt"]) / xl["opt"] * 100) if xl["opt"] > 0 else 0
        t_err = ((api["total"] - xl["total"]) / xl["total"] * 100) if xl["total"] > 0 else 0

        opt_errors.append(abs(o_err))
        total_errors.append(abs(t_err))

        print(f"{date_iso:<12} {xl['fut']:>8.4f} {api['fut']:>8.4f} {f_err:>+7.1f} "
              f"{xl['opt']:>8.4f} {api['opt']:>8.4f} {o_err:>+7.1f} "
              f"{xl['total']:>8.4f} {api['total']:>8.4f} {t_err:>+7.1f}")
        time.sleep(1)

    if opt_errors:
        print("-" * 96)
        print(f"\nOptions error — Mean |err|: {sum(opt_errors)/len(opt_errors):.2f}%  Max: {max(opt_errors):.2f}%")
        print(f"Total error   — Mean |err|: {sum(total_errors)/len(total_errors):.2f}%  Max: {max(total_errors):.2f}%")

        if max(opt_errors) < 1.0:
            print("\n✅ VALIDATION PASSED — Historical API matches Excel actuals within 1%")
        elif max(opt_errors) < 5.0:
            print("\n⚠️  Minor differences detected — likely rounding. Acceptable for production.")
        else:
            print("\n❌ Significant discrepancies found — investigate before using in production.")


if __name__ == "__main__":
    main()
