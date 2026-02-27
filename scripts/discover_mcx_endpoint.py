#!/usr/bin/env python3
"""
Run this on your LOCAL machine (not cloud) to discover the MCX historical
detailed-report API endpoint.

It fetches the MCX historical-data page's JavaScript, then tries known
endpoint naming patterns to find the one that returns PremiumValue.

Usage:
    python discover_mcx_endpoint.py
"""

import requests, json, re, time

BASE = "https://www.mcxindia.com"

HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/json; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Origin": BASE,
    "Referer": BASE + "/market-data/historical-data",
}

# ── Step 1: Scrape the page JS for endpoint names ──────────────────────
print("=" * 60)
print("STEP 1: Scanning MCX website for API endpoint names...")
print("=" * 60)

session = requests.Session()
try:
    page = session.get(BASE + "/market-data/historical-data",
                       headers={"User-Agent": HEADERS["User-Agent"]}, timeout=15)
    print(f"Page status: {page.status_code}, length: {len(page.text)}")

    # Find all backpage.aspx endpoint references in page HTML/JS
    endpoints = set(re.findall(r'/[Bb]ack[Pp]age\.aspx/(\w+)', page.text))
    print(f"Endpoints found in page HTML: {endpoints}")

    # Also check linked JS files
    js_urls = re.findall(r'src="(/[^"]+\.js[^"]*)"', page.text)
    for js_url in js_urls[:10]:  # limit to first 10
        try:
            js_resp = session.get(BASE + js_url, timeout=10)
            more = set(re.findall(r'/[Bb]ack[Pp]age\.aspx/(\w+)', js_resp.text))
            if more:
                print(f"  {js_url}: found {more}")
                endpoints.update(more)
        except:
            pass

    print(f"\nAll discovered endpoints: {sorted(endpoints)}")
except Exception as e:
    print(f"Failed to scan page: {e}")
    endpoints = set()

# ── Step 2: Try known & guessed endpoint patterns ──────────────────────
print("\n" + "=" * 60)
print("STEP 2: Testing candidate endpoints for detailed report...")
print("=" * 60)

# Candidate endpoint names (known + guessed patterns)
candidates = list(endpoints) + [
    # Known endpoints
    "GetDateWiseBhavCopy",
    # Likely patterns for historical detailed report
    "GetDateWiseDetailedReport",
    "GetDetailedReport",
    "GetHistoricalData",
    "GetHistoricalReport",
    "GetDateWiseReport",
    "GetCommodityWiseReport",
    "GetDateWiseTurnoverReport",
    "GetMarketDataReport",
    "GetDayWiseDetailedReport",
    "GetDetailedBhavCopy",
    "GetHistoricalDataReport",
    "GetHistoricalDetailedReport",
    "GetMarketActivity",
    "GetDateWiseMarketActivity",
    "GetMarketSummary",
    "GetDateWiseMarketSummary",
    "GetTurnoverReport",
    "GetDateWiseTurnover",
    "GetPremiumTurnover",
    "GetDateWiseData",
    "GetCommodityData",
    "GetCommodityHistoricalData",
    "GetDayWiseReport",
]
# Deduplicate
candidates = list(dict.fromkeys(candidates))

# Common payloads to try
payloads = [
    {"Date": "20/02/2026", "InstrumentName": "ALL"},
    {"FromDate": "20/02/2026", "ToDate": "20/02/2026"},
    {"FromDate": "20/02/2026", "ToDate": "20/02/2026", "InstrumentName": "ALL"},
    {"Date": "20/02/2026"},
    {"fromDate": "20/02/2026", "toDate": "20/02/2026"},
    {},
]

found = []
for ep_name in candidates:
    url = f"{BASE}/backpage.aspx/{ep_name}"
    for payload in payloads:
        try:
            resp = session.post(url, headers=HEADERS, json=payload, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                d = data.get("d", {})
                rows = d.get("Data") if isinstance(d, dict) else None
                if rows and isinstance(rows, list) and len(rows) > 0:
                    keys = list(rows[0].keys())
                    has_premium = any('prem' in k.lower() for k in keys)
                    print(f"\n✅ {ep_name} (payload: {payload})")
                    print(f"   Rows: {len(rows)}, Fields: {len(keys)}")
                    print(f"   Keys: {keys[:20]}")
                    print(f"   Has PremiumValue: {has_premium}")
                    if has_premium:
                        # Show a sample option row
                        for r in rows:
                            if r.get('InstrumentName') == 'OPTFUT' or 'Option' in str(r.get('InstrumentName', '')):
                                print(f"   Sample OPTFUT: { {k: r[k] for k in keys if 'prem' in k.lower() or 'value' in k.lower() or k in ('Symbol','Close','Volume')} }")
                                break
                    found.append((ep_name, payload, has_premium, keys))
                    break  # found working payload, skip others
            time.sleep(0.2)
        except:
            pass

# ── Step 3: Summary ────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
if found:
    premium_eps = [f for f in found if f[2]]
    other_eps = [f for f in found if not f[2]]
    if premium_eps:
        print("\n🎯 ENDPOINTS WITH PremiumValue:")
        for ep, payload, _, keys in premium_eps:
            print(f"   Endpoint: /backpage.aspx/{ep}")
            print(f"   Payload:  {json.dumps(payload)}")
            print(f"   Fields:   {keys}")
    else:
        print("\n⚠️  No endpoints found with PremiumValue field.")

    if other_eps:
        print("\nOther working endpoints (without PremiumValue):")
        for ep, payload, _, keys in other_eps:
            print(f"   /backpage.aspx/{ep} -> {len(keys)} fields")
else:
    print("\n❌ No working endpoints found. MCX may be blocking this IP.")
    print("   Try running this from your local machine (not cloud/VM).")
