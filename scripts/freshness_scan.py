#!/usr/bin/env python3
"""
freshness_scan.py — deterministic MCX data-freshness scan (read-only).

Reads .claude/state/freshness-manifest.json, computes the expected latest
completed trading day, and queries max(date) for each DB-backed domain. The
Quarterly P&L domain is code-backed (api/quarterly.py) and checked separately.
Emits a single JSON object to stdout. Writes/mutates nothing.

Used by the `freshness-scanner` subagent and directly by /freshness-check.

Usage:
  python3 scripts/freshness_scan.py           # human + JSON
  python3 scripts/freshness_scan.py --json     # JSON only
"""
import sys, os, json
from datetime import timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lib.mcx_config import now_ist, is_trading_day, supabase_read  # noqa: E402

MANIFEST = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        ".claude", "state", "freshness-manifest.json")


def expected_trading_day(ref=None):
    """Latest COMPLETED trading day. Today counts only after the 23:30 IST
    session close; otherwise walk back to the previous trading day."""
    d = ref or now_ist()
    today = d.date()
    complete_today = is_trading_day(today) and (d.hour > 23 or (d.hour == 23 and d.minute >= 30))
    cur = today if complete_today else today - timedelta(days=1)
    while not is_trading_day(cur):
        cur -= timedelta(days=1)
    return cur


def _minus_trading_days(d, n):
    cur = d
    while n > 0:
        cur -= timedelta(days=1)
        if is_trading_day(cur):
            n -= 1
    return cur


def _max_date(table, date_col):
    try:
        rows = supabase_read(table, f"?select={date_col}&order={date_col}.desc&limit=1")
        return rows[0][date_col] if rows else None
    except Exception as e:
        return f"ERROR: {str(e)[:80]}"


def _check_quarterly(report_lag_days):
    """STALE when the most recently reportable fiscal quarter is missing from
    QUARTERLY_ACTUALS (a quarter is 'reportable' report_lag_days after it ends)."""
    try:
        from api.quarterly import QUARTERLY_ACTUALS, _get_quarter_bounds
        d = now_ist().date()
        # The quarter that ended most recently and is now past its reporting lag.
        probe = d - timedelta(days=report_lag_days)
        q_label, _, _, _, q_end = _get_quarter_bounds(probe)
        # _get_quarter_bounds returns the quarter CONTAINING probe; we want the
        # last COMPLETED quarter as of probe, i.e. walk to the quarter whose end < probe.
        # Simpler: the latest actual we should already have is the quarter that
        # ended on/before `probe`.
        have = {a["quarter"] for a in QUARTERLY_ACTUALS}
        # Determine last fully-ended quarter as of `probe`
        from datetime import date
        y = probe.year
        # quarter ends: Mar31, Jun30, Sep30, Dec31
        ends = [(date(y, 3, 31), f"Q4 FY{str(y)[-2:]}"),
                (date(y, 6, 30), f"Q1 FY{str(y+1)[-2:]}"),
                (date(y, 9, 30), f"Q2 FY{str(y+1)[-2:]}"),
                (date(y, 12, 31), f"Q3 FY{str(y+1)[-2:]}"),
                (date(y - 1, 12, 31), f"Q3 FY{str(y)[-2:]}")]
        past = [(e, lbl) for e, lbl in ends if e <= probe]
        past.sort()
        last_reportable = past[-1][1] if past else None
        latest_have = QUARTERLY_ACTUALS[-1]["quarter"] if QUARTERLY_ACTUALS else None
        stale = last_reportable is not None and last_reportable not in have
        return {"latest_present": latest_have, "last_reportable": last_reportable,
                "status": "STALE" if stale else "OK"}
    except Exception as e:
        return {"status": "ERROR", "error": str(e)[:100]}


def scan():
    with open(MANIFEST) as f:
        manifest = json.load(f)
    exp = expected_trading_day()
    exp_iso = exp.strftime("%Y-%m-%d")
    out = {"as_of": now_ist().strftime("%Y-%m-%dT%H:%M:%S%z"),
           "expected_trading_day": exp_iso, "domains": []}
    stale = []
    for dom in manifest["domains"]:
        name = dom["domain"]
        if dom.get("source", "").startswith("code") or dom.get("cadence") == "quarterly":
            q = _check_quarterly(dom.get("report_lag_days", 45))
            entry = {"domain": name, "kind": "code", **q, "fixer": dom.get("fixer")}
            if q.get("status") == "STALE":
                stale.append(name)
            out["domains"].append(entry)
            continue
        grace = dom.get("grace_days", 0)
        min_ok = _minus_trading_days(exp, grace).strftime("%Y-%m-%d")
        latest = _max_date(dom["table"], dom["date_col"])
        if isinstance(latest, str) and latest.startswith("ERROR"):
            status = "ERROR"
        elif latest is None:
            status = "EMPTY"
        elif latest >= min_ok:
            status = "OK"
        else:
            status = "STALE"
        entry = {"domain": name, "table": dom["table"], "latest": latest,
                 "expected": exp_iso, "min_acceptable": min_ok, "grace_days": grace,
                 "status": status, "fixer": dom.get("fixer"),
                 "depends_on": dom.get("depends_on", [])}
        if status in ("STALE", "EMPTY", "ERROR"):
            stale.append(name)
        out["domains"].append(entry)
    out["stale_domains"] = stale
    out["overall"] = "OK" if not stale else "DEGRADED"
    return out


def main():
    result = scan()
    if "--json" in sys.argv:
        print(json.dumps(result))
        return
    print(f"Expected latest trading day: {result['expected_trading_day']}")
    print(f"Overall: {result['overall']}   Stale: {result['stale_domains'] or 'none'}\n")
    for d in result["domains"]:
        mark = {"OK": "✓", "STALE": "✗", "EMPTY": "∅", "ERROR": "!", None: "?"}.get(d.get("status"), "?")
        latest = d.get("latest") or d.get("latest_present") or "-"
        print(f"  {mark} {d['domain']:18s} latest={str(latest):12s} status={d.get('status')}")
    print("\n" + json.dumps(result))


if __name__ == "__main__":
    main()
