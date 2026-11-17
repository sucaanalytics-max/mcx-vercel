#!/usr/bin/env python3
"""
Phase 4 (performance) regression tests:

  1. Parallel pagination (lib.supabase.supabase_read_all): the same rows in
     the same order as the sequential loop, for every cap / order / size;
     pages really run concurrently; a failing page raises; the row count from
     the first page bounds the requests.
  2. CDN cache headers: every endpoint sends s-maxage on a good 200, and an
     error is never cacheable.
  3. ?fields=page projections (OI participants, analytics) are sub-trees of
     the full payload with every key the pages read.
  4. The adaptive curve's snapshot read is shared, per reader.

Pure: every Supabase call is stubbed.
Run: scripts/run_tests.sh
"""
import io, os, sys, json, time, threading, email.message
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

FAILED = []


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}" + (f"  ({detail})" if detail and not cond else ""))
    if not cond:
        FAILED.append(name)


# ── 1. Parallel pagination ─────────────────────────────────────────────────

class FakeTable:
    """n rows, 7 per date, served like PostgREST: limit/offset over the order,
    at most 1000 per request. Records every request and the peak number of
    requests in flight."""

    def __init__(self, n, delay=0.0, fail_offset=None):
        self.rows = [{"trading_date": f"2026-{1 + i // 210:02d}-{1 + (i // 7) % 30:02d}", "i": i}
                     for i in range(n)]
        self.delay, self.fail_offset = delay, fail_offset
        self.requests, self.live, self.peak = [], 0, 0
        self.lock = threading.Lock()

    def _page(self, params):
        q = dict(x.split("=", 1) for x in params.lstrip("?").split("&"))
        desc = q["order"].startswith("trading_date.desc")
        rows = sorted(self.rows, key=lambda r: (r["trading_date"], r["i"]), reverse=desc)
        off, lim = int(q["offset"]), min(int(q["limit"]), 1000)
        return off, rows[off:off + lim]

    def read(self, table, params, timeout=10):
        with self.lock:
            self.requests.append(params)
            self.live += 1
            self.peak = max(self.peak, self.live)
        try:
            if self.delay:
                time.sleep(self.delay)
            off, rows = self._page(params)
            if self.fail_offset is not None and off == self.fail_offset:
                raise Exception("Supabase 500: boom")
            return rows
        finally:
            with self.lock:
                self.live -= 1

    def counted(self, table, params, timeout=10):
        return self.read(table, params, timeout), len(self.rows)


def read_all(t, counted=False, **kw):
    """supabase_read_all over FakeTable t, with or without the first-page count."""
    import lib.supabase as sb
    if not counted:
        return sb.supabase_read_all("mcx_commodity_daily", "?select=*&order=trading_date.asc",
                                    read=t.read, **kw)
    real = sb.supabase_read, sb._read_counted
    try:
        sb.supabase_read, sb._read_counted = t.read, t.counted
        return sb.supabase_read_all("mcx_commodity_daily", "?select=*&order=trading_date.asc", **kw)
    finally:
        sb.supabase_read, sb._read_counted = real


def test_pagination():
    print("\n[1] parallel pagination")
    cases = [(0, None), (5, None), (999, None), (1000, None), (1001, None), (2500, None),
             (8000, None), (8000, 1200), (8000, 2000), (8000, 5000), (25000, 20000), (3000, 10000)]
    for n, cap in cases:
        for newest in (False, True):
            for counted in (False, True):
                seq = read_all(FakeTable(n), max_rows=cap, newest_first=newest, workers=1)
                par = read_all(FakeTable(n), counted=counted, max_rows=cap, newest_first=newest)
                check(f"{n} rows, max_rows={cap}, newest_first={newest}, count={counted}: same rows, same order",
                      [r["i"] for r in par] == [r["i"] for r in seq],
                      f"{len(par)} vs {len(seq)}")
    # Sanity on the sequential baseline itself
    rows = read_all(FakeTable(8000), max_rows=1200, newest_first=True, workers=1)
    check("baseline: newest_first keeps the newest 1,200, ascending",
          [r["i"] for r in rows] == list(range(6800, 8000)))

    # 200 ms a page: sequential is 1.6 s, concurrent ~0.4 s. The margin absorbs
    # scheduler jitter when the whole suite runs at once (a 50 ms page with a
    # 0.25 s limit failed intermittently under the push gate's load).
    t = FakeTable(8000, delay=0.2)
    t0 = time.time()
    rows = read_all(t, counted=True, max_rows=None)
    dt = time.time() - t0
    check("all 8,000 rows", len(rows) == 8000 and len({r["i"] for r in rows}) == 8000)
    check("pages run concurrently", t.peak > 1, f"peak {t.peak}")
    check("8 pages take ~2 round trips, not 8", dt < 0.2 * 5, f"{dt:.2f}s")
    check("with the count: exactly the 8 pages plus the one probe the loop also made",
          len(t.requests) == 9, f"{len(t.requests)} requests")
    offs = sorted(int(dict(x.split('=', 1) for x in p.lstrip('?').split('&'))["offset"]) for p in t.requests)
    check("every offset requested once", offs == list(range(0, 9000, 1000)), str(offs))

    t = FakeTable(2500)
    read_all(t, max_rows=None)
    from lib.supabase import PAGE_WORKERS
    check("no count: one wave past the last page at most", len(t.requests) <= 1 + PAGE_WORKERS,
          f"{len(t.requests)} requests")
    t = FakeTable(25000)
    read_all(t, counted=True, max_rows=20000)
    check("a cap stops at max_rows' pages (20), as the loop did", len(t.requests) == 20, str(len(t.requests)))
    t = FakeTable(500)
    read_all(t, counted=True)
    check("one short page: one request, no pool", len(t.requests) == 1)

    for off in (0, 3000, 7000):
        raised = None
        try:
            read_all(FakeTable(8000, fail_offset=off), counted=True, max_rows=None)
        except Exception as e:
            raised = str(e)
        check(f"a failing page (offset {off}) raises its error", raised == "Supabase 500: boom", str(raised))

    # The count comes from PostgREST's Content-Range header
    import lib.supabase as sb
    import urllib.request

    class Resp:
        def __init__(self, rng): self.headers = {"Content-Range": rng}
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def read(self): return b"[]"

    sent = []
    real = urllib.request.urlopen
    try:
        for rng, want in (("0-999/21900", 21900), ("*/0", 0), ("0-9/*", None), ("", None)):
            urllib.request.urlopen = lambda req, timeout=None, _r=rng: (sent.append(req), Resp(_r))[1]
            got = sb._read_counted("mcx_snapshots", "?select=id")
            check(f"Content-Range {rng!r} -> total {want}", got == ([], want), str(got))
    finally:
        urllib.request.urlopen = real
    check("the first page asks for the count", sent and sent[0].get_header("Prefer") == "count=exact",
          str(sent and sent[0].header_items()))


# ── 2. CDN cache headers ───────────────────────────────────────────────────

def call(mod, path):
    """Drive mod.handler.do_GET for `path`: (status, headers dict)."""
    cls = mod.handler
    h = cls.__new__(cls)
    h.path, h.command, h.request_version = path, "GET", "HTTP/1.1"
    h.requestline, h.client_address = f"GET {path} HTTP/1.1", ("127.0.0.1", 0)
    h.headers, h.wfile, h.close_connection = email.message.Message(), io.BytesIO(), True
    h.do_GET()
    head, _, body = h.wfile.getvalue().partition(b"\r\n\r\n")
    lines = head.decode().split("\r\n")
    hdrs = {k.lower(): v.strip() for k, _, v in (l.partition(":") for l in lines[1:])}
    return int(lines[0].split()[1]), hdrs, json.loads(body)


def test_cache_headers():
    print("\n[2] CDN cache headers")
    import api.analytics as an, api.valuation as va, api.models as mo, api.commodities as co
    import api.commodity_dashboard as cd, api.exchange_dashboard as ex, api.refresh as rf
    import lib.hourly_analysis as ha, lib.home_view as hv, lib.intraday_curves as ic
    from lib.api_handler import CACHE_DAILY, CACHE_INTRADAY, CACHE_SNAPSHOT
    ok = lambda **kw: {"success": True}

    def boom(**kw):
        raise RuntimeError("supabase down")

    # (module, path, [(module_or_obj, attr)] to stub, expected Cache-Control)
    cases = [
        (an, "/api/analytics", [(an, "generate_analytics")], CACHE_DAILY),
        (an, "/api/analytics?fields=page", [(an, "generate_analytics")], CACHE_DAILY),
        (an, "/api/analytics?section=hourly_accuracy", [(ha, "generate_hourly_accuracy")], CACHE_DAILY),
        (va, "/api/valuation?range=60D", [(va, "generate_valuation")], CACHE_DAILY),
        (mo, "/api/models?range=Max", [(mo, "generate_models_response")], CACHE_DAILY),
        (mo, "/api/models?view=momentum", [(mo, "generate_momentum_response")], CACHE_DAILY),
        (mo, "/api/models?view=options", [(mo, "generate_options_response")], CACHE_DAILY),
        (co, "/api/commodities?view=signals", [(co, "generate_commodity_analytics")], CACHE_DAILY),
        (co, "/api/commodities?view=icomdex", [(co, "generate_icomdex")], CACHE_DAILY),
        (cd, "/api/commodity_dashboard?range=60D", [(cd, "generate_commodity_dashboard")], CACHE_DAILY),
        (cd, "/api/commodity_dashboard?view=margins", [(cd, "generate_margin_dashboard")], CACHE_DAILY),
        (cd, "/api/commodity_dashboard?view=oi_participants&fields=page",
         [(cd, "generate_oi_participants_dashboard")], CACHE_DAILY),
        (ex, "/api/exchange_dashboard", [(ex, "generate_exchange_dashboard")], CACHE_INTRADAY),
        (ex, "/api/exchange_dashboard?view=intraday_curve&days=30", [(ic, "generate_intraday_curves")], CACHE_INTRADAY),
        (ex, "/api/exchange_dashboard?view=home", [(hv, "generate_home")], None),   # its own, longer
    ]
    for mod, path, stubs, want in cases:
        saved = [(o, a, getattr(o, a)) for o, a in stubs]
        try:
            for o, a in stubs:
                setattr(o, a, ok)
            st, h, _ = call(mod, path)
            cc = h.get("cache-control", "")
            check(f"{path}: 200 cacheable at the CDN", st == 200 and "s-maxage=" in cc
                  and (want is None or cc == want), cc)
            for o, a in stubs:
                setattr(o, a, lambda **kw: {"success": False, "error": "no data"})
            st, h, _ = call(mod, path)
            check(f"{path}: success:false never cached", "s-maxage" not in h.get("cache-control", ""),
                  h.get("cache-control"))
            for o, a in stubs:
                setattr(o, a, boom)
            st, h, body = call(mod, path)
            check(f"{path}: exception -> 500, never cached",
                  st == 500 and "s-maxage" not in h.get("cache-control", "") and body.get("success") is False,
                  f"{st} {h.get('cache-control')}")
        finally:
            for o, a, f in saved:
                setattr(o, a, f)
    for c in (CACHE_DAILY, CACHE_INTRADAY):
        check(f"{c!r}: serves stale while revalidating", "stale-while-revalidate=" in c)
    check("refresh snapshot: none in the browser, 30 s at the edge", CACHE_SNAPSHOT == "public, max-age=0, s-maxage=30")
    src = open(os.path.join(ROOT, "api", "refresh.py")).read()
    check("/api/refresh caches its snapshot answer (only)", src.count("cache=CACHE_SNAPSHOT") == 1)


# ── 3. ?fields=page projections ────────────────────────────────────────────

def subtree(small, full, path="$"):
    """None if every key/value of `small` is in `full` with the same value."""
    if isinstance(small, dict):
        if not isinstance(full, dict):
            return f"{path}: not a dict"
        for k, v in small.items():
            if k not in full:
                return f"{path}.{k}: not in the full payload"
            bad = subtree(v, full[k], f"{path}.{k}")
            if bad:
                return bad
        return None
    if isinstance(small, list):
        if not isinstance(full, list) or len(small) != len(full):
            return f"{path}: list differs"
        for i, (a, b) in enumerate(zip(small, full)):
            bad = subtree(a, b, f"{path}[{i}]")
            if bad:
                return bad
        return None
    return None if small == full else f"{path}: {small!r} != {full!r}"


def test_page_fields():
    print("\n[3] ?fields=page projections")
    from lib.oi_participants_dashboard import page_fields as oi_page
    from api.analytics import page_fields as an_page
    dates = ["2026-11-13", "2026-11-14", "2026-11-17"]
    series = {"total": [1, 2, 3], "ma7": [1.0, 1.5, 2.0], "ma30": [1.0, 1.2, 1.4], "fpo_long": [0, 0, 0]}
    oi = {"success": True, "as_of": "2026-11-17", "snapshot_dates": 3,
          "participants": [{"commodity": "GOLD", "instrument": "Futures", "total_participation": 9}],
          "hedger_speculator": [{"x": 1}], "cross_commodity": [{"y": 2}],
          "trend": {"dates": dates, "ALL_Futures": dict(series), "ALL_Options": dict(series),
                    "ALL_Overall": dict(series), "GOLD_Futures": dict(series)},
          "net_positioning": {"dates": dates, "GOLD_Futures": {"hedger_net": [1, 2, 3]}},
          "growth": [{"commodity": "GOLD", "instrument": "Futures", "current": 9, "wow_pct": 1.0}],
          "growth_overall": {"Overall": {"current": 9}, "Futures": {"current": 5}, "Options": {"current": 4}},
          "monthly_growth": {"months": []}, "commodities": [], "unique_commodities": []}
    slim = oi_page(oi)
    check("OI: a sub-tree of the full payload", subtree(slim, oi) is None, subtree(slim, oi))
    need = [("as_of",), ("participants",), ("growth",), ("growth_overall",), ("trend", "dates"),
            ("trend", "ALL_Futures", "total"), ("trend", "ALL_Options", "total")]

    def has(d, p):
        for k in p:
            if not isinstance(d, dict) or k not in d:
                return False
            d = d[k]
        return True
    check("OI: every key lab.js Positioning reads", all(has(slim, p) for p in need),
          [p for p in need if not has(slim, p)])
    check("OI: drops the unread series", "net_positioning" not in slim and "GOLD_Futures" not in slim["trend"]
          and "ma7" not in slim["trend"]["ALL_Futures"])
    check("OI: an error passes through", oi_page({"success": False, "error": "x"}) == {"success": False, "error": "x"})

    an = {"success": True, "as_of": "2026-11-17 19:45 IST",
          "factor_series": {"dates": dates, "ecm_z": [0.1, None, 0.3]},
          "rolling_ic": [{"date": d, "ensemble_ic": 0.1} for d in dates],
          "regime": {"current": "BULL", "volatility": {"annualized_pct": 30.1}},
          "hmm_regime": {"current": {"state": "TRANSITION", "avg_position": 0.2, "position_vol": 0.3},
                         "history": [{"date": d, "state": "BULL"} for d in dates]},
          "rolling_metrics": [{"date": d, "sharpe_ratio": 1.2, "win_rate": 0.55, "avg_win": 0.02,
                               "avg_loss": -0.01, "profit_factor": 1.6} for d in dates],
          "factor_decomposition": {}, "weight_sensitivity": [{"ecm_weight": 0.3, "is_current": True}],
          "evaluation": {}, "data_quality": {}}
    slim = an_page(an)
    check("analytics: a sub-tree of the full payload", subtree(slim, an) is None, subtree(slim, an))
    check("analytics: keeps what lab.js Diagnostics reads",
          all(k in slim for k in ("rolling_ic", "weight_sensitivity", "factor_series", "regime"))
          and slim["hmm_regime"]["current"] == an["hmm_regime"]["current"]
          and all(set(r) == {"date", "sharpe_ratio", "win_rate"} for r in slim["rolling_metrics"]))
    check("analytics: drops the HMM history", "history" not in slim["hmm_regime"])
    check("analytics: the full payload is not modified", "history" in an["hmm_regime"]
          and "avg_win" in an["rolling_metrics"][0])
    for f in ("lab.js",):
        js = open(os.path.join(ROOT, "assets", "js", f)).read()
    check("lab.js asks for the page fields",
          "/api/analytics?fields=page" in js and "view=oi_participants&fields=page" in js)
    check("lab.js reads no dropped analytics field",
          not any(k in js for k in ("avg_win", "avg_loss", "profit_factor", "hmm_regime.history",
                                    "net_positioning", "hedger_speculator", "cross_commodity",
                                    "monthly_growth", "ma7", "ma30")))


# ── 4. The adaptive curve's snapshot read is shared ────────────────────────

def test_adaptive_snapshots():
    print("\n[4] adaptive curve: one snapshot read for every curve")
    import lib.mcx_config as cfg
    calls = []

    def reader(*a, **k):
        calls.append(a)
        return []
    real = cfg.supabase_read_all
    try:
        cfg._adaptive_rows_cache.clear()
        cfg.supabase_read_all = reader
        for close in (1410, 1435):
            for vf in (None, lambda s: s.get("fut_notl_cr") or 0, lambda s: s.get("opt_prem_cr") or 0):
                cfg._compute_adaptive_weights(vol_fn=vf, close_min=close)
        check("six curves, one read", len(calls) == 1, str(len(calls)))
        other = []
        cfg.supabase_read_all = lambda *a, **k: other.append(a) or []
        cfg._compute_adaptive_weights(close_min=1410)
        check("another reader (a test fixture) gets its own read", len(other) == 1)
    finally:
        cfg.supabase_read_all = real
        cfg._adaptive_rows_cache.clear()


if __name__ == "__main__":
    test_pagination()
    test_cache_headers()
    test_page_fields()
    test_adaptive_snapshots()
    print("\n" + "=" * 72)
    if FAILED:
        print(f"{len(FAILED)} FAILED:")
        for f in FAILED:
            print("  - " + f)
        sys.exit(1)
    print("All tests passed.")
