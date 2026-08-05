# Time Period Toggles Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give every time-series chart and table in the MCX dashboard its own persistent time-period chip toggle (`30D/60D/Q/1Y/2Y/Max` vocabulary), per the approved spec at `docs/superpowers/specs/2026-08-05-time-period-toggles-design.md`.

**Architecture:** One shared client-side toggle factory + per-(url,range) fetch cache in `index.html`; server `?range=`/`?days=` params on endpoints that truncate at the DB (copying the proven `api/models.py` momentum pattern); payload widening + client-side slicing where the server already holds full history.

**Tech Stack:** Vanilla JS + Chart.js 4.4.1 (CDN) in a single `index.html`; Python 3.12 Vercel functions (`http.server.BaseHTTPRequestHandler`) reading Supabase via REST helpers in `lib/mcx_config.py`. No build step, no test framework — verification is direct generator-function calls (Python) and Playwright against a Vercel preview deploy (client).

## Global Constraints

- **Repo root:** `/Users/pranayagarwal/Dropbox/My Mac (Pranay's MacBook Air)/Documents/MCX/mcx-vercel` — the STRAIGHT-apostrophe path (a curly-apostrophe duplicate exists; never touch it). All paths below are relative to this root.
- **Hard cap: 12 Vercel functions.** NEVER create a new file under `api/`. All server changes extend existing handlers via query params.
- **Range vocabulary (exact):** `RANGE_DAYS = {"30D": 30, "60D": 60, "Q": 63, "1Y": 252, "2Y": 504, "Max": None}` (already defined at `api/models.py:244`). Commodities-backed controls (Tasks 12–13) omit `Max`. Unit-based sets: quarters `4Q/8Q(/All)`, months `3M/6M/12M/24M(/All)`.
- **Anchor rule:** client date/count slicing always slices the tail of the data array (anchored to last data date), never computes cutoffs from `new Date()`.
- **Persistence:** `localStorage` key `mcx.range.<key>`, one per control.
- **Do NOT commit** the pre-existing dirty files `scripts/commodity_price_refresh.py`, `scripts/margin_refresh.py`, `scripts/mcx_relay.py`, `scripts/mcxccl_scraper.py`, or `.DS_Store`. Stage only files this plan touches (`index.html`, `api/*.py`, `lib/*.py`, docs).
- **Line numbers** in this plan are as of commit `76ad1a2` and drift as you edit — always locate anchors by the quoted code, not the number alone.
- **Python check after every server task:** `python3 -m py_compile api/<file>.py` plus the task's generator assertion.
- **Commits:** one per task, message format given per task. Never `git add -A`.
- **Production deploy only in Task 14 after user approval.** Intermediate verification uses `vercel deploy` (preview).

---

### Task 1: Shared client infrastructure (factory, caches, CSS)

**Files:**
- Modify: `index.html` — CSS block (after `.margin-chip-ctrl` rules, ~line 1183) and top of the main `<script>` block (immediately after the `let` globals near the top of the script, before the first function definitions ~line 3200)
- Modify: `index.html:3677` (`doRefresh` success branch)

**Interfaces:**
- Produces (all later tasks consume these exact names):
  - `RANGE_TRADING_DAYS: {[key:string]: number|null}`
  - `RANGE_LABELS: {[key:string]: string}`
  - `rangeState: {[controlKey:string]: string}` — current selection per control
  - `makeRangeToggle({key, containerId, ranges, defaultRange, labelIds, onChange}) -> string` (returns initial range; renders chips; persists; updates labels; calls `onChange(rangeKey)` on click only, NOT on init)
  - `fetchRanged(url) -> Promise<object>` — per-URL memoized JSON fetch
  - `clearRangedCache() -> void`
  - `sliceTailByRange(arr, rangeKey) -> array` (trading-day keys)
  - `sliceTailByCount(arr, n) -> array` (`n == null` → whole array)
  - CSS classes `.range-chips`, `.scroll-table-wrap`

- [ ] **Step 1: Add CSS.** In the `<style>` block, directly after the `.margin-chip-ctrl:hover` rule (line 1183), insert:

```css
.range-chips { display:inline-flex; flex-wrap:wrap; gap:2px; vertical-align:middle; }
.scroll-table-wrap { max-height:420px; overflow-y:auto; }
.scroll-table-wrap table thead th { position:sticky; top:0; background:var(--bg-card); z-index:1; }
```

- [ ] **Step 2: Add the JS infrastructure.** Near the top of the main script block (right before the first `function` definition, after the global `let` declarations), insert:

```js
// ══════════════════════════════════════════════════════════════════════════
//  SHARED TIME-PERIOD TOGGLE INFRASTRUCTURE (see docs/superpowers/specs/
//  2026-08-05-time-period-toggles-design.md)
// ══════════════════════════════════════════════════════════════════════════
const RANGE_TRADING_DAYS = { '30D':30, '60D':60, 'Q':63, '1Y':252, '2Y':504, 'Max':null };
const RANGE_LABELS = {
  '30D':'(30 days)', '60D':'(60 days)', 'Q':'(quarter)', '1Y':'(1 year)',
  '2Y':'(2 years)', 'Max':'(max history)',
  '3M':'(3 months)', '6M':'(6 months)', '12M':'(12 months)', '24M':'(24 months)',
  '4Q':'(4 quarters)', '8Q':'(8 quarters)', 'All':'(all)'
};
const rangeState = {};        // controlKey -> selected range key
const rangedFetchCache = {};  // full URL -> Promise of parsed JSON

function makeRangeToggle(cfg) {
  const saved = localStorage.getItem('mcx.range.' + cfg.key);
  const initial = (saved && cfg.ranges.indexOf(saved) !== -1) ? saved : cfg.defaultRange;
  rangeState[cfg.key] = initial;
  const el = document.getElementById(cfg.containerId);
  if (!el) return initial;
  el.classList.add('range-chips');
  el.innerHTML = cfg.ranges.map(r =>
    '<span class="margin-chip' + (r === initial ? ' active' : '') + '" data-range="' + r + '">' + r + '</span>'
  ).join('');
  el.querySelectorAll('.margin-chip').forEach(chip => {
    chip.onclick = () => {
      const r = chip.getAttribute('data-range');
      if (rangeState[cfg.key] === r) return;
      rangeState[cfg.key] = r;
      localStorage.setItem('mcx.range.' + cfg.key, r);
      el.querySelectorAll('.margin-chip').forEach(c =>
        c.className = c.getAttribute('data-range') === r ? 'margin-chip active' : 'margin-chip');
      updateRangeLabels(cfg, r);
      cfg.onChange(r);
    };
  });
  updateRangeLabels(cfg, initial);
  return initial;
}

function updateRangeLabels(cfg, r) {
  (cfg.labelIds || []).forEach(id => {
    const s = document.getElementById(id);
    if (s) s.textContent = RANGE_LABELS[r] || ('(' + r + ')');
  });
}

function fetchRanged(url) {
  if (!rangedFetchCache[url]) {
    rangedFetchCache[url] = fetch(url).then(r => r.json())
      .catch(e => { delete rangedFetchCache[url]; throw e; });
  }
  return rangedFetchCache[url];
}

function clearRangedCache() {
  Object.keys(rangedFetchCache).forEach(k => { delete rangedFetchCache[k]; });
}

function sliceTailByRange(arr, rangeKey) {
  const n = RANGE_TRADING_DAYS[rangeKey];
  return (n == null) ? arr : arr.slice(-n);
}

function sliceTailByCount(arr, n) {
  return (n == null) ? arr : arr.slice(-n);
}
```

- [ ] **Step 3: Invalidate cache on auto-refresh.** In `doRefresh` (line 3651), inside `if (data.success) {` (line 3677), add as the first statement: `clearRangedCache();`

- [ ] **Step 4: Syntax check.** Run: `node --check <(sed -n '/<script>/,/<\/script>/p' index.html | sed '1d;$d')` if node exists; otherwise open the file in a browser (`open index.html`) and confirm zero console syntax errors (fetches will fail locally — that's fine, only syntax matters here).

- [ ] **Step 5: Commit**

```bash
git add index.html
git commit -m "feat(toggles): shared range-toggle factory, fetch cache, scroll-table CSS"
```

---

### Task 2: Fair Value chart — server `?range=` + chips (exemplar vertical slice)

**Files:**
- Modify: `api/valuation.py:86-100` (`_fetch_precomputed_valuations`), `:135-145` (`generate_valuation` signature + fetch), `:179-181` (history slice), `:219-225` (data_quality), `:257-262` (handler `do_GET`)
- Modify: `index.html:2142-2143` (chart title HTML), and `loadValuation` (fetch at `index.html:4220`)

**Interfaces:**
- Consumes: Task 1 (`makeRangeToggle`, `fetchRanged`, `rangeState`)
- Produces: `GET /api/valuation?range=<30D|60D|Q|1Y|2Y|Max>` returning the existing payload with `history` sized to the range and `data_quality.range` echoed. Client control key: `'valChart'`.

- [ ] **Step 1: Add range constants to `api/valuation.py`.** After the `import math` line (~line 34), add:

```python
RANGE_DAYS = {"30D": 30, "60D": 60, "Q": 63, "1Y": 252, "2Y": 504, "Max": None}
DEFAULT_RANGE = "60D"
```

- [ ] **Step 2: Let the fetch take `limit=None`.** Replace `_fetch_precomputed_valuations` (lines 86–100) query construction so the limit is optional:

```python
def _fetch_precomputed_valuations(limit=90):
    """Fetch pre-computed valuations from mcx_valuation table. limit=None fetches all."""
    if not SUPABASE_ANON_KEY:
        return []
    try:
        q = (
            f"?select=trading_date,daily_rev_cr,ma45_rev_cr,annualized_rev_cr,"
            f"pat_cr,eps,close_price,implied_pe,fair_value_bear,fair_value_base,"
            f"fair_value_bull,signal,pe_mean_used,pe_sd_used"
            f"&order=trading_date.desc"
        )
        if limit is not None:
            q += f"&limit={limit}"
        rows = supabase_read("mcx_valuation", q)
        return sorted(rows, key=lambda r: r["trading_date"])
    except Exception:
        return []
```

- [ ] **Step 3: Range-parametrize the generator.** Change the signature `def generate_valuation():` (line 135) to `def generate_valuation(range_key=DEFAULT_RANGE):` and immediately inside add:

```python
    if range_key not in RANGE_DAYS:
        range_key = DEFAULT_RANGE
    window = RANGE_DAYS[range_key]
```

Replace `precomputed = _fetch_precomputed_valuations(limit=90)` (line 142) with:

```python
    fetch_limit = None if window is None else max(90, window + 10)
    precomputed = _fetch_precomputed_valuations(limit=fetch_limit)
```

Replace `for row in precomputed[-60:]:` (line 181) with:

```python
    display_rows = precomputed if window is None else precomputed[-window:]
    for row in display_rows:
```

In the returned `data_quality` dict (line 219), add `"range": range_key,`.

- [ ] **Step 4: Wire the handler.** Replace `do_GET` (lines 257–262) with:

```python
    def do_GET(self):
        try:
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(self.path).query)
            range_key = qs.get("range", [DEFAULT_RANGE])[0]
            result = generate_valuation(range_key=range_key)
            self.send_json(result)
        except Exception as e:
            self.send_json({"success": False, "error": str(e)[:200]}, 500)
```

- [ ] **Step 5: Verify server locally.** Run from repo root:

```bash
python3 -m py_compile api/valuation.py && python3 -c "
import sys; sys.path.insert(0, '.')
from api.valuation import generate_valuation
for rk, expect in [('60D', 60), ('1Y', 252), ('Max', None), ('BOGUS', 60)]:
    r = generate_valuation(range_key=rk)
    assert r['success'], r.get('error')
    n = len(r['history'])
    if expect is None:
        assert n > 504, f'Max returned only {n}'
    else:
        assert n == expect or n == r['data_quality']['valuation_rows'], (rk, n)
    print(rk, '->', n, 'rows,', r['history'][0]['date'], '..', r['history'][-1]['date'])
"
```

Expected: `60D -> 60 rows`, `1Y -> 252 rows`, `Max -> ~1105 rows`, `BOGUS -> 60 rows` (falls back to default), each ending at the latest trading date.

- [ ] **Step 6: Add the chips container to the card title.** At `index.html:2142`, the Fair Value chart title reads (locate by text) `Fair Value vs Market Price — 60-Day History` (or similar with a hardcoded "60-Day"). Replace that title line so it becomes:

```html
<div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px">
  <span>Fair Value vs Market Price <span id="valChartRangeLabel">(60 days)</span></span>
  <span id="valChartRange"></span>
</div>
```

Keep the original wrapper element/classes of the title (reuse whatever tag/class the current title uses — only the inner structure changes).

- [ ] **Step 7: Wire the client.** In `loadValuation` (fetch at `index.html:4220`), replace the `fetch('/api/valuation')…` call with:

```js
fetchRanged('/api/valuation?range=' + encodeURIComponent(rangeState['valChart'] || '60D'))
```

(keep the existing `.then(...)` chain / `await` structure and error handling exactly as-is). Then, in the init section near the bottom of the script (directly after the `loadHero();` call at line 3778 — start a block comment `// ── Range toggle registrations ──` that later tasks append to), add:

```js
makeRangeToggle({
  key: 'valChart', containerId: 'valChartRange',
  ranges: ['30D','60D','Q','1Y','2Y','Max'], defaultRange: '60D',
  labelIds: ['valChartRangeLabel'],
  onChange: () => loadValuation()
});
```

- [ ] **Step 8: Verify client on a preview deploy.**

```bash
vercel deploy 2>&1 | tail -3   # note the preview URL
```

With Playwright (browser MCP): navigate to `<preview-url>`, click the "Fair Value" tab, confirm the chart renders at 60 points; click `1Y`, confirm the x-axis span visibly widens and the title label reads "(1 year)"; reload the page and confirm `1Y` is still the active chip.

- [ ] **Step 9: Commit**

```bash
git add api/valuation.py index.html
git commit -m "feat(toggles): Fair Value chart range param + chips"
```

---

### Task 3: ECM Spread chart — server `?range=` + chips

**Files:**
- Modify: `api/models.py:76-92` (`_fetch_model_signals`), `:95-97` (`generate_models_response` signature), `:137-141` (display slice), `:181-193` (response), `:400-414` (handler)
- Modify: `index.html:2337-2338` (ECM chart title), `loadModels` (fetch at `index.html:4515`), init block

**Interfaces:**
- Consumes: Task 1 infra.
- Produces: `GET /api/models?range=<key>` (no `view` param → ECM view) with `history` sized to range, `data_quality.range` echoed. σ-bands (`ecm_band_*`) still computed with the 60-obs rolling window over a fetch that includes ≥60 rows of warm-up before the display window. Client control key: `'ecmChart'`.

- [ ] **Step 1: Optional limit in `_fetch_model_signals`.** Replace lines 76–92 query building (mirror the momentum fetch at lines 255–275):

```python
def _fetch_model_signals(limit=120):
    """Fetch pre-computed model signals from Supabase. limit=None fetches all."""
    if not SUPABASE_ANON_KEY:
        return []
    try:
        q = (
            f"?select=trading_date,close_price,fair_value_base,"
            f"ecm_spread,ecm_spread_pct,ecm_spread_zscore,ecm_half_life_days,ecm_signal,"
            f"mf_revenue_z,mf_turnover_z,mf_volume_z,mf_volatility_z,mf_composite_z,mf_signal,"
            f"ensemble_score,ensemble_signal,"
            f"position_score,conviction,signal_momentum,position_velocity,conviction_2d_ma"
            f"&order=trading_date.desc"
        )
        if limit is not None:
            q += f"&limit={limit}"
        rows = supabase_read("mcx_model_signals", q)
        return sorted(rows, key=lambda r: r["trading_date"])
    except Exception:
        return []
```

- [ ] **Step 2: Range-parametrize.** Change `def generate_models_response():` (line 95) to `def generate_models_response(range_key=DEFAULT_RANGE):` (note: `RANGE_DAYS`/`DEFAULT_RANGE` already exist in this file at lines 244–252 — they are defined *below* this function, which is fine at call time, but move the two definitions **above** `generate_models_response` to keep top-down readability). Inside, replace `rows = _fetch_model_signals(limit=120)` (line 97) with:

```python
    if range_key not in RANGE_DAYS:
        range_key = DEFAULT_RANGE
    window = RANGE_DAYS[range_key]
    # +60 warm-up rows so the rolling σ-bands are well-formed at the window start
    fetch_limit = None if window is None else max(120, window + 60)
    rows = _fetch_model_signals(limit=fetch_limit)
```

Replace `display_rows = rows[-60:]` (line 140) with `display_rows = rows if window is None else rows[-window:]`. In `data_quality` (lines 187–192) add `"range": range_key,`.

- [ ] **Step 3: Handler.** In `do_GET` (lines 400–414), the `else:` branch becomes:

```python
            else:
                range_key = qs.get("range", [DEFAULT_RANGE])[0]
                result = generate_models_response(range_key=range_key)
```

- [ ] **Step 4: Verify.**

```bash
python3 -m py_compile api/models.py && python3 -c "
import sys; sys.path.insert(0, '.')
from api.models import generate_models_response
for rk in ['60D','1Y','Max']:
    r = generate_models_response(range_key=rk)
    assert r['success']
    h = r['history']
    print(rk, '->', len(h), 'rows; first-row bands present:', 'ecm_band_1up' in h[0])
"
```

Expected: 60 / 252 / ~1085 rows; `bands present: True` for 60D and 1Y (warm-up guarantees ≥30-obs windows at the start of the display slice).

- [ ] **Step 5: HTML + client wiring.** At `index.html:2337` (ECM chart title containing "ECM Spread % — 60-Day History"), apply the same title restructure as Task 2 Step 6 with span ids `ecmChartRangeLabel` / `ecmChartRange` and text `ECM Spread % <span id="ecmChartRangeLabel">(60 days)</span>` (keep the "(±1σ / ±1.5σ)" suffix text in the title span). In `loadModels` (fetch at `index.html:4515`) replace the `/api/models` fetch with `fetchRanged('/api/models?range=' + encodeURIComponent(rangeState['ecmChart'] || '60D'))`, keeping the rest of the chain. Append to the init block:

```js
makeRangeToggle({
  key: 'ecmChart', containerId: 'ecmChartRange',
  ranges: ['30D','60D','Q','1Y','2Y','Max'], defaultRange: '60D',
  labelIds: ['ecmChartRangeLabel'],
  onChange: () => loadModels()
});
```

- [ ] **Step 6: Verify on preview** (same procedure as Task 2 Step 8, on the Fair Value tab's ECM chart), **then commit:**

```bash
git add api/models.py index.html
git commit -m "feat(toggles): ECM spread chart range param + chips"
```

---

### Task 4: Revenue spark chart — `api/history.py?days=` + chips

**Files:**
- Modify: `api/history.py:65-84` (`_fetch_supabase_history`), `:86-110` (generator signature + trading-day collection), `:208-212` (period average), `:268-276` (handler)
- Modify: `index.html:1573` (spark title), `loadHero`/`renderHero`/`renderSparkline` (`index.html:3695-3773`), init block

**Interfaces:**
- Consumes: Task 1 infra.
- Produces: `GET /api/history?days=<30|60|63|252|504|0>` (0 = all). Response unchanged in shape plus new `period_avg` (mean of all valid points in the window) and `days_requested`. `ma_45` keeps its name/semantics (mean of last ≤45 valid points) so the hero KPI is unaffected. Client control key: `'spark'`. Function is renamed `generate_history_45d` → `generate_history` (handler updated; no other file imports it — verify with `grep -rn "generate_history_45d" --include=*.py .`).

- [ ] **Step 1: Parametrize the Supabase fetch.** Replace `_fetch_supabase_history()` (lines 65–84) with:

```python
def _fetch_supabase_history(limit=60):
    """Fetch cached daily revenue from Supabase. limit=None fetches all rows."""
    if not SUPABASE_ANON_KEY:
        return {}
    try:
        q = "?select=trading_date,total_rev_cr,source&order=trading_date.desc"
        if limit is not None:
            q += f"&limit={limit}"
        rows = supabase_read("mcx_daily_revenue", q)
        return {
            r["trading_date"]: {
                "rev": r["total_rev_cr"],
                "source": r.get("source", "unknown"),
            }
            for r in rows if r.get("total_rev_cr")
        }
    except Exception:
        return {}
```

- [ ] **Step 2: Generalize the generator.** Rename `def generate_history_45d():` (line 86) to `def generate_history(days=60):` and replace the trading-day collection (lines 98–109) with a hybrid: Supabase actual dates form the base list (they ARE trading days), and the recent 75-calendar-day walk still provides today + gap-fill candidates (the `MCX_HOLIDAYS_2026` list only covers recent holidays, so the walk must stay short):

```python
    window = None if days in (0, None) else int(days)

    # ── Tier 1 fetch first: actual trading dates come from Supabase ────
    fetch_limit = None if window is None else max(60, window + 15)
    supabase_cache = _fetch_supabase_history(limit=fetch_limit)

    # Recent walk (last 75 calendar days) supplies today + recent gap dates;
    # MCX_HOLIDAYS_2026 only covers the current period so keep the walk short.
    recent_walk = []
    d = today - timedelta(days=75)
    while d <= today:
        ds = d.strftime("%Y-%m-%d")
        if d.weekday() < 5 and ds not in MCX_HOLIDAYS_2026:
            recent_walk.append(ds)
        d += timedelta(days=1)

    all_date_strs = sorted(set(list(supabase_cache.keys()) + recent_walk))
    if window is not None:
        all_date_strs = all_date_strs[-window:]
    trading_days = [datetime.strptime(ds, "%Y-%m-%d") for ds in all_date_strs]
```

The existing Pass 1 / Pass 2 loops (lines 133–206) keep working unchanged on this `trading_days` list (they iterate `td` datetimes and use `date_str in supabase_cache`). The `td == today` comparison at line 140 must become `td.strftime("%Y-%m-%d") == today.strftime("%Y-%m-%d")` since `trading_days` entries are now naive datetimes parsed from strings while `today` came from `now_ist()`.

- [ ] **Step 3: Period average.** After the `ma_45` computation (`valid = [...]` / `ma_45 = ...`, lines 208–209), replace with:

```python
    valid = [h["adr"] for h in history if h["adr"] is not None]
    ma_45 = round(sum(valid[-45:]) / len(valid[-45:]), 2) if valid else 0.0
    period_avg = round(sum(valid) / len(valid), 2) if valid else 0.0
```

and add to the return dict: `"period_avg": period_avg, "days_requested": days,`.

- [ ] **Step 4: Handler.** In `do_GET` (line 268), replace `result = generate_history_45d()` with:

```python
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(self.path).query)
            try:
                days = int(qs.get("days", ["60"])[0])
            except ValueError:
                days = 60
            if days not in (0, 30, 60, 63, 252, 504):
                days = 60
            result = generate_history(days=days)
```

- [ ] **Step 5: Verify.**

```bash
python3 -m py_compile api/history.py && python3 -c "
import sys; sys.path.insert(0, '.')
from api.history import generate_history
for d in [30, 60, 252, 0]:
    r = generate_history(days=d)
    n = len(r['history'])
    print(d, '->', n, 'pts, ma_45:', r['ma_45'], 'period_avg:', r['period_avg'],
          'synthetic:', r['data_quality']['synthetic'])
    if d not in (0,): assert n <= d + 1
"
```

Expected: 30/60/252 return ≤ that many points ending today; `days=0` returns ~1150; **synthetic count stays ≤ a handful for every window** (older dates come straight from Supabase, so no synthetic back-fill explosion — if synthetic grows with the window, Step 2 is wrong).

- [ ] **Step 6: HTML + client.** At `index.html:1573` (title "45-Day Revenue Trend (₹ Cr/day)"), restructure as in Task 2 Step 6 with text `Revenue Trend (₹ Cr/day) <span id="sparkRangeLabel">(60 days)</span>` and container `sparkRange`. In `loadHero` (line 3695) change the fetch to:

```js
const days = RANGE_TRADING_DAYS[rangeState['spark'] || '60D'];
const resp = await fetch('/api/history?days=' + (days == null ? 0 : days));
```

In `renderHero` (line 3705) pass the new average through: change `renderSparkline(history, ma45)` (line 3724) to `renderSparkline(history, data.period_avg || ma45)`; in `renderSparkline` change the dashed dataset label `'45d MA'` (line 3756) to `'Period Avg'`. Register in the init block:

```js
makeRangeToggle({
  key: 'spark', containerId: 'sparkRange',
  ranges: ['30D','60D','Q','1Y','2Y','Max'], defaultRange: '60D',
  labelIds: ['sparkRangeLabel'],
  onChange: () => loadHero()
});
```

**Ordering note:** the top-level `loadHero();` call (line 3778) must run *after* this registration so `rangeState['spark']` is set — move `loadHero();` below the init block if needed.

- [ ] **Step 7: Verify on preview** (spark chart widens on `1Y`, hero "45-Day Avg" KPI value unchanged across chips), **then commit:**

```bash
git add api/history.py index.html
git commit -m "feat(toggles): revenue spark days param, period-avg line, chips"
```

---

### Task 5: Intraday curves + Hourly accuracy charts — surface existing `days` params

**Files:**
- Modify: `index.html:1656-1720` (intraday card header/footer), `:3352` (`loadIntradayCurve` fetch), `:2524-2533` (hourly canvases' card titles), `:5292` (`loadHourlyAccuracy` fetch), init block

**Interfaces:**
- Consumes: Task 1 infra; existing server params `exchange_dashboard?view=intraday_curve&days=` (cap 90, `api/exchange_dashboard.py:420-421`) and `analytics?section=hourly_accuracy&days=` (cap 180, `api/analytics.py:359-360`). **No server changes in this task.**
- Produces: control keys `'intraday'` (chips `30D/60D/Q`, default `30D`), `'hourlyRev'`, `'hourlySig'`, `'hourlyFwd'` (chips `30D/60D/Q/Max`, default `Q`; `Max` maps to `days=180`).

- [ ] **Step 1: Intraday chips.** In the intraday accordion card header (near the `.curve-tab` sub-view buttons at `index.html:1656-1657`), add `<span id="intradayRange"></span>` alongside the bucket/cumulative buttons, and in the footer note at line 1720 replace the hardcoded "last 30 trading days" text with `last <span id="intradayRangeLabel">(30 days)</span>` — actually use the plain form: change the footer to `Weights computed over <span id="intradayRangeLabel">(30 days)</span> of snapshots.`

- [ ] **Step 2: Intraday fetch.** In `loadIntradayCurve` (fetch at line 3352), replace `days=30` with:

```js
'days=' + RANGE_TRADING_DAYS[rangeState['intraday'] || '30D']
```

and route the fetch through `fetchRanged(...)` keeping the existing `.then()` handling. Register:

```js
makeRangeToggle({
  key: 'intraday', containerId: 'intradayRange',
  ranges: ['30D','60D','Q'], defaultRange: '30D',
  labelIds: ['intradayRangeLabel'],
  onChange: () => loadIntradayCurve()
});
```

- [ ] **Step 3: Hourly chips.** Each of the three hourly cards (canvases `hourlyRevenueChart` line 2524, `hourlySignalChart` line 2528, `hourlyForwardChart` line 2533) gets a chips container + label span in its card title (`hourlyRevRange`/`hourlyRevRangeLabel`, `hourlySigRange`/`hourlySigRangeLabel`, `hourlyFwdRange`/`hourlyFwdRangeLabel`), same title restructure as Task 2 Step 6.

- [ ] **Step 4: Hourly fetch split.** `loadHourlyAccuracy` (line 5292) currently does one `fetch('/api/analytics?section=hourly_accuracy&days=90')` then calls the three render functions (`renderHourlyAccuracy` 5305, then 5344, 5372). Refactor so each chart fetches its own range (shared ranges dedupe via `fetchRanged`):

```js
const HOURLY_DAYS = { '30D': 30, '60D': 60, 'Q': 63, 'Max': 180 };
function hourlyUrl(key) {
  return '/api/analytics?section=hourly_accuracy&days=' + HOURLY_DAYS[rangeState[key] || 'Q'];
}
function loadHourlyAccuracy() {
  fetchRanged(hourlyUrl('hourlyRev')).then(d => renderHourlyAccuracy(d));
  fetchRanged(hourlyUrl('hourlySig')).then(d => renderHourlySignalChart(d));
  fetchRanged(hourlyUrl('hourlyFwd')).then(d => renderHourlyForwardChart(d));
}
```

Adapt the three render-call names to the actual function names at lines 5305/5344/5372 (read them first; if the three renders are currently inlined in one function, split them so each takes the full response `d` and renders one chart). Register three toggles, each `ranges: ['30D','60D','Q','Max'], defaultRange: 'Q'`, with its own `labelIds`, `onChange` calling the matching single-chart load (e.g. `onChange: () => fetchRanged(hourlyUrl('hourlyRev')).then(d => renderHourlyAccuracy(d))`).

- [ ] **Step 5: Verify on preview.** Predictor tab → expand intraday accordion → chips flip between 30D/60D/Q and the footer label follows. Analytics tab → each hourly chart re-renders independently; two charts set to the same range trigger exactly one network request (check the Network panel or `browser_network_requests`).

- [ ] **Step 6: Commit**

```bash
git add index.html
git commit -m "feat(toggles): intraday + hourly charts surface existing days params"
```

---

### Task 6: Analytics tab — widen payload; IC/Perf/HMM/Correlation controls

**Files:**
- Modify: `api/analytics.py:52-66` (corr — keep computing, client takes over display), `:107-108`, `:193`, `:234-235`, `:285-318` (response)
- Modify: `index.html:2429-2437` (corr card title), `:2445/2469/2499` (three chart card titles), `renderAnalytics` (`index.html:4897-5068`), init block

**Interfaces:**
- Consumes: Task 1 infra.
- Produces: `/api/analytics` response gains `factor_series: {dates: string[], ecm_z: (number|null)[], rev_z: (number|null)[], turn_z: (number|null)[], position_score: (number|null)[]}` and returns **full** `rolling_ic`, `rolling_metrics`, `hmm_regime.history` (no tail slices). Client keys: `'icChart'`, `'perfChart'`, `'hmmChart'` (chips `30D/60D/Q/1Y/2Y/Max`, default `60D`), `'corrTable'` (chips `60D/Q/1Y/2Y/Max`, default `1Y`). Client Pearson helper `pearsonJs(xs, ys) -> number|null`.

- [ ] **Step 1: Widen server payload.** In `api/analytics.py`: delete line 108 (`ic_history = ic_history[-60:]`) and line 193 (`rolling_metrics = rolling_metrics[-60:]`). In the HMM loop, remove the `if i >= len(pos_all) - 60:` gate (line 235) — keep the body (the `sig_idx` mapping) unconditional. In the return dict (line 285), add after `"factor_correlation": {...}`:

```python
        "factor_series": {
            "dates": [s["trading_date"] for s in signals],
            "ecm_z": [_f(s.get("ecm_spread_zscore")) for s in signals],
            "rev_z": [_f(s.get("mf_revenue_z")) for s in signals],
            "turn_z": [_f(s.get("mf_turnover_z")) for s in signals],
            "position_score": [_f(s.get("position_score")) for s in signals],
        },
```

- [ ] **Step 2: Verify server.**

```bash
python3 -m py_compile api/analytics.py && python3 -c "
import sys; sys.path.insert(0, '.')
from api.analytics import generate_analytics
r = generate_analytics()
assert r['success']
print('ic:', len(r['rolling_ic']), 'metrics:', len(r['rolling_metrics']),
      'hmm:', len(r['hmm_regime']['history']), 'series:', len(r['factor_series']['dates']))
import json; print('payload KB:', len(json.dumps(r))//1024)
"
```

Expected: all four counts ≈ 1,000+ (vs 60 before); payload well under 500 KB.

- [ ] **Step 3: Chip containers.** Add title chips + label spans (Task 2 Step 6 pattern) to the four cards: corr table (`corrTableRange`/`corrTableRangeLabel` — title text becomes `Signal Correlation Matrix <span id="corrTableRangeLabel">(1 year)</span>`, replacing the hardcoded "(120-day)"), `icChart` (`icChartRange`/`icChartRangeLabel`), `perfChart` (`perfChartRange`/`perfChartRangeLabel`), `hmmChart` (`hmmChartRange`/`hmmChartRangeLabel`).

- [ ] **Step 4: Client slicing.** `loadAnalytics` should store the response in a module-global `let analyticsCache = null;` and rendering must read window slices. In `renderAnalytics` (line 4897): where the IC chart consumes `data.rolling_ic`, use `sliceTailByRange(data.rolling_ic, rangeState['icChart'] || '60D')`; same for `data.rolling_metrics` with `'perfChart'` and `data.hmm_regime.history` with `'hmmChart'`. Split the per-chart rendering into three functions if currently monolithic (`renderIcChart(data)`, `renderPerfChart(data)`, `renderHmmChart(data)`) so a chip click re-renders only its chart from `analyticsCache` with **zero refetch**. Add the Pearson helper + client-side corr computation:

```js
function pearsonJs(xs, ys) {
  const pairs = [];
  for (let i = 0; i < xs.length; i++) {
    if (xs[i] != null && ys[i] != null) pairs.push([xs[i], ys[i]]);
  }
  const n = pairs.length;
  if (n < 10) return null;
  const mx = pairs.reduce((a, p) => a + p[0], 0) / n;
  const my = pairs.reduce((a, p) => a + p[1], 0) / n;
  let sxy = 0, sxx = 0, syy = 0;
  pairs.forEach(([x, y]) => { sxy += (x-mx)*(y-my); sxx += (x-mx)**2; syy += (y-my)**2; });
  return (sxx > 0 && syy > 0) ? sxy / Math.sqrt(sxx * syy) : null;
}

function renderCorrTable(data) {
  const fs = data.factor_series;
  if (!fs) return;
  const keys = ['ecm_z', 'rev_z', 'turn_z', 'position_score'];
  const sliced = keys.map(k => sliceTailByRange(fs[k], rangeState['corrTable'] || '1Y'));
  const matrix = keys.map((_, i) => keys.map((_, j) => pearsonJs(sliced[i], sliced[j])));
  // ...reuse the existing corrBody cell-painting code (index.html:4897+/corrColor 5069),
  // feeding it `matrix` and the existing labels instead of data.factor_correlation.matrix
}
```

(Port the existing cell-painting loop from the current corr rendering verbatim — only the matrix source changes.)

- [ ] **Step 5: Register four toggles** in the init block: `icChart`/`perfChart`/`hmmChart` with `ranges: ['30D','60D','Q','1Y','2Y','Max'], defaultRange: '60D'`, `onChange` re-rendering that one chart from `analyticsCache`; `corrTable` with `ranges: ['60D','Q','1Y','2Y','Max'], defaultRange: '1Y'`, `onChange: () => renderCorrTable(analyticsCache)`.

- [ ] **Step 6: Verify on preview.** Analytics tab: all three charts render at 60 pts by default; `Max` shows ~1,000 pts; corr matrix values change between `60D` and `2Y`; chip clicks cause **no** network request (payload cached). **Commit:**

```bash
git add api/analytics.py index.html
git commit -m "feat(toggles): analytics full-series payload; IC/perf/HMM/corr controls"
```

---

### Task 7: Timeline tab — trend chart + FY/Quarterly/Monthly grids

**Files:**
- Modify: `api/exchange_dashboard.py:189` (`q_order[:6]`), `:232` (`m_order[:3]`), `:372-378` (`daily_trend`)
- Modify: `index.html:2846-2848` (trend title), `:2823-2835` (grid section headers), `renderExchange`/`renderExd4Up` (`index.html:5822-5995`, grid configs 5864-5882), `renderExdTrendChart` (6016-6052), init block

**Interfaces:**
- Consumes: Task 1 infra.
- Produces: server returns full `daily_trend` (~1,150 pts), all quarters, all months (each month entry already carries `label`; the `"Avg Of Last 6 Months"` synthetic row stays appended last). Client keys: `'exdTrend'` (`30D/60D/Q/1Y/2Y/Max`, default `60D`), `'exdQtr'` (`4Q/8Q/All`, default `4Q`), `'exdMonth'` (`3M/6M/12M/All`, default `3M`). FY grid: no chips, cap removed. Client caches response in `let exdCache = null;`.

- [ ] **Step 1: Server widening.** In `api/exchange_dashboard.py`: line 189 `for q in q_order[:6]:` → `for q in q_order:`; line 232 `for i, mk in enumerate(m_order[:3]):` → `for i, mk in enumerate(m_order):`; line 374 `for r in data[-60:]:` → `for r in data:`.

- [ ] **Step 2: Verify server.**

```bash
python3 -m py_compile api/exchange_dashboard.py && python3 -c "
import sys; sys.path.insert(0, '.')
from api.exchange_dashboard import generate_exchange_dashboard
r = generate_exchange_dashboard()
assert r['success']
print('trend:', len(r['daily_trend']), 'quarterly:', len(r['quarterly']),
      'monthly:', len(r['monthly']), 'fy:', len(r['fy_summary']))
import json; print('payload KB:', len(json.dumps(r, default=str))//1024)
"
```

Expected: trend ≈ 1,150; quarterly ≈ 19 (all since 2022); monthly ≈ 56; payload < 400 KB.

- [ ] **Step 3: Client — cache + trend chips.** In `loadExchange`, store the payload: `exdCache = data;` before rendering. Trend title (line 2846, "Daily Revenue Trend (Last 60 Days)") gets the Task-2-style restructure (`exdTrendRange`/`exdTrendRangeLabel`). In `renderExdTrendChart` (6016), slice input: `const trend = sliceTailByRange(data.daily_trend, rangeState['exdTrend'] || '60D');` and render from `trend`.

- [ ] **Step 4: Grids.** In the `renderExd4Up` configs (lines 5864–5882): FY grid config `maxRows: 3` → `maxRows: 99`. Quarterly grid: `maxRows` becomes dynamic — `maxRows: ({'4Q':4, '8Q':8, 'All':99})[rangeState['exdQtr'] || '4Q']`; Monthly grid: `maxRows: ({'3M':3, '6M':6, '12M':12, 'All':99})[rangeState['exdMonth'] || '3M']` — note the monthly grid also renders the `is_average` 6-month row; keep it visible regardless of range (filter it out of the count: pass real months to the row cap, then append the average row). Add chips containers next to the "Quarterly Breakdown" and "Monthly Breakdown" section headers (`exdQtrRange`, `exdMonthRange`; no label spans needed — the chip itself communicates). Register both toggles with `onChange: () => renderExchange(exdCache)` (or the narrower grid-only re-render if `renderExd4Up` can be invoked per-grid — prefer that if the call sites at 5864-5882 are separable).

- [ ] **Step 5: Verify on preview.** Timeline tab: trend chart follows chips up to Max (~1,150 pts); FY grid shows all ~5 FYs; Quarterly `8Q` shows 8 quarter panels; Monthly `12M` shows 12 + the average row; no refetch on chip clicks. **Commit:**

```bash
git add api/exchange_dashboard.py index.html
git commit -m "feat(toggles): timeline trend + FY/quarterly/monthly grid ranges"
```

---

### Task 8: Margins tab — history chart narrowing + changes table window

**Files:**
- Modify: `lib/margin_dashboard.py:175` (changes cap)
- Modify: `index.html:2888-2890` (history chart title), `:2880-2882` (changes card title), `updateMarginChart` (6382-6430), changes-table builder (6349-6381), init block

**Interfaces:**
- Consumes: Task 1 infra. `margin_history = {dates: string[], [symbol]: (number|null)[]}` already ships all ~640 snapshot dates.
- Produces: server `margin_changes` uncapped (list of `{date, symbol, old_total, new_total, change, direction}`, newest first). Client keys: `'marginHist'` (`30D/60D/Q/1Y/2Y/Max`, default `60D` — **deliberate narrowing** from all-history), `'marginChanges'` (`30D/60D/Q/1Y/Max`, default `60D`).

- [ ] **Step 1: Server.** `lib/margin_dashboard.py:175`: `"margin_changes": margin_changes[:50],` → `"margin_changes": margin_changes[:1000],` (safety cap far above the realistic few-hundred total; keeps payload bounded).

- [ ] **Step 2: Verify server.**

```bash
python3 -m py_compile lib/margin_dashboard.py && python3 -c "
import sys; sys.path.insert(0, '.')
from lib.margin_dashboard import generate_margin_dashboard
r = generate_margin_dashboard()
assert r['success']
print('dates:', len(r['margin_history']['dates']), 'changes:', len(r['margin_changes']))
"
```

Expected: dates ≈ 640, changes ≥ 50 (was capped at exactly 50).

- [ ] **Step 3: History chart slicing.** In `updateMarginChart` (6382-6430): the chart currently plots `margin_history.dates` in full. Compute the window before building datasets:

```js
const allDates = marginData.margin_history.dates;
const dates = sliceTailByRange(allDates, rangeState['marginHist'] || '60D');
const startIdx = allDates.length - dates.length;
```

and slice every symbol series with `.slice(startIdx)` where the datasets are built (mirror the OIP pattern at `index.html:7226-7227`). Add chips + label span to the history chart title (line 2888-2890): `marginHistRange`/`marginHistRangeLabel`. Register with `onChange: () => updateMarginChart()` — confirm `updateMarginChart` reads the cached margins payload from its existing module state (it is invoked by `applyMarginFilter`; keep that flow, chips just re-invoke it).

- [ ] **Step 4: Changes table window + scroll.** In the changes-table builder (6349-6381), filter before rendering:

```js
const winDates = sliceTailByRange(marginData.margin_history.dates, rangeState['marginChanges'] || '60D');
const minDate = winDates[0];
const changes = marginData.margin_changes.filter(c => c.date >= minDate);
```

Wrap the table in `<div class="scroll-table-wrap">…</div>` (HTML at 2880-2882). Add chips container `marginChangesRange` in that card title; register toggle (`ranges: ['30D','60D','Q','1Y','Max']`, default `'60D'`, `onChange` re-runs the table builder from cached data).

- [ ] **Step 5: Verify on preview.** Margins tab: history chart now opens at ~60 sessions (not 2.5 years); `Max` restores full history; commodity chips still compose with the range; changes table shows >50 rows on `Max` and scrolls with a sticky header. **Commit:**

```bash
git add lib/margin_dashboard.py index.html
git commit -m "feat(toggles): margins history narrowing + date-windowed changes table"
```

---

### Task 9: Momentum tab — split shared chips into three per-control toggles

**Files:**
- Modify: `index.html:2944-2957` (regime title row — remove `momRangeChips`), `:2967` (price title), `:2976` (table title), `:6440-6478` (state/loader/setter), `renderMomentum` (6480-6536), `renderMomTable` (7004+), init block

**Interfaces:**
- Consumes: Task 1 infra; existing server `?view=momentum&range=` (unchanged).
- Produces: keys `'momRegime'`, `'momPrice'` (charts), `'momTable'` (table) — all `ranges: ['30D','60D','Q','1Y','2Y','Max']`; charts default `'60D'`, table default `'30D'`. Deleted globals: `momCurrentRange`, `momDataCacheByRange`, `MOM_RANGE_LABELS`, `setMomRange` (grep for stray references after deletion: `grep -n "momCurrentRange\|setMomRange\|MOM_RANGE_LABELS\|momDataCacheByRange" index.html` must return nothing). New helper `momUrl(key)`.

- [ ] **Step 1: HTML.** In the regime title row (2944-2957): delete the entire `<span id="momRangeChips">…</span>` block, replace with `<span id="momRegimeRange"></span>` (keep the `momBandsToggle` chip beside it). Price chart title (2967): `Price &amp; Composite Signal <span id="momPriceRangeLabel">(60 days)</span> <span id="momPriceRange"></span>`. Signal table title (2976): `Signal History <span id="momTableRangeLabel">(30 days)</span>` plus `<span id="momTableRange"></span>`, and wrap the table's `overflow-x:auto` div content in `scroll-table-wrap` as well (add the class alongside: `style="overflow-x:auto" class="scroll-table-wrap"`).

- [ ] **Step 2: Rewire the loader.** Replace `loadMomentum`/`setMomRange` (6452-6478) and the `momDataCacheByRange`/`momCurrentRange`/`MOM_RANGE_LABELS` globals (6440-6450) with:

```js
function momUrl(key) {
  const r = rangeState[key] || (key === 'momTable' ? '30D' : '60D');
  return '/api/models?view=momentum&range=' + encodeURIComponent(r);
}

function loadMomentum() {
  // Hero/KPIs ride the regime-chart fetch (snapshot is identical across ranges)
  fetchRanged(momUrl('momRegime')).then(d => {
    if (!d.success) { document.getElementById('momAsOf').textContent = 'Error: ' + (d.error || 'No data'); return; }
    renderMomentumHero(d);
    renderMomRegimeChart(d.history || []);
  }).catch(e => { document.getElementById('momAsOf').textContent = 'Fetch error: ' + e.message; });
  fetchRanged(momUrl('momPrice')).then(d => { if (d.success) renderMomPriceChart(d.history || []); });
  fetchRanged(momUrl('momTable')).then(d => { if (d.success) renderMomTable(d.history || []); });
}
```

Split `renderMomentum` (6480-6536): everything from `const snap = d.snapshot;` down to the KPI cards (through line 6528) becomes `renderMomentumHero(d)`; delete the trailing three render calls (6531-6535) — `loadMomentum` now drives them. `momShowBands`'s setter (`setMomShowBands`, ~6872) currently re-renders both charts — update it to re-render each from its own cached fetch: `fetchRanged(momUrl('momRegime')).then(d => renderMomRegimeChart(d.history||[])); fetchRanged(momUrl('momPrice')).then(d => renderMomPriceChart(d.history||[]));`

- [ ] **Step 3: Fix the table (existing inconsistency).** In `renderMomTable` (7004+), delete the `hist.slice(-15)` truncation — render the full array it receives (the range already sized it).

- [ ] **Step 4: Register three toggles** in the init block: `momRegime` (labelIds `['momRangeLabel']` — the existing span at 2946, default `'60D'`, `onChange: () => fetchRanged(momUrl('momRegime')).then(d => { renderMomentumHero(d); renderMomRegimeChart(d.history||[]); })`), `momPrice` (labelIds `['momPriceRangeLabel']`, default `'60D'`), `momTable` (labelIds `['momTableRangeLabel']`, default `'30D'`).

- [ ] **Step 5: Verify on preview.** Momentum tab: three independent chip rows; setting regime=1Y while price=60D renders different x-spans; table at `2Y` shows ~500 scrollable rows; two controls on the same range produce a single network request; `grep` from Interfaces returns nothing. **Commit:**

```bash
git add index.html
git commit -m "feat(toggles): momentum per-control ranges; signal table follows range"
```

---

### Task 10: OI Participants tab — standard vocabulary, per-chart controls, growth-chart fix

**Files:**
- Modify: `index.html:3050-3061` (remove tab-level Period row), `:3119/3127/3149` (three chart card titles), `:7038` (`oipTimePeriod` global), `:7102-7109` (`oipSetPeriod`), `:7158-7168` (`oipFilterDates`), `oipRenderHeroChart` (7221+), `oipRenderGrowthChart` (7288+), `updateOipCompChart` (7365+), init block

**Interfaces:**
- Consumes: Task 1 infra. Data starts Sep 2024 (~23 months) → no `2Y` chip.
- Produces: keys `'oipHero'`, `'oipComp'` (`ranges: ['30D','60D','Q','1Y','Max']`, default `'Max'`), `'oipGrowth'` (`ranges: ['3M','6M','12M','All']`, default `'All'`). `oipFilterDates(allDates, key)` re-signature (count-based tail slice). Deleted: `oipTimePeriod`, `oipSetPeriod` (grep clean: `grep -n "oipTimePeriod\|oipSetPeriod" index.html`).

- [ ] **Step 1: HTML.** Delete the whole Period `<div>` (3052-3061, label + `oipTimeBtns`). Add chips containers into each chart card title: hero chart (canvas 3119) → `oipHeroRange`; growth chart (3127) → `oipGrowthRange`; composition chart (3149) → `oipCompRange` (next to the existing `oipCompSelect` dropdown).

- [ ] **Step 2: Re-signature the filter.** Replace `oipFilterDates` (7158-7168) with:

```js
function oipFilterDates(allDates, key) {
  return sliceTailByRange(allDates, rangeState[key] || 'Max');
}
```

Update the two call sites: `oipRenderHeroChart` (7226) → `oipFilterDates(allDates, 'oipHero')`; `updateOipCompChart` (7374) → `oipFilterDates(allDates, 'oipComp')`. Delete `oipSetPeriod` (7102-7109) and the `var oipTimePeriod = 'ALL'` global (7038).

- [ ] **Step 3: Growth chart honors a period (existing inconsistency fix).** In `oipRenderGrowthChart` (7288+), the months axis comes from `data.monthly_growth.months` (plus parallel value arrays). Slice both together:

```js
const mCount = { '3M': 3, '6M': 6, '12M': 12, 'All': null }[rangeState['oipGrowth'] || 'All'];
const months = sliceTailByCount(data.monthly_growth.months, mCount);
const startIdx = data.monthly_growth.months.length - months.length;
```

and `.slice(startIdx)` every parallel series the function plots.

- [ ] **Step 4: Register three toggles**, each `onChange: () => oipRenderAll()` (cheap — all client-side; `oipRenderAll` at 7193 re-renders everything from `oipCache`).

- [ ] **Step 5: Verify on preview.** OIP tab: hero + composition charts default to full history and narrow on `60D`; growth chart narrows to 3 bars on `3M`; hedger chart and current-distribution table unchanged (excluded snapshots); persistence survives reload; grep from Interfaces returns nothing. **Commit:**

```bash
git add index.html
git commit -m "feat(toggles): OIP standard vocabulary per-chart; growth chart honors range"
```

---

### Task 11: Quarterly P&L tab — client-side `4Q/8Q/All` slices

**Files:**
- Modify: `index.html:2773` (trend chart card title), `:2789` (table card title), `renderQtrTrendChart` (5661-5706), `renderQtrTable` (5754-5798), `loadQuarterly`/`renderQuarterly` (5586-5658), init block

**Interfaces:**
- Consumes: Task 1 infra; `/api/quarterly` payload (hardcoded `QUARTERLY_ACTUALS` in `api/quarterly.py:24` + a projected current quarter). **No server change.**
- Produces: keys `'qtrChart'`, `'qtrTable'` (`ranges: ['4Q','8Q','All']`, default `'All'`), helper `sliceQuartersKeepProjection(list, rangeKey)`. Client cache `let qtrCache = null;` set in `loadQuarterly`.

- [ ] **Step 1: Inspect the payload shape first.** Read `api/quarterly.py` return dict and `renderQtrTrendChart` (5661) to learn the exact field names for the quarters array and the projected-quarter flag (e.g. `is_projected` / `projected: true`). Then define (adjusting the flag name to what you found):

```js
function sliceQuartersKeepProjection(list, rangeKey) {
  const n = { '4Q': 4, '8Q': 8, 'All': null }[rangeKey];
  if (n == null) return list;
  const projected = list.filter(q => q.is_projected);
  const actual = list.filter(q => !q.is_projected);
  return actual.slice(-n).concat(projected);
}
```

- [ ] **Step 2: Wire.** `loadQuarterly` stores `qtrCache = data;`. `renderQtrTrendChart` consumes `sliceQuartersKeepProjection(<quarters array>, rangeState['qtrChart'] || 'All')`; `renderQtrTable` same with `'qtrTable'`. Add chips containers `qtrChartRange` (title 2773) and `qtrTableRange` (title 2789); register both toggles with `onChange` re-rendering that one element from `qtrCache`.

- [ ] **Step 3: Verify on preview.** Quarterly tab: `4Q` shows last 4 actual quarters + the faded projected quarter in both chart and table; `All` shows everything (current behavior). **Commit:**

```bash
git add index.html
git commit -m "feat(toggles): quarterly P&L 4Q/8Q/All client slices"
```

---

### Task 12: Commodities revenue breakdown — `?range=` (2Y cap) for trend chart + qtr/month tables

**Files:**
- Modify: `api/commodity_dashboard.py:75-92` (signature + start_date), `:216-259` (quarterly/monthly slices), `:295-303` (trend slice), `:305-315` (response), `:320-336` (handler)
- Modify: `index.html:2691-2693` (trend title), `:2684/2688` (table card titles), `renderCmdDashboard` (6075-6214), init block

**Interfaces:**
- Consumes: Task 1 infra.
- Produces: `GET /api/commodity_dashboard?range=<30D|60D|Q|1Y|2Y>` (**no Max** — 2Y cap per spec). `daily_trend` sized to range; `quarterly` last 9; `monthly` last 24; `"range"` echoed. Client keys: `'cmdTrend'` (`30D/60D/Q/1Y/2Y`, default `'60D'`), `'cmdQtr'` (`4Q/8Q`, default `'8Q'`), `'cmdMonth'` (`3M/6M/12M/24M`, default `'3M'`). Client cache `let cmdDashCache = null;`.

- [ ] **Step 1: Server signature + fetch window.** In `api/commodity_dashboard.py`, above `generate_commodity_dashboard` add:

```python
RANGE_DAYS = {"30D": 30, "60D": 60, "Q": 63, "1Y": 252, "2Y": 504}
DEFAULT_RANGE = "60D"
```

Change `def generate_commodity_dashboard():` (line 75) to `def generate_commodity_dashboard(range_key=DEFAULT_RANGE):` with validation `if range_key not in RANGE_DAYS: range_key = DEFAULT_RANGE`. After the existing `start_date = date(2000 + yy - 2, 4, 1)` (line 84), add the 2Y guard (504 trading days ≈ 26 calendar months can marginally exceed the 2-FY window in early April):

```python
    if range_key == "2Y":
        today_d = ist.date() if hasattr(ist, "date") else ist
        start_date = min(start_date, today_d - timedelta(days=800))
```

- [ ] **Step 2: Widen the aggregate slices.** Line 223 `last_6_quarters = sorted_quarters[-6:]` → `last_quarters = sorted_quarters[-9:]` (rename the loop variable use at line 226 accordingly). Line 259 `last_3_months = sorted_months[-3:]` → `last_months = sorted_months[-24:]` (rename at 262). Line 296 `trend_data = daily_data[-60:]` → `trend_data = daily_data[-RANGE_DAYS[range_key]:]`. Add `"range": range_key,` to the return dict (line 305).

- [ ] **Step 3: Handler.** In `do_GET` (line 335), default branch: `result = generate_commodity_dashboard(range_key=qs.get("range", [DEFAULT_RANGE])[0])`.

- [ ] **Step 4: Verify server** (this endpoint is the heavy one — watch runtime):

```bash
python3 -m py_compile api/commodity_dashboard.py && python3 -c "
import sys, time; sys.path.insert(0, '.')
from api.commodity_dashboard import generate_commodity_dashboard
for rk in ['60D', '2Y']:
    t0 = time.time()
    r = generate_commodity_dashboard(range_key=rk)
    assert r['success']
    print(rk, '-> trend:', len(r['daily_trend']), 'qtrs:', len(r['quarterly']),
          'months:', len(r['monthly']), f'{time.time()-t0:.1f}s')
"
```

Expected: 60D trend=60, 2Y trend=504; quarterly ≈ 9; monthly ≈ 24; 2Y runtime comfortably under the 60 s `maxDuration`.

- [ ] **Step 5: Client.** `loadCmdDashboard` (fetch 6058): `fetchRanged('/api/commodity_dashboard?range=' + encodeURIComponent(rangeState['cmdTrend'] || '60D'))`, store `cmdDashCache = data;`. Trend title (2691, "Commodity Trend (Last 60 Days)") → Task-2 restructure with `cmdTrendRange`/`cmdTrendRangeLabel`. Quarterly table title (near 2684) gets `cmdQtrRange`; monthly (2688) gets `cmdMonthRange`. In the render fns: quarterly table consumes `sliceTailByCount(data.quarterly, {'4Q':4,'8Q':8}[rangeState['cmdQtr']||'8Q'])`; monthly consumes `sliceTailByCount(data.monthly, {'3M':3,'6M':6,'12M':12,'24M':24}[rangeState['cmdMonth']||'3M'])`; wrap the monthly table in `scroll-table-wrap`. Register: `cmdTrend` (`onChange: () => loadCmdDashboard()` — server-ranged, refetch), `cmdQtr`/`cmdMonth` (`onChange` re-renders that table from `cmdDashCache`, no refetch).

**Watch out:** the trend chart's *default* stays 60D so the initial payload stays small; the table chips slice whatever the current payload holds — the payload always contains 9 quarters/24 months regardless of `range`, so table chips work at any trend range.

- [ ] **Step 6: Verify on preview** (trend chart 2Y renders ~504 stacked bars without jank; 8Q shows 8 quarter columns; commodity summary table unchanged) **then commit:**

```bash
git add api/commodity_dashboard.py index.html
git commit -m "feat(toggles): commodity dashboard range param (2Y cap) + table windows"
```

---

### Task 13: Commodity signals — `?range=` (2Y cap) for Sector Rotation + Momentum table

**Files:**
- Modify: `api/commodities.py:237-251` (signature + cutoff), `:289-291` (rotation window), `:311-317` (momentum window), `:356-369` (response), handler `do_GET` (below line 399 — read it first; it mirrors the others)
- Modify: `index.html:2732-2734` (rotation title), `:2743` (momentum table title), `renderCommodity`/rotation render (5455-5579), init block

**Interfaces:**
- Consumes: Task 1 infra.
- Produces: `GET /api/commodities?view=signals&range=<30D|60D|Q|1Y|2Y>` — rotation dates and per-commodity momentum windows sized to range; fetch cutoff scales (`cal_days = int(window * 1.55) + 25`); `"range"` echoed in `data_quality`. Client keys: `'sectorRot'`, `'cmdMomentum'` (both `30D/60D/Q/1Y/2Y`, default `'60D'`). **Today's lineup + movers stay latest-date/1-day (excluded).**

- [ ] **Step 1: Server.** Above `generate_commodity_analytics` (line 237) add the same `RANGE_DAYS`/`DEFAULT_RANGE` block as Task 12. Change signature to `def generate_commodity_analytics(range_key=DEFAULT_RANGE):` with validation, then replace the fixed cutoff (line 243):

```python
    window = RANGE_DAYS[range_key]
    cal_days = int(window * 1.55) + 25   # trading→calendar buffer (was fixed 150)
    cutoff = (ist_now - timedelta(days=cal_days)).strftime("%Y-%m-%d")
```

Line 291 `dates = dates[-60:]` → `dates = dates[-window:]`. Line 317 `recent = c_rows[-60:]` → `recent = c_rows[-window:]`. Add `"range": range_key,` inside `data_quality` (line 363). Handler: read `range` from qs in the signals branch and pass through.

- [ ] **Step 2: Verify server.**

```bash
python3 -m py_compile api/commodities.py && python3 -c "
import sys, time; sys.path.insert(0, '.')
from api.commodities import generate_commodity_analytics
for rk in ['60D', '2Y']:
    t0 = time.time()
    r = generate_commodity_analytics(range_key=rk)
    assert r['success']
    print(rk, '-> rotation:', len(r['sector_rotation']),
          'momentum rows:', len(r['commodity_momentum']), f'{time.time()-t0:.1f}s')
"
```

Expected: rotation 60 → ~504 dates; momentum table rows similar count of commodities but `days` per row grows; 2Y runtime acceptable (~15k rows paginated).

- [ ] **Step 3: Client.** `loadCommodity` (fetch 5423): URL becomes `'/api/commodities?view=signals&range=' + encodeURIComponent(rangeState['sectorRot'] || '60D')` via `fetchRanged`. **Note:** rotation chart and momentum table share this endpoint but have separate keys — fetch each independently (two `fetchRanged` calls with each control's range; identical ranges dedupe): rotation render consumes its fetch, momentum-table render consumes its own. Today's-lineup + movers tables render from the `'sectorRot'` fetch (any range returns the same latest-date data). Titles: rotation (2732, "Sector Rotation (60-Day Turnover Share)") → `sectorRotRange`/`sectorRotRangeLabel`; momentum (2743, "Commodity Momentum (60-Day Rolling)") → `cmdMomentumRange`/`cmdMomentumRangeLabel`. Register both toggles with `onChange` refetching + re-rendering only their element.

- [ ] **Step 4: Verify on preview, then commit:**

```bash
git add api/commodities.py index.html
git commit -m "feat(toggles): sector rotation + commodity momentum range param (2Y cap)"
```

---

### Task 14: Full verification sweep + production gate

**Files:** none (verification only; fixes loop back into the responsible task's files)

- [ ] **Step 1: Fresh preview deploy.** `vercel deploy 2>&1 | tail -3`

- [ ] **Step 2: Playwright full sweep** against the preview URL. For every control below: click each chip → assert the chart/table changes (dataset length via tooltip density or table row count) and the title label updates; no console errors.

| Tab | Controls |
|---|---|
| Daily Predictor | spark, intraday |
| Fair Value | valChart, ecmChart |
| Analytics | icChart, perfChart, hmmChart, corrTable, hourlyRev, hourlySig, hourlyFwd |
| Commodities | cmdTrend, cmdQtr, cmdMonth, sectorRot, cmdMomentum |
| Quarterly P&L | qtrChart, qtrTable |
| Timeline | exdTrend, exdQtr, exdMonth (+ FY grid shows all FYs) |
| Margins | marginHist, marginChanges |
| Momentum | momRegime, momPrice, momTable |
| OI Participants | oipHero, oipGrowth, oipComp |

- [ ] **Step 3: Persistence test.** Set non-default ranges on ≥3 controls across different tabs, reload, confirm all restored.

- [ ] **Step 4: Regression test.** In a fresh browser context (empty localStorage), compare every tab's default view against production (`https://<prod-domain>`): identical except the nine deliberate `†` changes listed in spec §4 (spark 45→60d, corr 120d→1Y, hourly 90→63d, cmdQtr 6→8, Timeline FY cap removed, Timeline qtr 3→4, marginHist all→60D, marginChanges 50-rows→60D-window, momTable 15→30D).

- [ ] **Step 5: Excluded-views check.** Top futures/options, commodity lineup, movers, current margins, OIP current table + hedger chart, Forecast tab, weekly/DoW panels — all unchanged, no stray chips.

- [ ] **Step 6: STOP — ask the user for production approval.** Report sweep results. Only after explicit approval: `vercel deploy --prod`, then re-run a spot-check of 3 controls on production.

---

## Self-review notes (kept for the executor)

- Spec §4 wiring matrix ↔ tasks: Predictor→4,5 · Fair Value→2,3 · Analytics→5,6 · Commodities→12,13 · Quarterly→11 · Timeline→7 · Margins→8 · Momentum→9 · OIP→10 · shared infra→1 · verification→14. Spec §6 exclusions honored (no task touches them).
- All `rangeState` keys used exactly once each; container ids follow `<key>Range`, labels `<key>RangeLabel`.
- Tasks 2–13 are independent after Task 1 lands; execute in numeric order anyway so preview checks accrete.
