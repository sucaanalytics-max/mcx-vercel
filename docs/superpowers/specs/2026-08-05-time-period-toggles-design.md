# Time Period Toggles — Design Spec

**Date:** 2026-08-05
**Status:** Approved design, pending user spec review
**Scope decisions (user-approved):** per-chart toggles · `30D/60D/Q/1Y/2Y/Max` vocabulary · core charts + inconsistency fixes + Timeline grids + fixed-window tables · per-control persistence · Commodities endpoints capped at 2Y (no Max) · URL deep-linking explicitly out of scope.

---

## 1. Goal

Every chart and table in the MCX dashboard that displays a time series (or a windowed aggregate) gets its own time-period toggle, using one consistent UI, one consistent range vocabulary, and remembered selections. Views with no time dimension are explicitly excluded (§6).

## 2. Range system

### 2.1 Vocabulary

One standard set everywhere, in trading days (matches `RANGE_DAYS` in `api/models.py:243`):

| Chip | Trading days |
|---|---|
| `30D` | 30 |
| `60D` | 60 |
| `Q` | 63 |
| `1Y` | 252 |
| `2Y` | 504 |
| `Max` | all available |

Rules:

- **Subset rule:** each control shows only the chips its data honestly supports (e.g. intraday charts show `30D/60D/Q`; OI participants data starts Sep 2024, so no `2Y` chip — `Max` covers it).
- **Unit rule:** views whose natural unit is not a trading day use unit-appropriate labels from the same UI: quarterly views `4Q/8Q/All`, monthly views `3M/6M/12M/All` (or `24M` where capped at 2 years).
- **Anchor rule:** date cutoffs anchor to the **last data date** in the series, never `new Date()` — the existing `oipFilterDates` behavior (`index.html:7158`) — so MCX's 1–2 day publishing lag never produces empty chart tails.
- **Commodities cap:** controls backed by `mcx_commodity_daily` (~40k rows) and `mcx_commodity_signals` (~20k rows) stop at `2Y`. No `Max` chip on those.

### 2.2 Labels

A single `RANGE_LABELS` map (generalizing `MOM_RANGE_LABELS`, `index.html:6457`) drives dynamic text: card-title suffixes like "(60 days)", and footer captions such as the intraday "last 30 trading days" note (`index.html:1720`). Static period text in titles/footers of every wired control is replaced by a labeled `<span>` the factory updates.

## 3. Client architecture

### 3.1 Shared toggle factory

One function in the main `<script>` block:

```js
makeRangeToggle({
  key,          // unique control key, e.g. 'valChart' — also the storage key suffix
  containerId,  // div that receives the chip buttons
  ranges,       // ordered subset of the vocabulary, e.g. ['30D','60D','Q','1Y','2Y','Max']
  defaultRange, // used when nothing persisted
  labelIds,     // [] of span ids to receive RANGE_LABELS text
  onChange,     // (rangeKey) => void — re-render (and refetch if server-ranged)
})
```

Responsibilities: render `.margin-chip` pills (existing CSS, `index.html:1179–1181`), manage the `.active` class, restore persisted selection, write persistence on click, update labels, invoke `onChange`.

### 3.2 Persistence

`localStorage` key `mcx.range.<key>`, one per control (the theme toggle, `index.html:3218`, is the existing precedent). Invalid/stale stored values (not in the control's `ranges`) fall back to `defaultRange`.

### 3.3 Fetch caching

A generic per-endpoint, per-range cache (generalizing `momDataCacheByRange`, `index.html:6454`):

```js
fetchRanged(url, rangeKey) // returns cached promise per (url, rangeKey)
```

- Toggling back to an already-seen range never refetches.
- Controls on the same endpoint+range share one fetch (e.g. the three Momentum controls, the three Hourly charts).
- `doRefresh()` (`index.html:3651`) clears the cache so auto-refresh keeps intraday data live; each control then re-renders **with its current selection** (selections live in per-control state, never reset by refresh).

### 3.4 Long tables

Any table that can exceed ~20 rows at large ranges (Signal History, Margin Changes, monthly commodity table at 24M) is wrapped in a `max-height` scroll container with a sticky header. No pagination.

## 4. Wiring matrix — every control

Strategy legend: **CS** = client-side slice (server already returns, or will now return, the full series) · **SP** = server `?range=`/`?days=` param (endpoint truncates at the DB today) · **CX** = count expander for aggregated grids.

Defaults preserve current behavior unless noted (†).

### Daily Predictor tab

| Control | Chips | Default | Strategy | Notes |
|---|---|---|---|---|
| Revenue spark (`sparkChart`, `index.html:3727`) | 30D/60D/Q/1Y/2Y/Max | 60D † (was 45d) | SP | `api/history.py` gets `?days=`; generalize the 45-day walk (`history.py:99–106`); 3-tier fill (Supabase → Alpha Vantage → synthetic) unchanged and rarely triggers pre-2026. MA line becomes flat **period average**, label updated accordingly. |
| Intraday curves card (`intradayChart` + `cumulativeChart` — one control; bucket/cumulative are sub-views of the same card) | 30D/60D/Q | 30D | SP | The server `days` param **already exists** (`api/exchange_dashboard.py:420`, cap 90); the client just stops hardcoding `days=30` (`index.html:3352`). Footer caption becomes dynamic. |

### Fair Value tab

| Control | Chips | Default | Strategy | Notes |
|---|---|---|---|---|
| Fair Value vs Price (`valChart`) | 30D/60D/Q/1Y/2Y/Max | 60D | SP | `api/valuation.py`: add `?range=` → `RANGE_DAYS`; replace `limit=90` (`:96`) and `[-60:]` (`:181`) with window-sized fetch + tail slice; echo `range` in response. |
| ECM Spread (`ecmChart`) | 30D/60D/Q/1Y/2Y/Max | 60D | SP | `api/models.py` ECM branch: add `?range=` reusing its own `RANGE_DAYS`; replace `limit=120` (`:88`) and `[-60:]` (`:140`). σ-bands remain computed over the returned window. |

### Analytics tab

`api/analytics.py` stops tail-slicing computed series and additionally ships the raw z-score series; all Analytics controls are then CS.

| Control | Chips | Default | Strategy | Notes |
|---|---|---|---|---|
| Rolling IC (`icChart`) | 30D/60D/Q/1Y/2Y/Max | 60D | CS | Server returns full `ic_history` (drop `[-60:]`, `analytics.py:108`); the 60-day *computation* window is unchanged — the toggle selects the displayed tail. |
| Rolling Performance (`perfChart`) | 30D/60D/Q/1Y/2Y/Max | 60D | CS | Full `rolling_metrics` (drop `[-60:]`, `:193`). |
| HMM Regime (`hmmChart`) | 30D/60D/Q/1Y/2Y/Max | 60D | CS | Full state series (drop last-60 keep, `:235`). |
| Signal Correlation Matrix (`corrTable`) | 60D/Q/1Y/2Y/Max | 1Y † (was 120d) | CS | Server ships raw `ecm_z / rev_z / turn_z / pos_score` series; Pearson computed client-side over the selected window. Title "(120-day)" becomes dynamic. |
| Hourly Revenue Accuracy (`hourlyRevenueChart`) | 30D/60D/Q/Max | Q † (was 90d) | SP | `days` param **already exists** (`analytics.py:359`, cap 180); `Max` maps to 180. Client stops hardcoding `days=90` (`index.html:5292`). |
| Hourly Signal Match (`hourlySignalChart`) | 30D/60D/Q/Max | Q † | SP | Same fetch, own chips (shared per-range cache). |
| Hourly Forward Returns (`hourlyForwardChart`) | 30D/60D/Q/Max | Q † | SP | Same. |

### Commodities tab (capped at 2Y throughout)

| Control | Chips | Default | Strategy | Notes |
|---|---|---|---|---|
| Commodity Trend (`cmdTrendChart`) | 30D/60D/Q/1Y/2Y | 60D | SP | `api/commodity_dashboard.py`: `?range=`; `daily_data[-60:]` (`:296`) becomes window slice; for `2Y`, extend `start_date` (`:84–90`) to 26 calendar months so 504 trading days always fit. |
| Quarterly by Commodity (`cmdQtrTable`) | 4Q/8Q | 8Q † (was 6) | SP | Widen `sorted_quarters[-6:]` (`:223`); client slices per chip. |
| Monthly by Commodity (`cmdMonthTable`) | 3M/6M/12M/24M | 3M | SP | Widen `sorted_months[-3:]` (`:259`); scroll container at 12M+. |
| Sector Rotation (`sectorRotationChart`) | 30D/60D/Q/1Y/2Y | 60D | SP | `api/commodities.py`: `?range=` sizes the 150-day cutoff (`:243`) up to 26 months; `dates[-60:]` (`:291`) becomes window slice. |
| Commodity Momentum (`momentumTable`) | 30D/60D/Q/1Y/2Y | 60D | SP | Same endpoint/param; `c_rows[-60:]` (`:314`) window-sized. Rolling stats recompute over the selected window. |

### Quarterly P&L tab

| Control | Chips | Default | Strategy | Notes |
|---|---|---|---|---|
| Quarterly Trend (`qtrTrendChart`) | 4Q/8Q/All | All | CS | Slice of hardcoded `QUARTERLY_ACTUALS` payload client-side; projected quarter always shown. |
| Historical Quarterly P&L (`qtrTable`) | 4Q/8Q/All | All | CS | Same. |

### Timeline tab

`api/exchange_dashboard.py` already holds all ~1,150 rows in memory; it widens payloads, and everything here is CS/CX.

| Control | Chips | Default | Strategy | Notes |
|---|---|---|---|---|
| Daily Revenue Trend (`exdTrendChart`) | 30D/60D/Q/1Y/2Y/Max | 60D | CS | Widen `daily_trend` (`exchange_dashboard.py:374`) to the full series (~40 KB); client slices. |
| FY Summary grid (`exdFyGrid`) | — (no chips) | all FYs † (was 3) | CX | Remove client `maxRows: 3` (`index.html:5864–5868`); only ~5 FYs exist. |
| Quarterly grid (`exdQtrGrid`) | 4Q/8Q/All | 4Q † (was 3) | CX | Widen server `q_order[:6]` (`:189`) to all; chips drive client row count. |
| Monthly grid (`exdMonthGrid`) | 3M/6M/12M/All | 3M | CX | Widen server month slices (`:225–232`) to all. |

### Margins tab

| Control | Chips | Default | Strategy | Notes |
|---|---|---|---|---|
| Margin History (`marginHistoryChart`) | 30D/60D/Q/1Y/2Y/Max | 60D † (was all 2.5y — deliberate narrowing) | CS | Server already ships all dates (`margin_dashboard.py:136–142`); add date-window slice client-side. Composes with the existing commodity chips. |
| Recent Margin Changes (`marginChangesTable`) | 30D/60D/Q/1Y/Max | 60D † (was last-50-rows) | CS | Server returns **all** changes with dates (widen `[:50]`, `margin_dashboard.py:172` — changes are sparse, a few hundred rows total); client filters by date window; scroll container. |

### Momentum tab (existing shared chip row is **split per control**)

| Control | Chips | Default | Strategy | Notes |
|---|---|---|---|---|
| Revenue Regime (`momRegimeChart`) | 30D/60D/Q/1Y/2Y/Max | 60D | SP | `?range=` already implemented server-side (`models.py:243–252`); the shared `fetchRanged` cache means multiple controls at the same range cost one fetch. |
| Price & Signal (`momPriceChart`) | 30D/60D/Q/1Y/2Y/Max | 60D | SP | Same. |
| Signal History table (`momSignalTable`) | 30D/60D/Q/1Y/2Y/Max | 30D † (was fixed 15 rows — the existing inconsistency, now fixed) | SP | Rows = selected window; scroll container. |

### OI Participants tab (chips re-mapped from `1M/3M/6M/1Y/ALL`; data starts Sep 2024 so no `2Y`)

| Control | Chips | Default | Strategy | Notes |
|---|---|---|---|---|
| Participant Trend (`oipHeroChart`) | 30D/60D/Q/1Y/Max | Max | CS | Replace `oipFilterDates` month math with trading-day windows off the date array; instrument/commodity filters remain tab-level and compose. |
| Monthly Growth (`oipGrowthChart`) | 3M/6M/12M/All | All | CS | **Now honors a period** (existing inconsistency fixed) — filters `monthly_growth.months`. |
| Category Composition (`oipCompChart`) | 30D/60D/Q/1Y/Max | Max | CS | Same date-window filter; commodity `<select>` composes. |

## 5. Server changes summary

No new files under `api/` — the project is at Vercel Hobby's hard 12-function cap (`vercel.json`, `CLAUDE.md` "Critical Constraints"). All changes are query params or payload widening on existing handlers, copying the proven `api/models.py` pattern (param → `RANGE_DAYS` → sized fetch → tail slice → echo `range` + `history_returned`).

| File | Change |
|---|---|
| `api/valuation.py` | `?range=` param; window-sized fetch replaces `limit=90` + `[-60:]`. |
| `api/models.py` | `?range=` on the ECM (default-view) branch, reusing the existing `RANGE_DAYS`. |
| `api/history.py` | `?days=` param (values validated against `RANGE_DAYS` values, `0` = all); generalize the 45-trading-day walk; period-average line. |
| `api/analytics.py` | Return full computed series (drop three tail slices); add raw z-score series for the client-side correlation matrix. |
| `api/exchange_dashboard.py` | `daily_trend` → full series; quarterly/monthly aggregates → all periods. (`intraday_curve` view: no change, `days` param exists.) |
| `api/commodity_dashboard.py` | `?range=` (≤ 2Y): `start_date` extension to 26 months when needed; widen daily/quarterly/monthly slices. |
| `api/commodities.py` | `?range=` (≤ 2Y) sizes the signals-view cutoff and window slices. |
| `lib/margin_dashboard.py` | Margin-changes list widened from `[:50]` to all, each row carrying its date. |
| `lib/hourly_analysis.py`, `lib/intraday_curves.py` | No change — existing `days` params are simply surfaced in the UI. |

CDN caching: range params are part of the URL, so existing `Cache-Control` headers give correct per-range CDN entries automatically.

## 6. Explicitly excluded (no time dimension, or the period is intrinsic)

- Live-snapshot views: top futures/options charts + tables (C2/C3/T1/T2), position metrics (T4), factor decomposition (T7), HMM stats (T8), commodity lineup (T18), current margins (T22), OIP current distribution (T25), hedger-vs-speculator bar (C26 — a snapshot composition, no time axis).
- Windows that *define* the metric: Top Movers (1-day delta, T19), OIP Growth-by-Commodity (WoW/MoM/QoQ/YoY columns, T26), Commodity Summary (columns are the periods: 5d/45d/month/quarter/FY, T15), regime/weight-sensitivity analytics (T6/T9), Timeline weekly + day-of-week comparison panels.
- No data/time dimension: Forecast tab (calculator), hardcoded FY model-notes (T3) and methodology tables, quarter-to-date build-up chart (C19), dead Backtest tab (endpoint disabled).
- Rejected scope: URL hash deep-linking of period state.

## 7. Risks & mitigations

1. **Commodities fetch size at 2Y** (~17k of 40k rows): `supabase_read_all` pagination already handles it; aggregation happens server-side; CDN caches per URL (30-min TTL on `commodities`); per-range client cache prevents repeat hits. The `Max` chip was removed from these controls by design.
2. **Chart.js density at Max** (~1,150 daily points): fine for line charts; for the stacked commodity bars the cap is 2Y (~504 bars). Fallback lever if needed: client-side weekly thinning above 1Y.
3. **Widened payloads** (`analytics` ~60–80 KB, `exchange_dashboard` +~40 KB): trivial next to the 346 KB HTML; Vercel edge compression applies.
4. **Auto-refresh interplay**: `doRefresh` clears the range cache and re-renders; selections persist in per-control state + localStorage, so a refresh never resets a user's zoom.
5. **Momentum split** (one chip row → three): per-range shared fetch cache keeps API traffic identical to today for same-range use.

## 8. Verification plan

1. **Server, local:** call each modified generator function directly (`generate_*` in `api/`/`lib/`) for every supported range value; assert series length, first/last dates against the window, and that `Max`/default paths still match current production output where behavior is meant to be unchanged.
2. **Preview deploy:** `vercel deploy` (preview, not prod); Playwright sweep: for every control, click each chip and assert the rendered dataset length/label changes; reload the page and assert persisted selections are restored; confirm excluded views unchanged.
3. **Regression:** with all toggles at their defaults, every chart/table matches production visually (defaults chosen to preserve current behavior except the deliberate `†` changes in §4).
4. Production deploy only after the sweep passes.
