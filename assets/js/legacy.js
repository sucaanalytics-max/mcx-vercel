// ════════════════════════════════════════════════════════════════════════════
//  GLOBALS
// ════════════════════════════════════════════════════════════════════════════
let autoRefreshInterval = null;
let nextRefreshAt = null;
const AUTO_REFRESH_MS = 2 * 60 * 1000;  // 2 minutes

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
  const saved = MCX.storage.get('mcx.range.' + cfg.key);
  const initial = (saved && cfg.ranges.indexOf(saved) !== -1) ? saved : cfg.defaultRange;
  rangeState[cfg.key] = initial;
  const el = document.getElementById(cfg.containerId);
  if (!el) return initial;
  el.classList.add('range-chips');
  el.setAttribute('role', 'group');
  if (!el.hasAttribute('aria-label')) el.setAttribute('aria-label', 'Time range');
  const LONG = { '30D': '30 days', '60D': '60 days', 'Q': 'quarter', '1Y': '1 year', '2Y': '2 years', 'Max': 'all history', '4Q': '4 quarters', '8Q': '8 quarters', '3M': '3 months', '6M': '6 months', '12M': '12 months', '24M': '24 months' };
  el.innerHTML = cfg.ranges.map(r =>
    '<button type="button" class="margin-chip' + (r === initial ? ' active' : '') + '" data-range="' + r + '" aria-pressed="' + (r === initial) + '"'
    + (LONG[r] ? ' title="' + LONG[r] + '"' : '') + '>' + r + '</button>'
  ).join('');
  el.querySelectorAll('.margin-chip').forEach(chip => {
    chip.onclick = () => {
      const r = chip.getAttribute('data-range');
      if (rangeState[cfg.key] === r) return;
      rangeState[cfg.key] = r;
      MCX.storage.set('mcx.range.' + cfg.key, r);
      el.querySelectorAll('.margin-chip').forEach(c => {
        const on = c.getAttribute('data-range') === r;
        c.classList.toggle('active', on);
        c.setAttribute('aria-pressed', String(on));
      });
      updateRangeLabels(cfg, r);
      try {
        const ret = cfg.onChange(r);
        if (ret && typeof ret.catch === 'function') {
          ret.catch(e => console.warn('[range] ' + cfg.key + ' update failed:', e));
        }
      } catch (e) {
        console.warn('[range] ' + cfg.key + ' update failed:', e);
      }
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

function fmtInt(n) { return parseFloat(n).toLocaleString('en-IN', {maximumFractionDigits:0}); }
function fmtDec(n,d=2) { return parseFloat(n).toFixed(d); }

// ════════════════════════════════════════════════════════════════════════════
//  THEME
// ════════════════════════════════════════════════════════════════════════════
// MCX.theme (core.js) sets html.dark; the router redraws each page's charts after a change.
function toggleTheme() { MCX.theme.cycle(); }

// The session profile charts read the theme when drawn, so draw them again.
// (Today's other charts are SVG coloured by CSS variables.)
function rethemeToday() {
  if (curveDataCache) {
    renderDynamicBucketChart(curveDataCache);
    const cumEl = document.getElementById('curveViewCumulative');
    if (cumEl && cumEl.style.display !== 'none') renderCumulativeChart(curveDataCache);
  } else {
    renderIntradayChart();
  }
}

// ════════════════════════════════════════════════════════════════════════════
//  ACCORDION
// ════════════════════════════════════════════════════════════════════════════
function toggleAcc(id) {
  const panel = document.getElementById(id + '-panel');
  const trigger = panel.previousElementSibling;
  const open = panel.classList.toggle('open');
  trigger.setAttribute('aria-expanded', open);
}

// ════════════════════════════════════════════════════════════════════════════
//  TOAST
// ════════════════════════════════════════════════════════════════════════════
function showToast(msg, type='') {
  const t = document.createElement('div');
  t.className = 'toast ' + type;
  t.textContent = msg;
  document.body.appendChild(t);
  setTimeout(() => t.remove(), 3000);
}

// ════════════════════════════════════════════════════════════════════════════
//  COOKIE MODAL
// ════════════════════════════════════════════════════════════════════════════
function openCookieModal() {
  document.getElementById('cookieTextarea').value = MCX.storage.get('mcxCookie') || '';
  document.getElementById('cookieModal').classList.remove('hidden');
}
function closeCookieModal() { document.getElementById('cookieModal').classList.add('hidden'); }
function saveCookie() {
  const val = document.getElementById('cookieTextarea').value.trim();
  if (val) { MCX.storage.set('mcxCookie', val); closeCookieModal(); showToast('Cookie saved', 'success'); }
}
document.addEventListener('keydown', e => { if (e.key === 'Escape') closeCookieModal(); });

// ════════════════════════════════════════════════════════════════════════════
//  INTRADAY VOLUME CURVE (DYNAMIC)
// ════════════════════════════════════════════════════════════════════════════
let intradayChartInst = null, cumulativeChartInst = null;
let curveDataCache = null, curveLoading = false;
const INTRADAY_BUCKETS = [
  { label: '09:00–10:30', tag: 'Opening + metals',  weight: 0.06 },
  { label: '10:30–12:30', tag: 'Mid-morning',       weight: 0.10 },
  { label: '12:30–15:00', tag: 'Post-lunch lull',   weight: 0.07 },
  { label: '15:00–17:00', tag: 'Pre-evening',       weight: 0.10 },
  { label: '17:00–19:30', tag: 'Europe open',       weight: 0.18 },
  { label: '19:30–22:00', tag: '★ NYMEX open',      weight: 0.34 },
  { label: '22:00–23:30', tag: 'Late session',      weight: 0.15 },
];

function switchCurveView(view) {
  document.getElementById('curveViewBucket').style.display = view === 'bucket' ? 'block' : 'none';
  document.getElementById('curveViewCumulative').style.display = view === 'cumulative' ? 'block' : 'none';
  const btnB = document.getElementById('btnCurveBucket'), btnC = document.getElementById('btnCurveCum');
  if (view === 'bucket') { btnB.style.background = 'var(--accent)'; btnB.style.color = 'white'; btnC.style.background = 'transparent'; btnC.style.color = 'var(--text-secondary)'; }
  else { btnC.style.background = 'var(--accent)'; btnC.style.color = 'white'; btnB.style.background = 'transparent'; btnB.style.color = 'var(--text-secondary)'; }
  if (view === 'cumulative' && curveDataCache) renderCumulativeChart(curveDataCache);
}

async function loadIntradayCurve() {
  if (curveLoading) return;
  curveLoading = true;
  try {
    const data = await fetchRanged('/api/exchange_dashboard?view=intraday_curve&days=' + RANGE_TRADING_DAYS[rangeState['intraday'] || '30D']);
    if (!data.success) throw new Error(data.error);
    curveDataCache = data;
    renderDynamicBucketChart(data);
    renderCurveStats(data);
    // One fetch feeds both sub-views: refresh cumulative chart too if it's the visible one.
    const cumEl = document.getElementById('curveViewCumulative');
    if (cumEl && cumEl.style.display !== 'none') renderCumulativeChart(data);
    document.getElementById('intradayBadge').textContent = `${data.rolling_average.days_used}d dynamic`;
  } catch (e) {
    console.warn('Dynamic curve failed, using static:', e);
    renderIntradayChart();
    document.getElementById('intradayBadge').textContent = '7 buckets (static)';
  } finally { curveLoading = false; }
}

function renderDynamicBucketChart(data) {
  const isDark = document.documentElement.classList.contains('dark');
  const gridColor = isDark ? '#333' : '#E8E6E1';
  const tickColor = isDark ? '#888' : '#6B6560';
  const labels = data.static_model.buckets.map(b => b.label);
  const staticW = data.static_model.buckets.map(b => b.weight * 100);
  const avgW = data.rolling_average.buckets.map(b => b.weight * 100);
  const datasets = [
    { label: 'Static Model', data: staticW, backgroundColor: isDark ? '#555' : '#ccc', borderRadius: 2 },
    { label: `${data.rolling_average.days_used}d Average`, data: avgW, backgroundColor: isDark ? '#5B9CF5' : '#0958D9', borderRadius: 2 },
  ];
  if (data.today && data.today.partial_buckets) {
    const todayW = data.today.partial_buckets.map(b => b.weight !== null ? b.weight * 100 : null);
    if (todayW.some(v => v !== null)) {
      datasets.push({ label: 'Today', data: todayW, backgroundColor: isDark ? '#FF6B47' : '#D4380D', borderRadius: 2 });
    }
  }
  if (intradayChartInst) intradayChartInst.destroy();
  intradayChartInst = new Chart(document.getElementById('intradayChart'), {
    type: 'bar', data: { labels, datasets },
    options: {
      indexAxis: 'y', responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: true, position: 'bottom', labels: { font: { size: 10, family: "'JetBrains Mono'" } } },
        tooltip: { callbacks: { label: ctx => `${ctx.dataset.label}: ${ctx.raw !== null ? ctx.raw.toFixed(1) + '%' : 'n/a'}` } } },
      scales: {
        y: { ticks: { color: tickColor, font: { family: "'JetBrains Mono'", size: 10 } }, grid: { display: false } },
        x: { max: 40, ticks: { color: tickColor, font: { family: "'JetBrains Mono'", size: 9 }, callback: v => v + '%' }, grid: { color: gridColor } }
      }
    }
  });
  const legend = document.getElementById('intradayLegend');
  if (legend) {
    legend.innerHTML = `
      <span><span class="dot" style="background:${isDark ? '#555' : '#ccc'}"></span> Static Model</span>
      <span><span class="dot" style="background:${isDark ? '#5B9CF5' : '#0958D9'}"></span> 30d Average</span>
      <span><span class="dot" style="background:${isDark ? '#FF6B47' : '#D4380D'}"></span> Today</span>
      <span>Evening: ${data.rolling_average.evening_pct}%</span>
    `;
  }
}

function renderCumulativeChart(data) {
  const isDark = document.documentElement.classList.contains('dark');
  const gridColor = isDark ? '#333' : '#E8E6E1';
  const tickColor = isDark ? '#888' : '#6B6560';
  // Build cumulative points from static and average bucket weights
  const edges = [0, 90, 210, 360, 480, 630, 780, 870]; // elapsed_min at bucket boundaries
  const staticCum = [], avgCum = [];
  let sc = 0, ac = 0;
  staticCum.push({x: 0, y: 0}); avgCum.push({x: 0, y: 0});
  for (let i = 0; i < 7; i++) {
    sc += data.static_model.buckets[i].weight * 100;
    ac += data.rolling_average.buckets[i].weight * 100;
    staticCum.push({x: edges[i + 1], y: sc});
    avgCum.push({x: edges[i + 1], y: ac});
  }
  // P10/P90 band
  const p10Cum = [], p90Cum = [];
  let p10c = 0, p90c = 0;
  p10Cum.push({x: 0, y: 0}); p90Cum.push({x: 0, y: 0});
  if (data.percentiles) {
    for (let i = 0; i < 7; i++) {
      p10c += data.percentiles.p10[i] * 100;
      p90c += data.percentiles.p90[i] * 100;
      p10Cum.push({x: edges[i + 1], y: p10c});
      p90Cum.push({x: edges[i + 1], y: p90c});
    }
  }
  const datasets = [
    { label: 'p10–p90 range', data: p90Cum, borderColor: 'transparent', backgroundColor: isDark ? 'rgba(91,156,245,0.12)' : 'rgba(9,88,217,0.08)', fill: '+1', pointRadius: 0, tension: 0.3 },
    { label: '', data: p10Cum, borderColor: 'transparent', backgroundColor: 'transparent', fill: false, pointRadius: 0, tension: 0.3 },
    { label: 'Static Model', data: staticCum, borderColor: isDark ? '#666' : '#aaa', borderDash: [4,3], borderWidth: 1.5, fill: false, pointRadius: 0, tension: 0.3 },
    { label: `${data.rolling_average.days_used}d Average`, data: avgCum, borderColor: isDark ? '#5B9CF5' : '#0958D9', borderWidth: 2, fill: false, pointRadius: 2, tension: 0.3 },
  ];
  // Today's cumulative
  if (data.today && data.today.cumulative_curve) {
    const todayPts = data.today.cumulative_curve.map(p => ({x: p.elapsed_min, y: (p.volume_cr / data.today.total_volume_cr * (data.today.elapsed_min / 870)) * 100}));
    // Actually compute today's cumulative % properly
    const todayTotal = data.today.cumulative_curve[data.today.cumulative_curve.length - 1]?.volume_cr || 1;
    const todayCum = data.today.cumulative_curve.map(p => ({x: p.elapsed_min, y: p.volume_cr / todayTotal * 100}));
    datasets.push({ label: 'Today', data: todayCum, borderColor: isDark ? '#FF6B47' : '#D4380D', borderWidth: 2.5, fill: false, pointRadius: 0, tension: 0.3 });
  }
  if (cumulativeChartInst) cumulativeChartInst.destroy();
  cumulativeChartInst = new Chart(document.getElementById('cumulativeChart'), {
    type: 'line', data: { datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: true, position: 'bottom', labels: { font: { size: 10, family: "'JetBrains Mono'" }, filter: item => item.text !== '' } } },
      scales: {
        x: { type: 'linear', min: 0, max: 870, ticks: { color: tickColor, font: { family: "'JetBrains Mono'", size: 9 },
          callback: v => { const h = Math.floor((v + 540) / 60), m = (v + 540) % 60; return `${h}:${m.toString().padStart(2,'0')}`; } },
          grid: { color: gridColor } },
        y: { min: 0, max: 105, ticks: { color: tickColor, font: { family: "'JetBrains Mono'", size: 9 }, callback: v => v + '%' }, grid: { color: gridColor } }
      }
    }
  });
}

function renderCurveStats(data) {
  const el = document.getElementById('curveStats');
  if (!el) return;
  const divg = data.divergences || [];
  const maxDiv = divg.reduce((a, b) => Math.abs(b.diff_pct) > Math.abs(a.diff_pct) ? b : a, {diff_pct: 0, label: '-'});
  el.innerHTML = `
    <div style="padding:6px;border:1px solid var(--border);border-radius:var(--radius)">
      <div style="font-size:13px;font-weight:600">${data.rolling_average.evening_pct}%</div>
      <div style="color:var(--text-secondary)">Evening Session (actual avg)</div>
    </div>
    <div style="padding:6px;border:1px solid var(--border);border-radius:var(--radius)">
      <div style="font-size:13px;font-weight:600">${data.rolling_average.days_used}</div>
      <div style="color:var(--text-secondary)">Trading days analyzed</div>
    </div>
    <div style="padding:6px;border:1px solid var(--border);border-radius:var(--radius)">
      <div style="font-size:13px;font-weight:600">${maxDiv.label}</div>
      <div style="color:var(--text-secondary)">Largest drift: ${maxDiv.diff_pct > 0 ? '+' : ''}${maxDiv.diff_pct}%</div>
    </div>
    ${data.today ? `<div style="padding:6px;border:1px solid var(--border);border-radius:var(--radius)">
      <div style="font-size:13px;font-weight:600">${data.today.total_volume_cr.toLocaleString()} Cr</div>
      <div style="color:var(--text-secondary)">Today @ ${Math.floor((data.today.elapsed_min + 540) / 60)}:${((data.today.elapsed_min + 540) % 60).toString().padStart(2,'0')}</div>
    </div>` : ''}
  `;
}

function renderIntradayChart() {
  // Static fallback
  const isDark = document.documentElement.classList.contains('dark');
  const gridColor = isDark ? '#333' : '#E8E6E1';
  const tickColor = isDark ? '#888' : '#6B6560';
  const labels = INTRADAY_BUCKETS.map(b => b.label);
  const values = INTRADAY_BUCKETS.map(b => b.weight * 100);
  const colors = INTRADAY_BUCKETS.map(b => {
    if (b.weight >= 0.30) return isDark ? '#FF6B47' : '#D4380D';
    if (b.weight >= 0.15) return isDark ? '#FAAD14' : '#D48806';
    return isDark ? '#5B9CF5' : '#0958D9';
  });
  if (intradayChartInst) intradayChartInst.destroy();
  intradayChartInst = new Chart(document.getElementById('intradayChart'), {
    type: 'bar', data: { labels, datasets: [{ data: values, backgroundColor: colors, borderWidth: 0, borderRadius: 2 }] },
    options: {
      indexAxis: 'y', responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false }, tooltip: { callbacks: { label: ctx => `${INTRADAY_BUCKETS[ctx.dataIndex].weight * 100}% — ${INTRADAY_BUCKETS[ctx.dataIndex].tag}` } } },
      scales: {
        y: { ticks: { color: tickColor, font: { family: "'JetBrains Mono'", size: 10 } }, grid: { display: false } },
        x: { max: 40, ticks: { color: tickColor, font: { family: "'JetBrains Mono'", size: 9 }, callback: v => v + '%' }, grid: { color: gridColor } }
      }
    }
  });
  const legend = document.getElementById('intradayLegend');
  if (legend) {
    legend.innerHTML = `
      <span><span class="dot" style="background:${isDark ? '#FF6B47' : '#D4380D'}"></span> ≥30% Prime</span>
      <span><span class="dot" style="background:${isDark ? '#FAAD14' : '#D48806'}"></span> 15–29% High</span>
      <span><span class="dot" style="background:${isDark ? '#5B9CF5' : '#0958D9'}"></span> <15% Regular</span>
      <span>Evening (17:00–23:30) = 67%</span>
    `;
  }
}

// ════════════════════════════════════════════════════════════════════════════
//  UPDATE UI FROM /api/refresh
// ════════════════════════════════════════════════════════════════════════════
function updateSnapshotFromAPI(d) {
  // The Today page renders from MCX.store 'refresh' (today.js). This keeps the
  // header, the session profile and the Scenarios seed in step.
  const totalRev = d.proj_rev_cr;          // projected revenue for the day (final after the close), ₹ Cr
  MCX.store.set('liveRevenue', { value: totalRev, live: !d.session_closed });
  // Top-right meta: show the SNAPSHOT'S OWN timestamp (the timeframe of the data
  // on screen), converted to IST regardless of viewer timezone — not the client
  // clock. This tells viewers how fresh the data is (and surfaces staleness).
  {
    const _meta = document.getElementById('refreshMeta');
    const _t = MCX.parseTs(d.timestamp);   // ISO on GET, "HH:MM IST, DD Mon YYYY" after a manual refresh
    if (_t) {
      const _tz = { timeZone: 'Asia/Kolkata' };
      const _hhmm = _t.toLocaleTimeString('en-GB', { ..._tz, hour: '2-digit', minute: '2-digit' });
      const _day = _t.toLocaleDateString('en-GB', { ..._tz, day: '2-digit', month: 'short' });
      const _today = new Date().toLocaleDateString('en-GB', { ..._tz, day: '2-digit', month: 'short' });
      _meta.textContent = 'as of ' + _hhmm + ' IST' + (_day === _today ? '' : ' · ' + _day);
      _meta.title = 'Data as of the last snapshot — ' + _t.toLocaleString('en-GB', _tz);
    } else {
      _meta.textContent = '—';
    }
  }
  // Session profile: redraw with the new snapshot
  if (curveDataCache) { curveDataCache = null; loadIntradayCurve(); } else { renderIntradayChart(); }
  seedForecastFromAPI(totalRev);
}

// ════════════════════════════════════════════════════════════════════════════
//  REFRESH
// ════════════════════════════════════════════════════════════════════════════
function isInsideTradingHours() {
  const now = new Date();
  const istMin = (now.getUTCHours() * 60 + now.getUTCMinutes()) + 330;
  return istMin >= 535 && istMin <= 1415;
}

function startAutoRefresh() {
  if (autoRefreshInterval) return;
  nextRefreshAt = Date.now() + AUTO_REFRESH_MS;
  autoRefreshInterval = setInterval(() => {
    if (isInsideTradingHours()) doRefresh(true);
    nextRefreshAt = Date.now() + AUTO_REFRESH_MS;
  }, AUTO_REFRESH_MS);
}

async function doRefresh(silent) {
  const btn = document.getElementById('refreshBtn');
  btn.classList.add('loading');
  btn.disabled = true;

  try {
    let data;
    if (!silent) {
      // Manual click: try live MCX fetch first, fall back to cached
      try {
        const liveResp = await fetch('/api/refresh', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: '{}'
        });
        data = await liveResp.json();
        if (!data.success) throw new Error(data.error);
      } catch(e) {
        const cacheResp = await fetch('/api/refresh');
        data = await cacheResp.json();
      }
    } else {
      const resp = await fetch('/api/refresh');
      data = await resp.json();
    }

    MCX.store.set('refresh', data);
    if (data.success) {
      clearRangedCache();
      updateSnapshotFromAPI(data);
      if (!silent) showToast('Data refreshed', 'success');
      if (!autoRefreshInterval && isInsideTradingHours()) startAutoRefresh();
    } else {
      if (!silent) showToast('Refresh failed: ' + (data.error || ''), 'error');
    }
  } catch(e) {
    MCX.store.set('refresh', { success: false, error: 'Network error' });
    if (!silent) showToast('Network error', 'error');
  } finally {
    btn.classList.remove('loading');
    btn.disabled = false;
  }
}

// ════════════════════════════════════════════════════════════════════════════
//  INIT
// ════════════════════════════════════════════════════════════════════════════
// ── Range toggle registrations ──
makeRangeToggle({
  key: 'intraday', containerId: 'intradayRange',
  ranges: ['30D','60D','Q'], defaultRange: '30D',
  labelIds: ['intradayRangeLabel'],
  onChange: () => loadIntradayCurve()
});

// Auto-trigger first refresh
setTimeout(() => doRefresh(true), 800);

// Page switching and each page's loaders live in shell.js (MCX.router).

// ════════════════════════════════════════════════════════════════════════════
//  FORECAST MODEL — Revenue → EPS → Share Price
//  Uses same fee schedule as Daily Predictor (mcx_config.py)
// ════════════════════════════════════════════════════════════════════════════
// Shares, trading days and the current price come from the backend (value.js fills them from
// /api/valuation and the live price); the rest are editable FY26-based defaults.
const FC = {
  dailyRev: 12.62, pe: 42, opex: 700, otherIncome: 126,
  taxRate: 20.30, shares: null, tradingDays: null, currentPrice: null
};

function getFcInputs() {
  return {
    dailyRev: parseFloat(document.getElementById('fcRevInput').value) || FC.dailyRev,
    pe: parseFloat(document.getElementById('fcPeInput').value) || FC.pe,
    opex: parseFloat(document.getElementById('fcAdvOpex').value) || FC.opex,
    otherIncome: parseFloat(document.getElementById('fcAdvOther').value) || FC.otherIncome,
    taxRate: parseFloat(document.getElementById('fcAdvTax').value) || FC.taxRate,
    shares: parseFloat(document.getElementById('fcAdvShares').value) || FC.shares,
    tradingDays: parseFloat(document.getElementById('fcAdvDays').value) || FC.tradingDays,
    currentPrice: parseFloat(document.getElementById('fcAdvCMP').value) || FC.currentPrice,
  };
}

function calcModel(dailyRev, pe, inp) {
  const annualRev = dailyRev * inp.tradingDays;
  const ebitda = annualRev - inp.opex;
  const pbt = ebitda + inp.otherIncome;
  const tax = pbt > 0 ? pbt * (inp.taxRate / 100) : 0;
  const pat = pbt > 0 ? pbt - tax : 0;
  const eps = inp.shares > 0 ? pat / inp.shares : 0;
  const price = eps * pe;
  const mcap = price * inp.shares;
  return { annualRev, ebitda, pbt, tax, pat, eps, price, mcap };
}

function fmtInr(n) { return Math.round(n).toLocaleString('en-IN'); }

// ── PAT PREDICTOR ──────────────────────────────────────────────────────
function recomputePatPredictor() {
  const sharesEl = document.getElementById('fcAdvShares');
  const cmpEl = document.getElementById('fcAdvCMP');
  const shares = parseFloat(sharesEl && sharesEl.value) || FC.shares || 0;
  const cmp = parseFloat(cmpEl && cmpEl.value) || FC.currentPrice || 0;
  const fmtCr = n => Number.isFinite(n) ? Math.round(n).toLocaleString('en-IN') : '—';
  const fmtPctSign = n => {
    if (!Number.isFinite(n)) return '—';
    const sign = n > 0 ? '+' : '';
    return sign + n.toFixed(1) + '%';
  };
  const setText = (sel, val) => {
    const el = document.querySelector(sel);
    if (el) el.textContent = val;
  };
  const setUpside = (sel, val, n) => {
    const el = document.querySelector(sel);
    if (!el) return;
    el.textContent = val;
    el.classList.remove('chg-pos','chg-neg','chg-neutral');
    el.classList.add(n > 0 ? 'chg-pos' : n < 0 ? 'chg-neg' : 'chg-neutral');
  };

  // Trend table (FY26/27/28)
  let fy27Days = 256;
  ['26','27','28'].forEach(yr => {
    const get = (k) => parseFloat((document.querySelector('[data-pt-input="'+k+'"][data-pt-yr="'+yr+'"]') || {}).value) || 0;
    const adr = get('adr'), days = get('days'), other = get('other'), margin = get('margin'), pe = get('pe');
    const opRev = adr * days;
    const totRev = opRev + other;
    const pat = totRev * margin / 100;
    const eps = shares > 0 ? pat / shares : 0;
    const px = eps * pe;
    setText('[data-pt-out="opRev"][data-pt-yr="'+yr+'"]', fmtCr(opRev));
    setText('[data-pt-out="totRev"][data-pt-yr="'+yr+'"]', fmtCr(totRev));
    setText('[data-pt-out="pat"][data-pt-yr="'+yr+'"]', fmtCr(pat));
    setText('[data-pt-out="eps"][data-pt-yr="'+yr+'"]', MCX.fmt.eps(eps));
    setText('[data-pt-out="px"][data-pt-yr="'+yr+'"]', '₹' + fmtCr(px));
    if (yr === '27') fy27Days = days || 256;
  });

  // Scenario table (FY27 Bear/Base/Bull)
  const discount = parseFloat((document.getElementById('patScenDiscount') || {}).value) || 0;
  ['bear','base','bull'].forEach(sc => {
    const get = (k) => parseFloat((document.querySelector('[data-ps-input="'+k+'"][data-ps-sc="'+sc+'"]') || {}).value) || 0;
    const growth = get('growth'), adr = get('adr'), other = get('other'), margin = get('margin'), pe = get('pe');
    const adrEff = adr * (1 + growth / 100);
    const totInc = adrEff * fy27Days + other;
    const pat = totInc * margin / 100;
    const eps = shares > 0 ? pat / shares : 0;
    const px = eps * pe;
    const upside = cmp > 0 ? (px - cmp) / cmp * 100 : 0;
    const tgtPx = (1 + discount / 100) > 0 ? px / (1 + discount / 100) : px;
    setText('[data-ps-out="totInc"][data-ps-sc="'+sc+'"]', fmtCr(totInc));
    setText('[data-ps-out="eps"][data-ps-sc="'+sc+'"]', MCX.fmt.eps(eps));
    setText('[data-ps-out="px"][data-ps-sc="'+sc+'"]', '₹' + fmtCr(px));
    setUpside('[data-ps-out="upside"][data-ps-sc="'+sc+'"]', fmtPctSign(upside), upside);
    setText('[data-ps-out="tgtPx"][data-ps-sc="'+sc+'"]', '₹' + fmtCr(tgtPx));
  });
}

function recalcForecast() {
  const inp = getFcInputs();
  if (!inp.shares || !inp.tradingDays || !inp.currentPrice) return;   // waiting for the backend figures (value.js)
  const base = calcModel(inp.dailyRev, inp.pe, inp);

  // Annual label under slider
  document.getElementById('fcRevAnnual').textContent = `Annual: ₹${fmtInr(base.annualRev)} Cr`;

  // Model hero card
  document.getElementById('fcModelPrice').textContent = '₹' + fmtInr(base.price);
  document.getElementById('fcModelSub').textContent =
    `EPS: ₹${base.eps.toFixed(2)} · PE: ${inp.pe.toFixed(1)}x · MCap: ₹${fmtInr(base.mcap)} Cr`;
  const upside = ((base.price / inp.currentPrice - 1) * 100);
  const ud = document.getElementById('fcModelDelta');
  ud.textContent = `${upside >= 0 ? '+' : ''}${upside.toFixed(1)}% vs CMP ₹${fmtInr(inp.currentPrice)}`;
  ud.style.color = upside >= 0 ? 'var(--positive)' : 'var(--negative)';

  // CMP display
  document.getElementById('fcCmpPrice').textContent = '₹' + fmtInr(inp.currentPrice);

  // P&L waterfall
  document.getElementById('fcWfRevenue').textContent = '₹' + fmtInr(base.annualRev) + ' Cr';
  document.getElementById('fcWfOpex').textContent = '(₹' + fmtInr(inp.opex) + ' Cr)';
  document.getElementById('fcWfEbitda').textContent = '₹' + fmtInr(base.ebitda) + ' Cr';
  document.getElementById('fcWfOther').textContent = '₹' + fmtInr(inp.otherIncome) + ' Cr';
  document.getElementById('fcWfPbt').textContent = '₹' + fmtInr(base.pbt) + ' Cr';
  document.getElementById('fcWfTax').textContent = '(₹' + fmtInr(base.tax) + ' Cr)';
  document.getElementById('fcWfTaxLabel').textContent = inp.taxRate.toFixed(2);
  document.getElementById('fcWfPat').textContent = '₹' + fmtInr(base.pat) + ' Cr';
  document.getElementById('fcWfEps').textContent = '₹' + base.eps.toFixed(2);
  document.getElementById('fcWfSharesLabel').textContent = inp.shares.toFixed(3);
  document.getElementById('fcWfDaysLabel').textContent = Math.round(inp.tradingDays);
  document.getElementById('fcWfPrice').textContent = '₹' + fmtInr(base.price);

  // Scenarios
  const bearRevPct = parseFloat(document.getElementById('scBearRevPct').value) || -15;
  const bearPePct  = parseFloat(document.getElementById('scBearPePct').value) || -10;
  const bullRevPct = parseFloat(document.getElementById('scBullRevPct').value) || 15;
  const bullPePct  = parseFloat(document.getElementById('scBullPePct').value) || 10;

  const bear = calcModel(inp.dailyRev * (1 + bearRevPct/100), inp.pe * (1 + bearPePct/100), inp);
  const bull = calcModel(inp.dailyRev * (1 + bullRevPct/100), inp.pe * (1 + bullPePct/100), inp);

  renderScenario('Bear', bear, inp.dailyRev * (1 + bearRevPct/100), inp.pe * (1 + bearPePct/100), inp.currentPrice);
  renderScenario('Base', base, inp.dailyRev, inp.pe, inp.currentPrice);
  renderScenario('Bull', bull, inp.dailyRev * (1 + bullRevPct/100), inp.pe * (1 + bullPePct/100), inp.currentPrice);

  // Implied metrics — reverse-engineer from CMP
  calcImpliedMetrics(inp, base);

  // PAT Predictor (picks up live CMP + shares from Advanced Assumptions)
  recomputePatPredictor();
}

function renderScenario(name, m, dailyRev, pe, cmp) {
  const id = 'sc' + name;
  document.getElementById(id + 'Price').textContent = '₹' + fmtInr(m.price);
  const delta = ((m.price / cmp - 1) * 100);
  document.getElementById(id + 'Delta').textContent = `${delta >= 0 ? '+' : ''}${delta.toFixed(1)}% vs CMP`;
  document.getElementById(id + 'Rev').textContent = '₹' + dailyRev.toFixed(2) + ' Cr/d';
  document.getElementById(id + 'Annual').textContent = '₹' + fmtInr(m.annualRev) + ' Cr';
  document.getElementById(id + 'Pe').textContent = pe.toFixed(1) + 'x';
  document.getElementById(id + 'Eps').textContent = '₹' + m.eps.toFixed(2);
  document.getElementById(id + 'Pat').textContent = '₹' + fmtInr(m.pat) + ' Cr';
}

// ── Implied Metrics: reverse-engineer from CMP ──────────────────────────
function calcImpliedMetrics(inp, base) {
  const cmp = inp.currentPrice;
  const pe = inp.pe;
  const shares = inp.shares;
  const taxRate = inp.taxRate / 100;
  const opex = inp.opex;
  const otherIncome = inp.otherIncome;
  const tradingDays = inp.tradingDays;

  // Path A: At your PE, what revenue is implied?
  const imEps = pe > 0 ? cmp / pe : 0;
  const imPat = imEps * shares;
  const imPbt = (1 - taxRate) > 0 ? imPat / (1 - taxRate) : 0;
  const imEbitda = imPbt - otherIncome;
  const imAnnualRev = imEbitda + opex;
  const imDailyRev = tradingDays > 0 ? imAnnualRev / tradingDays : 0;
  const dailyRevDelta = inp.dailyRev > 0 ? ((imDailyRev / inp.dailyRev - 1) * 100) : 0;

  // Path B: At your revenue, what PE is implied?
  const modelEps = base.eps;
  const imPe = modelEps > 0 ? cmp / modelEps : 0;
  const peDelta = pe > 0 ? ((imPe / pe - 1) * 100) : 0;

  // Market snapshot
  const mcap = cmp * shares;
  const modelPrice = base.price;
  const premDisc = cmp > 0 ? ((modelPrice / cmp - 1) * 100) : 0;

  // Render
  document.getElementById('imEps').textContent = '₹' + imEps.toFixed(2);
  document.getElementById('imPat').textContent = '₹' + fmtInr(imPat) + ' Cr';
  document.getElementById('imPbt').textContent = '₹' + fmtInr(imPbt) + ' Cr';
  document.getElementById('imEbitda').textContent = '₹' + fmtInr(imEbitda) + ' Cr';
  document.getElementById('imAnnualRev').textContent = '₹' + fmtInr(imAnnualRev) + ' Cr';
  document.getElementById('imDailyRev').textContent = '₹' + imDailyRev.toFixed(2) + ' Cr';

  const drdEl = document.getElementById('imDailyRevDelta');
  drdEl.textContent = `${dailyRevDelta >= 0 ? '+' : ''}${dailyRevDelta.toFixed(1)}%`;
  drdEl.style.color = dailyRevDelta >= 0 ? 'var(--positive)' : 'var(--negative)';

  document.getElementById('imPe').textContent = imPe.toFixed(1) + 'x';
  const pedEl = document.getElementById('imPeDelta');
  pedEl.textContent = `${peDelta >= 0 ? '+' : ''}${peDelta.toFixed(1)}%`;
  pedEl.style.color = Math.abs(peDelta) < 5 ? 'var(--text-secondary)' : (peDelta >= 0 ? 'var(--negative)' : 'var(--positive)');

  document.getElementById('imMcap').textContent = '₹' + fmtInr(mcap) + ' Cr';
  document.getElementById('imPriceCmp').textContent = '₹' + fmtInr(modelPrice) + ' / ₹' + fmtInr(cmp);
  const pdEl = document.getElementById('imPremDisc');
  pdEl.textContent = `${premDisc >= 0 ? '+' : ''}${premDisc.toFixed(1)}% ${premDisc >= 0 ? 'premium' : 'discount'}`;
  pdEl.style.color = premDisc >= 0 ? 'var(--positive)' : 'var(--negative)';
}

// Slider <-> Input sync
function syncSlider(sliderId, inputId) {
  const slider = document.getElementById(sliderId);
  const input = document.getElementById(inputId);
  slider.addEventListener('input', () => { input.value = slider.value; recalcForecast(); });
  input.addEventListener('input', () => {
    if (parseFloat(input.value) >= parseFloat(slider.min) && parseFloat(input.value) <= parseFloat(slider.max))
      slider.value = input.value;
    recalcForecast();
  });
}
syncSlider('fcRevSlider', 'fcRevInput');
syncSlider('fcPeSlider', 'fcPeInput');

// Advanced inputs + scenario adjustments trigger recalc
['fcAdvOpex','fcAdvOther','fcAdvTax','fcAdvShares','fcAdvDays','fcAdvCMP',
 'scBearRevPct','scBearPePct','scBullRevPct','scBullPePct'].forEach(id => {
  document.getElementById(id).addEventListener('input', recalcForecast);
});

// PAT Predictor inputs trigger live recompute
document.querySelectorAll('[data-pt-input], [data-ps-input], #patScenDiscount')
  .forEach(el => el.addEventListener('input', recomputePatPredictor));
recomputePatPredictor();

// Seed forecast default daily rev from API when available
function seedForecastFromAPI(totalRev) {
  if (totalRev > 0) {
    document.getElementById('fcRevSlider').value = totalRev.toFixed(2);
    document.getElementById('fcRevInput').value = totalRev.toFixed(2);
    recalcForecast();
    const page = document.getElementById('scenarios');           // value.js redraws the lede on input
    if (page) page.dispatchEvent(new Event('input'));
  }
}

// ── Auto-fetch CMP from /api/mcxprice (indianapi.in → Yahoo fallback) ────
let _lastCMPPrice = null;
let _lastCMPTimer = null;

// Trailing-twelve-month EPS: the last four reported quarters' PAT over diluted shares,
// published by value.js from /api/quarterly (MCX.store 'ttmEps').
const ttmEps = () => (MCX.store.get('ttmEps') || {}).eps || null;

// "EPS (TTM) · PE · MCap" under the current price, from the live price and backend figures
function renderCmpMeta() {
  const metaEl = document.getElementById('fcCmpMeta');
  const price = FC.currentPrice, eps = ttmEps(), shares = FC.shares;
  if (!metaEl || !price) return;
  metaEl.textContent = `EPS (TTM): ${eps ? '₹' + eps.toFixed(2) : '—'} · PE: ${eps ? (price / eps).toFixed(1) + 'x' : '—'} · `
    + `MCap: ${shares ? '₹' + Math.round(price * shares).toLocaleString('en-IN') + ' Cr' : '—'}`;
}

async function fetchLiveCMP() {
  try {
    const resp = await fetch('/api/mcxprice');
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    if (data.error) { console.warn('CMP fetch:', data.error); return; }
    MCX.store.set('price', data);          // Fair value (value.js) reads the live price from here

    const price = Math.round(data.price);
    const priceChanged = (_lastCMPPrice !== null && _lastCMPPrice !== price);
    _lastCMPPrice = price;

    // Update hero CMP display
    const fcPriceEl = document.getElementById('fcCmpPrice');
    fcPriceEl.textContent = '₹' + price.toLocaleString('en-IN');
    if (priceChanged) {
      fcPriceEl.classList.remove('price-flash');
      void fcPriceEl.offsetWidth;
      fcPriceEl.classList.add('price-flash');
    }

    // Update the price input and FC, unless the user has typed their own price
    const cmpInput = document.getElementById('fcAdvCMP');
    if (!cmpInput.dataset.userSet) { cmpInput.value = price; FC.currentPrice = price; }

    renderCmpMeta();

    // Source indicator
    const srcEl = document.getElementById('fcCmpSource');
    if (srcEl) {
      const src = data.source || '?';
      const cached = data.cached ? ' (cached)' : '';
      const changeTxt = data.change_pct != null ? ` · ${data.change_pct >= 0 ? '+' : ''}${data.change_pct.toFixed(2)}%` : '';
      srcEl.textContent = `Live: ${src}${cached}${changeTxt}`;
    }

    // "Last updated" relative timestamp
    const updEl = document.getElementById('fcCmpLastUpdated');
    if (updEl) {
      const fetchedAt = data.fetched_at ? new Date(data.fetched_at) : new Date();
      const formatAgo = () => {
        const secs = Math.round((Date.now() - fetchedAt.getTime()) / 1000);
        if (secs < 5) return 'just now';
        if (secs < 60) return secs + 's ago';
        if (secs < 3600) return Math.floor(secs / 60) + 'm ago';
        return Math.floor(secs / 3600) + 'h ago';
      };
      updEl.textContent = 'Updated ' + formatAgo();
      updEl.style.color = '';
      if (_lastCMPTimer) clearInterval(_lastCMPTimer);
      _lastCMPTimer = setInterval(() => { updEl.textContent = 'Updated ' + formatAgo(); }, 10000);
    }

    // Recalculate model with new CMP
    recalcForecast();

    console.log(`CMP updated: ₹${price} (${data.source})`);
  } catch (e) {
    console.warn('CMP auto-fetch failed:', e.message);
    const updEl = document.getElementById('fcCmpLastUpdated');
    if (updEl && updEl.textContent) {
      updEl.style.color = 'var(--warning, #e67e22)';
      updEl.textContent = updEl.textContent.replace('Updated', 'Stale ·');
    }
  }
}

// URL cookie sync
(function() {
  const params = new URLSearchParams(window.location.search);
  const urlCookie = params.get('cookie');
  if (urlCookie) {
    MCX.storage.set('mcxCookie', urlCookie);
    window.history.replaceState({}, '', window.location.pathname + window.location.hash);
    setTimeout(() => doRefresh(), 1200);
  }
  // Render intraday chart (static fallback, dynamic loads on accordion open)
  renderIntradayChart();
  // Auto-fetch live CMP on page load
  fetchLiveCMP();
  // Refresh CMP every 60 s, only in NSE hours, while a page that shows it (Scenarios, Fair value) is on screen
  MCX.poll.every('cmp', fetchLiveCMP, 60 * 1000, () =>
    MCX.market.nseOpen() && !document.hidden && ['val-scen', 'val-fv'].includes(MCX.router.current()));
})();
