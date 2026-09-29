// ════════════════════════════════════════════════════════════════════════════
//  GLOBALS
// ════════════════════════════════════════════════════════════════════════════
let valChartInst = null;
let valCache = null;
let valLoading = false;
let ecmChartInst = null;
let mdlCache = null;
let mdlLoading = false;
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
  el.innerHTML = cfg.ranges.map(r =>
    '<span class="margin-chip' + (r === initial ? ' active' : '') + '" data-range="' + r + '">' + r + '</span>'
  ).join('');
  el.querySelectorAll('.margin-chip').forEach(chip => {
    chip.onclick = () => {
      const r = chip.getAttribute('data-range');
      if (rangeState[cfg.key] === r) return;
      rangeState[cfg.key] = r;
      MCX.storage.set('mcx.range.' + cfg.key, r);
      el.querySelectorAll('.margin-chip').forEach(c =>
        c.className = c.getAttribute('data-range') === r ? 'margin-chip active' : 'margin-chip');
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
  key: 'valChart', containerId: 'valChartRange',
  ranges: ['30D','60D','Q','1Y','2Y','Max'], defaultRange: '60D',
  labelIds: ['valChartRangeLabel'],
  onChange: () => loadValuation()
});

makeRangeToggle({
  key: 'ecmChart', containerId: 'ecmChartRange',
  ranges: ['30D','60D','Q','1Y','2Y','Max'], defaultRange: '60D',
  labelIds: ['ecmChartRangeLabel'],
  onChange: () => loadModels()
});

makeRangeToggle({
  key: 'intraday', containerId: 'intradayRange',
  ranges: ['30D','60D','Q'], defaultRange: '30D',
  labelIds: ['intradayRangeLabel'],
  onChange: () => loadIntradayCurve()
});

makeRangeToggle({
  key: 'hourlyRev', containerId: 'hourlyRevRange',
  ranges: ['30D','60D','Q','Max'], defaultRange: 'Q',
  labelIds: ['hourlyRevRangeLabel'],
  onChange: () => loadHourlySingle('hourlyRev', renderHourlyAccuracy, hourlyRevFail)
});

makeRangeToggle({
  key: 'hourlySig', containerId: 'hourlySigRange',
  ranges: ['30D','60D','Q','Max'], defaultRange: 'Q',
  labelIds: ['hourlySigRangeLabel'],
  onChange: () => loadHourlySingle('hourlySig', renderHourlySignalChart)
});

makeRangeToggle({
  key: 'hourlyFwd', containerId: 'hourlyFwdRange',
  ranges: ['30D','60D','Q','Max'], defaultRange: 'Q',
  labelIds: ['hourlyFwdRangeLabel'],
  onChange: () => loadHourlySingle('hourlyFwd', renderHourlyForwardChart)
});

makeRangeToggle({
  key: 'icChart', containerId: 'icChartRange',
  ranges: ['30D','60D','Q','1Y','2Y','Max'], defaultRange: '60D',
  labelIds: ['icChartRangeLabel'],
  onChange: () => { if (analyticsCache) renderIcChart(analyticsCache); }
});

makeRangeToggle({
  key: 'perfChart', containerId: 'perfChartRange',
  ranges: ['30D','60D','Q','1Y','2Y','Max'], defaultRange: '60D',
  labelIds: ['perfChartRangeLabel'],
  onChange: () => { if (analyticsCache) renderPerfChart(analyticsCache); }
});

makeRangeToggle({
  key: 'hmmChart', containerId: 'hmmChartRange',
  ranges: ['30D','60D','Q','1Y','2Y','Max'], defaultRange: '60D',
  labelIds: ['hmmChartRangeLabel'],
  onChange: () => { if (analyticsCache) renderHmmChart(analyticsCache); }
});

makeRangeToggle({
  key: 'corrTable', containerId: 'corrTableRange',
  ranges: ['60D','Q','1Y','2Y','Max'], defaultRange: '1Y',
  labelIds: ['corrTableRangeLabel'],
  onChange: () => { if (analyticsCache) renderCorrTable(analyticsCache); }
});

makeRangeToggle({
  key: 'marginHist', containerId: 'marginHistRange',
  ranges: ['30D','60D','Q','1Y','Max'], defaultRange: '60D',
  labelIds: ['marginHistRangeLabel'],
  onChange: () => updateMarginChart(Object.keys(marginSelectedSymbols))
});

makeRangeToggle({
  key: 'marginChanges', containerId: 'marginChangesRange',
  ranges: ['30D','60D','Q','1Y','Max'], defaultRange: '60D',
  onChange: () => renderMarginChangesTable()
});

makeRangeToggle({
  key: 'momRegime', containerId: 'momRegimeRange',
  ranges: ['30D','60D','Q','1Y','2Y','Max'], defaultRange: '60D',
  labelIds: ['momRangeLabel'],
  onChange: () => { momHideTooltip(); return fetchRanged(momUrl('momRegime')).then(d => { if (d.success) { renderMomentumHero(d); renderMomRegimeChart(d.history||[]); } }); }
});

makeRangeToggle({
  key: 'momPrice', containerId: 'momPriceRange',
  ranges: ['30D','60D','Q','1Y','2Y','Max'], defaultRange: '60D',
  labelIds: ['momPriceRangeLabel'],
  onChange: () => { momHideTooltip(); return fetchRanged(momUrl('momPrice')).then(d => { if (d.success) renderMomPriceChart(d.history||[]); }); }
});

makeRangeToggle({
  key: 'momTable', containerId: 'momTableRange',
  ranges: ['30D','60D','Q','1Y','2Y','Max'], defaultRange: '30D',
  labelIds: ['momTableRangeLabel'],
  onChange: () => fetchRanged(momUrl('momTable')).then(d => { if (d.success) renderMomTable(d.history||[]); })
});

makeRangeToggle({
  key: 'oipHero', containerId: 'oipHeroRange',
  ranges: ['30D','60D','Q','1Y','Max'], defaultRange: 'Max',
  onChange: () => oipRenderAll()
});

makeRangeToggle({
  key: 'oipGrowth', containerId: 'oipGrowthRange',
  ranges: ['3M','6M','12M','All'], defaultRange: 'All',
  onChange: () => oipRenderAll()
});

makeRangeToggle({
  key: 'oipComp', containerId: 'oipCompRange',
  ranges: ['30D','60D','Q','1Y','Max'], defaultRange: 'Max',
  onChange: () => oipRenderAll()
});

// Auto-trigger first refresh
setTimeout(() => doRefresh(true), 800);

// Page switching and each page's loaders live in shell.js (MCX.router).

// ════════════════════════════════════════════════════════════════════════════
//  FORECAST MODEL — Revenue → EPS → Share Price
//  Uses same fee schedule as Daily Predictor (mcx_config.py)
// ════════════════════════════════════════════════════════════════════════════
const FC = {
  dailyRev: 12.62, pe: 42, opex: 700, otherIncome: 126,
  taxRate: 20.30, shares: 25.5, tradingDays: 250, currentPrice: 2406
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
  const shares = parseFloat(sharesEl && sharesEl.value) || 25.5;
  const cmp = parseFloat(cmpEl && cmpEl.value) || 2722;
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
  let fy27Days = 254;
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
    if (yr === '27') fy27Days = days || 254;
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
  document.getElementById('fcWfSharesLabel').textContent = inp.shares.toFixed(1);
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
  }
}

// ── Auto-fetch CMP from /api/mcxprice (indianapi.in → Yahoo fallback) ────
let _lastCMPPrice = null;
let _lastCMPTimer = null;

// Latest reported trailing-twelve-month EPS (₹, diluted) — FY26 actual
// (Q1–Q4 FY26, PAT ₹1,331 Cr / 25.451 Cr shares). Update alongside
// QUARTERLY_ACTUALS in api/quarterly.py on each new result.
const CMP_TTM_EPS = 52.3;
const CMP_DILUTED_SHARES_CR = 25.451; // post 1:5 split Jan 2026

async function fetchLiveCMP() {
  try {
    const resp = await fetch('/api/mcxprice');
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    if (data.error) { console.warn('CMP fetch:', data.error); return; }

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

    // Update advanced CMP input + FC constant
    document.getElementById('fcAdvCMP').value = price;
    FC.currentPrice = price;

    // Live-derive the hero meta line — previously a hardcoded HTML string
    // that froze at an old price (EPS/PE/MCap internally consistent with
    // ₹2,396 while the CMP above showed live). PE & MCap now track the live
    // price; EPS is the latest reported TTM (see CMP_TTM_EPS above).
    const metaEl = document.getElementById('fcCmpMeta');
    if (metaEl) {
      const pe = price / CMP_TTM_EPS;
      const mcapCr = Math.round(price * CMP_DILUTED_SHARES_CR);
      metaEl.textContent =
        `EPS (TTM): ₹${CMP_TTM_EPS.toFixed(2)} · PE: ${pe.toFixed(1)}x · MCap: ₹${mcapCr.toLocaleString('en-IN')} Cr`;
    }

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

    // ── Update Fair Value tab market price in real-time ──
    const valPriceEl = document.getElementById('valPrice');
    if (valPriceEl) {
      valPriceEl.textContent = '₹' + price.toLocaleString('en-IN');
      if (priceChanged) {
        valPriceEl.classList.remove('price-flash');
        void valPriceEl.offsetWidth;
        valPriceEl.classList.add('price-flash');
      }
      const src = data.source || '?';
      const cached = data.cached ? ' (cached)' : '';
      const fetchedAt = data.fetched_at ? new Date(data.fetched_at) : new Date();
      const secs = Math.round((Date.now() - fetchedAt.getTime()) / 1000);
      const ago = secs < 5 ? 'just now' : secs < 60 ? secs + 's ago' : Math.floor(secs / 60) + 'm ago';
      document.getElementById('valPriceDate').textContent = `Live · ${src}${cached} · ${ago}`;
    }
    // Recalculate Fair Value metrics with live price
    if (valCache && valCache.snapshot) {
      const s = valCache.snapshot;
      const fvBase = s.fair_value && s.fair_value.base;
      const fvBear = s.fair_value && s.fair_value.bear;
      const fvBull = s.fair_value && s.fair_value.bull;
      const eps = s.current_eps;
      if (fvBase > 0) {
        // Upside/downside
        const upside = ((fvBase - price) / price * 100);
        const upsideEl = document.getElementById('valUpside');
        if (upsideEl) {
          const arrow = upside >= 0 ? '↑' : '↓';
          const cls = upside >= 0 ? 'val-upside' : 'val-downside';
          upsideEl.innerHTML = `<span class="${cls}" style="font-size:18px;font-weight:800">${arrow} ${Math.abs(upside).toFixed(1)}% ${upside >= 0 ? 'upside' : 'downside'} to base</span>`;
        }
        // Implied P/E
        if (eps > 0) {
          document.getElementById('valImpliedPE').textContent = `Implied P/E: ${(price / eps).toFixed(1)}x`;
        }
        // Signal classification (mirrors valuation.py classify_signal)
        let sig = 'FAIR';
        if (price < fvBear) sig = 'DEEP_VALUE';
        else if (price < fvBase * 0.95) sig = 'UNDERVALUED';
        else if (price <= fvBase * 1.05) sig = 'FAIR';
        else if (price <= fvBull) sig = 'OVERVALUED';
        else sig = 'STRETCHED';
        const sigLabels = { 'DEEP_VALUE': 'Deep Value', 'UNDERVALUED': 'Undervalued', 'FAIR': 'Fair Value', 'OVERVALUED': 'Overvalued', 'STRETCHED': 'Stretched' };
        const sigBadge = document.getElementById('valSignalBadge');
        if (sigBadge) sigBadge.innerHTML = `<span class="val-signal ${sig}">${sigLabels[sig]}</span>`;
      }
    }

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
  // Refresh CMP every 60 s, only in NSE hours, while Scenarios (the page that shows it) is on screen
  MCX.poll.every('cmp', fetchLiveCMP, 60 * 1000, () =>
    MCX.market.nseOpen() && !document.hidden && MCX.router.current() === 'val-scen');
})();

// ════════════════════════════════════════════════════════════════════════════
//  VALUATION TAB — EPS-Path Fair Value Model (Model A)
// ════════════════════════════════════════════════════════════════════════════

async function loadValuation() {
  if (valLoading) return;
  valLoading = true;

  try {
    const data = await fetchRanged('/api/valuation?range=' + encodeURIComponent(rangeState['valChart'] || '60D'));
    if (!data.success) throw new Error(data.error || 'No valuation data');
    data._ts = Date.now();
    valCache = data;
    renderValuation(data);
  } catch (e) {
    console.error('Valuation load error:', e);
    document.getElementById('valPrice').textContent = 'Error';
    document.getElementById('valFairBase').textContent = e.message.slice(0, 60);
  } finally {
    valLoading = false;
  }
  // Also load Models B + C + Ensemble (parallel, non-blocking)
  loadModels();
}

function renderValuation(data) {
  const s = data.snapshot;
  const pe = data.pe_bands;
  const dq = data.data_quality;

  // ── Hero cards ──
  // Use live CMP if available (from fetchLiveCMP), else fall back to EOD price from API
  const livePrice = FC.currentPrice;
  const price = livePrice || s.latest_price;
  if (livePrice) {
    document.getElementById('valPrice').textContent = `₹${Math.round(livePrice).toLocaleString('en-IN')}`;
    document.getElementById('valPriceDate').textContent = `Live · ${document.getElementById('fcCmpSource')?.textContent?.replace('Live: ', '') || 'auto'}`;
  } else {
    document.getElementById('valPrice').textContent = price ? `₹${Math.round(price).toLocaleString('en-IN')}` : '—';
    document.getElementById('valPriceDate').textContent = s.latest_price_date ? `As of ${s.latest_price_date}` : '';
  }

  const fvBase = s.fair_value.base;
  document.getElementById('valFairBase').textContent = fvBase ? `₹${Math.round(fvBase).toLocaleString('en-IN')}` : '—';
  document.getElementById('valFairRange').textContent =
    `Bear ₹${Math.round(s.fair_value.bear).toLocaleString('en-IN')} · Bull ₹${Math.round(s.fair_value.bull).toLocaleString('en-IN')}`;

  // Signal badge + Upside — recalculate with live price if available
  const fvBear = s.fair_value.bear;
  const fvBull = s.fair_value.bull;
  const eps = s.current_eps;
  const sigLabels = {
    'DEEP_VALUE': 'Deep Value', 'UNDERVALUED': 'Undervalued', 'FAIR': 'Fair Value',
    'OVERVALUED': 'Overvalued', 'STRETCHED': 'Stretched', 'NO_PRICE': 'No Price', 'NO_DATA': 'No Data'
  };

  let sig = s.signal || 'NO_DATA';
  let upsidePct = s.upside_to_base_pct;
  let impliedPE = s.implied_pe;

  // Recalculate with live price (mirrors valuation.py classify_signal)
  if (livePrice && fvBase > 0) {
    upsidePct = (fvBase - livePrice) / livePrice * 100;
    if (eps > 0) impliedPE = (livePrice / eps).toFixed(1);
    if (livePrice < fvBear) sig = 'DEEP_VALUE';
    else if (livePrice < fvBase * 0.95) sig = 'UNDERVALUED';
    else if (livePrice <= fvBase * 1.05) sig = 'FAIR';
    else if (livePrice <= fvBull) sig = 'OVERVALUED';
    else sig = 'STRETCHED';
  }

  const signalEl = document.getElementById('valSignalBadge');
  signalEl.innerHTML = `<span class="val-signal ${sig}">${sigLabels[sig] || sig}</span>`;

  // Upside
  const upsideEl = document.getElementById('valUpside');
  if (upsidePct !== null && upsidePct !== undefined) {
    const pct = upsidePct;
    const cls = pct >= 0 ? 'val-upside' : 'val-downside';
    const arrow = pct >= 0 ? '↑' : '↓';
    upsideEl.innerHTML = `<span class="${cls}" style="font-size:18px;font-weight:800">${arrow} ${Math.abs(pct).toFixed(1)}% ${pct >= 0 ? 'upside' : 'downside'} to base</span>`;
  } else {
    upsideEl.textContent = '—';
  }
  document.getElementById('valImpliedPE').textContent = impliedPE ? `Implied P/E: ${impliedPE}x` : 'Implied P/E: —';

  // ── EPS Chain ──
  const c = s.eps_chain;
  if (c) {
    document.getElementById('valC_ma45').textContent = `₹${c.ma45_rev_cr} Cr`;
    document.getElementById('valC_annRev').textContent = `₹${Math.round(c.annual_total_rev_cr).toLocaleString('en-IN')} Cr`;
    document.getElementById('valC_pat').textContent = `₹${Math.round(c.pat_cr).toLocaleString('en-IN')} Cr`;
    document.getElementById('valC_eps').textContent = `₹${c.eps}`;
    document.getElementById('valC_pe').textContent = pe.mean ? `${pe.mean}x` : '—';
    document.getElementById('valC_fv').textContent = `₹${Math.round(fvBase).toLocaleString('en-IN')}`;
  }

  // ── P/E Gauge ──
  document.getElementById('valPeMeta').textContent = `mean ${pe.mean}x · sd ${pe.sd}x · ${pe.data_points} obs`;
  document.getElementById('valMethodPE').textContent =
    `Dynamic: mean ${pe.mean}x ± ${pe.sd}x SD (${pe.data_points} observations)`;
  if (c) {
    document.getElementById('valMethodAnn').textContent =
      `45DMA × ${c.trading_days} trading days + ₹${c.non_fo_rev_cr} Cr non-F&O revenue and other income`;
    document.getElementById('valMethodMargin').textContent =
      `${Math.round(c.pat_margin * 100)}% of total income (PAT ÷ total income, FY26 and Q1 FY27)`;
  }

  renderPEGauge(s, pe);

  // ── Chart ──
  renderValuationChart(data.history);

  // ── Data Quality ──
  document.getElementById('valDQ_rows').textContent = dq.valuation_rows;
  document.getElementById('valDQ_history').textContent = dq.history_returned;
  document.getElementById('valDQ_window').textContent = `${dq.revenue_window} days`;
  document.getElementById('valDQ_latest').textContent = dq.latest_valuation_date;
  document.getElementById('valDQ_asof').textContent = data.as_of;
}

function renderPEGauge(snapshot, peBands) {
  const price = snapshot.latest_price;
  const fvBear = snapshot.fair_value.bear;
  const fvBull = snapshot.fair_value.bull;
  if (!price || !fvBear || !fvBull) return;

  // Gauge range: 0.7× bear to 1.3× bull
  const gaugeMin = Math.round(fvBear * 0.7);
  const gaugeMax = Math.round(fvBull * 1.3);
  const range = gaugeMax - gaugeMin;

  // Position needle
  const needlePct = Math.max(0, Math.min(100, ((price - gaugeMin) / range) * 100));
  document.getElementById('valNeedle').style.left = needlePct + '%';

  // Update labels
  document.getElementById('valGaugeMin').textContent = `₹${gaugeMin.toLocaleString('en-IN')}`;
  document.getElementById('valGaugeMax').textContent = `₹${gaugeMax.toLocaleString('en-IN')}`;

  // Set zone widths based on actual fair value positions
  const bearPct = ((fvBear - gaugeMin) / range) * 100;
  const baseFv = snapshot.fair_value.base;
  const baseLow = baseFv * 0.95;
  const baseHigh = baseFv * 1.05;
  const underPct = ((baseLow - fvBear) / range) * 100;
  const fairPct = ((baseHigh - baseLow) / range) * 100;
  const overPct = ((fvBull - baseHigh) / range) * 100;
  const stretchPct = 100 - bearPct - underPct - fairPct - overPct;

  document.getElementById('valGZ_deep').style.width = Math.max(bearPct, 5) + '%';
  document.getElementById('valGZ_under').style.width = Math.max(underPct, 5) + '%';
  document.getElementById('valGZ_fair').style.width = Math.max(fairPct, 5) + '%';
  document.getElementById('valGZ_over').style.width = Math.max(overPct, 5) + '%';
  document.getElementById('valGZ_stretch').style.width = Math.max(stretchPct, 5) + '%';
}

function renderValuationChart(history) {
  if (!history || history.length === 0) return;
  const canvas = document.getElementById('valChart');
  if (!canvas) return;

  const labels = history.map(h => h.date.slice(5)); // MM-DD
  const prices = history.map(h => h.price);
  const fairBear = history.map(h => h.fair_bear);
  const fairBase = history.map(h => h.fair_base);
  const fairBull = history.map(h => h.fair_bull);

  // Append live CMP as "today" if available and not already in history
  const livePrice = FC.currentPrice;
  if (livePrice && history.length > 0) {
    const today = new Date().toISOString().slice(0, 10);
    const lastDate = history[history.length - 1].date;
    if (today > lastDate) {
      const last = history[history.length - 1];
      labels.push(today.slice(5));
      prices.push(livePrice);
      fairBear.push(last.fair_bear);
      fairBase.push(last.fair_base);
      fairBull.push(last.fair_bull);
    }
  }

  const isDark = document.documentElement.classList.contains('dark');
  const gridColor = isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.06)';
  const textColor = isDark ? '#888' : '#999';

  if (valChartInst) {
    valChartInst.destroy();
    valChartInst = null;
  }

  valChartInst = new Chart(canvas.getContext('2d'), {
    type: 'line',
    data: {
      labels,
      datasets: [
        {
          label: 'Market Price',
          data: prices,
          borderColor: isDark ? '#E0DDD8' : '#1A1A1A',
          backgroundColor: 'transparent',
          borderWidth: 2.5,
          pointRadius: 0,
          pointHoverRadius: 4,
          tension: 0.2,
          spanGaps: true,
          order: 1,
        },
        {
          label: 'Fair Value (Base)',
          data: fairBase,
          borderColor: isDark ? '#5B9CF5' : '#0958D9',
          backgroundColor: 'transparent',
          borderWidth: 2,
          borderDash: [6, 3],
          pointRadius: 0,
          tension: 0.2,
          order: 2,
        },
        {
          label: 'Bear',
          data: fairBear,
          borderColor: isDark ? '#52C41A' : '#1B7D3A',
          backgroundColor: 'transparent',
          borderWidth: 1,
          borderDash: [3, 3],
          pointRadius: 0,
          tension: 0.2,
          fill: false,
          order: 3,
        },
        {
          label: 'Bull',
          data: fairBull,
          borderColor: isDark ? '#FF4D4F' : '#CF1322',
          backgroundColor: 'transparent',
          borderWidth: 1,
          borderDash: [3, 3],
          pointRadius: 0,
          tension: 0.2,
          fill: '-1', // fill between bear and bull
          order: 4,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: {
          display: true,
          position: 'top',
          align: 'end',
          labels: {
            font: { family: "'JetBrains Mono'", size: 10 },
            color: textColor,
            boxWidth: 16,
            padding: 12,
          },
        },
        tooltip: {
          backgroundColor: isDark ? '#333' : '#1A1A1A',
          titleFont: { family: "'JetBrains Mono'", size: 10 },
          bodyFont: { family: "'JetBrains Mono'", size: 11 },
          padding: 10,
          callbacks: {
            label: ctx => `${ctx.dataset.label}: ₹${Math.round(ctx.parsed.y).toLocaleString('en-IN')}`,
          },
        },
      },
      scales: {
        x: {
          grid: { color: gridColor },
          ticks: {
            font: { family: "'JetBrains Mono'", size: 9 },
            color: textColor,
            maxRotation: 45,
            maxTicksLimit: 12,
          },
        },
        y: {
          grid: { color: gridColor },
          ticks: {
            font: { family: "'JetBrains Mono'", size: 10 },
            color: textColor,
            callback: v => '₹' + v.toLocaleString('en-IN'),
          },
        },
      },
    },
  });
}

// ════════════════════════════════════════════════════════════════════════════
//  MODELS B + C + ENSEMBLE — Multi-Model Signal Dashboard
// ════════════════════════════════════════════════════════════════════════════

async function loadModels() {
  if (mdlLoading) return;
  mdlLoading = true;

  try {
    const data = await fetchRanged('/api/models?range=' + encodeURIComponent(rangeState['ecmChart'] || '60D'));
    if (!data.success) throw new Error(data.error || 'No model data');
    mdlCache = data;
    renderModels(data);
  } catch (e) {
    console.error('Models load error:', e);
    document.getElementById('mdlEnsembleScore').textContent = 'N/A';
  } finally {
    mdlLoading = false;
  }
}

function renderModels(data) {
  const snap = data.snapshot;
  if (!snap) return;

  // ── Ensemble Banner ──
  const ensScore = snap.ensemble.score;
  const ensSig = snap.ensemble.signal || 'NO_DATA';
  document.getElementById('mdlEnsembleScore').textContent =
    ensScore !== null && ensScore !== undefined ? ensScore.toFixed(3) : '—';
  document.getElementById('mdlEnsembleScore').style.color =
    ensScore > 0.5 ? 'var(--positive)' : ensScore < -0.5 ? 'var(--negative)' : 'var(--text)';

  const ensLabels = {
    'STRONG_BUY': 'Strong Buy', 'BUY': 'Buy', 'NEUTRAL': 'Neutral',
    'SELL': 'Sell', 'STRONG_SELL': 'Strong Sell', 'NO_DATA': 'No Data'
  };
  document.getElementById('mdlEnsembleSignalBadge').innerHTML =
    `<span class="mdl-signal ${ensSig}">${ensLabels[ensSig] || ensSig}</span>`;

  // ── Ensemble Gauge Needle ──
  if (ensScore !== null && ensScore !== undefined) {
    const clamped = Math.max(-3, Math.min(3, ensScore));
    const needlePct = ((clamped + 3) / 6) * 100; // map [-3,+3] → [0%,100%]
    document.getElementById('mdlEnsNeedle').style.left = needlePct + '%';
  }

  // ── ECM Panel ──
  const ecm = snap.ecm;
  const ecmSig = ecm.signal || 'NO_DATA';
  const ecmLabels = {
    'STRONG_REVERT_UP': 'Strong Revert Up', 'MILD_REVERT_UP': 'Mild Revert Up',
    'NEUTRAL': 'Neutral', 'MILD_EXTEND_DOWN': 'Mild Extend Down',
    'STRONG_EXTEND_DOWN': 'Strong Extend Down', 'NO_DATA': 'No Data'
  };
  document.getElementById('mdlEcmSignalBadge').innerHTML =
    `<span class="mdl-signal ${ecmSig}">${ecmLabels[ecmSig] || ecmSig}</span>`;

  const spreadPct = ecm.spread_pct;
  document.getElementById('mdlEcmSpreadPct').textContent =
    spreadPct !== null && spreadPct !== undefined ? `${spreadPct > 0 ? '+' : ''}${spreadPct.toFixed(1)}%` : '—';
  document.getElementById('mdlEcmSpreadPct').style.color =
    spreadPct < 0 ? 'var(--positive)' : spreadPct > 0 ? 'var(--negative)' : 'var(--text)';

  const ecmZ = ecm.z_score;
  document.getElementById('mdlEcmZscore').textContent =
    ecmZ !== null && ecmZ !== undefined ? ecmZ.toFixed(3) : '—';
  document.getElementById('mdlEcmZscore').style.color =
    ecmZ < -1 ? 'var(--positive)' : ecmZ > 1 ? 'var(--negative)' : 'var(--text)';

  const halfLife = ecm.half_life_days;
  document.getElementById('mdlEcmHalfLife').textContent =
    halfLife !== null && halfLife !== undefined ? `${halfLife}d` : '—';

  // ── Multi-Factor Panel ──
  const mf = snap.multi_factor;
  const mfSig = mf.signal || 'NO_DATA';
  const mfLabels = {
    'STRONG_BUY': 'Strong Buy', 'BUY': 'Buy', 'NEUTRAL': 'Neutral',
    'SELL': 'Sell', 'STRONG_SELL': 'Strong Sell', 'NO_DATA': 'No Data'
  };
  document.getElementById('mdlMfSignalBadge').innerHTML =
    `<span class="mdl-signal ${mfSig}">${mfLabels[mfSig] || mfSig}</span>`;

  renderFactorBar('rev', mf.revenue_z, 40);
  renderFactorBar('turn', mf.turnover_z, 25);
  renderFactorBar('vol', mf.volume_z, 20);
  renderFactorBar('ivol', mf.volatility_z, 15);

  const comp = mf.composite_z;
  document.getElementById('mdlMfComposite').textContent =
    comp !== null && comp !== undefined ? comp.toFixed(3) : '—';
  document.getElementById('mdlMfComposite').style.color =
    comp > 0.5 ? 'var(--positive)' : comp < -0.5 ? 'var(--negative)' : 'var(--text)';

  // ── Position & Conviction Panel (3D-2) ──
  const pos = snap.position || {};
  const posScore = pos.score;
  const posConv = pos.conviction;
  const posMom = pos.momentum;
  const posVel = pos.velocity_1d;
  const posConvMA = pos.conviction_2d_ma;

  document.getElementById('mdlPosScore').textContent =
    posScore !== null && posScore !== undefined ? (posScore > 0 ? '+' : '') + posScore.toFixed(4) : '—';
  document.getElementById('mdlPosScore').style.color =
    posScore > 0.25 ? 'var(--positive)' : posScore < -0.25 ? 'var(--negative)' : 'var(--text)';

  document.getElementById('mdlPosConviction').textContent =
    posConv !== null && posConv !== undefined ? (posConv * 100).toFixed(1) + '%' : '—';
  document.getElementById('mdlPosConviction').style.color =
    posConv > 0.5 ? 'var(--positive)' : 'var(--text)';

  document.getElementById('mdlPosMomentum').textContent =
    posMom !== null && posMom !== undefined ? (posMom > 0 ? '+' : '') + posMom.toFixed(3) : '—';
  document.getElementById('mdlPosMomentum').style.color =
    posMom > 0.5 ? 'var(--positive)' : posMom < -0.5 ? 'var(--negative)' : 'var(--text)';

  document.getElementById('mdlPosVelocity').textContent =
    posVel !== null && posVel !== undefined ? (posVel > 0 ? '+' : '') + posVel.toFixed(4) : '—';
  document.getElementById('mdlPosVelocity').style.color =
    posVel > 0.01 ? 'var(--positive)' : posVel < -0.01 ? 'var(--negative)' : 'var(--text)';

  document.getElementById('mdlPosConvMA').textContent =
    posConvMA !== null && posConvMA !== undefined ? (posConvMA * 100).toFixed(1) + '%' : '—';

  // Position gauge needle: map [-1, +1] → [0%, 100%]
  if (posScore !== null && posScore !== undefined) {
    const clamped = Math.max(-1, Math.min(1, posScore));
    const needlePct = ((clamped + 1) / 2) * 100;
    document.getElementById('mdlPosNeedle').style.left = needlePct + '%';
  }

  // ── Data Quality extension ──
  if (data.data_quality) {
    document.getElementById('valDQ_mdlRows').textContent = data.data_quality.total_rows || '—';
  }

  // ── ECM Chart ──
  renderECMChart(data.history);
}

function renderFactorBar(key, z, weight) {
  const bar = document.getElementById(`mdlBar_${key}`);
  const val = document.getElementById(`mdlVal_${key}`);
  if (!bar || !val) return;

  if (z === null || z === undefined) {
    bar.style.width = '0';
    val.textContent = '—';
    return;
  }

  val.textContent = z.toFixed(2);
  val.style.color = z > 0 ? 'var(--positive)' : z < 0 ? 'var(--negative)' : 'var(--text)';

  // Bar fill: center = 50%, max z display ±3
  const clampZ = Math.max(-3, Math.min(3, z));
  const pct = Math.abs(clampZ) / 3 * 50; // 0-50% of the bar width
  bar.className = `mdl-factor-bar-fill ${z >= 0 ? 'pos' : 'neg'}`;

  if (z >= 0) {
    bar.style.left = '50%';
    bar.style.width = pct + '%';
  } else {
    bar.style.left = (50 - pct) + '%';
    bar.style.width = pct + '%';
  }
}

function renderECMChart(history) {
  if (!history || history.length === 0) return;
  const canvas = document.getElementById('ecmChart');
  if (!canvas) return;

  const labels = history.map(h => h.date.slice(5));
  const spreadPcts = history.map(h => h.ecm_spread_pct);
  const ecmZs = history.map(h => h.ecm_z);
  const posScores = history.map(h => h.position_score);

  // Use server-computed rolling 60-day bands (per-point, matching backend z-scores)
  const band1up  = history.map(h => h.ecm_band_1up  ?? null);
  const band1dn  = history.map(h => h.ecm_band_1dn  ?? null);
  const band15up = history.map(h => h.ecm_band_15up ?? null);
  const band15dn = history.map(h => h.ecm_band_15dn ?? null);
  const meanLine = history.map(h => h.ecm_band_mean ?? null);

  const isDark = document.documentElement.classList.contains('dark');
  const gridColor = isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.06)';
  const textColor = isDark ? '#888' : '#999';

  if (ecmChartInst) {
    ecmChartInst.destroy();
    ecmChartInst = null;
  }

  ecmChartInst = new Chart(canvas.getContext('2d'), {
    type: 'line',
    data: {
      labels,
      datasets: [
        {
          label: 'Spread %',
          data: spreadPcts,
          borderColor: isDark ? '#E0DDD8' : '#1A1A1A',
          backgroundColor: 'transparent',
          borderWidth: 2,
          pointRadius: 0,
          pointHoverRadius: 4,
          tension: 0.2,
          spanGaps: true,
          order: 1,
        },
        {
          label: 'Mean',
          data: meanLine,
          borderColor: isDark ? '#5B9CF5' : '#0958D9',
          borderWidth: 1,
          borderDash: [4, 4],
          pointRadius: 0,
          fill: false,
          spanGaps: true,
          order: 5,
        },
        {
          label: '+1σ',
          data: band1up,
          borderColor: isDark ? 'rgba(250,173,20,0.4)' : 'rgba(173,104,0,0.3)',
          borderWidth: 1,
          borderDash: [3, 3],
          pointRadius: 0,
          fill: false,
          spanGaps: true,
          order: 4,
        },
        {
          label: '−1σ',
          data: band1dn,
          borderColor: isDark ? 'rgba(250,173,20,0.4)' : 'rgba(173,104,0,0.3)',
          borderWidth: 1,
          borderDash: [3, 3],
          pointRadius: 0,
          fill: '-1',
          backgroundColor: isDark ? 'rgba(250,173,20,0.04)' : 'rgba(173,104,0,0.04)',
          spanGaps: true,
          order: 3,
        },
        {
          label: '+1.5σ',
          data: band15up,
          borderColor: isDark ? 'rgba(255,77,79,0.3)' : 'rgba(207,19,34,0.2)',
          borderWidth: 1,
          borderDash: [2, 4],
          pointRadius: 0,
          fill: false,
          spanGaps: true,
          order: 6,
        },
        {
          label: '−1.5σ',
          data: band15dn,
          borderColor: isDark ? 'rgba(255,77,79,0.3)' : 'rgba(207,19,34,0.2)',
          borderWidth: 1,
          borderDash: [2, 4],
          pointRadius: 0,
          fill: false,
          spanGaps: true,
          order: 6,
        },
        {
          label: 'Position',
          data: posScores,
          borderColor: isDark ? '#b39ddb' : '#7e57c2',
          backgroundColor: isDark ? 'rgba(179,157,219,0.08)' : 'rgba(126,87,194,0.08)',
          borderWidth: 2,
          pointRadius: 0,
          pointHoverRadius: 3,
          tension: 0.3,
          fill: true,
          spanGaps: true,
          yAxisID: 'y1',
          order: 2,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: {
          display: true,
          position: 'top',
          align: 'end',
          labels: {
            font: { family: "'JetBrains Mono'", size: 9 },
            color: textColor,
            boxWidth: 12,
            padding: 8,
            filter: item => ['Spread %', 'Mean', '+1σ', '+1.5σ', 'Position'].includes(item.text),
          },
        },
        tooltip: {
          backgroundColor: isDark ? '#333' : '#1A1A1A',
          titleFont: { family: "'JetBrains Mono'", size: 10 },
          bodyFont: { family: "'JetBrains Mono'", size: 11 },
          padding: 10,
          callbacks: {
            label: ctx => {
              if (ctx.dataset.label === 'Spread %') {
                const z = ecmZs[ctx.dataIndex];
                return `Spread: ${ctx.parsed.y?.toFixed(2)}% (z: ${z !== null ? z.toFixed(3) : '—'})`;
              }
              if (ctx.dataset.label === 'Position') {
                return `Position: ${ctx.parsed.y?.toFixed(4)}`;
              }
              return `${ctx.dataset.label}: ${ctx.parsed.y?.toFixed(2)}%`;
            },
          },
        },
      },
      scales: {
        x: {
          grid: { color: gridColor },
          ticks: {
            font: { family: "'JetBrains Mono'", size: 9 },
            color: textColor,
            maxRotation: 45,
            maxTicksLimit: 12,
          },
        },
        y: {
          grid: { color: gridColor },
          ticks: {
            font: { family: "'JetBrains Mono'", size: 10 },
            color: textColor,
            callback: v => v.toFixed(1) + '%',
          },
        },
        y1: {
          position: 'right',
          grid: { drawOnChartArea: false },
          min: -1, max: 1,
          ticks: {
            font: { family: "'JetBrains Mono'", size: 9 },
            color: isDark ? '#b39ddb' : '#7e57c2',
            stepSize: 0.5,
            callback: v => v.toFixed(1),
          },
          title: {
            display: true,
            text: 'Position [-1,+1]',
            font: { family: "'JetBrains Mono'", size: 9 },
            color: isDark ? '#b39ddb' : '#7e57c2',
          },
        },
      },
    },
  });
}

// ════════════════════════════════════════════════════════════════════════════
//  ANALYTICS TAB (3D-3)
// ════════════════════════════════════════════════════════════════════════════
let analyticsCache = null, anLoading = false;
let icChartInst = null, perfChartInst = null;

async function loadAnalytics() {
  if (anLoading) return;
  anLoading = true;
  try {
    const data = await fetchRanged('/api/analytics');
    if (!data.success) throw new Error(data.error || 'No analytics data');
    analyticsCache = data;
    renderAnalytics(data);
  } catch (e) {
    console.error('Analytics load error:', e);
  } finally {
    anLoading = false;
  }
  // Load hourly accuracy in parallel (separate API call)
  loadHourlyAccuracy();
}

function renderAnalytics(data) {
  renderCorrTable(data);
  renderIcChart(data);

  // ── Regime ──
  const reg = data.regime || {};
  const regEl = document.getElementById('anRegime');
  regEl.textContent = reg.current || '—';
  regEl.style.color = reg.current === 'BULL' ? 'var(--positive)' : reg.current === 'BEAR' ? 'var(--negative)' : 'var(--text)';
  document.getElementById('anRegimeDur').textContent = reg.duration_days ? `${reg.duration_days} days` : '—';
  document.getElementById('anRegimeSplit').textContent =
    `${reg.bull_days || 0} / ${reg.neutral_days || 0} / ${reg.bear_days || 0}`;
  const vol = reg.volatility || {};
  document.getElementById('anVol').textContent = vol.annualized_pct ? `${vol.annualized_pct}%` : '—';
  document.getElementById('anVolRegime').textContent = vol.regime || '—';

  renderPerfChart(data);

  // ── Factor Decomposition ──
  const dec = data.factor_decomposition || {};
  const fmtDec = v => v !== undefined ? (v > 0 ? '+' : '') + v.toFixed(3) : '—';
  document.getElementById('anDecompEcm').textContent = fmtDec(dec.ecm_contribution);
  document.getElementById('anDecompRev').textContent = fmtDec(dec.mf_revenue_part);
  document.getElementById('anDecompTurn').textContent = fmtDec(dec.mf_turnover_part);
  document.getElementById('anDecompTotal').textContent = fmtDec(dec.ensemble_score);

  renderHmmChart(data);

  // ── Weight Sensitivity (3F-2) ──
  const ws = data.weight_sensitivity || [];
  if (ws.length > 0) {
    let wsHtml = '';
    for (const w of ws) {
      const isCurrent = w.is_current;
      const rowStyle = isCurrent ? 'font-weight:700;background:rgba(39,174,96,0.08)' : '';
      wsHtml += `<tr style="${rowStyle}">
        <td>${(w.ecm_weight * 100).toFixed(0)}%</td>
        <td>${(w.mf_weight * 100).toFixed(0)}%</td>
        <td style="font-family:var(--mono)">${w.ic != null ? w.ic.toFixed(4) : '—'}</td>
        <td style="font-family:var(--mono)">${w.hit_rate ? (w.hit_rate * 100).toFixed(1) + '%' : '—'}</td>
        <td>${isCurrent ? '✓ Current' : ''}</td>
      </tr>`;
    }
    document.getElementById('weightSensBody').innerHTML = wsHtml;
  }
}

// ── Client-side Pearson correlation (mirrors lib/mcx_config.py pearson()) ──
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

// ── Correlation Matrix (client-computed from factor_series, range-controlled) ──
function renderCorrTable(data) {
  const isDark = document.documentElement.classList.contains('dark');
  const fs = data.factor_series;
  if (!fs) return;
  const keys = ['ecm_z', 'rev_z', 'turn_z', 'position_score'];
  const sliced = keys.map(k => sliceTailByRange(fs[k], rangeState['corrTable'] || '1Y'));
  const matrix = keys.map((_, i) => keys.map((_, j) => pearsonJs(sliced[i], sliced[j])));

  const labels = ['ECM Z', 'Rev Z', 'Turn Z', 'Pos Score'];
  let html = '';
  for (let i = 0; i < 4; i++) {
    html += `<tr><td style="font-weight:700">${labels[i]}</td>`;
    for (let j = 0; j < 4; j++) {
      const v = matrix[i][j];
      const bg = v !== null ? corrColor(v, isDark) : 'transparent';
      html += `<td style="background:${bg};padding:6px 10px">${v !== null ? v.toFixed(2) : '—'}</td>`;
    }
    html += '</tr>';
  }
  document.getElementById('corrBody').innerHTML = html;
}

// ── Rolling IC Chart (range-controlled tail slice) ──
function renderIcChart(data) {
  const isDark = document.documentElement.classList.contains('dark');
  const gridColor = isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.06)';
  const txtColor = isDark ? '#888' : '#999';
  const icData = sliceTailByRange(data.rolling_ic || [], rangeState['icChart'] || '60D');
  if (icData.length > 0) {
    const canvas = document.getElementById('icChart');
    if (icChartInst) { icChartInst.destroy(); icChartInst = null; }
    icChartInst = new Chart(canvas.getContext('2d'), {
      type: 'line',
      data: {
        labels: icData.map(d => d.date.slice(5)),
        datasets: [{
          label: 'Ensemble IC',
          data: icData.map(d => d.ensemble_ic),
          borderColor: '#27ae60',
          borderWidth: 2,
          pointRadius: 0,
          tension: 0.3,
          fill: { target: 'origin', above: 'rgba(39,174,96,0.1)', below: 'rgba(231,76,60,0.1)' },
        }],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: {
          x: { grid: { color: gridColor }, ticks: { font: { size: 9 }, color: txtColor, maxTicksLimit: 10 } },
          y: { grid: { color: gridColor }, ticks: { font: { size: 10 }, color: txtColor }, min: -0.5, max: 0.7 },
        },
      },
    });
  }
}

// ── Rolling Performance Chart (range-controlled tail slice) ──
function renderPerfChart(data) {
  const isDark = document.documentElement.classList.contains('dark');
  const gridColor = isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.06)';
  const txtColor = isDark ? '#888' : '#999';
  const perfData = sliceTailByRange(data.rolling_metrics || [], rangeState['perfChart'] || '60D');
  if (perfData.length > 0) {
    const canvas = document.getElementById('perfChart');
    if (perfChartInst) { perfChartInst.destroy(); perfChartInst = null; }
    perfChartInst = new Chart(canvas.getContext('2d'), {
      type: 'line',
      data: {
        labels: perfData.map(m => m.date.slice(5)),
        datasets: [
          { label: 'Sharpe', data: perfData.map(m => m.sharpe_ratio), borderColor: '#9b59b6', borderWidth: 2, pointRadius: 0, tension: 0.3, yAxisID: 'y' },
          { label: 'Win Rate', data: perfData.map(m => m.win_rate * 100), borderColor: '#16a085', borderWidth: 2, pointRadius: 0, tension: 0.3, yAxisID: 'y1' },
        ],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { labels: { font: { size: 9 }, color: txtColor, boxWidth: 12 } } },
        scales: {
          x: { grid: { color: gridColor }, ticks: { font: { size: 9 }, color: txtColor, maxTicksLimit: 10 } },
          y: { grid: { color: gridColor }, ticks: { font: { size: 10 }, color: txtColor }, title: { display: true, text: 'Sharpe', font: { size: 9 }, color: txtColor } },
          y1: { position: 'right', grid: { drawOnChartArea: false }, ticks: { font: { size: 10 }, color: txtColor, callback: v => v.toFixed(0) + '%' }, title: { display: true, text: 'Win Rate %', font: { size: 9 }, color: txtColor } },
        },
      },
    });
  }
}

// ── HMM Regime Detection chart (range-controlled tail slice; current-state readout is unsliced) ──
function renderHmmChart(data) {
  const isDark = document.documentElement.classList.contains('dark');
  const gridColor = isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.06)';
  const txtColor = isDark ? '#888' : '#999';
  const hmm = data.hmm_regime || {};
  const hmmCur = hmm.current || {};
  const hmmState = hmmCur.state || 'UNKNOWN';
  const hmmEl = document.getElementById('hmmCurrentState');
  hmmEl.textContent = hmmState;
  hmmEl.style.color = hmmState === 'BULL' ? 'var(--positive)' : hmmState === 'BEAR' ? 'var(--negative)' : hmmState === 'TRANSITION' ? '#f39c12' : 'var(--text)';
  document.getElementById('hmmConfidence').textContent = hmmCur.confidence != null ? (hmmCur.confidence * 100).toFixed(0) + '%' : '—';
  document.getElementById('hmmAvgPos').textContent = hmmCur.avg_position != null ? hmmCur.avg_position.toFixed(4) : '—';
  document.getElementById('hmmPosVol').textContent = hmmCur.position_vol != null ? hmmCur.position_vol.toFixed(4) : '—';

  // HMM history chart
  const hmmHist = sliceTailByRange(hmm.history || [], rangeState['hmmChart'] || '60D');
  if (hmmHist.length > 0) {
    const canvas = document.getElementById('hmmChart');
    if (window._hmmChartInst) { window._hmmChartInst.destroy(); window._hmmChartInst = null; }
    const stateMap = { 'BULL': 1, 'NEUTRAL': 0, 'BEAR': -1, 'TRANSITION': 0.5, 'UNKNOWN': 0 };
    const stateColors = hmmHist.map(h => {
      const s = h.state;
      return s === 'BULL' ? 'rgba(39,174,96,0.5)' : s === 'BEAR' ? 'rgba(231,76,60,0.5)' : s === 'TRANSITION' ? 'rgba(243,156,18,0.5)' : 'rgba(149,165,166,0.3)';
    });
    window._hmmChartInst = new Chart(canvas.getContext('2d'), {
      type: 'bar',
      data: {
        labels: hmmHist.map(h => h.date.slice(5)),
        datasets: [{
          label: 'Regime',
          data: hmmHist.map(h => stateMap[h.state] || 0),
          backgroundColor: stateColors,
          borderWidth: 0,
        }, {
          label: 'Confidence',
          data: hmmHist.map(h => h.confidence || 0),
          type: 'line',
          borderColor: '#9b59b6',
          borderWidth: 1.5,
          pointRadius: 0,
          tension: 0.3,
          yAxisID: 'y1',
        }],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { labels: { font: { size: 9 }, color: txtColor, boxWidth: 10 } } },
        scales: {
          x: { grid: { color: gridColor }, ticks: { font: { size: 8 }, color: txtColor, maxTicksLimit: 10 } },
          y: { grid: { color: gridColor }, min: -1.2, max: 1.2, ticks: { font: { size: 9 }, color: txtColor, callback: v => v === 1 ? 'BULL' : v === -1 ? 'BEAR' : v === 0.5 ? 'TRANS' : v === 0 ? 'NEUT' : '' } },
          y1: { position: 'right', grid: { drawOnChartArea: false }, min: 0, max: 1, ticks: { font: { size: 9 }, color: txtColor, callback: v => (v * 100).toFixed(0) + '%' }, title: { display: true, text: 'Confidence', font: { size: 9 }, color: txtColor } },
        },
      },
    });
  }
}

function corrColor(v, isDark) {
  if (v === null) return 'transparent';
  const abs = Math.abs(v);
  if (v > 0) return isDark ? `rgba(39,174,96,${abs * 0.4})` : `rgba(39,174,96,${abs * 0.3})`;
  return isDark ? `rgba(231,76,60,${abs * 0.4})` : `rgba(231,76,60,${abs * 0.3})`;
}

// ════════════════════════════════════════════════════════════════════════════
//  HOURLY PREDICTOR ACCURACY (Part of Analytics Tab)
// ════════════════════════════════════════════════════════════════════════════
let hourlyRevChartInst = null, hourlySigChartInst = null, hourlyFwdChartInst = null;

const HOURLY_DAYS = { '30D': 30, '60D': 60, 'Q': 63, 'Max': 180 };
function hourlyUrl(key) {
  return '/api/analytics?section=hourly_accuracy&days=' + HOURLY_DAYS[rangeState[key] || 'Q'];
}
function hourlyCheckSuccess(d) {
  if (!d.success) throw new Error(d.error || 'No hourly data');
  return d;
}
// hourlyRev is the only control whose failure should surface in the shared
// #hourlyConvergence summary — hourlySig/hourlyFwd failures must not clobber
// a valid convergence summary that hourlyRev already rendered successfully.
function hourlyRevFail(e) {
  console.warn('Hourly accuracy load error:', e);
  const el = document.getElementById('hourlyConvergence');
  if (el) el.textContent = 'Hourly accuracy data unavailable — requires sufficient snapshot history.';
}
function hourlyChartFail(e) {
  console.warn('Hourly accuracy load error:', e);
}

// Shared single-chart loader: used by loadHourlyAccuracy's initial parallel
// load AND by each control's onChange, so the fetch→check→render→fail chain
// isn't repeated per call site.
function loadHourlySingle(key, renderFn, onFail) {
  return fetchRanged(hourlyUrl(key)).then(hourlyCheckSuccess).then(renderFn).catch(onFail || hourlyChartFail);
}

function loadHourlyAccuracy() {
  loadHourlySingle('hourlyRev', renderHourlyAccuracy, hourlyRevFail);
  loadHourlySingle('hourlySig', renderHourlySignalChart);
  loadHourlySingle('hourlyFwd', renderHourlyForwardChart);
}

function renderHourlyAccuracy(data) {
  const isDark = document.documentElement.classList.contains('dark');
  const gridColor = isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.06)';
  const txtColor = isDark ? '#888' : '#999';
  const monoFont = { family: "'JetBrains Mono'", size: 9 };

  // ── Revenue MAPE Chart ──
  const revData = data.revenue_accuracy || [];
  if (revData.length > 0) {
    const labels = revData.map(r => r.label);
    const maeVals = revData.map(r => r.mae_pct);
    const p90Vals = revData.map(r => r.p90_error_pct);
    if (hourlyRevChartInst) hourlyRevChartInst.destroy();
    hourlyRevChartInst = new Chart(document.getElementById('hourlyRevenueChart'), {
      type: 'line',
      data: {
        labels,
        datasets: [
          { label: 'MAE %', data: maeVals, borderColor: isDark ? '#5B9CF5' : '#0958D9', borderWidth: 2, fill: false, pointRadius: 3, tension: 0.3 },
          { label: 'P90 Error %', data: p90Vals, borderColor: isDark ? '#FF6B47' : '#D4380D', borderWidth: 1.5, borderDash: [3,3], fill: false, pointRadius: 2, tension: 0.3 },
        ]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { display: true, position: 'bottom', labels: { font: monoFont } } },
        scales: {
          y: { title: { display: true, text: 'Error %', font: monoFont, color: txtColor }, ticks: { color: txtColor, font: monoFont }, grid: { color: gridColor } },
          x: { ticks: { color: txtColor, font: monoFont }, grid: { display: false } }
        }
      }
    });
  }

  // ── Convergence summary ──
  const conv = data.convergence || {};
  const convEl = document.getElementById('hourlyConvergence');
  if (convEl) {
    let text = '';
    if (conv.revenue_5pct) text += `Revenue projection stabilizes to <5% error by ${conv.revenue_5pct.label}. `;
    if (conv.signal_90pct) text += `Signal matches EOD 90%+ by ${conv.signal_90pct.label}. `;
    // Curve bias
    const bias = data.curve_bias || [];
    const overBias = bias.filter(b => b.mean_bias_pct > 2);
    const underBias = bias.filter(b => b.mean_bias_pct < -2);
    if (overBias.length) text += `Over-projects at: ${overBias.map(b => b.label).join(', ')}. `;
    if (underBias.length) text += `Under-projects at: ${underBias.map(b => b.label).join(', ')}. `;
    const dq = data.data_quality || {};
    text += `(${dq.days_with_snapshots || 0} days analyzed)`;
    convEl.textContent = text || 'Insufficient data for convergence analysis.';
  }
}

function renderHourlySignalChart(data) {
  const isDark = document.documentElement.classList.contains('dark');
  const gridColor = isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.06)';
  const txtColor = isDark ? '#888' : '#999';
  const monoFont = { family: "'JetBrains Mono'", size: 9 };

  // ── Signal Stability Chart ──
  const sigData = data.signal_stability || [];
  if (sigData.length > 0) {
    const labels = sigData.map(s => s.label);
    const matchRates = sigData.map(s => s.signal_match_rate != null ? s.signal_match_rate * 100 : null);
    if (hourlySigChartInst) hourlySigChartInst.destroy();
    hourlySigChartInst = new Chart(document.getElementById('hourlySignalChart'), {
      type: 'line',
      data: {
        labels,
        datasets: [
          { label: 'Signal Match %', data: matchRates, borderColor: isDark ? '#52C41A' : '#1B7D3A', borderWidth: 2, fill: true,
            backgroundColor: isDark ? 'rgba(82,196,26,0.1)' : 'rgba(27,125,58,0.08)', pointRadius: 3, tension: 0.3 },
        ]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { display: true, position: 'bottom', labels: { font: monoFont } } },
        scales: {
          y: { min: 0, max: 105, title: { display: true, text: 'Match Rate %', font: monoFont, color: txtColor }, ticks: { color: txtColor, font: monoFont }, grid: { color: gridColor } },
          x: { ticks: { color: txtColor, font: monoFont }, grid: { display: false } }
        }
      }
    });
  }
}

function renderHourlyForwardChart(data) {
  const isDark = document.documentElement.classList.contains('dark');
  const gridColor = isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.06)';
  const txtColor = isDark ? '#888' : '#999';
  const monoFont = { family: "'JetBrains Mono'", size: 9 };

  // ── Forward Accuracy Chart ──
  const fwdData = data.forward_accuracy || [];
  if (fwdData.length > 0) {
    const labels = fwdData.map(f => f.label);
    const hit1d = fwdData.map(f => f.hit_rate_1d != null ? f.hit_rate_1d * 100 : null);
    const hit5d = fwdData.map(f => f.hit_rate_5d != null ? f.hit_rate_5d * 100 : null);
    const ic5d = fwdData.map(f => f.ic_5d != null ? f.ic_5d * 100 : null);
    if (hourlyFwdChartInst) hourlyFwdChartInst.destroy();
    hourlyFwdChartInst = new Chart(document.getElementById('hourlyForwardChart'), {
      type: 'bar',
      data: {
        labels,
        datasets: [
          { label: 'Hit Rate 1d %', data: hit1d, backgroundColor: isDark ? '#5B9CF5' : '#0958D9', borderRadius: 2 },
          { label: 'Hit Rate 5d %', data: hit5d, backgroundColor: isDark ? '#FF6B47' : '#D4380D', borderRadius: 2 },
          { label: 'IC 5d (x100)', data: ic5d, type: 'line', borderColor: isDark ? '#52C41A' : '#1B7D3A', borderWidth: 1.5, fill: false, pointRadius: 2, yAxisID: 'y1' },
        ]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { display: true, position: 'bottom', labels: { font: monoFont } } },
        scales: {
          y: { min: 30, max: 80, title: { display: true, text: 'Hit Rate %', font: monoFont, color: txtColor }, ticks: { color: txtColor, font: monoFont }, grid: { color: gridColor } },
          y1: { position: 'right', title: { display: true, text: 'IC x100', font: monoFont, color: txtColor }, ticks: { color: txtColor, font: monoFont }, grid: { display: false } },
          x: { ticks: { color: txtColor, font: monoFont }, grid: { display: false } }
        }
      }
    });
  }
}

// ════════════════════════════════════════════════════════════════════════════
//  MARGINS TAB
// ════════════════════════════════════════════════════════════════════════════
var marginCache = null;
var marginCacheTs = 0;
var marginHistChart = null;
var marginHistData = null; // stored for chart re-renders on commodity selection change

var MARGIN_COLORS = [
  '#ef4444', '#f59e0b', '#9ca3af', '#3b82f6', '#f97316',
  '#10b981', '#8b5cf6', '#ec4899', '#14b8a6', '#6366f1',
  '#84cc16', '#f43f5e', '#06b6d4', '#a855f7', '#d97706',
];

function loadMargins() {
  if (marginCache && (Date.now() - marginCacheTs < 300000)) { renderMargins(marginCache); return; }
  document.getElementById('marginAsOf').textContent = 'Loading…';
  fetch('/api/commodity_dashboard?view=margins')
    .then(function(r) { return r.json(); })
    .then(function(d) {
      if (d.success) { marginCache = d; marginCacheTs = Date.now(); renderMargins(d); }
      else { document.getElementById('marginAsOf').textContent = d.error || 'No data'; }
    })
    .catch(function(e) { document.getElementById('marginAsOf').textContent = 'Error: ' + e; });
}

var marginFullData = null;       // full API response, stored for re-filtering
var marginSelectedSymbols = {};  // map sym→true for selected commodities

function renderMargins(data) {
  var asOfText = 'As of ' + data.as_of + ' \u2022 ' + data.snapshot_dates + ' snapshot(s)';
  document.getElementById('marginAsOf').innerHTML = asOfText +
    (data.snapshot_dates === 1 ? '<br><span style="color:var(--text-secondary);font-style:italic">First snapshot collected today. Change tracking begins with tomorrow\u2019s data.</span>' : '');

  marginFullData = data;
  marginHistData = (data.margin_history && data.margin_history.dates && data.margin_history.dates.length > 1)
    ? { hist: data.margin_history, commodities: data.commodities } : null;

  // Build commodity chips — all selected by default
  marginSelectedSymbols = {};
  var container = document.getElementById('marginChipContainer');
  container.innerHTML = '';
  data.commodities.forEach(function(sym) {
    marginSelectedSymbols[sym] = true;
    var chip = document.createElement('span');
    chip.className = 'margin-chip active';
    chip.textContent = sym;
    chip.setAttribute('data-sym', sym);
    chip.onclick = function() { toggleMarginCommodity(sym); };
    container.appendChild(chip);
  });
  document.getElementById('marginCommodityFilter').style.display = '';

  applyMarginFilter();
}

function toggleMarginCommodity(sym) {
  if (marginSelectedSymbols[sym]) {
    delete marginSelectedSymbols[sym];
  } else {
    marginSelectedSymbols[sym] = true;
  }
  var chips = document.getElementById('marginChipContainer').children;
  for (var i = 0; i < chips.length; i++) {
    if (chips[i].getAttribute('data-sym') === sym) {
      chips[i].className = marginSelectedSymbols[sym] ? 'margin-chip active' : 'margin-chip';
      break;
    }
  }
  applyMarginFilter();
}

function setAllMarginCommodities(selectAll) {
  if (!marginFullData) return;
  marginSelectedSymbols = {};
  if (selectAll) {
    marginFullData.commodities.forEach(function(sym) { marginSelectedSymbols[sym] = true; });
  }
  var chips = document.getElementById('marginChipContainer').children;
  for (var i = 0; i < chips.length; i++) {
    chips[i].className = selectAll ? 'margin-chip active' : 'margin-chip';
  }
  applyMarginFilter();
}

function renderMarginChangesTable() {
  if (!marginFullData) return;
  var data = marginFullData;
  var selected = marginSelectedSymbols;
  var changesSection = document.getElementById('marginChangesSection');
  var winDates = sliceTailByRange(data.margin_history.dates, rangeState['marginChanges'] || '60D');
  var minDate = winDates.length ? winDates[0] : null;
  var changes = (data.margin_changes || []).filter(function(c) {
    return selected[c.symbol] && (minDate == null || c.date >= minDate);
  });
  if (changes.length > 0) {
    changesSection.style.display = '';
    var ctbl = document.getElementById('marginChangesTable');
    var ch = '<thead><tr><th>Date</th><th>Symbol</th><th>Old Total %</th><th>New Total %</th><th>Change</th></tr></thead><tbody>';
    changes.forEach(function(c) {
      var cls = c.direction === 'up' ? 'chg-neg' : 'chg-pos';
      var arrow = c.direction === 'up' ? '\u25B2' : '\u25BC';
      ch += '<tr>' +
        '<td>' + c.date + '</td>' +
        '<td style="font-weight:600">' + c.symbol + '</td>' +
        '<td>' + c.old_total.toFixed(1) + '</td>' +
        '<td>' + c.new_total.toFixed(1) + '</td>' +
        '<td><span class="' + cls + '">' + arrow + ' ' + Math.abs(c.change).toFixed(1) + '%</span></td>' +
        '</tr>';
    });
    ch += '</tbody>';
    ctbl.innerHTML = ch;
  } else {
    changesSection.style.display = 'none';
  }
}

function applyMarginFilter() {
  if (!marginFullData) return;
  var data = marginFullData;
  var selected = marginSelectedSymbols;
  var isDark = document.documentElement.classList.contains('dark');
  var multiSnapshot = data.snapshot_dates > 1;

  // ── Current Margins Table ──
  var tbl = document.getElementById('marginCurrentTable');
  var h = '<thead><tr>' +
    '<th>Symbol</th><th>Initial %</th><th>Total %</th><th>Change</th>' +
    '<th>ELM Long %</th><th>Delivery %</th><th>Last Changed</th>' +
    '</tr></thead><tbody>';

  var filtered = data.current_margins.filter(function(m) { return selected[m.symbol]; });
  filtered.forEach(function(m) {
    var changeCell = '';
    var rowBg = '';
    if (m.change_pct !== null && m.change_pct !== undefined && m.change_pct !== 0) {
      var cls = m.change_pct > 0 ? 'chg-neg' : 'chg-pos';
      var arrow = m.change_pct > 0 ? '\u25B2' : '\u25BC';
      changeCell = '<span class="' + cls + '">' + arrow + ' ' + Math.abs(m.change_pct).toFixed(1) + '%</span>';
      if (m.change_pct > 0) {
        rowBg = isDark ? 'background:rgba(255,77,79,0.1)' : 'background:rgba(207,19,34,0.08)';
      } else {
        rowBg = isDark ? 'background:rgba(82,196,26,0.1)' : 'background:rgba(27,125,58,0.08)';
      }
    } else if (m.change_pct === 0) {
      changeCell = '<span style="color:var(--text-secondary)">\u2014</span>';
    } else {
      changeCell = multiSnapshot ? '<span style="color:var(--text-secondary)">N/A</span>' : '\u2014';
    }

    var lastChangedHtml = '\u2014';
    if (m.last_change_date) {
      var daysSince = Math.floor((new Date(data.as_of) - new Date(m.last_change_date)) / 86400000);
      if (daysSince <= 7) {
        lastChangedHtml = '<span style="font-weight:600;color:var(--text-primary)">' + m.last_change_date + '</span>';
      } else {
        lastChangedHtml = m.last_change_date;
      }
    }

    h += '<tr style="' + rowBg + '">' +
      '<td style="font-weight:600">' + m.symbol + '</td>' +
      '<td>' + (m.initial_margin_pct != null ? m.initial_margin_pct.toFixed(1) : '\u2014') + '</td>' +
      '<td style="font-weight:600">' + (m.total_margin_pct != null ? m.total_margin_pct.toFixed(1) : '\u2014') + '</td>' +
      '<td>' + changeCell + '</td>' +
      '<td>' + (m.elm_long_pct != null ? m.elm_long_pct.toFixed(1) : '\u2014') + '</td>' +
      '<td>' + (m.delivery_margin_pct != null ? m.delivery_margin_pct.toFixed(1) : '\u2014') + '</td>' +
      '<td style="font-size:10px;color:var(--text-secondary)">' + lastChangedHtml + '</td>' +
      '</tr>';
  });
  if (!filtered.length) h += '<tr><td colspan="7" style="text-align:center;color:var(--text-secondary);padding:16px">No commodities selected</td></tr>';
  h += '</tbody>';
  tbl.innerHTML = h;

  // ── Margin Changes Log ──
  renderMarginChangesTable();

  // ── Margin History Chart ──
  var chartSection = document.getElementById('marginChartSection');
  if (marginHistData) {
    var syms = Object.keys(selected);
    if (syms.length > 0) {
      chartSection.style.display = '';
      updateMarginChart(syms);
    } else {
      chartSection.style.display = 'none';
    }
  } else {
    chartSection.style.display = 'none';
  }
}

function updateMarginChart(syms) {
  if (!marginHistData) return;
  var hist = marginHistData.hist;
  if (!syms || !syms.length) return;

  var allDates = hist.dates;
  var dates = sliceTailByRange(allDates, rangeState['marginHist'] || '60D');
  var startIdx = allDates.length - dates.length;

  var isDark = document.documentElement.classList.contains('dark');
  var gridColor = isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.06)';
  var txtColor = isDark ? '#888' : '#999';
  var chartColors = ['#ef4444','#3b82f6','#10b981','#f59e0b','#8b5cf6','#ec4899',
    '#14b8a6','#f97316','#6366f1','#84cc16','#06b6d4','#d97706',
    '#a855f7','#f43f5e','#9ca3af','#22d3ee','#4ade80','#fb923c','#c084fc'];

  var labels = dates.map(function(d) {
    var dt = new Date(d);
    return dt.getDate() + ' ' + dt.toLocaleString('en', {month:'short', year:'2-digit'});
  });

  var datasets = syms.map(function(sym, i) {
    return {
      label: sym,
      data: (hist[sym] || []).slice(startIdx),
      borderColor: chartColors[i % chartColors.length],
      backgroundColor: 'transparent',
      borderWidth: syms.length > 6 ? 1.2 : 2,
      pointRadius: syms.length > 6 ? 0 : (dates.length < 30 ? 3 : 0),
      pointHoverRadius: 4,
      tension: 0.2,
    };
  });

  var ctx = document.getElementById('marginHistoryChart').getContext('2d');
  if (marginHistChart) marginHistChart.destroy();
  marginHistChart = new Chart(ctx, {
    type: 'line',
    data: { labels: labels, datasets: datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { labels: { color: txtColor, font: {size: syms.length > 6 ? 9 : 11}, usePointStyle: true, pointStyle: 'line' } },
        tooltip: { mode: 'index', intersect: false },
      },
      scales: {
        x: { ticks: { color: txtColor, font: {size: 9}, maxRotation: 45, maxTicksLimit: 15 }, grid: { display: false } },
        y: { title: { display: true, text: 'Total Margin %', color: txtColor }, ticks: { color: txtColor }, grid: { color: gridColor } }
      }
    }
  });
}


// ════════════════════════════════════════════════════════════════════════════
//  MOMENTUM TAB — Entry / Exit Signals (10D/45D Revenue Regime + ADR)
// ════════════════════════════════════════════════════════════════════════════

let momRegimeChartInst = null;
let momPriceChartInst = null;
let momShowBands = false;

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

function renderMomentumHero(d) {
  const snap = d.snapshot;
  const hist = d.history || [];
  const stats = d.regime_stats || {};

  // As-of
  document.getElementById('momAsOf').textContent = 'As of ' + (d.as_of || snap.date);

  // Hero: Composite Signal
  const sigDesc = {
    'STRONG_BUY': 'Highest conviction entry',
    'BUY': 'Good entry, confirm with price action',
    'HOLD': 'Maintain positions, no new longs',
    'WATCH': 'Possible reversal, wait for confirmation',
    'SELL': 'Exit longs, edge fully disappears'
  };
  const sig = snap.composite_signal || '--';
  document.getElementById('momSignalBadge').innerHTML =
    '<span class="mom-signal ' + sig + '">' + sig.replace('_', ' ') + '</span>';
  document.getElementById('momSignalDesc').textContent = sigDesc[sig] || '';

  // Hero: Regime
  const regime = snap.regime || '--';
  document.getElementById('momRegimeBadge').innerHTML =
    '<span class="mom-regime ' + regime + '">' + regime + '</span>';
  document.getElementById('momRatioText').textContent =
    '10D/45D Ratio: ' + (snap.ratio_10d_45d != null ? snap.ratio_10d_45d.toFixed(3) : '--');
  document.getElementById('momMaText').textContent =
    'MA10: ' + fmtCr(snap.ma10_rev_cr) + ' vs MA45: ' + fmtCr(snap.ma45_rev_cr);

  // Hero: ADR Signal
  const adrSig = snap.adr_signal || '--';
  const adrColor = adrSig === 'BREAKOUT' ? 'var(--positive)' :
                   adrSig === 'BULL_CONT' ? '#0958D9' :
                   adrSig === 'OVERSOLD' ? 'var(--warning)' : 'var(--text-secondary)';
  document.getElementById('momAdrBadge').innerHTML =
    '<span style="display:inline-block;font-family:var(--mono);font-size:13px;font-weight:800;padding:5px 14px;border:2px solid ' + adrColor + ';color:' + adrColor + ';text-transform:uppercase;letter-spacing:1px;margin-top:8px">' + adrSig.replace('_', ' ') + '</span>';
  document.getElementById('momAdrRatioText').textContent =
    'ADR Ratio: ' + (snap.adr_ratio != null ? snap.adr_ratio.toFixed(3) : '--');
  document.getElementById('momPriceMomText').textContent =
    'Price Mom 5D: ' + (snap.price_mom_5d != null ? (snap.price_mom_5d * 100).toFixed(2) + '%' : '--');

  // KPI cards
  document.getElementById('momPrice').textContent = snap.close_price != null ? '\u20B9' + snap.close_price.toLocaleString('en-IN', {maximumFractionDigits:1}) : '--';
  document.getElementById('momStreak').textContent = (stats.current_streak || '--') + 'd ' + (stats.streak_regime || '');
  document.getElementById('momRevenue').textContent = fmtCr(snap.fno_rev_cr);
  document.getElementById('momDailyRange').textContent = snap.close_price && hist.length > 0 && hist[hist.length-1].daily_range != null
    ? '\u20B9' + hist[hist.length-1].daily_range.toFixed(1)
    : '--';
}

function fmtCr(v) {
  return v != null ? '\u20B9' + v.toFixed(2) + ' Cr' : '--';
}

// Long ranges span multiple years \u2014 keep the year in the X-axis label so
// "03-15" isn't ambiguous between 2024 and 2026.
function momFmtDateLabel(d, rangeKey) {
  const longRange = (rangeKey === '1Y' || rangeKey === '2Y' || rangeKey === 'Max');
  return longRange ? d : d.slice(5);
}

// Sample mean + standard deviation (n-1) over the visible window.
function momMeanStd(arr) {
  const vals = arr.filter(v => v != null && !Number.isNaN(v));
  const n = vals.length;
  if (n < 2) return null;
  const mean = vals.reduce((s, v) => s + v, 0) / n;
  const variance = vals.reduce((s, v) => s + (v - mean) * (v - mean), 0) / (n - 1);
  return { mean: mean, sd: Math.sqrt(variance), n: n };
}

// Theme-aware semantic palette for control bands \u2014 \u03bc (green), \u00b11\u03c3 (amber),
// \u00b12\u03c3 (red). Exposed as a function so the tooltip + stats strip can use
// the same colors as the chart.
function momBandColors(isDark) {
  return isDark
    ? { mean: '#22C55E', s1: '#FBBF24', s2: '#EF4444' }
    : { mean: '#16A34A', s1: '#D97706', s2: '#DC2626' };
}

// Chart.js plugin: draws Mean \u00b1 1\u03c3 / \u00b12\u03c3 as shaded background zones plus
// a single solid mean line. Zones use the same green/amber/red semantic
// the chart has used previously \u2014 inside \u00b11\u03c3 is green ("normal"), the
// 1\u03c3\u21922\u03c3 ring is amber ("warning"), beyond \u00b12\u03c3 is red ("extreme").
//
// Two render hooks:
//   beforeDatasetsDraw \u2014 paints the fills behind the data line
//   afterDatasetsDraw  \u2014 paints the mean line on top of the data
function makeBandsPlugin(stats, yAxisId, isDark) {
  const col = momBandColors(isDark);
  // Slightly stronger alpha in dark mode so the tints register.
  const fill = isDark
    ? {
        green: 'rgba(34,197,94,0.10)',
        amber: 'rgba(251,191,36,0.10)',
        red:   'rgba(239,68,68,0.08)',
      }
    : {
        green: 'rgba(22,163,74,0.07)',
        amber: 'rgba(217,119,6,0.07)',
        red:   'rgba(220,38,38,0.05)',
      };

  return {
    id: 'controlBands',

    beforeDatasetsDraw(chart) {
      if (!momShowBands || !stats) return;
      const ax = chart.scales[yAxisId];
      if (!ax) return;
      const { ctx: c, chartArea: { left, right, top, bottom } } = chart;
      const w = right - left;

      // Clamp helper so zones don't spill outside chartArea.
      const clip = function(y) { return Math.max(top, Math.min(bottom, y)); };
      const yMean    = clip(ax.getPixelForValue(stats.mean));
      const yPlus1   = clip(ax.getPixelForValue(stats.mean + stats.sd));
      const yMinus1  = clip(ax.getPixelForValue(stats.mean - stats.sd));
      const yPlus2   = clip(ax.getPixelForValue(stats.mean + 2 * stats.sd));
      const yMinus2  = clip(ax.getPixelForValue(stats.mean - 2 * stats.sd));

      c.save();

      // Outside \u00b12\u03c3 \u2014 red (above +2\u03c3 + below -2\u03c3)
      c.fillStyle = fill.red;
      if (yPlus2 > top)    c.fillRect(left, top, w, yPlus2 - top);
      if (yMinus2 < bottom) c.fillRect(left, yMinus2, w, bottom - yMinus2);

      // 1\u03c3 \u2192 2\u03c3 rings \u2014 amber (both sides)
      c.fillStyle = fill.amber;
      if (yPlus1 > yPlus2)    c.fillRect(left, yPlus2, w, yPlus1 - yPlus2);
      if (yMinus2 > yMinus1)  c.fillRect(left, yMinus1, w, yMinus2 - yMinus1);

      // Inside \u00b11\u03c3 \u2014 green
      c.fillStyle = fill.green;
      if (yMinus1 > yPlus1) c.fillRect(left, yPlus1, w, yMinus1 - yPlus1);

      c.restore();
    },

    afterDatasetsDraw(chart) {
      if (!momShowBands || !stats) return;
      const ax = chart.scales[yAxisId];
      if (!ax) return;
      const { ctx: c, chartArea: { left, right, top, bottom } } = chart;

      // Lines drawn on top of the data, muted so the data line and
      // zone fills lead the visual hierarchy. μ retains the most
      // presence; ±2σ is most muted.
      const lines = [
        { v: stats.mean + 2 * stats.sd, color: col.s2,   dash: [3, 4],  lw: 1.5, alpha: 0.45 },
        { v: stats.mean - 2 * stats.sd, color: col.s2,   dash: [3, 4],  lw: 1.5, alpha: 0.45 },
        { v: stats.mean + stats.sd,     color: col.s1,   dash: [10, 4], lw: 1.75, alpha: 0.55 },
        { v: stats.mean - stats.sd,     color: col.s1,   dash: [10, 4], lw: 1.75, alpha: 0.55 },
        { v: stats.mean,                color: col.mean, dash: [],      lw: 2.5, alpha: 0.75 },
      ];

      c.save();
      c.lineCap = 'round';
      lines.forEach(function(l) {
        const y = ax.getPixelForValue(l.v);
        if (y < top || y > bottom) return;
        c.globalAlpha = l.alpha;
        c.strokeStyle = l.color;
        c.setLineDash(l.dash);
        c.lineWidth = l.lw;
        c.beginPath();
        c.moveTo(left, y);
        c.lineTo(right, y);
        c.stroke();
      });
      c.globalAlpha = 1;
      c.setLineDash([]);
      c.restore();
    }
  };
}

// Formats a band value: ratios \u2192 3 dp, prices \u2192 \u20b9 + 1 dp.
function momFmtBandVal(v, isPrice) {
  if (v == null || Number.isNaN(v)) return '--';
  return isPrice
    ? '\u20b9' + v.toLocaleString('en-IN', { maximumFractionDigits: 1 })
    : v.toFixed(3);
}

// Small DOM helpers — used by stats strip + tooltip to avoid innerHTML.
function _momEl(tag, style, text) {
  const e = document.createElement(tag);
  if (style) e.style.cssText = style;
  if (text != null) e.textContent = text;
  return e;
}
function _momAppend(parent, children) {
  children.forEach(function(c) { if (c) parent.appendChild(c); });
  return parent;
}

// Renders the stats strip below a chart title — colored σ symbols match
// their band line colors for visual continuity with the chart.
function momRenderStatsLine(elId, stats, isPrice) {
  const el = document.getElementById(elId);
  if (!el) return;
  el.replaceChildren();
  if (!stats) { el.style.display = 'none'; return; }

  const isDark = document.documentElement.classList.contains('dark');
  const col = momBandColors(isDark);
  const f = function(v) { return momFmtBandVal(v, isPrice); };
  const dot = function() { return _momEl('span', 'opacity:0.4;margin:0 8px', '·'); };
  const sym = function(color, text) { return _momEl('span', 'color:' + color + ';font-weight:700', text); };

  _momAppend(el, [
    _momEl('span', null, 'n=' + stats.n),
    dot(),
    sym(col.mean, 'μ'), _momEl('span', null, ' ' + f(stats.mean)),
    dot(),
    _momEl('span', 'opacity:0.7', 'σ'), _momEl('span', null, ' ' + f(stats.sd)),
    dot(),
    sym(col.s1, '±1σ'), _momEl('span', null, ' [' + f(stats.mean - stats.sd) + ', ' + f(stats.mean + stats.sd) + ']'),
    dot(),
    sym(col.s2, '±2σ'), _momEl('span', null, ' [' + f(stats.mean - 2 * stats.sd) + ', ' + f(stats.mean + 2 * stats.sd) + ']'),
  ]);
  el.style.display = momShowBands ? '' : 'none';
}

// ── Custom HTML tooltip ─────────────────────────────────────────────────
// Card-style DOM tooltip with sections, color swatches, and theme-aware
// styling. Built with createElement + textContent — no innerHTML.

function ensureMomTooltipEl() {
  let el = document.getElementById('momChartTooltip');
  if (el) return el;
  el = document.createElement('div');
  el.id = 'momChartTooltip';
  el.style.cssText = [
    'position:absolute',
    'pointer-events:none',
    'z-index:9999',
    'background:var(--bg-card,#fff)',
    'border:1px solid var(--border,#E0DDD8)',
    'border-radius:8px',
    'box-shadow:0 8px 24px rgba(0,0,0,0.18)',
    'padding:0',
    "font-family:'JetBrains Mono',monospace",
    'font-size:11px',
    'color:var(--text-primary,#1A1A1A)',
    'opacity:0',
    'transition:opacity 0.12s',
    'min-width:240px',
    'max-width:340px',
    'overflow:hidden'
  ].join(';');
  document.body.appendChild(el);
  return el;
}

function momHideTooltip() {
  const el = document.getElementById('momChartTooltip');
  if (el) el.style.opacity = '0';
}

function momFmtDataValue(label, v, isPrice) {
  if (v == null) return '--';
  if (label && label.indexOf('Rev') !== -1) return '₹' + v.toFixed(2) + ' Cr';
  if (label === 'MCX Price') return '₹' + v.toLocaleString('en-IN', { maximumFractionDigits: 1 });
  if (label && label.indexOf('Ratio') !== -1) return v.toFixed(3);
  return v.toFixed(2);
}

const REGIME_COLORS = { HOT: '#D4380D', COLD: '#0958D9', NEUTRAL: '#6B6560' };
const SIGNAL_COLORS = { STRONG_BUY: '#1B7D3A', BUY: '#0958D9', SELL: '#CF1322', WATCH: '#FA8C16', HOLD: '#6B6560' };
const LABEL_STYLE = 'color:var(--text-secondary);font-size:10px';

function makeMomTooltipHandler(opts) {
  // opts: { hist, stats, isPrice, includeSignalContext }
  return function(ctx) {
    const { chart, tooltip } = ctx;
    const el = ensureMomTooltipEl();

    if (tooltip.opacity === 0) { el.style.opacity = '0'; return; }
    const dp0 = tooltip.dataPoints && tooltip.dataPoints[0];
    if (!dp0) { el.style.opacity = '0'; return; }
    const idx = dp0.dataIndex;
    const row = opts.hist[idx];
    if (!row) { el.style.opacity = '0'; return; }

    const isDark = document.documentElement.classList.contains('dark');
    const col = momBandColors(isDark);

    el.replaceChildren();

    // Header — date
    el.appendChild(_momEl(
      'div',
      'padding:8px 12px;font-weight:700;font-size:12px;letter-spacing:0.2px;border-bottom:1px solid var(--border)',
      row.date
    ));

    // Series rows
    const body = _momEl('div', 'padding:6px 12px');
    tooltip.dataPoints.forEach(function(dp) {
      const ds = chart.data.datasets[dp.datasetIndex];
      if (dp.raw == null) return;
      const swatch = (typeof ds.borderColor === 'string' && ds.borderColor !== 'transparent') ? ds.borderColor : (ds.backgroundColor || '#999');
      const val = momFmtDataValue(ds.label, dp.raw, opts.isPrice);
      _momAppend(body, [_momAppend(_momEl('div', 'display:flex;align-items:center;gap:8px;padding:3px 0'), [
        _momEl('span', 'display:inline-block;width:10px;height:10px;background:' + swatch + ';border-radius:50%;flex-shrink:0'),
        _momEl('span', 'flex:1;' + LABEL_STYLE, ds.label),
        _momEl('span', 'font-weight:700;font-variant-numeric:tabular-nums', val),
      ])]);
    });
    el.appendChild(body);

    // Context rows (Price chart only)
    if (opts.includeSignalContext) {
      const regime = row.regime || '--';
      const adr = (row.adr_signal || '--').replace('_', ' ');
      const sig = row.composite_signal || '--';
      const regC = REGIME_COLORS[regime] || '#6B6560';
      const sigC = SIGNAL_COLORS[sig] || '#6B6560';
      _momAppend(el, [_momAppend(
        _momEl('div', 'padding:6px 12px;border-top:1px solid var(--border);display:grid;grid-template-columns:56px 1fr;gap:3px 12px'),
        [
          _momEl('span', LABEL_STYLE, 'Regime'),
          _momEl('span', 'font-weight:700;color:' + regC, regime),
          _momEl('span', LABEL_STYLE, 'ADR'),
          _momEl('span', 'font-weight:700', adr),
          _momEl('span', LABEL_STYLE, 'Signal'),
          _momEl('span', 'font-weight:700;color:' + sigC, sig.replace('_', ' ')),
        ]
      )]);
    }

    // Bands section
    if (momShowBands && opts.stats) {
      const s = opts.stats;
      const f = function(v) { return momFmtBandVal(v, opts.isPrice); };
      const section = _momEl('div', 'padding:6px 12px;border-top:1px solid var(--border);background:color-mix(in srgb, var(--text-primary) 3%, transparent)');
      section.appendChild(_momEl(
        'div',
        'font-size:9px;color:var(--text-secondary);letter-spacing:1.5px;font-weight:700;margin-bottom:4px',
        'CONTROL BANDS  ·  n=' + s.n
      ));
      const makeRow = function(swatchEl, sym, symColor, valueText, extraText) {
        const r = _momEl('div', 'display:flex;align-items:center;gap:10px;padding:2px 0');
        const swrap = _momEl('span', 'display:inline-flex;width:18px;height:10px;align-items:center;justify-content:center');
        swrap.appendChild(swatchEl);
        r.appendChild(swrap);
        r.appendChild(_momEl('span', 'color:' + symColor + ';font-weight:700;min-width:30px', sym));
        const valEl = _momEl('span', 'font-weight:700;font-variant-numeric:tabular-nums', valueText);
        if (extraText) {
          valEl.appendChild(_momEl('span', LABEL_STYLE + ';margin-left:8px;font-weight:400', extraText));
        }
        r.appendChild(valEl);
        return r;
      };
      // Swatches mirror the chart's line styling.
      const solidSw = _momEl('span', 'display:inline-block;width:18px;height:3px;background:' + col.mean + ';border-radius:1px');
      const dashSw  = _momEl('span', 'display:inline-block;width:18px;height:0;border-top:2px dashed ' + col.s1);
      const dotSw   = _momEl('span', 'display:inline-block;width:18px;height:0;border-top:2px dotted ' + col.s2);
      section.appendChild(makeRow(solidSw, 'μ',   col.mean, f(s.mean), 'σ ' + f(s.sd)));
      section.appendChild(makeRow(dashSw,  '±1σ', col.s1,   '[' + f(s.mean - s.sd) + ', ' + f(s.mean + s.sd) + ']'));
      section.appendChild(makeRow(dotSw,   '±2σ', col.s2,   '[' + f(s.mean - 2 * s.sd) + ', ' + f(s.mean + 2 * s.sd) + ']'));
      el.appendChild(section);
    }

    // Position — flip horizontally near right edge, clamp vertically.
    const cRect = chart.canvas.getBoundingClientRect();
    const tw = el.offsetWidth;
    const th = el.offsetHeight;
    const off = 14;
    let x = cRect.left + window.scrollX + tooltip.caretX + off;
    const y0 = cRect.top + window.scrollY + tooltip.caretY - th / 2;
    if (x + tw > window.innerWidth + window.scrollX - 10) {
      x = cRect.left + window.scrollX + tooltip.caretX - tw - off;
    }
    const y = Math.max(window.scrollY + 10, Math.min(y0, window.scrollY + window.innerHeight - th - 10));
    el.style.left = x + 'px';
    el.style.top = y + 'px';
    el.style.opacity = '1';
  };
}

function setMomShowBands(on) {
  momShowBands = !!on;
  const chip = document.getElementById('momBandsToggle');
  if (chip) chip.className = momShowBands ? 'margin-chip active' : 'margin-chip';
  const rs = document.getElementById('momRegimeStats');
  const ps = document.getElementById('momPriceStats');
  if (rs && rs.childNodes.length) rs.style.display = momShowBands ? '' : 'none';
  if (ps && ps.childNodes.length) ps.style.display = momShowBands ? '' : 'none';
  momHideTooltip();
  fetchRanged(momUrl('momRegime')).then(d => { if (d.success) renderMomRegimeChart(d.history||[]); });
  fetchRanged(momUrl('momPrice')).then(d => { if (d.success) renderMomPriceChart(d.history||[]); });
}

function renderMomRegimeChart(hist) {
  const ctx = document.getElementById('momRegimeChart');
  if (momRegimeChartInst) { momRegimeChartInst.destroy(); momRegimeChartInst = null; }

  const labels = hist.map(r => momFmtDateLabel(r.date, rangeState.momRegime));
  const ma10 = hist.map(r => r.ma10_rev_cr);
  const ma45 = hist.map(r => r.ma45_rev_cr);
  const ratio = hist.map(r => r.ratio_10d_45d);

  const isDark = document.documentElement.classList.contains('dark');
  const gridColor = isDark ? '#333' : '#E0DDD8';
  const txtColor = isDark ? '#aaa' : '#6B6560';

  // Control bands on the 10D/45D Ratio (right axis) — sample stats over the
  // currently displayed window.
  const ratioStats = momMeanStd(ratio);
  momRenderStatsLine('momRegimeStats', ratioStats, false);

  momRegimeChartInst = new Chart(ctx, {
    type: 'line',
    data: {
      labels: labels,
      datasets: [
        { label: 'MA10 F&O Rev', data: ma10, borderColor: '#D4380D', backgroundColor: 'transparent', borderWidth: 2, pointRadius: 0, tension: 0.3, yAxisID: 'y' },
        { label: 'MA45 F&O Rev', data: ma45, borderColor: '#0958D9', backgroundColor: 'transparent', borderWidth: 2, pointRadius: 0, tension: 0.3, yAxisID: 'y' },
        { label: '10D/45D Ratio', data: ratio, borderColor: '#8B5CF6', backgroundColor: 'transparent', borderWidth: 2.5, borderDash: [2, 3], pointRadius: 0, tension: 0.3, yAxisID: 'y1' },
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false, interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: true, labels: { color: txtColor, font: { size: 10, family: "'JetBrains Mono', monospace" }, usePointStyle: true, pointStyle: 'line' } },
        tooltip: {
          enabled: false,
          external: makeMomTooltipHandler({ hist: hist, stats: ratioStats, isPrice: false, includeSignalContext: false })
        }
      },
      scales: {
        x: { ticks: { color: txtColor, font: {size: 9}, maxRotation: 45, maxTicksLimit: 12 }, grid: { display: false } },
        y: { position: 'left', title: { display: true, text: 'F&O Revenue (Cr)', color: txtColor, font: {size: 10} }, ticks: { color: txtColor }, grid: { display: false } },
        y1: { position: 'right', title: { display: true, text: '10D/45D Ratio', color: txtColor, font: {size: 10} }, ticks: { color: txtColor }, grid: { display: false },
              suggestedMin: 0.7, suggestedMax: 1.4 },
      }
    },
    plugins: [
      makeBandsPlugin(ratioStats, 'y1', isDark)
    ]
  });
}

function renderMomPriceChart(hist) {
  const ctx = document.getElementById('momPriceChart');
  if (momPriceChartInst) { momPriceChartInst.destroy(); momPriceChartInst = null; }

  const labels = hist.map(r => momFmtDateLabel(r.date, rangeState.momPrice));
  const prices = hist.map(r => r.close_price);
  const signals = hist.map(r => r.composite_signal);

  // Color the background segments by regime
  const regimeColors = hist.map(r => {
    if (r.regime === 'HOT') return 'rgba(212,56,13,0.08)';
    if (r.regime === 'COLD') return 'rgba(9,88,217,0.08)';
    return 'rgba(0,0,0,0)';
  });

  // Signal markers
  const buyPoints = hist.map((r,i) => (r.composite_signal === 'STRONG_BUY' || r.composite_signal === 'BUY') ? r.close_price : null);
  const sellPoints = hist.map((r,i) => r.composite_signal === 'SELL' ? r.close_price : null);

  const isDark = document.documentElement.classList.contains('dark');
  const gridColor = isDark ? '#333' : '#E0DDD8';
  const txtColor = isDark ? '#aaa' : '#6B6560';

  // Control bands on MCX Price (default left axis 'y').
  const priceStats = momMeanStd(prices);
  momRenderStatsLine('momPriceStats', priceStats, true);

  momPriceChartInst = new Chart(ctx, {
    type: 'line',
    data: {
      labels: labels,
      datasets: [
        { label: 'MCX Price', data: prices, borderColor: isDark ? '#E0DDD8' : '#1A1A1A', backgroundColor: 'transparent', borderWidth: 2, pointRadius: 0, tension: 0.3 },
        { label: 'BUY / STRONG BUY', data: buyPoints, borderColor: 'transparent', backgroundColor: '#1B7D3A', pointRadius: 6, pointStyle: 'triangle', showLine: false },
        { label: 'SELL', data: sellPoints, borderColor: 'transparent', backgroundColor: '#CF1322', pointRadius: 6, pointStyle: 'triangle', pointRotation: 180, showLine: false },
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false, interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: true, labels: { color: txtColor, font: { size: 10, family: "'JetBrains Mono', monospace" }, usePointStyle: true } },
        tooltip: {
          enabled: false,
          external: makeMomTooltipHandler({ hist: hist, stats: priceStats, isPrice: true, includeSignalContext: true })
        }
      },
      scales: {
        x: { ticks: { color: txtColor, font: {size: 9}, maxRotation: 45, maxTicksLimit: 12 }, grid: { display: false } },
        y: { title: { display: true, text: 'Price (\u20B9)', color: txtColor, font: {size: 10} }, ticks: { color: txtColor }, grid: { display: false } }
      }
    },
    plugins: [{
      id: 'regimeBackground',
      beforeDraw(chart) {
        const { ctx: c, chartArea: { top, bottom, left, right }, scales: { x } } = chart;
        if (!x) return;
        const w = (right - left) / (hist.length || 1);
        for (let i = 0; i < hist.length; i++) {
          c.fillStyle = regimeColors[i];
          const xPos = x.getPixelForValue(i) - w/2;
          c.fillRect(xPos, top, w, bottom - top);
        }
      }
    },
    makeBandsPlugin(priceStats, 'y', isDark)
    ]
  });
}

function renderMomTable(hist) {
  const tbody = document.getElementById('momSignalTableBody');
  const recent = hist.slice().reverse();
  tbody.innerHTML = recent.map(r => {
    const sigClass = r.composite_signal || '';
    const regClass = r.regime || '';
    const sigColor = sigClass === 'STRONG_BUY' ? 'var(--positive)' :
                     sigClass === 'BUY' ? '#0958D9' :
                     sigClass === 'SELL' ? 'var(--negative)' :
                     sigClass === 'WATCH' ? 'var(--warning)' : 'var(--text-secondary)';
    const regColor = regClass === 'HOT' ? '#D4380D' :
                     regClass === 'COLD' ? '#0958D9' : 'var(--text-secondary)';
    return '<tr>' +
      '<td style="font-family:var(--mono);font-size:11px">' + r.date + '</td>' +
      '<td style="font-family:var(--mono);font-size:11px">' + (r.fno_rev_cr != null ? r.fno_rev_cr.toFixed(2) : '--') + '</td>' +
      '<td style="font-family:var(--mono);font-size:11px">\u20B9' + (r.close_price != null ? r.close_price.toLocaleString('en-IN',{maximumFractionDigits:1}) : '--') + '</td>' +
      '<td style="font-family:var(--mono);font-size:11px">' + (r.ratio_10d_45d != null ? r.ratio_10d_45d.toFixed(3) : '--') + '</td>' +
      '<td style="font-family:var(--mono);font-size:11px;font-weight:700;color:' + regColor + '">' + (r.regime || '--') + '</td>' +
      '<td style="font-family:var(--mono);font-size:11px">' + (r.adr_signal || '--').replace('_',' ') + '</td>' +
      '<td style="font-family:var(--mono);font-size:11px;font-weight:700;color:' + sigColor + '">' + (r.composite_signal || '--').replace('_',' ') + '</td>' +
      '</tr>';
  }).join('');
}

// ════════════════════════════════════════════════════════════════════════════
//  OI PARTICIPANTS — Participant Category Analytics (Enhanced)
// ════════════════════════════════════════════════════════════════════════════
var oipCache = null;
var oipCacheTs = 0;
var oipHeroChartInst = null;
var oipGrowthChartInst = null;
var oipHedgerChartInst = null;
var oipCompChartInst = null;

// State
var oipInstrument = 'Overall';
var oipSelectedCommodities = {};
var oipShowMA = false;
var OIP_COLORS = [
  '#1890FF','#52C41A','#FAAD14','#722ED1','#FF4D4F','#13C2C2',
  '#EB2F96','#FA8C16','#2F54EB','#A0D911','#F5222D','#597EF7',
  '#36CFC9','#FFC53D','#9254DE','#FF7A45','#73D13D','#40A9FF','#F759AB'
];

function loadOIParticipants() {
  if (oipCache && (Date.now() - oipCacheTs < 600000)) { oipRenderAll(); return; }
  document.getElementById('oipAsOf').textContent = 'Loading...';
  fetch('/api/commodity_dashboard?view=oi_participants')
    .then(function(r) { return r.json(); })
    .then(function(d) {
      if (d.success) {
        oipCache = d;
        oipCacheTs = Date.now();
        oipInitControls(d);
        oipRenderAll();
      } else { document.getElementById('oipAsOf').textContent = d.error || 'No data'; }
    })
    .catch(function(e) { document.getElementById('oipAsOf').textContent = 'Error: ' + e; });
}

function oipVal(v) {
  if (v === -1) return '<span style="color:var(--text-secondary);font-style:italic">&lt;10</span>';
  if (v === null || v === undefined) return '--';
  return v.toLocaleString('en-IN');
}
function oipPair(l, s) { return oipVal(l) + ' / ' + oipVal(s); }

function oipGrowthHtml(pct) {
  if (pct === null || pct === undefined) return '<span style="color:var(--text-secondary)">N/A</span>';
  var cls = pct > 0 ? 'color:var(--positive)' : pct < 0 ? 'color:var(--negative)' : 'color:var(--text-secondary)';
  var arrow = pct > 0 ? '\u25B2 +' : pct < 0 ? '\u25BC ' : '';
  return '<span style="' + cls + ';font-weight:700">' + arrow + pct.toFixed(1) + '%</span>';
}

function oipInitControls(data) {
  // Build commodity chips
  var container = document.getElementById('oipCommodityChips');
  if (container.children.length > 0) return; // already init
  oipSelectedCommodities = { ALL: true };
  (data.unique_commodities || []).forEach(function(c) {
    oipSelectedCommodities[c] = false;
    var chip = document.createElement('span');
    chip.className = 'margin-chip';
    chip.setAttribute('data-cmd', c);
    chip.textContent = c;
    chip.onclick = function() { oipToggleCommodity(c); };
    container.appendChild(chip);
  });
  // Init composition select
  var sel = document.getElementById('oipCompSelect');
  if (sel.options.length === 0) {
    (data.commodities || []).forEach(function(c) {
      var o = document.createElement('option');
      o.value = c; o.textContent = c.replace('_', ' ');
      sel.appendChild(o);
    });
  }
}

function oipSetInstrument(inst) {
  oipInstrument = inst;
  document.querySelectorAll('#oipInstBtns .margin-chip').forEach(function(el) {
    el.className = el.getAttribute('data-inst') === inst ? 'margin-chip active' : 'margin-chip';
  });
  oipRenderAll();
}

function oipToggleCommodity(c) {
  if (c === 'ALL') {
    // Toggle exchange total — exclusive with individual commodities
    var wasAll = oipSelectedCommodities.ALL;
    Object.keys(oipSelectedCommodities).forEach(function(k) { oipSelectedCommodities[k] = false; });
    oipSelectedCommodities.ALL = !wasAll;
  } else {
    oipSelectedCommodities.ALL = false;
    oipSelectedCommodities[c] = !oipSelectedCommodities[c];
    // If nothing selected, select ALL
    var anySelected = Object.keys(oipSelectedCommodities).some(function(k) { return oipSelectedCommodities[k]; });
    if (!anySelected) oipSelectedCommodities.ALL = true;
  }
  // Update chip visuals
  document.querySelectorAll('[data-cmd]').forEach(function(el) {
    var key = el.getAttribute('data-cmd');
    el.className = oipSelectedCommodities[key] ? 'margin-chip active' : 'margin-chip';
    if (key === 'ALL') el.style.fontWeight = '700';
  });
  oipRenderAll();
}

function oipSelectAll(selectAll) {
  Object.keys(oipSelectedCommodities).forEach(function(k) {
    oipSelectedCommodities[k] = (k === 'ALL') ? !selectAll : selectAll;
  });
  if (!selectAll) oipSelectedCommodities.ALL = true;
  document.querySelectorAll('[data-cmd]').forEach(function(el) {
    var key = el.getAttribute('data-cmd');
    el.className = oipSelectedCommodities[key] ? 'margin-chip active' : 'margin-chip';
  });
  oipRenderAll();
}

function oipToggleMA() {
  oipShowMA = !oipShowMA;
  document.getElementById('oipMaToggle').className = oipShowMA ? 'margin-chip active' : 'margin-chip';
  oipRenderAll();
}

function oipFilterDates(allDates, key) {
  return sliceTailByRange(allDates, rangeState[key] || 'Max');
}

function oipGetSelectedKeys() {
  // Returns trend keys matching selected commodities + instrument
  var keys = [];
  if (oipSelectedCommodities.ALL) {
    keys.push('ALL_' + oipInstrument);
  } else {
    Object.keys(oipSelectedCommodities).forEach(function(c) {
      if (c !== 'ALL' && oipSelectedCommodities[c]) {
        keys.push(c + '_' + oipInstrument);
      }
    });
  }
  return keys;
}

function oipFmtMonth(dateStr) {
  var d = new Date(dateStr);
  var m = d.toLocaleString('en', { month: 'short' });
  var y = String(d.getFullYear()).slice(2);
  return m + "'" + y;
}

// ── MASTER RENDER ──
function oipRenderAll() {
  if (!oipCache) return;
  var data = oipCache;
  document.getElementById('oipAsOf').textContent = 'As of ' + data.as_of + ' \u2022 ' + data.snapshot_dates + ' snapshots';

  oipRenderSummaryCards(data);
  oipRenderHeroChart(data);
  oipRenderGrowthChart(data);
  oipRenderHedgerChart(data);
  oipRenderCurrentTable(data);
  oipRenderGrowthTable(data);
  updateOipCompChart();
}

// ── SUMMARY CARDS ──
function oipRenderSummaryCards(data) {
  var go = data.growth_overall[oipInstrument] || {};
  document.getElementById('oipStatTotal').textContent = go.current != null ? go.current.toLocaleString('en-IN') : '--';
  document.getElementById('oipStatWow').innerHTML = oipGrowthHtml(go.wow_pct);
  document.getElementById('oipStatMom').innerHTML = oipGrowthHtml(go.mom_pct);
  document.getElementById('oipStatQoq').innerHTML = oipGrowthHtml(go.qoq_pct);
  document.getElementById('oipStatYoy').innerHTML = oipGrowthHtml(go.yoy_pct);
  // Top commodity
  var topRow = data.participants[0];
  document.getElementById('oipStatTop').textContent = topRow ? topRow.commodity + ' (' + (topRow.total_participation || 0).toLocaleString('en-IN') + ')' : '--';
}

// ── HERO TREND CHART ──
function oipRenderHeroChart(data) {
  var tr = data.trend;
  if (!tr || !tr.dates) return;

  var allDates = tr.dates;
  var dates = oipFilterDates(allDates, 'oipHero');
  var startIdx = allDates.indexOf(dates[0]);
  var keys = oipGetSelectedKeys();
  if (keys.length === 0) return;

  var ctx = document.getElementById('oipHeroChart').getContext('2d');
  if (oipHeroChartInst) oipHeroChartInst.destroy();

  var labels = dates.map(oipFmtMonth);
  var datasets = [];
  keys.forEach(function(key, i) {
    var series = tr[key];
    if (!series) return;
    var totals = series.total.slice(startIdx, startIdx + dates.length);
    var label = key.replace('_', ' ');
    datasets.push({
      label: label, data: totals,
      borderColor: OIP_COLORS[i % OIP_COLORS.length],
      backgroundColor: 'transparent',
      borderWidth: 2, tension: 0.3, pointRadius: 0, spanGaps: true
    });
    if (oipShowMA && series.ma30) {
      var ma = series.ma30.slice(startIdx, startIdx + dates.length);
      datasets.push({
        label: label + ' (30d MA)', data: ma,
        borderColor: OIP_COLORS[i % OIP_COLORS.length],
        backgroundColor: 'transparent',
        borderWidth: 1.5, borderDash: [6, 3], tension: 0.3, pointRadius: 0, spanGaps: true
      });
    }
  });

  // Deduplicate monthly labels (show only first occurrence of each month)
  var seen = {};
  var cleanLabels = labels.map(function(l) {
    if (seen[l]) return '';
    seen[l] = true;
    return l;
  });

  oipHeroChartInst = new Chart(ctx, {
    type: 'line',
    data: { labels: cleanLabels, datasets: datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      scales: {
        x: { ticks: { maxTicksLimit: 14, font: { size: 9, family: 'var(--mono)' } }, grid: { color: 'rgba(128,128,128,0.12)' } },
        y: { beginAtZero: false, ticks: { font: { size: 10 }, callback: function(v) { return v >= 1000 ? (v/1000).toFixed(0) + 'k' : v; } }, grid: { color: 'rgba(128,128,128,0.12)' } }
      },
      plugins: {
        legend: { position: 'top', labels: { font: { size: 10 }, boxWidth: 12, usePointStyle: true } },
        tooltip: { mode: 'index', intersect: false, callbacks: {
          title: function(items) { return dates[items[0].dataIndex]; },
          label: function(ctx) { return ctx.dataset.label + ': ' + (ctx.parsed.y != null ? ctx.parsed.y.toLocaleString('en-IN') : '--'); }
        }}
      }
    }
  });
}

// ── MONTHLY GROWTH CHART ──
function oipRenderGrowthChart(data) {
  var mg = data.monthly_growth;
  if (!mg || !mg.months || mg.months.length < 2) return;
  document.getElementById('oipGrowthSection').style.display = '';

  var ctx = document.getElementById('oipGrowthChart').getContext('2d');
  if (oipGrowthChartInst) oipGrowthChartInst.destroy();

  var instKey = oipInstrument + '_mom_pct';
  var mCount = { '3M': 3, '6M': 6, '12M': 12, 'All': null }[rangeState['oipGrowth'] || 'All'];
  var monthKeys = sliceTailByCount(mg.months, mCount);
  var startIdx = mg.months.length - monthKeys.length;
  var pcts = (mg[instKey] || []).slice(startIdx);
  var months = monthKeys.map(function(m) {
    var parts = m.split('-');
    var d = new Date(parseInt(parts[0]), parseInt(parts[1]) - 1);
    return d.toLocaleString('en', { month: 'short', year: '2-digit' });
  });

  var bgColors = pcts.map(function(v) {
    if (v === null) return 'rgba(140,140,140,0.3)';
    return v >= 0 ? 'rgba(82,196,26,0.7)' : 'rgba(255,77,79,0.7)';
  });

  oipGrowthChartInst = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: months,
      datasets: [{ label: 'MoM Growth %', data: pcts, backgroundColor: bgColors, borderRadius: 3 }]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      scales: {
        x: { ticks: { font: { size: 9, family: 'var(--mono)' } }, grid: { display: false } },
        y: { ticks: { font: { size: 10 }, callback: function(v) { return v + '%'; } }, grid: { color: 'rgba(128,128,128,0.12)' } }
      },
      plugins: {
        legend: { display: false },
        tooltip: { callbacks: { label: function(ctx) { return (ctx.parsed.y != null ? ctx.parsed.y.toFixed(1) + '%' : 'N/A'); } } }
      }
    }
  });
}

// ── HEDGER VS SPECULATOR ──
function oipRenderHedgerChart(data) {
  if (!data.hedger_speculator || data.hedger_speculator.length === 0) return;
  document.getElementById('oipHedgerSection').style.display = '';

  var ctx = document.getElementById('oipHedgerChart').getContext('2d');
  if (oipHedgerChartInst) oipHedgerChartInst.destroy();

  var items = data.hedger_speculator.filter(function(x) { return x.total > 0; }).slice(0, 15);
  var labels = items.map(function(x) { return x.commodity + ' ' + x.instrument.charAt(0); });

  oipHedgerChartInst = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: labels,
      datasets: [
        { label: 'Hedgers (VCPs)', data: items.map(function(x) { return x.hedger_pct; }), backgroundColor: 'rgba(82,196,26,0.7)' },
        { label: 'Speculators (Prop+Others)', data: items.map(function(x) { return x.speculator_pct; }), backgroundColor: 'rgba(250,173,20,0.7)' },
        { label: 'Other', data: items.map(function(x) { return Math.max(0, 100 - x.hedger_pct - x.speculator_pct); }), backgroundColor: 'rgba(140,140,140,0.3)' }
      ]
    },
    options: {
      indexAxis: 'y', responsive: true, maintainAspectRatio: false,
      scales: {
        x: { stacked: true, max: 100, ticks: { callback: function(v) { return v + '%'; }, font: { size: 10 } }, grid: { color: 'rgba(128,128,128,0.12)' } },
        y: { stacked: true, ticks: { font: { size: 10, family: 'var(--mono)' } }, grid: { display: false } }
      },
      plugins: {
        legend: { position: 'top', labels: { font: { size: 10 }, boxWidth: 12 } },
        tooltip: { callbacks: { label: function(ctx) { return ctx.dataset.label + ': ' + ctx.parsed.x.toFixed(1) + '%'; } } }
      }
    }
  });
}

// ── CATEGORY COMPOSITION TREND ──
function updateOipCompChart() {
  if (!oipCache || !oipCache.trend) return;
  var sel = document.getElementById('oipCompSelect');
  var key = sel.value;
  var tr = oipCache.trend;
  if (!tr[key]) return;
  document.getElementById('oipCompSection').style.display = '';

  var allDates = tr.dates;
  var dates = oipFilterDates(allDates, 'oipComp');
  var startIdx = allDates.indexOf(dates[0]);

  var ctx = document.getElementById('oipCompChart').getContext('2d');
  if (oipCompChartInst) oipCompChartInst.destroy();

  var series = tr[key];
  var catColors = {
    vcp: 'rgba(82,196,26,0.7)', prop: 'rgba(250,173,20,0.7)',
    others: 'rgba(24,144,255,0.7)', foreign: 'rgba(114,46,209,0.7)',
    dfi: 'rgba(255,77,79,0.7)', fpo: 'rgba(140,140,140,0.5)'
  };
  var catLabels = { vcp: 'VCPs/Hedgers', prop: 'Proprietary', others: 'Others', foreign: 'Foreign', dfi: 'DFI', fpo: 'FPOs/Farmers' };
  var labels = dates.map(oipFmtMonth);

  var datasets = ['vcp', 'prop', 'others', 'foreign', 'dfi', 'fpo'].map(function(cat) {
    var longArr = series[cat + '_long'];
    var shortArr = series[cat + '_short'];
    if (!longArr) return null;
    return {
      label: catLabels[cat],
      data: longArr.slice(startIdx, startIdx + dates.length).map(function(v, i) {
        return (v || 0) + ((shortArr || [])[startIdx + i] || 0);
      }),
      backgroundColor: catColors[cat], fill: true, tension: 0.3, pointRadius: 0
    };
  }).filter(Boolean);

  oipCompChartInst = new Chart(ctx, {
    type: 'line',
    data: { labels: labels, datasets: datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      scales: {
        x: { ticks: { maxTicksLimit: 12, font: { size: 9 } }, grid: { color: 'rgba(128,128,128,0.12)' } },
        y: { stacked: true, ticks: { font: { size: 10 } }, grid: { color: 'rgba(128,128,128,0.12)' } }
      },
      plugins: {
        legend: { position: 'top', labels: { font: { size: 10 }, boxWidth: 12 } },
        tooltip: { mode: 'index', intersect: false }
      }
    }
  });
}

// ── CURRENT DISTRIBUTION TABLE ──
var oipTableSortCol = 'commodity';
var oipTableSortAsc = true;

function oipSortTable(col) {
  if (oipTableSortCol === col) { oipTableSortAsc = !oipTableSortAsc; }
  else { oipTableSortCol = col; oipTableSortAsc = col === 'commodity'; }
  oipRenderCurrentTable(oipCache);
}

function oipRenderCurrentTable(data) {
  function safeSum(rows, field) {
    return rows.reduce(function(a, r) { var v = r[field]; return a + (v > 0 ? v : 0); }, 0);
  }
  function subtotalRow(label, rows, bg) {
    return '<tr style="' + bg + 'font-weight:700;border-top:2px solid var(--border-heavy)">' +
      '<td style="text-align:left">' + label + '</td><td></td>' +
      '<td>' + safeSum(rows,'total_participation').toLocaleString('en-IN') + '</td>' +
      '<td>' + safeSum(rows,'fpo_long').toLocaleString('en-IN') + '/' + safeSum(rows,'fpo_short').toLocaleString('en-IN') + '</td>' +
      '<td style="color:var(--positive)">' + safeSum(rows,'vcp_long').toLocaleString('en-IN') + '/' + safeSum(rows,'vcp_short').toLocaleString('en-IN') + '</td>' +
      '<td style="color:var(--warning)">' + safeSum(rows,'prop_long').toLocaleString('en-IN') + '/' + safeSum(rows,'prop_short').toLocaleString('en-IN') + '</td>' +
      '<td>' + safeSum(rows,'dfi_long').toLocaleString('en-IN') + '/' + safeSum(rows,'dfi_short').toLocaleString('en-IN') + '</td>' +
      '<td>' + safeSum(rows,'foreign_long').toLocaleString('en-IN') + '/' + safeSum(rows,'foreign_short').toLocaleString('en-IN') + '</td>' +
      '<td style="color:var(--warning)">' + safeSum(rows,'others_long').toLocaleString('en-IN') + '/' + safeSum(rows,'others_short').toLocaleString('en-IN') + '</td></tr>';
  }
  function sortArrow(col) {
    if (oipTableSortCol !== col) return '';
    return oipTableSortAsc ? ' \u25B2' : ' \u25BC';
  }
  var thStyle = 'cursor:pointer;user-select:none';

  var tbl = document.getElementById('oipCurrentTable');
  var h = '<thead><tr>' +
    '<th style="text-align:left;' + thStyle + '" onclick="oipSortTable(\'commodity\')">Commodity' + sortArrow('commodity') + '</th>' +
    '<th style="' + thStyle + '" onclick="oipSortTable(\'instrument\')">Type' + sortArrow('instrument') + '</th>' +
    '<th style="' + thStyle + '" onclick="oipSortTable(\'total_participation\')">Total' + sortArrow('total_participation') + '</th>' +
    '<th style="' + thStyle + '" onclick="oipSortTable(\'fpo\')">FPOs L/S' + sortArrow('fpo') + '</th>' +
    '<th style="' + thStyle + '" onclick="oipSortTable(\'vcp\')">VCPs L/S' + sortArrow('vcp') + '</th>' +
    '<th style="' + thStyle + '" onclick="oipSortTable(\'prop\')">Prop L/S' + sortArrow('prop') + '</th>' +
    '<th style="' + thStyle + '" onclick="oipSortTable(\'dfi\')">DFI L/S' + sortArrow('dfi') + '</th>' +
    '<th style="' + thStyle + '" onclick="oipSortTable(\'foreign\')">For L/S' + sortArrow('foreign') + '</th>' +
    '<th style="' + thStyle + '" onclick="oipSortTable(\'others\')">Others L/S' + sortArrow('others') + '</th>' +
    '</tr></thead><tbody>';

  // Sort data
  var sc = oipTableSortCol;
  var dir = oipTableSortAsc ? 1 : -1;
  var sorted = data.participants.slice().sort(function(a, b) {
    if (sc === 'commodity') return dir * a.commodity.localeCompare(b.commodity);
    if (sc === 'instrument') return dir * a.instrument.localeCompare(b.instrument);
    if (sc === 'total_participation') return dir * ((a.total_participation||0) - (b.total_participation||0));
    // Category columns: sort by long+short total
    var aVal = (a[sc+'_long']>0?a[sc+'_long']:0) + (a[sc+'_short']>0?a[sc+'_short']:0);
    var bVal = (b[sc+'_long']>0?b[sc+'_long']:0) + (b[sc+'_short']>0?b[sc+'_short']:0);
    return dir * (aVal - bVal);
  });

  sorted.forEach(function(p) {
    h += '<tr><td style="font-weight:600;text-align:left">' + p.commodity + '</td>' +
      '<td style="font-size:10px;color:var(--text-secondary)">' + p.instrument.charAt(0) + '</td>' +
      '<td style="font-weight:700">' + (p.total_participation||0).toLocaleString('en-IN') + '</td>' +
      '<td>' + oipPair(p.fpo_long,p.fpo_short) + '</td>' +
      '<td style="color:var(--positive)">' + oipPair(p.vcp_long,p.vcp_short) + '</td>' +
      '<td style="color:var(--warning)">' + oipPair(p.prop_long,p.prop_short) + '</td>' +
      '<td>' + oipPair(p.dfi_long,p.dfi_short) + '</td>' +
      '<td>' + oipPair(p.foreign_long,p.foreign_short) + '</td>' +
      '<td style="color:var(--warning)">' + oipPair(p.others_long,p.others_short) + '</td></tr>';
  });
  var futR = sorted.filter(function(p) { return p.instrument==='Futures'; });
  var optR = sorted.filter(function(p) { return p.instrument==='Options'; });
  h += subtotalRow('Futures Sub-Total', futR, 'background:rgba(82,196,26,0.06);');
  h += subtotalRow('Options Sub-Total', optR, 'background:rgba(24,144,255,0.06);');
  h += subtotalRow('GRAND TOTAL', sorted, 'background:rgba(250,173,20,0.1);');
  tbl.innerHTML = h + '</tbody>';
}

// ── GROWTH TABLE ──
function oipRenderGrowthTable(data) {
  if (!data.growth || data.growth.length === 0) return;
  document.getElementById('oipGrowthTableSection').style.display = '';

  var filtered = data.growth.filter(function(g) { return g.instrument === oipInstrument || oipInstrument === 'Overall'; });
  if (oipInstrument !== 'Overall') {
    filtered = data.growth.filter(function(g) { return g.instrument === oipInstrument; });
  } else {
    filtered = data.growth.filter(function(g) { return g.instrument === 'Overall'; });
  }
  filtered.sort(function(a, b) { return (b.current || 0) - (a.current || 0); });

  var tbl = document.getElementById('oipGrowthTable');
  var h = '<thead><tr><th style="text-align:left">Commodity</th><th>Type</th><th>Current</th>' +
    '<th>WoW %</th><th>MoM %</th><th>QoQ %</th><th>YoY %</th></tr></thead><tbody>';

  filtered.forEach(function(g) {
    h += '<tr><td style="font-weight:600;text-align:left">' + g.commodity + '</td>' +
      '<td style="font-size:10px;color:var(--text-secondary)">' + (g.instrument||'').charAt(0) + '</td>' +
      '<td style="font-weight:700">' + (g.current||0).toLocaleString('en-IN') + '</td>' +
      '<td>' + oipGrowthHtml(g.wow_pct) + '</td>' +
      '<td>' + oipGrowthHtml(g.mom_pct) + '</td>' +
      '<td>' + oipGrowthHtml(g.qoq_pct) + '</td>' +
      '<td>' + oipGrowthHtml(g.yoy_pct) + '</td></tr>';
  });
  tbl.innerHTML = h + '</tbody>';
}

