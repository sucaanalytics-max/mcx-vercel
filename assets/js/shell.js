/* MCX Revenue Monitor: page routes and the app shell (rail, top bar, page chips,
   phone bottom bar). Loaded after the page scripts, whose loaders the routes call. */
(function () {
  'use strict';

  const SECTIONS = { today: 'Today', revenue: 'Revenue', value: 'Earnings & value', signals: 'Signals', lab: 'Lab' };

  // pages: the .page elements shown for the route. legacy: the old tab id that links here.
  const ROUTES = [
    { id: 'today', path: '/today', section: 'today', title: 'Today', pages: ['tabPredictor'], legacy: 'tabPredictor',
      mount: () => MCX.today.mount() },
    { id: 'rev-trends', path: '/revenue/trends', section: 'revenue', title: 'Trends', pages: ['tabExchange'], legacy: 'tabExchange',
      mount: () => MCX.revenue.trends.mount() },
    { id: 'rev-season', path: '/revenue/seasonality', section: 'revenue', title: 'Seasonality', pages: ['pageRevSeason'],
      mount: () => MCX.revenue.season.mount() },
    { id: 'rev-cmd', path: '/revenue/commodities', section: 'revenue', title: 'Commodities', pages: ['tabCommodity'], legacy: 'tabCommodity',
      mount: () => MCX.revenue.cmd.mount() },
    { id: 'val-q', path: '/value/quarter', section: 'value', title: 'Quarter P&L', pages: ['tabQuarterly'], legacy: 'tabQuarterly',
      mount: () => MCX.value.quarter.mount() },
    { id: 'val-fv', path: '/value/fair-value', section: 'value', title: 'Fair value', pages: ['tabValuation'], legacy: 'tabValuation',
      mount: () => MCX.value.fairValue.mount() },
    { id: 'val-scen', path: '/value/scenarios', section: 'value', title: 'Scenarios', pages: ['tabForecast'], legacy: 'tabForecast',
      mount: () => MCX.value.scenarios.mount() },
    { id: 'sig-mom', path: '/signals/momentum', section: 'signals', title: 'Momentum', pages: ['tabMomentum'], legacy: 'tabMomentum',
      mount: () => MCX.signals.momentum.mount() },
    { id: 'sig-ens', path: '/signals/ensemble', section: 'signals', title: 'Model ensemble', pages: ['pageSigEnsemble'],
      mount: () => MCX.signals.ensemble.mount() },
    { id: 'lab-diag', path: '/lab/diagnostics', section: 'lab', title: 'Diagnostics', pages: ['tabAnalytics'], legacy: 'tabAnalytics',
      mount: () => MCX.lab.diagnostics.mount() },
    { id: 'lab-pos', path: '/lab/positioning', section: 'lab', title: 'Positioning', pages: ['tabOIP'], legacy: 'tabOIP',
      mount: () => MCX.lab.positioning.mount() },
    { id: 'lab-mar', path: '/lab/margins', section: 'lab', title: 'Margins', pages: ['tabMargins'], legacy: 'tabMargins',
      mount: () => MCX.lab.margins.mount() },
  ];
  const LEGACY = {};
  ROUTES.forEach(r => { MCX.router.register(r); if (r.legacy) LEGACY[r.legacy] = r.path; });

  // Old inline handlers and bookmarks call switchTab('tabX')
  window.switchTab = tabId => MCX.router.go(LEGACY[tabId] || '/today');

  const $ = id => document.getElementById(id);
  const lastInSection = {};

  // ── Navigation state ─────────────────────────────────────────────────────
  MCX.router.onChange(r => {
    lastInSection[r.section] = r.path;
    $('presentBtn').hidden = r.id !== 'today';
    document.querySelectorAll('[data-route]').forEach(a => {
      if (a.dataset.route === r.id) a.setAttribute('aria-current', 'page'); else a.removeAttribute('aria-current');
    });
    // Bottom bar: the section's button is current, and returns to the page last seen there
    document.querySelectorAll('.bb-link[data-section]').forEach(a => {
      const on = a.dataset.section === r.section || (a.dataset.section === 'more' && r.section === 'lab');
      a.classList.toggle('is-active', on);
      if (a.tagName === 'A') {
        if (on) a.setAttribute('aria-current', 'page'); else a.removeAttribute('aria-current');
        if (lastInSection[a.dataset.section]) a.setAttribute('href', '#' + lastInSection[a.dataset.section]);
      }
    });
    // Breadcrumb and title
    const crumbs = $('crumbs');
    crumbs.textContent = '';
    if (r.section !== 'today') crumbs.append(span('crumb-section', SECTIONS[r.section]), span('crumb-sep', '/', true));
    const page = span('crumb-page', r.title);
    page.setAttribute('aria-current', 'page');
    crumbs.append(page);
    document.title = r.title + ' · MCX Revenue Monitor';
    // Sibling pages as chips on narrow screens
    const sub = $('subnav');
    sub.textContent = '';
    const siblings = MCX.router.all().filter(x => x.section === r.section);
    if (siblings.length > 1) siblings.forEach(x => {
      const a = document.createElement('a');
      a.className = 'chip';
      a.href = '#' + x.path;
      a.textContent = x.title;
      if (x.id === r.id) a.setAttribute('aria-current', 'page');
      sub.appendChild(a);
    });
    sub.setAttribute('aria-label', SECTIONS[r.section] + ' pages');
    const cur = sub.querySelector('[aria-current]');
    if (cur) cur.scrollIntoView({ block: 'nearest', inline: 'nearest' });
  });

  function span(cls, text, hidden) {
    const s = document.createElement('span');
    s.className = cls;
    s.textContent = text;
    if (hidden) s.setAttribute('aria-hidden', 'true');
    return s;
  }

  // ── Session status: header chip (set by the Today page) and the value beside Today ──
  MCX.store.on('session', v => {
    const pill = $('livePill');
    pill.textContent = v.label;
    pill.dataset.state = v.state === 'live' ? 'live' : 'final';
    if (v.state === 'pre') $('railLive').textContent = '';
  });
  MCX.store.on('liveRevenue', v => {
    const rail = $('railLive');
    rail.dataset.state = v.live ? 'live' : 'final';
    rail.textContent = '₹' + MCX.fmt.num(v.value, 2) + ' Cr' + (v.live ? ' proj.' : '');
    rail.title = (v.live ? 'Projected revenue for today: ₹' : 'Final revenue for today: ₹') + MCX.fmt.num(v.value, 2) + ' Cr';
  });

  // ── Theme and settings buttons (rail foot and More sheet) ───────────────
  const THEME_NAMES = { light: 'Light', dark: 'Dark', system: 'System' };
  function syncThemeButtons() {
    const mode = MCX.theme.mode(), next = MCX.theme.next();
    document.querySelectorAll('[data-action="theme"]').forEach(b => {
      b.setAttribute('aria-label', `Theme: ${THEME_NAMES[mode]}. Switch to ${THEME_NAMES[next].toLowerCase()}`);
      b.title = `Theme: ${THEME_NAMES[mode]} (click for ${THEME_NAMES[next].toLowerCase()})`;
      const lbl = b.querySelector('small');
      if (lbl) lbl.textContent = THEME_NAMES[mode];
    });
  }
  document.addEventListener('click', e => {
    const b = e.target.closest && e.target.closest('[data-action]');
    if (!b) return;
    if (b.dataset.action === 'theme') { MCX.theme.cycle(); syncThemeButtons(); }
    if (b.dataset.action === 'settings') { closeSheet(); openSettings(); }
    if (b.dataset.action === 'more') openSheet();
  });
  MCX.theme.onChange(() => MCX.router.retheme());
  syncThemeButtons();

  // ── More sheet ───────────────────────────────────────────────────────────
  const sheet = $('moreSheet'), moreBtn = $('moreBtn');
  function openSheet() {
    if (!sheet.showModal) return;
    sheet.showModal();
    moreBtn.setAttribute('aria-expanded', 'true');
  }
  function closeSheet() {
    if (sheet.open) sheet.close();
    moreBtn.setAttribute('aria-expanded', 'false');
  }
  sheet.addEventListener('close', () => moreBtn.setAttribute('aria-expanded', 'false'));   // Esc
  sheet.addEventListener('click', e => {
    if (e.target === sheet || (e.target.closest && e.target.closest('a'))) closeSheet();   // backdrop, or a page link
  });

  // ── Live snapshot: the refresh button, a refresh every 2 minutes in trading hours, the header time ──
  const AUTO_REFRESH_MS = 2 * 60 * 1000;
  let autoTimer = null, refreshing = false;
  const inTradingHours = () => { const t = MCX.market.ist(); return t.dow >= 1 && t.dow <= 5 && t.min >= 535 && t.min <= 1415; };
  function showSnapshotTime(d) {
    const meta = $('refreshMeta'), t = MCX.parseTs(d.timestamp);   // ISO on GET, "HH:MM IST, DD Mon YYYY" after a manual refresh
    if (!t) { meta.textContent = '—'; return; }
    const tz = { timeZone: 'Asia/Kolkata' };
    const hhmm = t.toLocaleTimeString('en-GB', { ...tz, hour: '2-digit', minute: '2-digit' });
    const day = t.toLocaleDateString('en-GB', { ...tz, day: '2-digit', month: 'short' });
    const today = new Date().toLocaleDateString('en-GB', { ...tz, day: '2-digit', month: 'short' });
    meta.textContent = 'as of ' + hhmm + ' IST' + (day === today ? '' : ' · ' + day);
    meta.title = 'Data as of the last snapshot: ' + t.toLocaleString('en-GB', tz);
  }
  async function refresh(manual) {
    if (refreshing) return;
    refreshing = true;
    const btn = $('refreshBtn');
    btn.classList.add('loading'); btn.disabled = true; btn.setAttribute('aria-busy', 'true');
    try {
      let data;
      if (manual) {             // a manual refresh asks the server to fetch from MCX first, with the saved cookie if any
        try {
          const cookie = MCX.storage.get('mcxCookie');
          data = await fetch('/api/refresh', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(cookie ? { cookie } : {}) }).then(r => r.json());
          if (!data.success) throw new Error(data.error);
        } catch (e) { data = await fetch('/api/refresh').then(r => r.json()); }
      } else {
        data = await fetch('/api/refresh').then(r => r.json());
      }
      MCX.store.set('refresh', data);
      if (data.success) {
        clearRangedCache();
        MCX.store.set('liveRevenue', { value: data.proj_rev_cr, live: !data.session_closed });
        showSnapshotTime(data);
        if (manual) MCX.ui.toast('Data refreshed');
        if (!autoTimer && inTradingHours()) autoTimer = setInterval(() => { if (inTradingHours() && !document.hidden) refresh(false); }, AUTO_REFRESH_MS);
      } else if (manual) {
        MCX.ui.toast('Refresh failed' + (data.error ? ': ' + data.error : ''), 'error');
      }
    } catch (e) {
      MCX.store.set('refresh', { success: false, error: 'Network error' });
      if (manual) MCX.ui.toast('Network error', 'error');
    } finally {
      refreshing = false;
      btn.classList.remove('loading'); btn.disabled = false; btn.removeAttribute('aria-busy');
    }
  }
  $('refreshBtn').addEventListener('click', () => refresh(true));
  MCX.refresh = refresh;

  // ── Share price: at load, then every minute in NSE hours on the pages that show it ──
  function fetchPrice() {
    return fetch('/api/mcxprice').then(r => { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); })
      .then(d => { if (d.error) throw new Error(d.error); MCX.store.set('price', d); })
      .catch(e => { console.warn('Share price:', e.message); MCX.store.set('priceStale', true); });
  }
  MCX.poll.every('cmp', fetchPrice, 60 * 1000, () => MCX.market.nseOpen() && !document.hidden && ['val-scen', 'val-fv'].includes(MCX.router.current()));

  // ── Data source settings: an MCX session cookie for the manual refresh ──
  const dlg = $('settingsDialog');
  function openSettings() {
    const c = MCX.storage.get('mcxCookie');
    $('cookieText').value = c || '';
    $('cookieStatus').textContent = c ? `A cookie is saved in this browser (${c.length} characters).` : 'No cookie saved; the server tries to get one itself.';
    if (dlg.showModal) dlg.showModal(); else dlg.setAttribute('open', '');
  }
  $('settingsSave').addEventListener('click', () => {
    const v = $('cookieText').value.trim();
    MCX.storage.set('mcxCookie', v);
    dlg.close();
    MCX.ui.toast(v ? 'Cookie saved in this browser' : 'Cookie cleared');
  });
  $('settingsCancel').addEventListener('click', () => dlg.close());
  dlg.addEventListener('click', e => { if (e.target === dlg) dlg.close(); });     // the backdrop
  // ?cookie=… in the URL: save it, drop it from the address bar, refresh from MCX
  const urlCookie = new URLSearchParams(location.search).get('cookie');
  if (urlCookie) {
    MCX.storage.set('mcxCookie', urlCookie);
    history.replaceState({}, '', location.pathname + location.hash);
  }

  // ── Methodology drawer: opened from any [data-method] button, or ?m=<topic> in the URL ──
  const md = $('methodDrawer');
  let mdReturn = null;
  function openMethod(topic) {
    const sec = $('md-' + topic) || $('md-data');
    document.querySelectorAll('.md-topic').forEach(x => { x.hidden = x !== sec; });
    document.querySelectorAll('#mdNav [data-topic]').forEach(b => { if (b.dataset.topic === sec.id.slice(3)) b.setAttribute('aria-current', 'true'); else b.removeAttribute('aria-current'); });
    if (!md.open) { mdReturn = document.activeElement; closeSheet(); if (md.showModal) md.showModal(); else md.setAttribute('open', ''); }
    md.querySelector('.drawer-body').scrollTop = 0;
    const h = sec.querySelector('h3'); h.tabIndex = -1; h.focus();
  }
  document.addEventListener('click', e => {
    const b = e.target.closest && e.target.closest('[data-method]');
    if (b) { e.preventDefault(); openMethod(b.dataset.method); }
  });
  $('mdNav').addEventListener('click', e => { const b = e.target.closest('[data-topic]'); if (b) openMethod(b.dataset.topic); });
  $('mdClose').addEventListener('click', () => md.close());
  md.addEventListener('click', e => { if (e.target === md) md.close(); });        // the backdrop
  md.addEventListener('close', () => { if (mdReturn && mdReturn.focus) mdReturn.focus(); mdReturn = null; });
  MCX.method = { open: openMethod };
  const mParam = new URLSearchParams(location.search).get('m');

  MCX.router.start({ fallback: 'today', legacy: LEGACY });
  setTimeout(() => refresh(!!urlCookie), 300);     // the first snapshot
  fetchPrice();
  if (mParam) setTimeout(() => openMethod(mParam), 400);
})();
