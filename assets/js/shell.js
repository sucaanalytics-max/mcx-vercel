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
    { id: 'sig-opt', path: '/signals/options', section: 'signals', title: 'Options', pages: ['pageSigOptions'],
      mount: () => MCX.signals.options.mount() },
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

  // ── Theme button (rail foot and More sheet) ───────────────
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
  let refreshing = false, refreshPoll = null;
  const inTradingHours = () => { const t = MCX.market.ist(); return t.dow >= 1 && t.dow <= 5 && t.min >= 535 && t.min <= 1415; };
  function showSnapshotTime(d) {
    const meta = $('refreshMeta'), t = MCX.parseTs(d.timestamp);   // ISO from GET /api/refresh
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
    if (refreshPoll) refreshPoll.last = Date.now();   // a manual refresh resets the 2-minute clock
    const btn = $('refreshBtn');
    btn.classList.add('loading'); btn.disabled = true; btn.setAttribute('aria-busy', 'true');
    try {
      // The relay pushes a snapshot every 15 minutes; a manual refresh re-reads it.
      // (The old POST path asked Vercel to fetch MCX itself, which Akamai blocks.)
      const data = await MCX.getJSON('/api/refresh');
      MCX.store.set('refresh', data);
      if (data.success) {
        clearRangedCache(manual ? undefined : { liveOnly: true });   // the button refetches everything
        MCX.store.set('liveRevenue', { value: data.proj_rev_cr, live: !data.session_closed });
        showSnapshotTime(data);
        if (manual) MCX.ui.toast('Data refreshed');
      } else if (manual) {
        MCX.ui.toast('Refresh failed' + (data.error ? ': ' + data.error : ''), 'error');
      }
    } catch (e) {
      MCX.store.set('refresh', { success: false, error: e && e.message ? e.message : 'Network error' });
      if (manual) MCX.ui.toast(e && e.message ? e.message : 'Network error', 'error');
    } finally {
      refreshing = false;
      btn.classList.remove('loading'); btn.disabled = false; btn.removeAttribute('aria-busy');
    }
  }
  $('refreshBtn').addEventListener('click', () => refresh(true));
  MCX.refresh = refresh;
  // Armed at load, whatever the first refresh does: it used to start only after a
  // successful refresh inside trading hours, so a tab opened before 08:55 IST or
  // after one failed fetch never auto-refreshed again that day.
  refreshPoll = MCX.poll.every('refresh', () => refresh(false), AUTO_REFRESH_MS,
    () => inTradingHours() && !document.hidden);

  // ── Share price: on the pages that show it (Fair value, Scenarios), when one opens and then
  // every minute in NSE hours. Other pages never read it, so it no longer loads with every page.
  const PRICE_PAGES = ['val-scen', 'val-fv'];
  let priceAt = 0, pricePoll = null;
  function fetchPrice() {
    priceAt = Date.now();
    if (pricePoll) pricePoll.last = priceAt;      // the pages' mount() kick then skips: one request, not two
    return MCX.getJSON('/api/mcxprice')
      .then(d => { if (!d || d.error) throw new Error((d && d.error) || 'No price'); MCX.store.set('price', d); })
      .catch(e => { console.warn('Share price:', e.message); });
  }
  pricePoll = MCX.poll.every('cmp', fetchPrice, 60 * 1000, () => MCX.market.nseOpen() && !document.hidden && PRICE_PAGES.includes(MCX.router.current()));
  MCX.router.onChange(r => { if (PRICE_PAGES.includes(r.id) && Date.now() - priceAt > 60 * 1000) fetchPrice(); });

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
  setTimeout(() => refresh(false), 300);     // the first snapshot (the price loads with its pages: router.onChange above)
  if (mParam) setTimeout(() => openMethod(mParam), 400);
})();
