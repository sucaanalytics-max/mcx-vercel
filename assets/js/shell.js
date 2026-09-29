/* MCX Revenue Monitor: page routes and the app shell (rail, top bar, page chips,
   phone bottom bar). Loaded after the page scripts, whose loaders the routes call. */
(function () {
  'use strict';

  const SECTIONS = { today: 'Today', revenue: 'Revenue', value: 'Earnings & value', signals: 'Signals', lab: 'Lab' };

  // pages: the .page elements shown for the route. legacy: the old tab id that links here.
  const ROUTES = [
    { id: 'today', path: '/today', section: 'today', title: 'Today', pages: ['tabPredictor'], legacy: 'tabPredictor',
      mount: () => MCX.today.mount(), retheme: () => rethemeToday() },
    { id: 'rev-trends', path: '/revenue/trends', section: 'revenue', title: 'Trends', pages: ['tabExchange'], legacy: 'tabExchange',
      mount: () => MCX.revenue.trends.mount() },
    { id: 'rev-season', path: '/revenue/seasonality', section: 'revenue', title: 'Seasonality', pages: ['pageRevSeason'],
      mount: () => MCX.revenue.season.mount() },
    { id: 'rev-cmd', path: '/revenue/commodities', section: 'revenue', title: 'Commodities', pages: ['tabCommodity'], legacy: 'tabCommodity',
      mount: () => MCX.revenue.cmd.mount() },
    { id: 'val-q', path: '/value/quarter', section: 'value', title: 'Quarter P&L', pages: ['tabQuarterly'], legacy: 'tabQuarterly',
      mount: () => loadQuarterly() },
    { id: 'val-fv', path: '/value/fair-value', section: 'value', title: 'Fair value', pages: ['tabValuation'], legacy: 'tabValuation',
      mount: () => loadValuation() },
    { id: 'val-scen', path: '/value/scenarios', section: 'value', title: 'Scenarios', pages: ['tabForecast'], legacy: 'tabForecast',
      mount: () => { recalcForecast(); MCX.poll.kick('cmp'); } },
    { id: 'sig-mom', path: '/signals/momentum', section: 'signals', title: 'Momentum', pages: ['tabMomentum'], legacy: 'tabMomentum',
      mount: () => loadMomentum() },
    { id: 'sig-ens', path: '/signals/ensemble', section: 'signals', title: 'Model ensemble', pages: ['pageSigEnsemble'],
      mount: () => loadModels() },
    { id: 'lab-diag', path: '/lab/diagnostics', section: 'lab', title: 'Diagnostics', pages: ['tabAnalytics'], legacy: 'tabAnalytics',
      mount: () => loadAnalytics() },
    { id: 'lab-pos', path: '/lab/positioning', section: 'lab', title: 'Positioning', pages: ['tabOIP'], legacy: 'tabOIP',
      mount: () => loadOIParticipants() },
    { id: 'lab-mar', path: '/lab/margins', section: 'lab', title: 'Margins', pages: ['tabMargins'], legacy: 'tabMargins',
      mount: () => loadMargins() },
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
    if (b.dataset.action === 'settings') { closeSheet(); openCookieModal(); }
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

  MCX.router.start({ fallback: 'today', legacy: LEGACY });
})();
