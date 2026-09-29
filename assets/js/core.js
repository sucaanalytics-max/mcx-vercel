/* MCX Revenue Monitor: shared helpers on window.MCX.
   A plain script loaded after Chart.js and before the page scripts, so every page can use it. */
(function () {
  'use strict';
  const MCX = window.MCX = window.MCX || {};

  // ── Formatting ───────────────────────────────────────────────────────────
  const MINUS = '−';
  function num(v, dp = 0) {
    const n = Number(v);
    if (v === null || v === undefined || v === '' || !Number.isFinite(n)) return '—';
    const s = Math.abs(n).toLocaleString('en-IN', { minimumFractionDigits: dp, maximumFractionDigits: dp });
    return (n < 0 && s.replace(/[0.,]/g, '') !== '' ? MINUS : '') + s;
  }
  MCX.fmt = {
    num,
    eps: v => num(v, 2),          // ₹ per share, always two decimals
  };

  // ── Timestamps ───────────────────────────────────────────────────────────
  // /api/refresh returns ISO 8601 on GET but "14:42 IST, 28 Sep 2026" after a
  // manual refresh (POST). new Date() can't read the second form.
  const MONTHS = { jan: 0, feb: 1, mar: 2, apr: 3, may: 4, jun: 5, jul: 6, aug: 7, sep: 8, oct: 9, nov: 10, dec: 11 };
  MCX.parseTs = function (s) {
    if (!s) return null;
    if (s instanceof Date) return isNaN(s) ? null : s;
    const m = /^\s*(\d{1,2}):(\d{2})\s*IST,\s*(\d{1,2})\s+([A-Za-z]{3})[A-Za-z]*\s+(\d{4})\s*$/.exec(String(s));
    if (m) {
      const mon = MONTHS[m[4].toLowerCase()];
      if (mon === undefined) return null;
      return new Date(Date.UTC(+m[5], mon, +m[3], +m[1], +m[2]) - 330 * 60000);   // IST is UTC+5:30
    }
    const d = new Date(s);
    return isNaN(d) ? null : d;
  };

  // ── Storage that can't throw (private windows, blocked site data) ───────
  MCX.storage = {
    get(key, fallback = null) {
      try { const v = window.localStorage.getItem(key); return v === null ? fallback : v; } catch (e) { return fallback; }
    },
    set(key, value) {
      try { window.localStorage.setItem(key, value); return true; } catch (e) { return false; }
    },
  };

  // ── Error state: message as text, never HTML, with an optional Retry ────
  MCX.ui = {
    error(el, message, retry) {
      if (typeof el === 'string') el = document.getElementById(el);
      if (!el) return;
      el.textContent = '';
      const box = document.createElement('div');
      box.className = 'mcx-error';
      box.setAttribute('role', 'alert');
      const msg = document.createElement('span');
      msg.textContent = message;
      box.appendChild(msg);
      if (retry) {
        const b = document.createElement('button');
        b.type = 'button';
        b.className = 'mcx-retry';
        b.textContent = 'Retry';
        b.addEventListener('click', retry);
        box.appendChild(b);
      }
      el.appendChild(box);
    },
  };

  // ── Market hours (IST) ───────────────────────────────────────────────────
  function istNow(d) {
    const ist = new Date((d || new Date()).getTime() + 330 * 60000);
    return { dow: ist.getUTCDay(), min: ist.getUTCHours() * 60 + ist.getUTCMinutes() };
  }
  MCX.market = {
    // NSE equity session, 09:15–15:30 IST on weekdays (exchange holidays not modelled)
    nseOpen(d) { const t = istNow(d); return t.dow >= 1 && t.dow <= 5 && t.min >= 555 && t.min <= 930; },
  };

  // ── Poll scheduler: run fn every ms, but only while when() holds ────────
  const polls = {};
  MCX.poll = {
    every(name, fn, ms, when) {
      if (polls[name]) clearInterval(polls[name].timer);
      const p = polls[name] = { fn, ms, when: when || (() => true), last: 0 };
      p.timer = setInterval(() => MCX.poll.kick(name), ms);
      return p;
    },
    // Run now if the condition holds and the last run is at least one interval old.
    kick(name) {
      const p = polls[name];
      if (!p || !p.when() || Date.now() - p.last < p.ms * 0.9) return;
      p.last = Date.now();
      p.fn();
    },
  };
  document.addEventListener('visibilitychange', () => {
    if (!document.hidden) Object.keys(polls).forEach(n => MCX.poll.kick(n));
  });

  // ── Store: shared values with change listeners ───────────────────────────
  const state = {}, subs = {};
  function safeCall(fn, v) { try { fn(v); } catch (e) { console.error(e); } }
  MCX.store = {
    get: key => state[key],
    set(key, value) { state[key] = value; (subs[key] || []).forEach(fn => safeCall(fn, value)); },
    // Calls fn now if the key already has a value, then on every change.
    on(key, fn) { (subs[key] = subs[key] || []).push(fn); if (key in state) safeCall(fn, state[key]); },
  };

  // ── Theme: light, dark or system, stored under mcxTheme ─────────────────
  // Applies html.dark, which the page CSS and chart code read.
  const THEMES = ['light', 'dark', 'system'];
  const darkQuery = window.matchMedia ? window.matchMedia('(prefers-color-scheme: dark)') : null;
  const themeSubs = [];
  let themeMode = null;                        // in-memory copy, for when storage is blocked
  MCX.theme = {
    mode() {
      const m = themeMode || MCX.storage.get('mcxTheme', 'system');
      return THEMES.includes(m) ? m : 'system';
    },
    isDark() { const m = MCX.theme.mode(); return m === 'dark' || (m === 'system' && !!darkQuery && darkQuery.matches); },
    next() { return THEMES[(THEMES.indexOf(MCX.theme.mode()) + 1) % THEMES.length]; },
    set(m) { themeMode = THEMES.includes(m) ? m : 'system'; MCX.storage.set('mcxTheme', themeMode); applyTheme(); },
    cycle() { MCX.theme.set(MCX.theme.next()); },
    // fn(isDark) runs when the resolved theme flips between light and dark
    onChange(fn) { themeSubs.push(fn); },
  };
  function applyTheme() {
    const root = document.documentElement;
    const dark = MCX.theme.isDark(), was = root.classList.contains('dark');
    root.classList.toggle('dark', dark);
    root.dataset.theme = MCX.theme.mode();
    if (dark !== was) themeSubs.forEach(fn => safeCall(fn, dark));
  }
  if (darkQuery) {
    const onSystem = () => { if (MCX.theme.mode() === 'system') applyTheme(); };
    if (darkQuery.addEventListener) darkQuery.addEventListener('change', onSystem); else darkQuery.addListener(onSystem);
  }
  applyTheme();

  // ── Router: #/section/page hashes ────────────────────────────────────────
  // Each route: { id, path, section, title, pages: [element ids], mount(), retheme() }.
  // mount runs every time the route is shown. retheme redraws charts after a
  // theme change; without one, mount is expected to redraw them.
  const routes = {}, byPath = {}, routeSubs = [], scrollMemo = {};
  let current = null, legacyMap = {}, fallbackId = null, userNav = false;
  MCX.router = {
    register(r) { routes[r.id] = r; byPath[r.path] = r; return r; },
    get: id => routes[id],
    all: () => Object.keys(routes).map(id => routes[id]),
    current: () => current,
    // Where a hash leads: { id, replace } where replace is the canonical hash to
    // swap in without a history entry (old tab ids, unknown routes), or null.
    resolve(hash) {
      const h = String(hash || '').replace(/^#/, '');
      if (h === '' || h === '/') return { id: fallbackId, replace: null };
      if (legacyMap[h]) return { id: byPath[legacyMap[h]] ? byPath[legacyMap[h]].id : fallbackId, replace: '#' + legacyMap[h] };
      const path = h.split('?')[0].replace(/\/+$/, '');
      if (byPath[path]) return { id: byPath[path].id, replace: null };
      return { id: fallbackId, replace: fallbackId ? '#' + routes[fallbackId].path : null };
    },
    go(path) { userNav = true; if (location.hash === '#' + path) show(byPath[path]); else location.hash = path; },
    onChange(fn) { routeSubs.push(fn); },
    start(opts) {
      legacyMap = opts.legacy || {};
      fallbackId = opts.fallback;
      // The router keeps each page's scroll position; the browser's would carry one page's offset to another.
      if ('scrollRestoration' in history) history.scrollRestoration = 'manual';
      window.addEventListener('hashchange', route);
      // Links into the app count as the user's own navigation: scroll to top and move focus.
      document.addEventListener('click', e => {
        const a = e.target.closest && e.target.closest('a[href^="#/"]');
        if (a && !e.defaultPrevented && e.button === 0 && !e.metaKey && !e.ctrlKey && !e.shiftKey
            && a.getAttribute('href') !== location.hash) userNav = true;
      });
      route();
    },
    // After a theme change: redraw the page on screen now, the others when next shown.
    retheme() {
      Object.keys(routes).forEach(id => { routes[id].dirty = id !== current; });
      const r = routes[current];
      if (r) run(r.retheme || r.mount, r.id);
    },
  };
  function run(fn, id) { if (!fn) return; try { fn(); } catch (e) { console.error('Route ' + id + ':', e); } }
  function route() {
    const res = MCX.router.resolve(location.hash);
    if (res.replace && res.replace !== location.hash) history.replaceState(history.state, '', location.pathname + location.search + res.replace);
    show(routes[res.id]);
  }
  function show(r) {
    if (!r) return;
    const prev = current, byUser = userNav;
    userNav = false;
    if (prev && prev !== r.id) scrollMemo[prev] = window.scrollY;
    document.querySelectorAll('.page').forEach(el => { el.hidden = !r.pages.includes(el.id); });
    current = r.id;
    routeSubs.forEach(fn => safeCall(fn, r));
    if (prev !== r.id) {
      window.scrollTo(0, byUser ? 0 : (scrollMemo[r.id] || 0));
      if (byUser) { const main = document.getElementById('main'); if (main) main.focus({ preventScroll: true }); }
    }
    if (r.dirty) { r.dirty = false; if (r.retheme) run(r.retheme, r.id); }
    run(r.mount, r.id);
  }

  // ── Chart.js reference line ──────────────────────────────────────────────
  // options.plugins.refLine = { value, label, color, dash, scale }. Replaces
  // options.plugins.annotation, whose plugin was never loaded, so nothing drew.
  if (window.Chart) {
    Chart.register({
      id: 'refLine',
      afterDatasetsDraw(chart, args, opts) {
        if (!opts || opts.value === undefined || opts.value === null) return;
        const scale = chart.scales[opts.scale || 'y'];
        if (!scale) return;
        const y = scale.getPixelForValue(opts.value);
        const { left, right, top, bottom } = chart.chartArea;
        if (y < top || y > bottom) return;
        const ctx = chart.ctx;
        ctx.save();
        ctx.strokeStyle = opts.color || '#8A857D';
        ctx.lineWidth = opts.width || 1;
        ctx.setLineDash(opts.dash || [6, 4]);
        ctx.beginPath();
        ctx.moveTo(left, y);
        ctx.lineTo(right, y);
        ctx.stroke();
        if (opts.label) {
          ctx.setLineDash([]);
          ctx.fillStyle = opts.labelColor || opts.color || '#8A857D';
          ctx.font = opts.font || '11px sans-serif';
          ctx.textAlign = 'right';
          ctx.textBaseline = 'bottom';
          ctx.fillText(opts.label, right - 4, y - 3);
        }
        ctx.restore();
      },
    });
  }
})();
