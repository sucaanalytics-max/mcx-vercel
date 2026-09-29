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

  // ── Dates: ISO 'YYYY-MM-DD' strings in, labels out (no time zone involved) ──
  const DOW = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
  const DOW_LONG = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
  const MON = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  const MON_LONG = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
  function dparts(iso) {
    const [y, m, d] = String(iso).split('-').map(Number);
    return { y, m, d, dow: new Date(Date.UTC(y, m - 1, d)).getUTCDay() };
  }
  Object.assign(MCX.fmt, {
    weekday: iso => DOW[dparts(iso).dow],                                   // Mon
    weekdayLong: iso => DOW_LONG[dparts(iso).dow],                          // Monday
    dayMonth: iso => { const p = dparts(iso); return p.d + ' ' + MON[p.m - 1]; },               // 28 Sep
    day: iso => { const p = dparts(iso); return DOW[p.dow] + ' ' + p.d + ' ' + MON[p.m - 1]; }, // Mon 28 Sep
    dateLong: iso => { const p = dparts(iso); return `${DOW_LONG[p.dow]} ${p.d} ${MON_LONG[p.m - 1]} ${p.y}`; },
    // 22–28 Sep · 1 Jul – 28 Sep · 12 Nov 2026 – 20 Jan 2027
    span(a, b) {
      const p = dparts(a), q = dparts(b);
      if (p.y !== q.y) return `${p.d} ${MON[p.m - 1]} ${p.y} – ${q.d} ${MON[q.m - 1]} ${q.y}`;
      if (p.m !== q.m) return `${p.d} ${MON[p.m - 1]} – ${q.d} ${MON[q.m - 1]}`;
      return p.d === q.d ? `${q.d} ${MON[q.m - 1]}` : `${p.d}–${q.d} ${MON[q.m - 1]}`;
    },
  });

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
    return { dow: ist.getUTCDay(), min: ist.getUTCHours() * 60 + ist.getUTCMinutes(), iso: ist.toISOString().slice(0, 10) };
  }
  MCX.market = {
    // Today's date (iso), minutes since midnight and weekday, in IST
    ist: d => istNow(d),
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

  // ── SVG charts: small string builders shared by the v2 pages ────────────
  // Colours come from CSS classes (v2.css), so charts follow the theme without redrawing.
  const esc = s => String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  function niceStep(span, n) {
    const raw = span / Math.max(n, 1), mag = Math.pow(10, Math.floor(Math.log10(raw)));
    const f = raw / mag;
    return (f <= 1 ? 1 : f <= 2 ? 2 : f <= 2.5 ? 2.5 : f <= 5 ? 5 : 10) * mag;
  }
  MCX.svg = {
    esc,
    // pad: [left, right, top, bottom]; y() maps a value in [vmin, vmax] to pixels
    frame(w, h, vmax, pad, vmin = 0) {
      const f = { w, h, pl: pad[0], pr: pad[1], pt: pad[2], pb: pad[3], vmin, vmax };
      f.iw = w - f.pl - f.pr; f.ih = h - f.pt - f.pb;
      f.y = v => f.pt + f.ih * (1 - (v - vmin) / (vmax - vmin));
      return f;
    },
    niceTicks(vmax, n, vmin = 0) {
      const step = niceStep(vmax - vmin, n), out = [];
      for (let v = Math.ceil(vmin / step) * step; v <= vmax + 1e-9; v += step) out.push(+v.toFixed(6));
      return out;
    },
    // A bar with rounded top corners, anchored at yBot
    barPath(x, yTop, yBot, w, r) {
      const hgt = yBot - yTop;
      if (hgt <= 0) return '';
      r = Math.min(r, hgt, w / 2);
      const f = v => v.toFixed(1);
      return `M${f(x)},${f(yBot)}L${f(x)},${f(yTop + r)}Q${f(x)},${f(yTop)} ${f(x + r)},${f(yTop)}`
           + `L${f(x + w - r)},${f(yTop)}Q${f(x + w)},${f(yTop)} ${f(x + w)},${f(yTop + r)}L${f(x + w)},${f(yBot)}Z`;
    },
    hatch: id => `<pattern id="${id}" width="5" height="5" patternUnits="userSpaceOnUse" patternTransform="rotate(45)"><line x1="0" y1="0" x2="0" y2="5" class="c-hatch-line"/></pattern>`,
    text: (x, y, cls, s, anchor, halo) => `<text x="${x.toFixed(1)}" y="${y.toFixed(1)}" class="${cls}${halo ? ' c-halo' : ''}"${anchor ? ` text-anchor="${anchor}"` : ''}>${s}</text>`,
    open: (w, h, label) => `<svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}" role="img" aria-label="${esc(label)}">`,
    // Horizontal gridlines with ₹/% tick labels on the left
    grid(f, ticks, label) {
      return ticks.map(v => {
        const y = f.y(v);
        return `<line x1="${f.pl}" x2="${f.w - f.pr}" y1="${y.toFixed(1)}" y2="${y.toFixed(1)}" class="${v === f.vmin ? 'c-axis' : 'c-grid'}"/>`
             + MCX.svg.text(f.pl - 8, y + 4, 'c-tick', label(v), 'end');
      }).join('');
    },
    // Push labels apart so none sit closer than `gap` px, staying inside [top, bottom]
    spread(items, gap, top, bottom) {
      const out = items.map(x => ({ ...x })).sort((a, b) => a.y - b.y);
      for (let i = 1; i < out.length; i++) if (out[i].y - out[i - 1].y < gap) out[i].y = out[i - 1].y + gap;
      const over = out.length ? out[out.length - 1].y - bottom : 0;
      if (over > 0) out.forEach(x => { x.y -= over; });
      for (let i = out.length - 2; i >= 0; i--) if (out[i + 1].y - out[i].y < gap) out[i].y = out[i + 1].y - gap;
      if (out.length && out[0].y < top) { const d = top - out[0].y; out.forEach(x => { x.y += d; }); }
      return out;
    },
    // Crosshair + tooltip for time-series charts. xs: pixel x of each point; tip(i) returns HTML (escaped by the caller).
    hover(box, f, xs, tip) {
      const svg = box.querySelector('svg');
      if (!svg || !xs.length) return;
      let tt = box.querySelector('.c-tip');
      if (!tt) { tt = document.createElement('div'); tt.className = 'c-tip'; tt.hidden = true; box.appendChild(tt); }
      const line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
      line.setAttribute('class', 'c-cross'); line.setAttribute('y1', f.pt); line.setAttribute('y2', f.h - f.pb); line.style.display = 'none';
      svg.appendChild(line);
      const hit = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
      hit.setAttribute('x', f.pl); hit.setAttribute('y', f.pt); hit.setAttribute('width', f.iw); hit.setAttribute('height', f.ih);
      hit.setAttribute('fill', 'transparent');
      svg.appendChild(hit);
      const move = e => {
        const r = svg.getBoundingClientRect();
        const x = (e.touches ? e.touches[0].clientX : e.clientX) - r.left;
        let i = 0, best = Infinity;
        xs.forEach((px, k) => { const d = Math.abs(px - x); if (d < best) { best = d; i = k; } });
        line.setAttribute('x1', xs[i]); line.setAttribute('x2', xs[i]); line.style.display = '';
        tt.innerHTML = tip(i); tt.hidden = false;
        const left = xs[i] + 12 + tt.offsetWidth > box.clientWidth ? xs[i] - 12 - tt.offsetWidth : xs[i] + 12;
        tt.style.left = Math.max(0, left) + 'px'; tt.style.top = f.pt + 'px';
      };
      const leave = () => { line.style.display = 'none'; tt.hidden = true; };
      hit.addEventListener('mousemove', move); hit.addEventListener('touchstart', move, { passive: true });
      hit.addEventListener('touchmove', move, { passive: true });
      hit.addEventListener('mouseleave', leave); hit.addEventListener('touchend', leave);
    },
  };

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
