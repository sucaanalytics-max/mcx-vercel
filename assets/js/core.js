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
