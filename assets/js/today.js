/* MCX Revenue Monitor: the Today page.
   Data: /api/refresh (the live snapshot, via MCX.store 'refresh', published by legacy.js's
   doRefresh), /api/exchange_dashboard?view=home (completed days, averages, measured
   projection error) and /api/quarterly (the quarter so far).
   Charts are SVG coloured by CSS variables, so they follow the theme without redrawing. */
(function () {
  'use strict';
  const fmt = MCX.fmt;
  const num = fmt.num;

  // ════════════════════════════════════════════════════════════════════════
  //  Model: pure functions (scripts/test_today_js.js)
  // ════════════════════════════════════════════════════════════════════════
  const RANKS = ['best', 'second-best', 'third-best', 'fourth-best', 'fifth-best'];
  const EVENING = 480;          // 17:00, minutes after 09:00
  const FIRST_RANGE_MIN = 30;   // no measured range before 09:30

  // live: a snapshot for today with the session open; closed: today's session is over;
  // pre: no snapshot for today (before the open, a holiday, or the relay hasn't reported)
  function sessionState(refresh, todayIso) {
    if (!refresh || !refresh.success || refresh.trading_date !== todayIso) return 'pre';
    return refresh.session_closed ? 'closed' : 'live';
  }

  // Linear interpolation of a grid value at `min` minutes after 09:00; ends are held flat
  function gridAt(grid, min, key) {
    const pts = (grid || []).filter(g => g[key] !== null && g[key] !== undefined);
    if (!pts.length) return null;
    if (min <= pts[0].min) return pts[0][key];
    const last = pts[pts.length - 1];
    if (min >= last.min) return last[key];
    for (let i = 1; i < pts.length; i++) {
      if (min <= pts[i].min) {
        const a = pts[i - 1], b = pts[i];
        return a[key] + (b[key] - a[key]) * (min - a.min) / (b.min - a.min);
      }
    }
    return null;
  }

  // error = projection / final − 1. With q10/q90 the 10th and 90th percentiles of past
  // errors at this time of day, the final lay in [P/(1+q90), P/(1+q10)] on 8 days in 10.
  function likelyRange(P, q10pct, q90pct) {
    if (!(P > 0) || q10pct === null || q10pct === undefined || q90pct === null || q90pct === undefined) return null;
    const lo = 1 + q90pct / 100, hi = 1 + q10pct / 100;
    return { lo: P / lo, hi: hi > 0.05 ? P / hi : null, q10: q10pct, q90: q90pct };
  }

  // Where the final landed relative to the projection, in %: below (negative side) and above
  function finalBand(q10pct, q90pct) {
    return { below: (1 - 1 / (1 + q90pct / 100)) * 100, above: q10pct > -95 ? (1 / (1 + q10pct / 100) - 1) * 100 : null };
  }

  const mean = xs => xs.reduce((a, b) => a + b, 0) / xs.length;
  const rankOf = (v, vals) => 1 + vals.filter(x => x > v).length;
  const pct = (a, b) => (a / b - 1) * 100;

  function headlineLive(P, range, avgs, elapsedPct) {
    const vals = avgs.filter(a => a.value !== null).map(a => a.value);
    const p1 = `₹${num(P, 1)} Cr`;
    if (range && vals.length && range.lo > Math.max(...vals)) return `Today is on course for about ${p1}, above every recent average.`;
    if (range && range.hi !== null && vals.length && range.hi < Math.min(...vals)) return `Today is on course for about ${p1}, below every recent average.`;
    if (!range) return `Today is tracking towards ${p1}, but it is too early to say how far to trust that.`;
    const r = range.hi !== null ? `₹${num(range.lo, 1)} to ₹${num(range.hi, 1)} Cr` : `₹${num(range.lo, 1)} Cr or more`;
    return elapsedPct < 50 ? `Today is tracking towards ${p1}, but it is early: the likely range is ${r}.`
                           : `Today is tracking towards ${p1}; the likely range is ${r}.`;
  }

  function deckLive(P, B, min, avgs, w21) {
    const vals = avgs.filter(a => a.value !== null).map(a => a.value);
    const rest = P - B;
    let s = `₹${num(B, 2)} Cr is booked so far. `;
    if (rest > 0.005) {
      s += min < EVENING
        ? `The projection expects the other ₹${num(rest, 2)} Cr later in the day, mostly in the evening hours (17:00–23:30), when trading is usually heaviest. `
        : `The projection expects the other ₹${num(rest, 2)} Cr before the close. `;
    }
    if (vals.length && P > Math.max(...vals)) s += `At ₹${num(P, 1)} Cr today would beat every average below. `;
    if (min < 720 && w21 !== null) s += `By 21:00, the final has landed within ${Math.round(w21)}% of the projection on 8 days in 10.`;
    return s.trim();
  }

  // Compare a day with the 45 completed days before it: "14% above its previous 45-day average"
  function vsPrev45(value, prev45) {
    if (!prev45 || prev45.length < 45) return null;
    const avg = mean(prev45.map(r => r.total));
    const c = pct(value, avg);
    return { avg, pct: c, first: prev45[0].date, last: prev45[prev45.length - 1].date,
             text: Math.abs(c) < 0.5 ? 'level with its previous 45-day average'
                                     : `${Math.abs(c).toFixed(0)}% ${c > 0 ? 'above' : 'below'} its previous 45-day average` };
  }

  function rankText(value, last60) {
    const r = rankOf(value, last60);
    return r <= RANKS.length ? `the ${RANKS[r - 1]} day of the last ${last60.length}` : null;
  }

  // One sentence on the six averages; only claims an ordering the numbers show
  function takeaway(avgs) {
    const all = avgs.filter(a => a.value !== null);
    const fy = avgs.find(a => a.key === 'fytd');
    const short = avgs.filter(a => /^d\d+$/.test(a.key) && a.value !== null);
    if (!fy || fy.value === null || !short.length || all.length < 3) return '';
    const vals = all.map(a => a.value);
    const desc = vals.every((v, i) => i === 0 || vals[i - 1] >= v);
    const asc = vals.every((v, i) => i === 0 || vals[i - 1] <= v);
    const hi = short.reduce((m, a) => (a.value > m.value ? a : m));
    const lo = short.reduce((m, a) => (a.value < m.value ? a : m));
    const relHi = pct(hi.value, fy.value), relLo = pct(lo.value, fy.value);
    const f = a => `₹${num(a.value, 2)} Cr`;
    if (desc && relHi >= 0.5) return `The shorter the window, the higher the average: the last ${hi.n} days averaged ${f(hi)}, ${Math.abs(relHi).toFixed(0)}% above the ${fy.period} average so far.`;
    if (asc && relLo <= -0.5) return `The shorter the window, the lower the average: the last ${lo.n} days averaged ${f(lo)}, ${Math.abs(relLo).toFixed(0)}% below the ${fy.period} average so far.`;
    if (Math.abs(relHi) < 2 && Math.abs(relLo) < 2) return `Recent days are running close to the ${fy.period} average so far of ${f(fy)}.`;
    const up = Math.abs(relHi) >= Math.abs(relLo);
    const a = up ? hi : lo, rel = up ? relHi : relLo, dir = rel >= 0 ? 'above' : 'below';
    return `Recent days are running ${Math.abs(rel) > 20 ? 'well ' : ''}${dir} the longer averages: the last ${a.n} averaged ${f(a)}, ${Math.abs(rel).toFixed(0)}% ${dir} the ${fy.period} average so far.`;
  }

  // What a window is compared with, and by how much
  function change(a) {
    if (!a.prev || a.chg_pct === null || a.chg_pct === undefined || a.n < 5) return null;
    const vs = a.key === 'qtd' || a.key === 'fytd' ? `all of ${a.prev.label}` : `${a.n} days before`;
    if (Math.abs(a.chg_pct) < 0.5) return { dir: 'level', pct: 0, vs };
    return { dir: a.chg_pct > 0 ? 'up' : 'down', pct: Math.abs(a.chg_pct), vs };
  }

  const spread = MCX.svg.spread, niceTicks = MCX.svg.niceTicks;

  // Rolling mean of the last n totals ending at each row (null until n rows exist)
  function rolling(rows, n) {
    let sum = 0;
    return rows.map((r, i) => { sum += r.total; if (i >= n) sum -= rows[i - n].total; return i >= n - 1 ? sum / n : null; });
  }

  MCX.todayModel = { sessionState, gridAt, likelyRange, finalBand, headlineLive, deckLive, vsPrev45, rankText,
                     takeaway, change, spread, niceTicks, rolling };
  if (window.MCX_TEST) return;

  // ════════════════════════════════════════════════════════════════════════
  //  Page
  // ════════════════════════════════════════════════════════════════════════
  const $ = id => document.getElementById(id);
  const esc = MCX.svg.esc;
  const INFO = '<svg aria-hidden="true" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M12 11v5"/><path d="M12 8h.01"/></svg>';
  const HOME_TTL = 10 * 60 * 1000, QTR_TTL = 5 * 60 * 1000;
  const GROUPS = [
    ['Gold', ['GOLD', 'GOLDM', 'GOLDTEN', 'GOLDPETAL', 'GOLDGUINEA']],
    ['Silver', ['SILVER', 'SILVERM', 'SILVERMIC', 'SILVER100']],
    ['Crude oil', ['CRUDEOIL', 'CRUDEOILM']],
    ['Natural gas', ['NATURALGAS', 'NATGASMINI']],
    ['Base metals', ['COPPER', 'ZINC', 'ALUMINIUM', 'LEAD', 'NICKEL', 'ZINCMINI', 'LEADMINI', 'ALUMINI']],
  ];
  const S = { refresh: undefined, home: null, homeAt: 0, homeErr: null, qtr: null, qtrAt: 0, kind: 'options', present: false, width: 0 };
  let loadingHome = null, loadingQtr = null;

  function loadHome(force) {
    if (!force && S.home && Date.now() - S.homeAt < HOME_TTL) return Promise.resolve();
    if (loadingHome) return loadingHome;
    loadingHome = fetch('/api/exchange_dashboard?view=home').then(r => r.json()).then(d => {
      if (!d.success) throw new Error(d.error || 'No data');
      S.home = d; S.homeAt = Date.now(); S.homeErr = null;
    }).catch(e => { S.homeErr = e.message || String(e); }).finally(() => { loadingHome = null; render(); });
    return loadingHome;
  }
  function loadQuarter() {
    if (S.qtr && Date.now() - S.qtrAt < QTR_TTL) return;
    if (loadingQtr) return;
    loadingQtr = fetch('/api/quarterly').then(r => r.json()).then(d => {
      if (d.success && d.current_quarter) { S.qtr = d.current_quarter; S.qtrAt = Date.now(); }
    }).catch(() => {}).finally(() => { loadingQtr = null; renderQuarter(); });
  }

  // Everything the sections need, from the three sources
  function derive() {
    const ist = MCX.market.ist();
    const r = S.refresh, h = S.home;
    const state = sessionState(r, ist.iso);
    const d = { ist, state, r, h, avgs: (h && h.averages) || [], daily: (h && h.daily) || [] };
    const grid = h && h.accuracy ? h.accuracy.grid : [];
    // Where the final landed relative to the projection made at `min`, on 8 days in 10
    d.band = min => {
      const a = gridAt(grid, min, 'q10_pct'), b = gridAt(grid, min, 'q90_pct');
      return a === null || b === null ? null : finalBand(a, b);
    };
    d.bandAt = key => { const g = grid.find(x => x.min === key); return g && g.q10_pct !== null ? finalBand(g.q10_pct, g.q90_pct) : null; };
    if (state === 'live') {
      d.P = r.proj_rev_cr; d.B = r.booked_rev_cr; d.min = r.elapsed_min;
      const early = d.min < FIRST_RANGE_MIN;
      d.q10 = early ? null : gridAt(grid, d.min, 'q10_pct');
      d.q90 = early ? null : gridAt(grid, d.min, 'q90_pct');
      d.range = likelyRange(d.P, d.q10, d.q90);
      d.nowBand = d.range ? finalBand(d.q10, d.q90) : null;
      d.clock = clock(d.min);
    }
    if (state === 'closed' || state === 'pre') {
      const todayFinal = h && h.today_final;
      if (state === 'closed' && !todayFinal) {
        d.last = { date: ist.iso, fut: r.proj_fut_rev_cr, opt: r.proj_opt_rev_cr, total: r.proj_rev_cr, provisional: true };
        d.before = d.daily;
      } else if (d.daily.length) {
        d.last = d.daily[d.daily.length - 1];
        d.before = d.daily.slice(0, -1);
      }
      if (d.last) {
        d.vs45 = vsPrev45(d.last.total, d.before.slice(-45));
        d.rank = rankText(d.last.total, d.before.slice(-59).map(x => x.total).concat(d.last.total));
      }
    }
    return d;
  }

  const clock = m => { const t = 540 + m; return `${String(Math.floor(t / 60)).padStart(2, '0')}:${String(t % 60).padStart(2, '0')}`; };
  const cr = (v, dp = 2) => `₹${num(v, dp)}`;
  const nb = s => s.replace(/ /g, '&nbsp;');

  // ── Lede and stats ───────────────────────────────────────────────────────
  function lede(d) {
    const date = fmt.dateLong(d.ist.iso);
    let kicker, head, deck = '';
    if (d.state === 'live') {
      kicker = `${date} · live session, ${Math.round(d.r.elapsed_pct)}% of trading hours gone`;
      head = headlineLive(d.P, d.range, d.avgs, d.r.elapsed_pct);
      const b21 = d.bandAt(720);
      deck = deckLive(d.P, d.B, d.min, d.avgs, b21 ? Math.max(b21.below, b21.above ?? 0) : null);
      if (d.h && d.h.today_us_holiday) deck += ' US markets are closed today; evenings are usually quieter on such days, so the projection may run high.';
      if (d.r.opening_artifact) deck += ' The first snapshot of the day looks stale, so treat this projection with care until the next update.';
    } else if (d.last) {
      const provisional = d.last.provisional;
      if (d.state === 'closed') {
        kicker = `${date} · session closed`;
        head = `Closed at ${cr(d.last.total)} Cr` + (d.vs45 ? `, ${d.vs45.text}` : '');
      } else {
        const when = daysBetween(d.last.date, d.ist.iso) <= 6 ? fmt.weekdayLong(d.last.date) : fmt.dayMonth(d.last.date);
        kicker = `${date} · ${d.h && !d.h.today_is_session ? 'no trading today' : d.ist.min < 540 ? 'before the open' : 'waiting for today’s first snapshot'}`;
        head = `${when} closed at ${cr(d.last.total)} Cr` + (d.rank ? `, ${d.rank}` : d.vs45 ? `, ${d.vs45.text}` : '');
      }
      const parts = [];
      if (d.vs45) parts.push(`That average (${fmt.span(d.vs45.first, d.vs45.last)}) was ${cr(d.vs45.avg)} Cr.`);
      if (d.last.opt) parts.push(`Options brought in ${cr(d.last.opt)} Cr of the day.`);
      if (d.rank && d.state === 'closed') parts.push(`The ${d.rank}.`);
      if (provisional) parts.push('This is the last projection of the session; the end-of-day record usually confirms it by 23:45.');
      if (d.state === 'pre') {
        parts.push(d.h && !d.h.today_is_session ? 'MCX is closed today. The next session’s projection appears after its first snapshot.'
                   : d.ist.min < 540 ? 'The live projection appears after the first snapshot, shortly after 09:00.'
                   : 'The relay has not reported a snapshot for today yet.');
      }
      deck = parts.join(' ');
    } else {
      kicker = fmt.dateLong(d.ist.iso);
      head = S.homeErr ? 'Today’s figures could not be loaded.' : 'Loading today’s revenue…';
    }
    $('tdKicker').textContent = kicker;
    $('tdHeadline').textContent = head;
    $('tdDeck').textContent = deck;
    return { kicker, head, deck };
  }

  function daysBetween(a, b) { return Math.round((Date.parse(b) - Date.parse(a)) / 86400000); }

  function stat(label, value, sub, lead) {
    return `<div class="stat${lead ? ' stat--lead' : ''}"><div class="stat-label">${label}</div>`
         + `<div class="stat-value">${value}</div>${sub ? `<div class="stat-sub">${sub}</div>` : ''}</div>`;
  }
  const rangeText = rg => !rg ? 'Too early for a measured range: the first check is at 09:30'
    : rg.hi !== null ? `Likely range ${nb(`${cr(rg.lo, 1)} to ${cr(rg.hi, 1)} Cr`)} (held on 8&nbsp;in&nbsp;10 past&nbsp;days)`
    : `Likely ${cr(rg.lo, 1)} Cr or more (held on 8&nbsp;in&nbsp;10 past&nbsp;days)`;

  function statsHtml(d) {
    const ma = d.h ? d.h.ma45 : null;
    if (d.state === 'live') {
      return stat('Projected revenue', `${cr(d.P)}<small>Cr</small>`, `<span class="phone-only">${cr(d.B)} Cr booked so far · </span>${rangeText(d.range)}`, true)
        + stat('Booked so far', `${cr(d.B)}<small>Cr</small>`, `by ${d.clock}, with ${Math.round(d.r.elapsed_pct)}% of today’s trading hours gone`)
        + stat('45-day average', ma ? `${cr(ma)}<small>Cr</small>` : '—', ma ? `today’s projection is ${Math.abs(pct(d.P, ma)).toFixed(0)}% ${d.P >= ma ? 'above' : 'below'} it` : '');
    }
    if (d.last) {
      const opt = d.last.opt / d.last.total * 100;
      const lab = d.state === 'closed' ? 'Today’s revenue' : `Last session, ${fmt.day(d.last.date)}`;
      return stat(lab, `${cr(d.last.total)}<small>Cr</small>`, d.last.provisional ? 'last projection of the session; final record due by 23:45' : 'final', true)
        + stat('Futures and options', `${cr(d.last.fut)} + ${cr(d.last.opt)}`, `options were ${opt.toFixed(0)}% of the day`)
        + stat('Previous 45-day average', d.vs45 ? `${cr(d.vs45.avg)}<small>Cr</small>` : '—', d.vs45 ? `${fmt.span(d.vs45.first, d.vs45.last)}; the day was ${d.vs45.text.replace(' its previous 45-day average', ' it')}` : '');
    }
    return stat('&nbsp;', '<span class="skel"></span>', '', true);
  }

  // ── SVG helpers (core.js) ────────────────────────────────────────────────
  const { frame, barPath, hatch, open: svgOpen } = MCX.svg;
  const txt = MCX.svg.text;

  // Revenue per day for the last completed days, then today (booked, projected rest, likely range)
  function sessionsSvg(o) {
    const { w, rows, live, ma45, uid } = o;
    const sc = o.scale || 1, fs = px => Math.round(px * sc);
    const compact = w < 520;
    const n = rows.length + (live ? 1 : 0);
    const h = o.h || (compact ? 210 : Math.round(380 * sc));
    const liveTop = live ? (live.range ? (live.range.hi !== null ? live.range.hi : live.P * 1.35) : live.P) : 0;
    const vmax = Math.max(...rows.map(r => r.total), liveTop, ma45 || 0) * (live ? 1.08 : 1.2);
    const f = frame(w, h, vmax, compact ? [40, 8, 22, 42] : [fs(54), live ? fs(150) : fs(24), fs(26), fs(46)]);
    const bw = f.iw / n, bwid = Math.min(bw * (compact ? 0.62 : 0.56), 72 * sc);
    const x0 = i => f.pl + bw * i + (bw - bwid) / 2;
    const base = f.y(0), y45 = ma45 ? f.y(ma45) : null;
    let desc = rows.map(r => `${fmt.day(r.date)} ₹${num(r.total, 2)} crore`).join(', ');
    if (live) desc += `; today ₹${num(live.B, 2)} crore booked so far and ₹${num(live.P, 2)} crore projected`
      + (live.range ? (live.range.hi !== null ? `, likely range ₹${num(live.range.lo, 1)} to ₹${num(live.range.hi, 1)} crore` : `, likely ₹${num(live.range.lo, 1)} crore or more`) : '');
    if (ma45) desc += `; 45-day average ₹${num(ma45, 2)} crore`;
    const s = [svgOpen(w, h, 'Revenue per trading day: ' + desc), `<defs>${hatch('sh-' + uid)}</defs>`];
    for (const v of niceTicks(vmax, compact ? 3 : 6)) {
      const y = f.y(v);
      s.push(`<line x1="${f.pl}" x2="${f.w - f.pr}" y1="${y.toFixed(1)}" y2="${y.toFixed(1)}" class="${v ? 'c-grid' : 'c-axis'}"/>`);
      s.push(txt(f.pl - 8, y + 4, 'c-tick', `₹${num(v, 0)}`, 'end'));
    }
    if (live && live.range) {
      const xa = x0(n - 1), yTop = live.range.hi !== null ? f.y(live.range.hi) : f.pt;
      s.push(`<rect x="${(xa - 6).toFixed(1)}" y="${yTop.toFixed(1)}" width="${(bwid + 12).toFixed(1)}" height="${(f.y(live.range.lo) - yTop).toFixed(1)}" rx="3" class="c-band"/>`);
    }
    if (y45 !== null) {
      const x45 = live && !compact ? x0(n - 1) + bwid + 8 : f.w - f.pr;
      s.push(`<line x1="${f.pl}" x2="${x45.toFixed(1)}" y1="${y45.toFixed(1)}" y2="${y45.toFixed(1)}" class="c-typical"/>`);
    }
    const stack = (i, fut, opt, title) => {
      const xa = x0(i), yf = f.y(fut);
      s.push(`<g><title>${esc(title)}</title><rect x="${xa.toFixed(1)}" y="${yf.toFixed(1)}" width="${bwid.toFixed(1)}" height="${Math.max(base - yf, 0).toFixed(1)}" class="c-fut"/>`
           + `<path d="${barPath(xa, f.y(fut + opt), yf - 2, bwid, 3)}" class="c-opt"/></g>`);
    };
    const valueLabel = (cx, v, text) => {
      let yl = f.y(v) - 7;
      if (y45 !== null && yl - fs(10) < y45 && y45 < yl + 3) yl = y45 - 5;      // keep the average line out of the label
      s.push(txt(cx, yl, 'c-label', text, 'middle', true));
    };
    rows.forEach((r, i) => {
      stack(i, r.fut, r.opt, `${fmt.day(r.date)}: ₹${num(r.total, 2)} Cr (futures ₹${num(r.fut, 2)}, options ₹${num(r.opt, 2)})${r.provisional ? ', last projection' : ''}`);
      valueLabel(x0(i) + bwid / 2, r.total, `₹${num(r.total, 2)}`);
    });
    if (live) {
      const i = n - 1, xa = x0(i), cx = xa + bwid / 2, yb = f.y(live.B);
      stack(i, live.bFut, live.bOpt, `Today so far: ₹${num(live.B, 2)} Cr booked (futures ₹${num(live.bFut, 2)}, options ₹${num(live.bOpt, 2)})`);
      s.push(`<g><title>${esc(`Today, projected: ₹${num(live.P, 2)} Cr`)}</title><path d="${barPath(xa, f.y(live.P), yb - 2, bwid, 3)}" fill="url(#sh-${uid})" class="c-proj"/></g>`);
      if (compact) {
        s.push(txt(cx, f.y(live.P) - 6, 'c-label2', `≈₹${num(live.P, 1)}`, 'middle', true));
      } else {
        const xl = xa + bwid + 14;
        const items = [{ y: f.y(live.P) + 5, s: `₹${num(live.P, 2)} projected`, cls: 'c-label' },
                       { y: f.y(live.B / 2) + 4, s: `₹${num(live.B, 2)} booked so far`, cls: 'c-label2' }];
        if (live.range) {
          items.push({ y: f.y(live.range.lo) + 4, s: `low ₹${num(live.range.lo, 1)}`, cls: 'c-label2' });
          items.push({ y: (live.range.hi !== null ? f.y(live.range.hi) : f.pt) + 4, s: live.range.hi !== null ? `high ₹${num(live.range.hi, 1)}` : 'no upper bound yet', cls: 'c-label2' });
        }
        spread(items, fs(15), f.pt + 4, base - 2).forEach(it => s.push(txt(xl, it.y, it.cls, it.s)));
      }
    }
    const labels = rows.map(r => r.date === o.todayIso ? ['Today', r.provisional ? 'last projection' : (compact ? fmt.dayMonth(r.date) : fmt.day(r.date)), true]
                                                        : [fmt.weekday(r.date), fmt.dayMonth(r.date), false]);
    if (live) labels.push(['Today', compact ? fmt.dayMonth(o.todayIso) : fmt.day(o.todayIso), true]);
    labels.forEach(([a, b, today], i) => {
      const cx = x0(i) + bwid / 2;
      s.push(txt(cx, base + (compact ? 17 : fs(20)), 'c-label', a, 'middle'));
      s.push(txt(cx, base + (compact ? 32 : fs(37)), today ? 'c-label2' : 'c-label3', b, 'middle'));
    });
    s.push('</svg>');
    return s.join('');
  }

  function sessionsLegend(live, compact) {
    const items = ['<span><i class="sw sw--fut"></i>Futures</span>', '<span><i class="sw sw--opt"></i>Options</span>'];
    if (live) {
      items.push(`<span><i class="sw sw--hatch"></i>${compact ? 'Rest of today (projected)' : 'Rest of today, projected'}</span>`);
      if (live.range) items.push(`<span><i class="sw sw--band"></i>${compact ? 'Likely range' : 'Likely range for today (held on 8 in 10 past days)'}</span>`);
    }
    items.push('<span><i class="sw sw--typical"></i>45-day average</span>');
    return items.join('');
  }

  // Where past finals landed relative to the projection made at each time of day (8 days in 10),
  // with the median as a line: a lean below zero means projections have run high.
  function trustSvg(w, grid, nowMin, nowBand) {
    const compact = w < 520;
    const pts = grid.filter(g => g.q10_pct !== null).map(g => {
      const b = finalBand(g.q10_pct, g.q90_pct);
      return { min: g.min, time: g.time, n: g.n, lo: -b.below, hi: b.above, med: (1 / (1 + g.median_pct / 100) - 1) * 100 };
    }).filter(p => p.hi !== null);
    if (!pts.length) return '';
    const hiMax = Math.min(100, Math.max(...pts.map(p => p.hi))), loMin = Math.max(-100, Math.min(...pts.map(p => p.lo)));
    const top = Math.ceil(hiMax / 10) * 10, bot = Math.floor(loMin / 10) * 10;
    const h = compact ? 190 : 220;
    const f = frame(w, h, top, [52, compact ? 10 : 16, 18, 30], bot);
    const X = m => f.pl + f.iw * m / 870, Y = v => f.y(Math.max(bot, Math.min(top, v)));
    const s = [svgOpen(w, h, 'Where the final landed relative to the projection made at each time of day, on 8 of 10 past days: '
      + pts.map(p => `${p.time} ${num(p.lo, 0)}% to +${num(p.hi, 0)}%`).join(', '))];
    s.push(MCX.svg.grid(f, niceTicks(top, compact ? 4 : 6, bot), v => (v > 0 ? '+' : '') + num(v, 0) + '%'));
    const zero = f.y(0);
    s.push(`<line x1="${f.pl}" x2="${f.w - f.pr}" y1="${zero.toFixed(1)}" y2="${zero.toFixed(1)}" class="c-axis"/>`);
    const up = pts.map(p => `${X(p.min).toFixed(1)},${Y(p.hi).toFixed(1)}`), dn = pts.map(p => `${X(p.min).toFixed(1)},${Y(p.lo).toFixed(1)}`).reverse();
    s.push(`<path d="M${up.join('L')}L${dn.join('L')}Z" class="c-band"/>`);
    s.push(`<path d="M${pts.map(p => `${X(p.min).toFixed(1)},${Y(p.med).toFixed(1)}`).join('L')}" class="c-line"/>`);
    pts.forEach(p => s.push(`<g><title>${p.time}: final ${num(p.lo, 0)}% to +${num(p.hi, 0)}% of the projection; median ${p.med >= 0 ? '+' : ''}${num(p.med, 0)}% (${p.n} days)</title>`
      + `<rect x="${(X(p.min) - 6).toFixed(1)}" y="${Y(p.hi).toFixed(1)}" width="12" height="${(Y(p.lo) - Y(p.hi)).toFixed(1)}" fill="transparent"/></g>`));
    if (nowMin !== null && nowBand && nowMin >= pts[0].min) {
      const xn = X(nowMin);
      s.push(`<line x1="${xn.toFixed(1)}" x2="${xn.toFixed(1)}" y1="${f.pt - 6}" y2="${(f.h - f.pb).toFixed(1)}" class="c-now"/>`);
      const right = xn < f.w - f.pr - 150;
      s.push(txt(xn + (right ? 8 : -8), f.pt + 8, 'c-label', `Now: −${num(nowBand.below, 0)}% to +${num(nowBand.above, 0)}%`, right ? null : 'end', true));
    }
    [[0, '09:00'], [180, '12:00'], [360, '15:00'], [540, '18:00'], [720, '21:00'], [870, '23:30']].forEach(([m, lab]) => {
      if (compact && (m === 180 || m === 540)) return;
      s.push(txt(X(m), h - 8, 'c-tick', lab, m === 0 ? 'start' : m === 870 ? 'end' : 'middle'));
    });
    s.push('</svg>');
    return s.join('');
  }

  // Revenue per completed day over the chosen range: futures and options stacked (context),
  // a rolling 20-day average (the trend, solid) and a rolling 45-day average (typical, dashed);
  // today as booked + projected rest with a thin range marker.
  function dailySvg(w, all, n, today, uid) {
    const compact = w < 520;
    const r20 = rolling(all, 20), r45 = rolling(all, 45);
    const k0 = Math.max(0, all.length - n);
    const rows = all.slice(k0), a20 = r20.slice(k0), a45 = r45.slice(k0);
    const m = rows.length + (today ? 1 : 0);
    if (!m) return { svg: '' };
    const top = Math.max(...rows.map(r => r.total), today ? (today.hi || today.value) : 0);
    const vmax = top * 1.08, h = compact ? 170 : 220;
    const f = frame(w, h, vmax, [44, compact ? 8 : 156, 12, 26]);
    const bw = f.iw / m, gap = bw > 5 ? 1.5 : bw > 2.5 ? 0.6 : 0;
    const cx = i => f.pl + bw * i + bw / 2;
    const s = [svgOpen(w, h, `Revenue per trading day for the last ${rows.length} days, with rolling 20-day and 45-day averages`
      + (today ? `; today ₹${num(today.value, 2)} crore ${today.label}` : '')), `<defs>${hatch('dh-' + uid)}</defs>`];
    s.push(MCX.svg.grid(f, niceTicks(vmax, compact ? 3 : 5), v => `₹${num(v, 0)}`));
    const r = bw > 6 ? 2 : 0, bwid = Math.max(bw - gap, 0.6);
    rows.forEach((d, i) => {
      const x = f.pl + bw * i + gap / 2, yf = f.y(d.fut);
      s.push(`<g class="c-soft"><rect x="${x.toFixed(2)}" y="${yf.toFixed(1)}" width="${bwid.toFixed(2)}" height="${Math.max(f.y(0) - yf, 0).toFixed(1)}" class="c-fut"/>`
        + `<path d="${barPath(x, f.y(d.total), yf - (bw > 4 ? 1 : 0), bwid, r)}" class="c-opt"/></g>`);
    });
    const line = (vals, cls) => {
      const pts = vals.map((v, i) => v === null ? null : `${cx(i).toFixed(1)},${f.y(v).toFixed(1)}`).filter(Boolean);
      return pts.length > 1 ? `<path d="M${pts.join('L')}" class="${cls}"/>` : '';
    };
    s.push(line(a45, 'c-typical'), line(a20, 'c-line'));
    if (today) {
      const i = rows.length, x = f.pl + bw * i + gap / 2, xc = cx(i);
      if (today.booked !== undefined) {
        s.push(`<path d="${barPath(x, f.y(today.value), f.y(today.booked), bwid, r)}" fill="url(#dh-${uid})" class="c-proj"/>`);
        s.push(`<rect x="${x.toFixed(2)}" y="${f.y(today.booked).toFixed(1)}" width="${bwid.toFixed(2)}" height="${(f.y(0) - f.y(today.booked)).toFixed(1)}" class="c-bar"/>`);
      } else {
        s.push(`<path d="${barPath(x, f.y(today.value), f.y(0), bwid, r)}" fill="url(#dh-${uid})" class="c-proj"/>`);
      }
      if (today.lo) {
        const y1 = f.y(today.hi || vmax), y2 = f.y(today.lo);
        s.push(`<line x1="${xc.toFixed(1)}" x2="${xc.toFixed(1)}" y1="${y1.toFixed(1)}" y2="${y2.toFixed(1)}" class="c-whisker"/>`
          + (today.hi ? `<line x1="${(xc - 4).toFixed(1)}" x2="${(xc + 4).toFixed(1)}" y1="${y1.toFixed(1)}" y2="${y1.toFixed(1)}" class="c-whisker"/>` : '')
          + `<line x1="${(xc - 4).toFixed(1)}" x2="${(xc + 4).toFixed(1)}" y1="${y2.toFixed(1)}" y2="${y2.toFixed(1)}" class="c-whisker"/>`);
      }
    }
    if (!compact) {
      const last = rows.length - 1, items = [];
      if (today) items.push({ y: f.y(today.value) + 4, s: `Today ₹${num(today.value, 2)} ${today.label}`, cls: 'c-label' });
      if (today && today.hi) items.push({ y: f.y(today.hi) + 4, s: `range to ₹${num(today.hi, 1)}`, cls: 'c-label2' });
      if (a20[last] !== null) items.push({ y: f.y(a20[last]) + 4, s: `20-day avg ₹${num(a20[last], 2)}`, cls: 'c-label' });
      if (a45[last] !== null) items.push({ y: f.y(a45[last]) + 4, s: `45-day avg ₹${num(a45[last], 2)}`, cls: 'c-label2' });
      spread(items, 15, f.pt + 4, f.y(0)).forEach(it => s.push(txt(f.w - f.pr + 12, it.y, it.cls, it.s)));
    }
    if (rows.length) s.push(txt(f.pl, h - 8, 'c-tick', fmt.dayMonth(rows[0].date)));
    s.push(txt(f.w - f.pr, h - 8, 'c-tick', today ? 'Today' : fmt.dayMonth(rows[rows.length - 1].date), 'end'));
    s.push('</svg>');
    const xs = rows.map((_, i) => cx(i));
    const tip = i => `<strong>${fmt.day(rows[i].date)}</strong><br>₹${num(rows[i].total, 2)} Cr<span class="c-tip-sub"> · futures ₹${num(rows[i].fut, 2)}, options ₹${num(rows[i].opt, 2)}</span>`
      + (a20[i] !== null ? `<br><span class="c-tip-sub">20-day avg ₹${num(a20[i], 2)}${a45[i] !== null ? ` · 45-day ₹${num(a45[i], 2)}` : ''}</span>` : '');
    return { svg: s.join(''), f, xs, tip };
  }

  // ── Sections ─────────────────────────────────────────────────────────────
  function chartRows(d) {
    if (d.state === 'live') return { rows: d.daily.slice(-5), live: liveBar(d) };
    if (d.state === 'closed' && d.last && d.last.provisional) return { rows: d.daily.slice(-4).concat([d.last]), live: null };
    return { rows: d.daily.slice(-5), live: null };
  }
  function liveBar(d) {
    return { P: d.P, B: d.B, bFut: d.r.booked_fut_rev_cr, bOpt: d.r.booked_opt_rev_cr, range: d.range };
  }

  function renderSessions(d) {
    const box = $('tdSessions');
    if (!d.h) {
      if (S.homeErr) MCX.ui.error(box, 'Could not load the last trading days: ' + S.homeErr, () => loadHome(true));
      return;
    }
    const w = Math.round(box.clientWidth) || 700;
    const { rows, live } = chartRows(d);
    $('tdChartTitle').textContent = d.state === 'live' ? 'Today and the last five trading days'
      : d.state === 'closed' ? 'Today and the four trading days before it' : 'The last five trading days';
    box.innerHTML = sessionsSvg({ w, rows, live, ma45: d.h.ma45, uid: 's', todayIso: d.ist.iso });
    $('tdSessionsLegend').innerHTML = sessionsLegend(live, w < 520);
    $('tdSessionsBasis').innerHTML = INFO + '<span>' + (live && live.range && d.nowBand && d.nowBand.above !== null
      ? `On 8 of 10 past days, the final landed between ${num(d.nowBand.below, 0)}% below and ${num(d.nowBand.above, 0)}% above the projection made at this time of day.`
      : 'Futures and options transaction fees only. MCX’s reported revenue also includes other items.') + '</span>';
  }

  function renderAverages(d) {
    const grid = $('tdAverages');
    if (!d.h) {
      if (S.homeErr) MCX.ui.error(grid, 'Could not load the averages: ' + S.homeErr, () => loadHome(true));
      return;
    }
    const avgs = d.avgs, max = Math.max(...avgs.map(a => a.value || 0));
    $('tdTakeaway').textContent = takeaway(avgs);
    grid.innerHTML = avgs.map(a => {
      const c = change(a);
      const win = a.n === 0 ? 'Starts after today’s close'
        : (a.key === 'qtd' || a.key === 'fytd' ? `${a.n} day${a.n === 1 ? '' : 's'}, ` : '') + fmt.span(a.first, a.last);
      const chg = !c ? '' : c.dir === 'level' ? `level with ${c.vs}`
        : `<span class="${c.dir}">${c.dir === 'up' ? '▲' : '▼'} ${c.pct.toFixed(0)}%</span> vs ${c.vs}`;
      let baseLine = '';
      if (c && a.key === 'fytd' && a.same_stretch_chg_pct !== null && a.same_stretch_chg_pct !== undefined) {
        const up = a.same_stretch_chg_pct >= 0;
        baseLine = `<span class="${up ? 'up' : 'down'}">${up ? '▲' : '▼'} ${Math.abs(a.same_stretch_chg_pct).toFixed(0)}%</span> vs same stretch`;
      } else if (c && a.key === 'qtd') baseLine = `${cr(a.prev.value)}, ${a.prev.n} days`;
      else if (c && a.prev) baseLine = `${cr(a.prev.value)}, ${fmt.span(a.prev.first, a.prev.last)}`;
      return `<div class="avg"><div class="avg-label">${esc(a.label)}</div>`
        + `<div class="avg-value">${a.value !== null ? cr(a.value) + '<small>Cr</small>' : '—'}</div>`
        + `<div class="avg-bar" aria-hidden="true"><i style="width:${a.value ? (a.value / max * 100).toFixed(1) : 0}%"></i></div>`
        + `<div class="avg-meta">${win}</div>${chg ? `<div class="avg-meta">${chg}</div>` : ''}${baseLine ? `<div class="avg-base">${baseLine}</div>` : ''}</div>`;
    }).join('');
    const q = avgs.find(a => a.key === 'qtd'), y = avgs.find(a => a.key === 'fytd');
    const parts = [d.h.today_final ? 'Completed MCX trading days, including today’s final figure.' : 'Completed MCX trading days only, so today is not included.'];
    if (q && y) parts.push(`${q.period} so far covers ${q.n} day${q.n === 1 ? '' : 's'} and ${y.period} so far ${y.n}.`);
    if (y && y.prev && y.same_stretch_chg_pct !== null && y.same_stretch_chg_pct !== undefined) {
      parts.push(`The quarter and year are compared with the whole previous quarter and year; against the same stretch of ${y.prev.label}, ${y.period} so far is ${y.same_stretch_chg_pct >= 0 ? 'up' : 'down'} ${Math.abs(y.same_stretch_chg_pct).toFixed(0)}%.`);
    }
    if (d.h.missing_sessions && d.h.missing_sessions.length) {
      parts.push(`No data for ${d.h.missing_sessions.map(fmt.dayMonth).join(', ')}, so windows spanning ${d.h.missing_sessions.length === 1 ? 'that day use' : 'those days use'} the days available.`);
    }
    $('tdAvgBasis').innerHTML = INFO + '<span>' + esc(parts.join(' ')) + '</span>';
  }

  function renderTrust(d) {
    const box = $('tdTrust'), side = $('tdTrustSide');
    const acc = d.h && d.h.accuracy;
    if (!acc || !acc.grid.some(g => g.q10_pct !== null)) { box.innerHTML = ''; side.innerHTML = d.h ? '<p class="note">Not enough past projections to measure yet.</p>' : ''; return; }
    const w = Math.round(box.clientWidth) || 700;
    box.innerHTML = trustSvg(w, acc.grid, d.state === 'live' ? d.min : null, d.nowBand);
    const row = (a, b) => `<div class="kv-row"><span>${a}</span><strong>${b}</strong></div>`;
    const fb = b => !b || b.above === null ? '—' : `−${num(b.below, 0)}% to +${num(b.above, 0)}%`;
    let rows, lean;
    if (d.state === 'live' && d.nowBand) {
      rows = row(`Now, ${d.clock}`, fb(d.nowBand)) + (d.min < 600 ? row('By 19:00', fb(d.bandAt(600))) : '')
        + (d.min < 720 ? row('By 21:00', fb(d.bandAt(720))) : '') + row('By 23:00', fb(d.bandAt(840)));
      lean = gridAt(acc.grid, d.min, 'median_pct');
    } else {
      rows = row('At 11:00', fb(d.bandAt(120))) + row('At 15:00', fb(d.bandAt(360))) + row('At 19:00', fb(d.bandAt(600))) + row('At 21:00', fb(d.bandAt(720)));
      lean = null;
    }
    let note = 'Where the final figure landed relative to the projection, on 8 of 10 past days.';
    if (lean !== null) {
      const medFinal = (1 / (1 + lean / 100) - 1) * 100;       // the median final, relative to the projection
      note += Math.abs(medFinal) < 3 ? ' At this time of day the projection has shown no clear lean either way.'
        : ` At this time of day the projection has tended to run ${medFinal < 0 ? 'high' : 'low'}: the median final was ${num(Math.abs(medFinal), 0)}% ${medFinal < 0 ? 'below' : 'above'} it.`;
    } else {
      note += ' The range starts wide and narrows through the evening, as more of the day’s trading is booked.';
    }
    const left = [acc.excluded_part_day ? `${acc.excluded_part_day} part-day session${acc.excluded_part_day === 1 ? '' : 's'}` : '',
                  acc.excluded_us_holiday ? `${acc.excluded_us_holiday} US market holiday${acc.excluded_us_holiday === 1 ? '' : 's'}` : ''].filter(Boolean);
    side.innerHTML = rows + `<p class="note">${note} Measured on the last ${acc.sessions} normal trading days (${fmt.span(acc.first, acc.last)})`
      + (left.length ? `; ${left.join(' and ')} left out` : '') + '.</p>';
  }

  function renderDaily(d) {
    const box = $('tdDaily');
    if (!d.h) { box.innerHTML = ''; return; }
    const n = RANGE_TRADING_DAYS[rangeState.spark || '60D'] || 60;
    let today = null;
    if (d.state === 'live') today = { value: d.P, booked: d.B, lo: d.range && d.range.lo, hi: d.range && d.range.hi, label: 'projected' };
    else if (d.state === 'closed' && d.last && d.last.provisional) today = { value: d.last.total, label: 'last projection' };
    const c = dailySvg(Math.round(box.clientWidth) || 900, d.daily, n, today, 'd');
    $('tdDailyTodayKey').hidden = !today;
    $('tdDailyRangeKey').hidden = !(today && today.lo);
    box.innerHTML = c.svg;
    if (c.f) MCX.svg.hover(box, c.f, c.xs, c.tip);
  }

  function renderDrivers(d) {
    const box = $('tdDrivers'), r = d.r;
    if (d.state === 'pre' || !r || !r.top_futures) {
      box.innerHTML = '<p class="note">Available once today’s first snapshot arrives.</p>';
      return;
    }
    const futPer = r.fut_notl_cr ? r.booked_fut_rev_cr / r.fut_notl_cr : 0;     // ₹ Cr of revenue per ₹ Cr of notional, from the backend
    const optPer = r.opt_prem_cr ? r.booked_opt_rev_cr / r.opt_prem_cr : 0;
    const group = sym => (GROUPS.find(([, ss]) => ss.includes(sym)) || ['Other'])[0];
    const by = {};
    (r.top_futures || []).forEach(c => { (by[group(c.sym)] = by[group(c.sym)] || [0, 0])[0] += c.notl * futPer; });
    (r.top_options || []).forEach(c => { (by[group(c.sym)] = by[group(c.sym)] || [0, 0])[1] += c.prem * optPer; });
    const rows = Object.entries(by).sort((a, b) => (b[1][0] + b[1][1]) - (a[1][0] + a[1][1]));
    const mx = Math.max(...rows.map(([, v]) => v[0] + v[1]), 1e-9);
    const covF = r.fut_notl_cr ? (r.top_futures || []).reduce((a, c) => a + c.notl, 0) / r.fut_notl_cr * 100 : 0;
    const covO = r.opt_prem_cr ? Math.min((r.top_options || []).reduce((a, c) => a + c.prem, 0) / r.opt_prem_cr * 100, 100) : 0;
    $('tdDrvSub').textContent = d.state === 'closed' ? 'Revenue booked today, by commodity' : 'Revenue booked so far today, by commodity';
    box.innerHTML = rows.map(([g, [fv, ov]]) =>
      `<div class="drv"><span class="drv-name">${esc(g)}</span><span class="drv-bar" role="img" aria-label="${esc(g)}: futures ₹${num(fv, 2)} crore, options ₹${num(ov, 2)} crore">`
      + `<i style="width:${(fv / mx * 100).toFixed(1)}%;background:var(--mcx-fut)"></i><i style="width:${(ov / mx * 100).toFixed(1)}%;background:var(--mcx-opt)"></i></span>`
      + `<span class="drv-val">${cr(fv + ov)}</span></div>`).join('')
      + `<div class="legend"><span><i class="sw sw--fut"></i>Futures</span><span><i class="sw sw--opt"></i>Options</span></div>`
      + `<p class="note">₹ crore, from the most active contracts, which cover ${covF.toFixed(0)}% of futures notional and ${covO.toFixed(0)}% of option premium. `
      + `So far: futures notional ${cr(r.fut_notl_cr, 0)} Cr; option premium ${cr(r.opt_prem_cr, 0)} Cr on ${cr(r.opt_notl_cr, 0)} Cr notional; `
      + `${num(r.active_futures)} futures and ${num(r.active_options)} option contracts traded.`
      + (r.day_type ? ` The projection treats today as a ${esc(String(r.day_type).toLowerCase())} day${r.day_description ? ` (${esc(r.day_description.split('.')[0].replace(/\s*\(n=\d+\)/, ''))})` : ''}.` : '') + '</p>';
  }

  function renderContracts(d) {
    const box = $('tdContracts'), r = d.r;
    document.querySelectorAll('#tdCtrSeg button').forEach(b => {
      b.setAttribute('aria-selected', String(b.dataset.kind === S.kind));
      b.tabIndex = b.dataset.kind === S.kind ? 0 : -1;     // one tab stop; arrows move between them
    });
    if (d.state === 'pre' || !r || !r.top_options) { box.innerHTML = '<p class="note">Available once today’s first snapshot arrives.</p>'; return; }
    const futPer = r.fut_notl_cr ? r.booked_fut_rev_cr / r.fut_notl_cr : 0;
    const optPer = r.opt_prem_cr ? r.booked_opt_rev_cr / r.opt_prem_cr : 0;
    let head, body;
    if (S.kind === 'futures') {
      $('tdCtrSub').textContent = 'Ranked by futures notional traded today';
      head = ['Futures contract', 'Notional ₹ Cr', 'Share of futures', 'Booked ₹ Cr'];
      body = (r.top_futures || []).slice(0, 8).map(c => [esc(c.sym), num(c.notl, 0), r.fut_notl_cr ? `${num(c.notl / r.fut_notl_cr * 100, 1)}%` : '—', num(c.notl * futPer, 2)]);
    } else {
      $('tdCtrSub').textContent = 'Ranked by option premium traded today';
      head = ['Option contract', 'Premium ₹ Cr', 'Prem / notional', 'Booked ₹ Cr'];
      body = (r.top_options || []).slice(0, 8).map(c => [esc(c.sym), num(c.prem, 0), c.notl ? `${num(c.prem / c.notl * 100, 2)}%` : '—', num(c.prem * optPer, 2)]);
    }
    box.innerHTML = `<table class="v2-table"><thead><tr>${head.map(x => `<th scope="col">${x}</th>`).join('')}</tr></thead>`
      + `<tbody>${body.map(row => `<tr>${row.map(x => `<td>${x}</td>`).join('')}</tr>`).join('')}</tbody></table>`;
  }

  function renderQuarter() {
    const box = $('tdQuarter'), q = S.qtr;
    if (!box) return;
    if (!q) { box.innerHTML = '<span class="skel"></span>'; return; }
    const left = q.trading_days_total - q.trading_days_elapsed;
    $('tdQtrTitle').textContent = `This quarter so far: ${q.quarter}`;
    box.innerHTML = `<div class="qtr-line"><span><strong>${cr(q.revenue_actual_cr, 1)} Cr</strong> <span class="muted">F&amp;O revenue booked in the first ${q.trading_days_elapsed} of ${q.trading_days_total} trading days</span></span>`
      + `<span class="muted">${left === 0 ? 'No trading days left' : `${left} trading day${left === 1 ? '' : 's'} left${q.today_in_remaining ? ', including today' : ''}`}</span>`
      + `<a href="#/value/quarter">Quarter P&amp;L →</a></div>`
      + `<div class="progress" role="progressbar" aria-label="Trading days elapsed in the quarter" aria-valuenow="${Math.round(q.completion_pct)}" aria-valuemin="0" aria-valuemax="100"><i style="width:${Math.min(q.completion_pct, 100)}%"></i></div>`;
  }

  function renderFoot(d) {
    const r = d.r;
    let s = 'MCX market data via relay, every 15 minutes.';
    if (r && r.success && r.fut_notl_cr && r.opt_prem_cr && r.booked_fut_rev_cr) {
      const fr = r.booked_fut_rev_cr / r.fut_notl_cr * 1e7 / 2, or = r.booked_opt_rev_cr / r.opt_prem_cr * 1e7 / 2;
      s += ` Revenue = futures notional × ₹${num(fr, 0)}/cr + option premium × ₹${num(or, 0)}/cr, both sides.`;
    }
    $('tdFoot').textContent = s;
  }

  function publishStatus(d) {
    const label = d.state === 'live' ? 'Live' : d.state === 'closed' ? 'Session closed'
      : !d.h ? 'Loading' : !d.h.today_is_session ? 'No trading today' : d.ist.min < 540 ? 'Opens 09:00 IST' : 'Waiting for data';
    MCX.store.set('session', { state: d.state, label });
    if (d.state === 'pre' && d.h) {
      const meta = $('refreshMeta');
      if (meta) meta.textContent = `data through ${fmt.dayMonth(d.h.sessions_through)}`;
    }
  }

  function render() {
    if (S.refresh === undefined && !S.home) return;        // nothing to show yet
    const d = derive();
    if (S.refresh === undefined && d.state === 'pre' && d.ist.min >= 540) return;   // wait for the first refresh during the day
    const l = lede(d);
    $('tdStats').innerHTML = statsHtml(d);
    renderSessions(d); renderAverages(d); renderTrust(d); renderDaily(d);
    renderDrivers(d); renderContracts(d); renderQuarter(); renderFoot(d);
    publishStatus(d);
    if (S.present) renderPresent(d, l);
    S.width = $('today').clientWidth;
  }

  // ── Present mode: headline, figures and the sessions chart, full screen ──
  function openPresent() {
    if (S.present) return;
    const el = document.createElement('div');
    el.className = 'present'; el.id = 'present';
    el.setAttribute('role', 'dialog'); el.setAttribute('aria-modal', 'true'); el.setAttribute('aria-label', 'Present mode');
    el.innerHTML = '<div class="present-in"><div class="present-top"><span class="present-brand">MCX Revenue Monitor<small>Tusk Invest</small></span>'
      + '<span style="display:flex;align-items:center;gap:12px"><span class="status" id="presentStatus"></span>'
      + '<button type="button" class="icon-btn" id="presentClose" aria-label="Leave present mode" title="Leave present mode (Esc)">'
      + '<svg aria-hidden="true" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round"><path d="M6 6l12 12M18 6L6 18"/></svg></button></span></div>'
      + '<h1 class="headline" id="presentHead"></h1><div class="present-body"><div id="presentStats"></div>'
      + '<div><div class="chart-box" id="presentChart"></div><div class="legend" id="presentLegend"></div><p class="note" id="presentNote"></p></div></div>'
      + '<div class="present-foot">Press Esc to leave presentation mode</div></div>';
    document.body.appendChild(el);
    S.present = true;
    el.querySelector('#presentClose').addEventListener('click', closePresent);
    if (el.requestFullscreen) el.requestFullscreen().catch(() => {});
    render();
    el.querySelector('#presentClose').focus();
  }
  function closePresent() {
    const el = $('present');
    if (!el) return;
    if (document.fullscreenElement) document.exitFullscreen().catch(() => {});
    el.remove();
    S.present = false;
    const b = $('presentBtn'); if (b) b.focus();
  }
  function renderPresent(d, l) {
    const el = $('present');
    if (!el) return;
    const status = $('livePill');
    el.querySelector('#presentStatus').textContent = `${status ? status.textContent : ''} · ${fmt.day(d.ist.iso)}${d.state === 'live' ? ` · ${d.clock} IST` : ''}`;
    el.querySelector('#presentStatus').dataset.state = d.state === 'live' ? 'live' : 'final';
    el.querySelector('#presentHead').textContent = l.head;
    el.querySelector('#presentStats').innerHTML = statsHtml(d).replace(/<span class="phone-only">.*?<\/span>/, '');
    const box = el.querySelector('#presentChart');
    if (d.h) {
      const { rows, live } = chartRows(d);
      const w = Math.round(box.clientWidth) || 900;
      box.innerHTML = sessionsSvg({ w, rows, live, ma45: d.h.ma45, uid: 'p', todayIso: d.ist.iso, scale: 1.3, h: Math.max(320, Math.min(520, window.innerHeight - 360)) });
      el.querySelector('#presentLegend').innerHTML = sessionsLegend(live, false);
      el.querySelector('#presentNote').textContent = live && live.range
        ? 'The range is where the final landed on 8 of 10 past days, relative to the projection made at this time of day. It narrows through the evening.' : '';
    }
  }

  // ── Wiring ───────────────────────────────────────────────────────────────
  MCX.store.on('refresh', r => {
    const was = S.refresh && S.refresh.session_closed;
    S.refresh = r;
    if (r && r.session_closed && !was) loadHome(true);      // the day just closed: pick up its final row when it lands
    render();
  });
  makeRangeToggle({
    key: 'spark', containerId: 'tdDailyRange', ranges: ['30D', '60D', 'Q', '1Y'], defaultRange: '60D',
    labelIds: ['tdDailyRangeLabel'], onChange: () => { if (S.home) renderDaily(derive()); },
  });
  $('tdCtrSeg').addEventListener('click', e => {
    const b = e.target.closest('button[data-kind]');
    if (!b || b.dataset.kind === S.kind) return;
    S.kind = b.dataset.kind;
    renderContracts(derive());
  });
  $('tdCtrSeg').addEventListener('keydown', e => {           // arrow keys move between the two tabs
    if (e.key !== 'ArrowLeft' && e.key !== 'ArrowRight') return;
    S.kind = S.kind === 'options' ? 'futures' : 'options';
    renderContracts(derive());
    document.querySelector(`#tdCtrSeg button[data-kind="${S.kind}"]`).focus();
  });
  $('tdProfile').addEventListener('toggle', e => { if (e.target.open) loadIntradayCurve(); });
  document.addEventListener('click', e => { if (e.target.closest && e.target.closest('[data-action="present"]')) openPresent(); });
  document.addEventListener('keydown', e => { if (e.key === 'Escape' && S.present) closePresent(); });
  document.addEventListener('fullscreenchange', () => { if (!document.fullscreenElement && S.present) closePresent(); });
  MCX.router.onChange(r => { if (r.id !== 'today' && S.present) closePresent(); });
  if (window.ResizeObserver) {
    let queued = false;
    new ResizeObserver(() => {
      if (queued) return;
      queued = true;
      requestAnimationFrame(() => { queued = false; if (Math.abs($('today').clientWidth - S.width) > 2 && S.home) render(); });
    }).observe($('today'));
  }
  window.addEventListener('resize', () => { if (S.present) render(); });

  MCX.today = {
    mount() { loadHome(); loadQuarter(); render(); },
  };
})();
