/* MCX Revenue Monitor: the Signals pages — Momentum and Model ensemble.
   Data: /api/models?view=momentum (lib/cron_momentum.py: revenue regime + price overlay) and
   /api/models (lib/cron_models.py: price-gap ECM + multi-factor + their blend).
   Every rule and weight stated on the page is the one in those two jobs.
   Charts are SVG coloured by CSS variables, so they follow the theme without redrawing. */
(function () {
  'use strict';
  const fmt = MCX.fmt, num = fmt.num, V = MCX.svg, esc = V.esc;

  // ════════════════════════════════════════════════════════════════════════
  //  Model: pure functions (scripts/test_signals_js.js)
  // ════════════════════════════════════════════════════════════════════════
  // lib/cron_momentum.py
  const HOT = 1.05, COLD = 0.95;
  // lib/cron_models.py: ensemble = 0.30 × (−ECM z) + 0.70 × (3/7 revenue z + 4/7 turnover z)
  const W = { ecm: 0.30, rev: 0.70 * 3 / 7, turn: 0.70 * 4 / 7 };

  // [pill state, glyph, label]
  const COMPOSITE = { STRONG_BUY: ['strong-up', '▲▲', 'Strong buy'], BUY: ['up', '▲', 'Buy'], HOLD: ['', '■', 'Hold'],
                      WATCH: ['caution', '!', 'Watch'], SELL: ['down', '▼', 'Sell'], STRONG_SELL: ['strong-down', '▼▼', 'Strong sell'],
                      NEUTRAL: ['', '■', 'Neutral'] };
  const REGIME = { HOT: ['up', '▲', 'Hot'], NEUTRAL: ['', '■', 'Neutral'], COLD: ['down', '▼', 'Cold'] };
  const OVERLAY = { BREAKOUT: ['up', '▲', 'Breakout'], BULL_CONT: ['up', '▲', 'Quiet rise'], OVERSOLD: ['caution', '!', 'Sharp fall'],
                    NEUTRAL: ['', '■', 'Neutral'] };
  // ECM: the gap's z-score against its own 60-day history; below its norm reads positive for the price
  const ECM = { STRONG_REVERT_UP: ['strong-up', '▲▲', 'Gap well below its norm'], MILD_REVERT_UP: ['up', '▲', 'Gap below its norm'],
                NEUTRAL: ['', '■', 'Gap near its norm'], MILD_EXTEND_DOWN: ['down', '▼', 'Gap above its norm'],
                STRONG_EXTEND_DOWN: ['strong-down', '▼▼', 'Gap well above its norm'] };

  function pill(map, key) {
    const m = map[key] || ['none', '', 'No data'];
    return `<span class="pill${m[0] ? ' pill--' + m[0] : ''}">${m[1] ? `<span aria-hidden="true">${m[1]}</span>` : ''}${esc(m[2])}</span>`;
  }
  const label = (map, key) => (map[key] || [0, 0, 'No data'])[2];

  // The run of `key`'s current value at the end of rows. capped: the run reaches the first row
  function streak(rows, key) {
    if (!rows.length) return null;
    const v = rows[rows.length - 1][key];
    let i = rows.length - 1;
    while (i > 0 && rows[i - 1][key] === v) i--;
    return { value: v, n: rows.length - i, start: rows[i].date, startIdx: i, capped: i === 0 };
  }

  // How far the 10-day average can move before the regime changes, holding the 45-day fixed
  function cushion(s) {
    const ma10 = s.ma10_rev_cr, ma45 = s.ma45_rev_cr;
    if (!(ma10 > 0) || !(ma45 > 0)) return null;
    const hot = HOT * ma45, cold = COLD * ma45;
    if (s.regime === 'HOT') return { to: 'cool', level: hot, gap: ma10 - hot, pct: (ma10 - hot) / ma10 * 100 };
    if (s.regime === 'COLD') return { to: 'warm', level: cold, gap: cold - ma10, pct: (cold - ma10) / ma10 * 100 };
    const up = hot - ma10, dn = ma10 - cold;
    return up <= dn ? { to: 'hot', level: hot, gap: up, pct: up / ma10 * 100, other: cold }
                    : { to: 'cold', level: cold, gap: dn, pct: dn / ma10 * 100, other: hot };
  }

  // Days `key` changed, most recent first, with the close over each run (to the next change, or to now)
  function changes(rows, key) {
    const out = [];
    for (let i = 1; i < rows.length; i++) {
      if (rows[i][key] === rows[i - 1][key]) continue;
      out.push({ i, date: rows[i].date, from: rows[i - 1][key], to: rows[i][key], close: rows[i].close_price, regime: rows[i].regime });
    }
    out.forEach((c, k) => {
      const next = out[k + 1], end = next ? next.i : rows.length - 1;
      c.days = end - c.i + (next ? 0 : 1);
      c.ongoing = !next;
      c.runPct = rows[end].close_price && c.close ? (rows[end].close_price / c.close - 1) * 100 : null;
    });
    return out.reverse();
  }

  const signed = (v, dp = 1) => `${v >= 0 ? '+' : '−'}${num(Math.abs(v), dp)}`;
  const crs = v => `₹${num(v, 2)} Cr`;
  const dayWord = n => `${n} trading day${n === 1 ? '' : 's'}`;
  const longDate = iso => { const p = fmt.dateLong(iso).split(' '); return `${p[1]} ${p[2]}`; };   // "17 August"

  function momLede(s, st, rows) {
    const comp = label(COMPOSITE, s.composite_signal).toLowerCase();
    const reg = (s.regime || '').toLowerCase();
    const run = st ? (st.capped ? `more than ${dayWord(st.n)}` : dayWord(st.n)) : null;
    const head = !run ? `Momentum reads ${comp}.`
      : s.regime === 'NEUTRAL' ? `Momentum reads ${comp}: revenue has been in the neutral zone for ${run}.`
      : `Momentum reads ${comp}: revenue has run ${reg} for ${run}.`;
    const parts = [`The 10-day average of ${crs(s.ma10_rev_cr)} is ${num(s.ratio_10d_45d, 2)}× the 45-day average of ${crs(s.ma45_rev_cr)}.`];
    const c = cushion(s);
    if (c) {
      if (c.to === 'cool') parts.push(`At today’s 45-day average, it would have to fall below ${crs(c.level)}, a ${num(c.pct, 0)}% drop, for the regime to cool.`);
      else if (c.to === 'warm') parts.push(`At today’s 45-day average, it would have to rise above ${crs(c.level)}, ${num(c.pct, 0)}% higher, to leave the cold regime.`);
      else parts.push(`At today’s 45-day average, it turns hot above ${crs(Math.max(c.level, c.other))} and cold below ${crs(Math.min(c.level, c.other))}.`);
    }
    if (st && !st.capped && rows[st.startIdx].close_price && s.close_price && s.regime !== 'NEUTRAL') {
      const p = (s.close_price / rows[st.startIdx].close_price - 1) * 100;
      parts.push(`The share price is ${Math.abs(p) < 0.05 ? 'unchanged' : `${p > 0 ? 'up' : 'down'} ${num(Math.abs(p), 1)}%`} since the regime turned ${reg} on ${longDate(st.start)}.`);
    }
    return { head, deck: parts.join(' ') };
  }

  function overlayText(s) {
    if (s.adr_ratio === null || s.adr_ratio === undefined || s.price_mom_5d === null || s.price_mom_5d === undefined) return '';
    return `5-day average daily move is ${num(s.adr_ratio, 2)}× the 20-day; price ${signed(s.price_mom_5d * 100)}% over 5 days`;
  }

  // Share price change over h rows, grouped by each day's signal. A day's signal uses that day's
  // MCX revenue, known only after the evening session (23:30), so it is first tradeable at the
  // next day's close: the change runs from row i + lag to row i + lag + h. Overlapping windows;
  // days without an outcome yet are left out. priceKey: 'close_price' (momentum) or 'price' (models).
  function forward(rows, key, order, h = 10, priceKey = 'close_price', lag = 1) {
    const g = {}, all = [];
    for (let i = 0; i + lag + h < rows.length; i++) {
      const a = rows[i + lag][priceKey], b = rows[i + lag + h][priceKey], k = rows[i][key];
      if (!(a > 0) || !(b > 0) || !k || k === 'NO_DATA') continue;
      const r = (b / a - 1) * 100;
      (g[k] = g[k] || []).push(r); all.push(r);
    }
    const stat = xs => xs.length ? { n: xs.length, avg: xs.reduce((p, q) => p + q, 0) / xs.length, up: xs.filter(x => x > 0).length / xs.length * 100 } : null;
    return { rows: order.filter(k => g[k]).map(k => ({ key: k, ...stat(g[k]) })), all: stat(all),
             first: rows.length ? rows[0].date : null, last: rows.length > h + lag ? rows[rows.length - 1 - h - lag].date : null };
  }

  // Each part's contribution to the ensemble score
  function contributions(s) {
    const e = s.ecm || {}, m = s.multi_factor || {};
    if ([e.z_score, m.revenue_z, m.turnover_z].some(v => v === null || v === undefined)) return null;
    const parts = [
      { key: 'rev', label: 'Revenue', weight: W.rev, reading: m.revenue_z, value: W.rev * m.revenue_z },
      { key: 'turn', label: 'Turnover', weight: W.turn, reading: m.turnover_z, value: W.turn * m.turnover_z },
      { key: 'ecm', label: 'Price gap to fair value', weight: W.ecm, reading: -e.z_score, value: -W.ecm * e.z_score },
    ];
    return { parts, total: parts.reduce((a, p) => a + p.value, 0) };
  }

  const zWords = z => Math.abs(z) < 0.5 ? 'close to' : z >= 1 ? 'well above' : z > 0 ? 'above' : z <= -1 ? 'well below' : 'below';
  function ensLede(s) {
    const e = s.ensemble || {}, c = contributions(s);
    const sig = label(COMPOSITE, e.signal).toLowerCase();
    if (!c || e.score === null || e.score === undefined) return { head: `The model ensemble reads ${sig}.`, deck: '' };
    const lead = c.parts.reduce((a, b) => (Math.abs(b.value) > Math.abs(a.value) ? b : a));
    const what = lead.key === 'ecm'
      ? `the price’s gap to fair value sitting ${zWords(s.ecm.z_score)} its 60-day norm`
      : `exchange ${lead.key === 'rev' ? 'revenue' : 'turnover'} running ${zWords(lead.reading)} its 60-day norm`;
    const head = `The model ensemble reads ${sig}, ${Math.abs(lead.value) < 0.15 ? 'with no part pulling hard either way' : `led by ${what}`}.`;
    const byName = { rev: 'revenue', turn: 'turnover', ecm: 'the price gap to fair value' };
    const list = c.parts.map(p => `${byName[p.key]} ${signed(p.value, 2)}`);
    const sc = e.score;
    const band = sc > 1.5 ? 'It would drop to buy below +1.5.'
      : sc > 0.5 ? 'It would read neutral below +0.5 and strong buy above +1.5.'
      : sc > -0.5 ? 'It would read buy above +0.5 and sell below −0.5.'
      : sc > -1.5 ? 'It would read neutral above −0.5 and strong sell below −1.5.'
      : 'It would rise to sell above −1.5.';
    return { head, deck: `The score of ${signed(sc, 2)} adds three parts: ${list[0]}, ${list[1]} and ${list[2]}. ${band}` };
  }

  function ecmText(s, bandMean) {
    const e = s.ecm || {};
    if (e.spread_pct === null || e.spread_pct === undefined || e.z_score === null || e.z_score === undefined) return '';
    const where = e.spread_pct >= 0 ? `${num(e.spread_pct, 1)}% above` : `${num(-e.spread_pct, 1)}% below`;
    let t = `The price, ₹${num(s.price, 1)}, is ${where} the data-driven fair value of ₹${num(s.fair_value_base, 0)}.`;
    if (bandMean !== null && bandMean !== undefined) t += ` Over the last 60 days the gap has averaged ${signed(bandMean)}%,`;
    else t += ' Against its last 60 days,';
    const z = e.z_score;
    t += ` so today it is ${num(Math.abs(z), 2)} standard deviations ${z < 0 ? 'below' : 'above'} its norm.`;
    t += Math.abs(z) <= 0.5 ? ' The model sees no pull either way.'
      : z < 0 ? ' The model expects the gap to widen back toward its norm, which reads as room for the price to rise relative to fair value.'
      : ' The model expects the gap to narrow back toward its norm, which reads as a drag on the price relative to fair value.';
    return t;
  }

  MCX.signalsModel = { HOT, COLD, W, streak, cushion, changes, momLede, overlayText, forward, contributions, ensLede, ecmText, pill,
                       COMPOSITE, REGIME, OVERLAY, ECM };
  if (window.MCX_TEST) return;

  // ════════════════════════════════════════════════════════════════════════
  //  Shared page pieces
  // ════════════════════════════════════════════════════════════════════════
  const $ = id => document.getElementById(id);
  const INFO = '<svg aria-hidden="true" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M12 11v5"/><path d="M12 8h.01"/></svg>';
  const RANGES = ['30D', '60D', 'Q', '1Y', '2Y', 'Max'];
  const txt = V.text;
  const stat = (lab, body, sub, lead) => `<div class="stat${lead ? ' stat--lead' : ''}"><div class="stat-label">${lab}</div>${body}${sub ? `<div class="stat-sub">${sub}</div>` : ''}</div>`;
  const val = s => `<div class="stat-value">${s}</div>`;
  const pillRow = s => `<div class="stat-pill">${s}</div>`;
  const yearOf = iso => iso.slice(2, 4);
  const xLabel = (iso, long) => long ? `${fmt.dayMonth(iso)} ’${yearOf(iso)}` : fmt.dayMonth(iso);
  const pctCell = v => v === null || v === undefined ? '—' : `<span class="${Math.abs(v) < 0.05 ? '' : v > 0 ? 'up' : 'down'}">${signed(v)}%</span>`;

  // Five evenly spaced date ticks along the bottom
  function dateTicks(rows, X, y, long, compact) {
    const n = rows.length, k = compact ? 3 : 5, out = [];
    for (let j = 0; j < k; j++) {
      const i = Math.round(j * (n - 1) / (k - 1));
      out.push(txt(X(i), y, 'c-tick', xLabel(rows[i].date, long), j === 0 ? 'start' : j === k - 1 ? 'end' : 'middle'));
    }
    return out.join('');
  }
  const path = (rows, X, Y, key) => {
    let d = '', pen = false;
    rows.forEach((r, i) => {
      const v = r[key];
      if (v === null || v === undefined) { pen = false; return; }
      d += `${pen ? 'L' : 'M'}${X(i).toFixed(1)},${Y(v).toFixed(1)}`; pen = true;
    });
    return d;
  };
  // A band between two series (either may be missing on a row)
  function bandPath(rows, X, Y, lo, hi) {
    const pts = rows.map((r, i) => (lo(r) === null || hi(r) === null ? null : i)).filter(i => i !== null);
    if (pts.length < 2) return '';
    const up = pts.map(i => `${X(i).toFixed(1)},${Y(hi(rows[i])).toFixed(1)}`);
    const dn = pts.map(i => `${X(i).toFixed(1)},${Y(lo(rows[i])).toFixed(1)}`).reverse();
    return `M${up.join('L')}L${dn.join('L')}Z`;
  }
  function span(vals, padFrac) {
    const xs = vals.filter(v => v !== null && v !== undefined && isFinite(v));
    const lo = Math.min(...xs), hi = Math.max(...xs), p = (hi - lo || Math.abs(hi) || 1) * padFrac;
    return [lo - p, hi + p];
  }

  // What followed each signal (full history, loaded when the section comes into view)
  function evidenceHtml(ev, map, h) {
    if (!ev || !ev.all) return '<p class="note">Not enough history yet.</p>';
    const row = (lab, s, cls) => `<tr${cls ? ` class="${cls}"` : ''}><td>${lab}</td><td>${num(s.n)}</td><td>${pctCell(s.avg)}</td><td>${num(s.up, 0)}%</td></tr>`;
    return `<div class="table-scroll"><table class="v2-table"><thead><tr><th scope="col">Signal that day</th><th scope="col">Days</th>`
      + `<th scope="col">Average change over ${h} days</th><th scope="col">Share of days up</th></tr></thead><tbody>`
      + ev.rows.map(r => row(pill(map, r.key), r)).join('')
      + `${row('All days', ev.all, 'total')}</tbody></table></div>`
      + `<p class="note">Signal days ${fmt.span(ev.first, ev.last)}. Each change runs from the next day’s close, the first chance to act, since a day’s signal needs MCX’s revenue from its evening session. It covers ${h} trading days, so the windows overlap and the counts overstate how much independent evidence there is. `
      + 'The rules were chosen on this same history, which flatters them. Compare each row with all days: MCX’s shares rose over most of this period.</p>';
  }
  function lazyEvidence(boxId, btnId, load) {
    const box = $(boxId);
    let started = false;
    const go = () => { if (started) return; started = true; box.innerHTML = '<span class="skel"></span>'; load(); };
    $(btnId).addEventListener('click', go);
    if (window.IntersectionObserver) {
      const io = new IntersectionObserver(es => { if (es.some(e => e.isIntersecting)) { io.disconnect(); go(); } }, { rootMargin: '200px' });
      io.observe(box);
    }
  }
  const fetchKey = r => (r === '2Y' || r === 'Max' ? r : '1Y');
  const n0 = r => RANGE_TRADING_DAYS[r];

  // ════════════════════════════════════════════════════════════════════════
  //  Momentum
  // ════════════════════════════════════════════════════════════════════════
  const MO = { data: null, err: null, logAll: false, width: 0 };
  const moUrl = () => '/api/models?view=momentum&range=' + fetchKey(rangeState.momRegime || '60D');

  function loadMomentum() {
    return fetchRanged(moUrl()).then(d => {
      if (!d.success) throw new Error(d.error || 'No data');
      MO.data = d; MO.err = null; renderMomentum();
    }).catch(e => { MO.err = e.message || String(e); renderMomentum(); });
  }

  function renderMomentum() {
    const d = MO.data;
    if (!d) {
      if (MO.err) { $('moHead').textContent = 'Momentum could not be loaded.'; MCX.ui.error($('moChart'), 'Could not load momentum: ' + MO.err, loadMomentum); }
      return;
    }
    const s = d.snapshot, rows = d.history || [];
    const st = streak(rows, 'regime');
    const l = momLede(s, st, rows);
    $('moKicker').textContent = `Momentum · end of day, ${fmt.dayMonth(s.date)}`;
    $('moHead').textContent = l.head;
    $('moDeck').textContent = l.deck;
    const c = cushion(s);
    const cz = !c ? stat('Room before the regime changes', val('—'))
      : c.to === 'cool' ? stat('Cushion before it cools', val(`₹${num(c.gap, 2)}<small>Cr/day</small>`), `10-day average must fall below ${crs(c.level)} (${num(c.pct, 0)}%), at today’s 45-day average`)
      : c.to === 'warm' ? stat('Gap before it warms', val(`₹${num(c.gap, 2)}<small>Cr/day</small>`), `10-day average must rise above ${crs(c.level)} (${num(c.pct, 0)}%), at today’s 45-day average`)
      : stat(`Room before it turns ${c.to}`, val(`₹${num(c.gap, 2)}<small>Cr/day</small>`), `turns ${c.to} ${c.to === 'hot' ? 'above' : 'below'} ${crs(c.level)} (${num(c.pct, 0)}%), at today’s 45-day average`);
    $('moStats').innerHTML = stat('Composite signal', pillRow(pill(COMPOSITE, s.composite_signal)), `${label(REGIME, s.regime)} regime, ${label(OVERLAY, s.adr_signal).toLowerCase()} price overlay`, true)
      + stat('Revenue regime', pillRow(pill(REGIME, s.regime)), `${st ? `${st.capped ? 'over ' : ''}${dayWord(st.n)} · ` : ''}10-day is ${num(s.ratio_10d_45d, 2)}× the 45-day`)
      + cz
      + stat('Price overlay', pillRow(pill(OVERLAY, s.adr_signal)), overlayText(s));
    renderMoChart();
    renderMoLog();
    $('moRules').innerHTML = rulesHtml();
    $('moFoot').textContent = `Computed after each close from MCX’s daily F&O revenue and the NSE share price; data through ${fmt.day(s.date)}.`;
    MO.width = $('momentum').clientWidth;
  }

  function renderMoChart() {
    const d = MO.data, box = $('moChart');
    const all = d.history || [], n = n0(rangeState.momRegime || '60D');
    const rows = n ? all.slice(-n) : all;
    if (rows.length < 2) { box.innerHTML = '<p class="note">Not enough history for a chart.</p>'; return; }
    const w = Math.round(box.clientWidth) || 800, compact = w < 520;
    const long = ['1Y', '2Y', 'Max'].includes(rangeState.momRegime) && rows.length > 200;
    const pl = compact ? 44 : 56, pr = compact ? 10 : 124;
    const stripY = 4, stripH = 10, aTop = 30, aH = compact ? 150 : 200, gap = 22, bH = compact ? 90 : 120;
    const bTop = aTop + aH + gap, h = bTop + bH + 26;
    const iw = w - pl - pr, X = i => pl + iw * i / (rows.length - 1);
    const [aLo, aHi] = span(rows.flatMap(r => [r.ma10_rev_cr, r.ma45_rev_cr * COLD, r.ma45_rev_cr * HOT]), 0.08);
    const YA = v => aTop + aH * (1 - (v - aLo) / (aHi - aLo));
    const [bLo, bHi] = span(rows.map(r => r.close_price), 0.1);
    const YB = v => bTop + bH * (1 - (v - bLo) / (bHi - bLo));
    const last = rows[rows.length - 1];
    const s = [V.open(w, h, `Revenue regime, 10-day and 45-day averages and share price, ${fmt.span(rows[0].date, last.date)}`)];
    // Regime strip
    const cw = iw / rows.length;
    rows.forEach((r, i) => s.push(`<rect x="${(pl + cw * i).toFixed(2)}" y="${stripY}" width="${(cw + 0.4).toFixed(2)}" height="${stripH}" class="rg rg-${esc(r.regime || 'NONE')}"/>`));
    // Panel A: neutral zone, averages
    V.niceTicks(aHi, compact ? 3 : 4, aLo).forEach(v => {
      s.push(`<line x1="${pl}" x2="${w - pr}" y1="${YA(v).toFixed(1)}" y2="${YA(v).toFixed(1)}" class="c-grid"/>`, txt(pl - 8, YA(v) + 4, 'c-tick', `₹${num(v, 0)}`, 'end'));
    });
    s.push(`<path d="${bandPath(rows, X, YA, r => r.ma45_rev_cr ? r.ma45_rev_cr * COLD : null, r => r.ma45_rev_cr ? r.ma45_rev_cr * HOT : null)}" class="c-band"/>`);
    s.push(`<path d="${path(rows, X, YA, 'ma45_rev_cr')}" class="c-typical"/>`, `<path d="${path(rows, X, YA, 'ma10_rev_cr')}" class="c-line"/>`);
    // Panel B: price
    V.niceTicks(bHi, compact ? 2 : 3, bLo).forEach(v => {
      s.push(`<line x1="${pl}" x2="${w - pr}" y1="${YB(v).toFixed(1)}" y2="${YB(v).toFixed(1)}" class="c-grid"/>`, txt(pl - 8, YB(v) + 4, 'c-tick', `₹${num(v, 0)}`, 'end'));
    });
    s.push(`<path d="${path(rows, X, YB, 'close_price')}" class="c-line c-line--price"/>`);
    // The current regime's start, when it is inside the range
    const st = streak(all, 'regime'), k0 = all.length - rows.length;
    if (st && !st.capped && st.startIdx >= k0 && last.regime !== 'NEUTRAL') {
      const xi = X(st.startIdx - k0), p = (last.close_price / all[st.startIdx].close_price - 1) * 100;
      s.push(`<line x1="${xi.toFixed(1)}" x2="${xi.toFixed(1)}" y1="${aTop - 4}" y2="${bTop + bH}" class="c-now"/>`);
      const t = `${label(REGIME, last.regime)} since ${fmt.dayMonth(st.start)} · price ${signed(p)}%`;
      const right = xi < w - pr - 190;
      s.push(txt(xi + (right ? 6 : -6), bTop - 6, 'c-label2', esc(t), right ? null : 'end', true));
    }
    // End labels
    if (!compact) {
      const xl = w - pr + 10, ma45 = last.ma45_rev_cr;
      const edge = last.regime === 'HOT' ? { v: ma45 * HOT, s: 'Cools' } : last.regime === 'COLD' ? { v: ma45 * COLD, s: 'Warms' } : null;
      const items = [{ y: YA(last.ma10_rev_cr) + 4, s: `10-day ${crs(last.ma10_rev_cr).replace(' Cr', '')}`, cls: 'c-label' },
                     { y: YA(ma45) + 4, s: `45-day ₹${num(ma45, 2)}`, cls: 'c-label2' }];
      if (edge) items.push({ y: YA(edge.v) + 4, s: `${edge.s} ₹${num(edge.v, 2)}`, cls: 'c-label2' });
      else items.push({ y: YA(ma45 * HOT) + 4, s: `Hot ₹${num(ma45 * HOT, 2)}`, cls: 'c-label2' }, { y: YA(ma45 * COLD) + 4, s: `Cold ₹${num(ma45 * COLD, 2)}`, cls: 'c-label2' });
      V.spread(items, 15, aTop, aTop + aH).forEach(it => s.push(txt(xl, it.y, it.cls, it.s)));
      s.push(txt(xl, YB(last.close_price) + 4, 'c-label', `Price ₹${num(last.close_price, 0)}`));
      s.push(txt(xl, stripY + 9, 'c-label3', 'Regime'));
    }
    s.push(dateTicks(rows, X, h - 6, long, compact), '</svg>');
    box.innerHTML = s.join('');
    const xs = rows.map((_, i) => X(i));
    V.hover(box, { pl, pt: stripY, iw, ih: bTop + bH - stripY, h, pb: h - bTop - bH }, xs, i => {
      const r = rows[i];
      return `<strong>${fmt.day(r.date)}</strong> · ${esc(label(COMPOSITE, r.composite_signal))}<br>`
        + `10-day ₹${num(r.ma10_rev_cr, 2)} · 45-day ₹${num(r.ma45_rev_cr, 2)} (${num(r.ratio_10d_45d, 2)}×, ${esc(label(REGIME, r.regime).toLowerCase())})<br>`
        + `<span class="c-tip-sub">Price ₹${num(r.close_price, 1)} · overlay ${esc(label(OVERLAY, r.adr_signal).toLowerCase())}</span>`;
    });
    $('moLegend').innerHTML = '<span><i class="sw sw--line"></i>10-day average revenue</span><span><i class="sw sw--typical"></i>45-day average</span>'
      + '<span><i class="sw sw--band"></i>Neutral zone, 0.95–1.05× the 45-day</span>'
      + '<span><i class="sw rg-HOT"></i>Hot</span><span><i class="sw rg-NEUTRAL"></i>Neutral</span><span><i class="sw rg-COLD"></i>Cold</span>'
      + '<span><i class="sw sw--line"></i>Share price (lower panel)</span>';
  }

  function renderMoLog() {
    const rows = MO.data.history || [];
    const ch = changes(rows, 'composite_signal');
    const shown = MO.logAll ? ch : ch.slice(0, 10);
    const box = $('moLog');
    if (!ch.length) { box.innerHTML = `<p class="note">No change in the composite over the loaded history (${fmt.span(rows[0].date, rows[rows.length - 1].date)}).</p>`; return; }
    box.innerHTML = `<div class="table-scroll"><table class="v2-table sg-log"><thead><tr><th scope="col">Date</th><th scope="col">Composite</th><th scope="col">Regime</th>`
      + `<th scope="col">Close</th><th scope="col">Price over the run</th></tr></thead><tbody>`
      + shown.map(c => `<tr><td>${fmt.day(c.date)}</td><td><span class="sg-chg">${pill(COMPOSITE, c.from)}<span aria-label="to">→</span>${pill(COMPOSITE, c.to)}</span></td>`
        + `<td>${pill(REGIME, c.regime)}</td><td>₹${num(c.close, 1)}</td>`
        + `<td>${pctCell(c.runPct)}<small>${c.ongoing ? `${dayWord(c.days)} so far` : dayWord(c.days)}</small></td></tr>`).join('')
      + '</tbody></table></div>'
      + (ch.length > 10 ? `<button type="button" class="more-btn" id="moLogMore">${MO.logAll ? 'Show the latest 10' : `Show all ${ch.length} changes since ${fmt.dayMonth(rows[0].date)} ’${yearOf(rows[0].date)}`}</button>` : '');
    const b = $('moLogMore');
    if (b) b.addEventListener('click', () => { MO.logAll = !MO.logAll; renderMoLog(); });
  }

  function rulesHtml() {
    const P = k => pill(COMPOSITE, k);
    const cell = (r, o) => {
      if (r === 'HOT') return P(o === 'OVERSOLD' ? 'BUY' : 'STRONG_BUY');
      if (r === 'NEUTRAL') return P(o === 'BREAKOUT' || o === 'BULL_CONT' ? 'BUY' : 'HOLD');
      return P(o === 'OVERSOLD' ? 'WATCH' : 'SELL');
    };
    const ov = ['BREAKOUT', 'BULL_CONT', 'NEUTRAL', 'OVERSOLD'];
    return `<div class="sg-regimes"><div>${pill(REGIME, 'HOT')}<span>10-day ÷ 45-day above ${HOT}</span></div>`
      + `<div>${pill(REGIME, 'NEUTRAL')}<span>Between ${COLD} and ${HOT}</span></div><div>${pill(REGIME, 'COLD')}<span>Below ${COLD}</span></div></div>`
      + '<p class="note">The price overlay compares the share price’s move over 5 days with its average daily move (close to close) over 5 days against 20 days, checked in this order:</p>'
      + '<ul class="sg-rules"><li><strong>Breakout</strong>: up more than 1.5%, with the 5-day average move above 1.5× the 20-day</li>'
      + '<li><strong>Quiet rise</strong>: up more than 1%, with the 5-day average move below 0.8× the 20-day</li>'
      + '<li><strong>Sharp fall</strong>: down more than 1%, with the 5-day average move above 1.3× the 20-day</li>'
      + '<li><strong>Neutral</strong>: none of these</li></ul>'
      + `<div class="table-scroll"><table class="v2-table sg-matrix"><caption>The composite: price overlay × regime</caption><thead><tr><th scope="col">Overlay</th>${['HOT', 'NEUTRAL', 'COLD'].map(r => `<th scope="col">${esc(label(REGIME, r))}</th>`).join('')}</tr></thead><tbody>`
      + ov.map(o => `<tr><th scope="row">${esc(label(OVERLAY, o))}</th>${['HOT', 'NEUTRAL', 'COLD'].map(r => `<td>${cell(r, o)}</td>`).join('')}</tr>`).join('')
      + '</tbody></table></div>';
  }

  makeRangeToggle({ key: 'momRegime', containerId: 'moRange', ranges: RANGES, defaultRange: '60D', labelIds: ['moRangeLabel'],
                    onChange: () => loadMomentum() });
  lazyEvidence('moEvidence', 'moEvBtn', () => fetchRanged('/api/models?view=momentum&range=Max').then(d => {
    if (!d.success) throw new Error(d.error || 'No data');
    const ev = forward(d.history || [], 'composite_signal', ['STRONG_BUY', 'BUY', 'HOLD', 'WATCH', 'SELL']);
    $('moEvidence').innerHTML = evidenceHtml(ev, COMPOSITE, 10);
  }).catch(e => MCX.ui.error($('moEvidence'), 'Could not load the full history: ' + (e.message || e))));

  // ════════════════════════════════════════════════════════════════════════
  //  Model ensemble
  // ════════════════════════════════════════════════════════════════════════
  const EN = { data: null, err: null, width: 0 };
  const enUrl = () => '/api/models?range=' + encodeURIComponent(rangeState.ecmChart || '60D');

  function loadModels() {
    return fetchRanged(enUrl()).then(d => {
      if (!d.success) throw new Error(d.error || 'No data');
      EN.data = d; EN.err = null; renderEnsemble();
    }).catch(e => { EN.err = e.message || String(e); renderEnsemble(); });
  }

  function renderEnsemble() {
    const d = EN.data;
    if (!d) {
      if (EN.err) { $('enHead').textContent = 'The model ensemble could not be loaded.'; MCX.ui.error($('enBuild'), 'Could not load the models: ' + EN.err, loadModels); }
      return;
    }
    const s = d.snapshot, rows = d.history || [], last = rows[rows.length - 1] || {};
    const l = ensLede(s);
    $('enKicker').textContent = `Model ensemble · end of day, ${fmt.dayMonth(s.date)}`;
    $('enHead').textContent = l.head;
    $('enDeck').textContent = l.deck;
    const e = s.ensemble || {}, pos = s.position || {}, ecm = s.ecm || {}, mf = s.multi_factor || {};
    const sc = e.score;
    $('enStats').innerHTML = stat('Ensemble score', val(sc === null || sc === undefined ? '—' : signed(sc, 2)) + pillRow(pill(COMPOSITE, e.signal)),
        'Buy above +0.5, strong buy above +1.5; sell below −0.5, strong sell below −1.5', true)
      + stat('Position the model implies', val(pos.score === null || pos.score === undefined ? '—' : `${num(Math.abs(pos.score) * 100, 0)}%<small>${pos.score >= 0 ? 'long' : 'short'}</small>`),
        `of a full position: the score ÷ 2, capped at 100%${pos.velocity_1d !== null && pos.velocity_1d !== undefined ? `; ${signed(pos.velocity_1d * 100, 1)} points on the day` : ''}`)
      + stat('Price gap to fair value', val(ecm.spread_pct === null || ecm.spread_pct === undefined ? '—' : `${signed(ecm.spread_pct)}%`),
        last.ecm_band_mean !== undefined && last.ecm_band_mean !== null ? `60-day average ${signed(last.ecm_band_mean)}%; z ${signed(ecm.z_score, 2)}` : '')
      + stat('Exchange activity', val(mf.composite_z === null || mf.composite_z === undefined ? '—' : signed(mf.composite_z, 2)),
        'standard deviations from the 60-day norm, revenue and turnover blended 3 : 4');
    renderBuild(s);
    renderEnHist();
    $('enEcmPill').innerHTML = pill(ECM, ecm.signal);
    $('enEcm').innerHTML = `<p class="sg-para">${esc(ecmText(s, last.ecm_band_mean))}</p>`
      + (ecm.half_life_days ? `<p class="note">The job also reports a reversion time of ${num(ecm.half_life_days, 1)} days. That is a rule of thumb, 60 ÷ (1 + |z|), not an estimate from the data.</p>` : '')
      + '<p class="note">Fair value here is the data-driven base on the Fair value page. The gap’s z-score uses the last 60 trading days (at least 30).</p>';
    $('enMfPill').innerHTML = pill(COMPOSITE, mf.signal);
    $('enMf').innerHTML = factorsHtml(mf);
    $('enFoot').textContent = `Computed after each close by the model job, after the fair-value job; data through ${fmt.day(s.date)}.`;
    EN.width = $('ensemble').clientWidth;
  }

  // Waterfall of contributions on the −3…+3 scale, with the signal bands behind it
  function renderBuild(s) {
    const box = $('enBuild'), c = contributions(s);
    if (!c) { box.innerHTML = '<p class="note">Not enough data for today’s score.</p>'; $('enWeights').innerHTML = ''; return; }
    const w = Math.round(box.clientWidth) || 700, compact = w < 520;
    const lw = compact ? 96 : 176, rw = compact ? 44 : 64, top = 26, rowH = compact ? 30 : 34;
    const h = top + rowH * 4 + 24;
    const lim = Math.max(3, Math.ceil(Math.max(Math.abs(c.total), ...c.parts.map(p => Math.abs(p.value))) + 0.5));
    const X = v => lw + (w - lw - rw) * (v + lim) / (2 * lim);
    const sv = [V.open(w, h, `Ensemble score ${signed(c.total, 2)}: ` + c.parts.map(p => `${p.label} ${signed(p.value, 2)}`).join(', '))];
    // Bands
    [[-lim, -1.5, 'Strong sell'], [-1.5, -0.5, 'Sell'], [-0.5, 0.5, 'Neutral'], [0.5, 1.5, 'Buy'], [1.5, lim, 'Strong buy']].forEach(([a, b, t], k) => {
      if (k === 2) sv.push(`<rect x="${X(a).toFixed(1)}" y="${top - 4}" width="${(X(b) - X(a)).toFixed(1)}" height="${rowH * 4 + 4}" class="c-band"/>`);
      if (!compact || k !== 1 && k !== 3) sv.push(txt((X(a) + X(b)) / 2, top - 10, 'c-label3', t, 'middle'));
    });
    [-1.5, -0.5, 0.5, 1.5].forEach(v => sv.push(`<line x1="${X(v).toFixed(1)}" x2="${X(v).toFixed(1)}" y1="${top - 4}" y2="${top + rowH * 4}" class="c-grid"/>`));
    sv.push(`<line x1="${X(0).toFixed(1)}" x2="${X(0).toFixed(1)}" y1="${top - 4}" y2="${top + rowH * 4}" class="c-axis"/>`);
    let run = 0;
    c.parts.forEach((p, k) => {
      const y = top + rowH * k + rowH / 2, a = X(run), b = X(run + p.value);
      sv.push(`<rect x="${Math.min(a, b).toFixed(1)}" y="${(y - 7).toFixed(1)}" width="${Math.max(Math.abs(b - a), 1.5).toFixed(1)}" height="14" rx="2" class="c-bar"/>`);
      if (k) sv.push(`<line x1="${a.toFixed(1)}" x2="${a.toFixed(1)}" y1="${(y - rowH + 7).toFixed(1)}" y2="${(y - 7).toFixed(1)}" class="c-cross"/>`);
      sv.push(txt(0, y + 4, 'c-label2', esc(compact ? p.label.replace('Price gap to fair value', 'Price gap') : `${p.label} (${num(p.weight * 100, 0)}%)`)));
      sv.push(txt(w, y + 4, 'c-label', signed(p.value, 2), 'end'));
      run += p.value;
    });
    const y = top + rowH * 3 + rowH / 2;
    sv.push(`<rect x="${Math.min(X(0), X(c.total)).toFixed(1)}" y="${(y - 8).toFixed(1)}" width="${Math.max(Math.abs(X(c.total) - X(0)), 2).toFixed(1)}" height="16" rx="2" class="c-ink"/>`);
    sv.push(txt(0, y + 4, 'c-label', 'Score'), txt(w, y + 4, 'c-label', signed(c.total, 2), 'end'));
    [-lim, -1.5, -0.5, 0, 0.5, 1.5, lim].forEach(v => { if (!compact || Number.isInteger(v)) sv.push(txt(X(v), h - 6, 'c-tick', v === 0 ? '0' : signed(v, Number.isInteger(v) ? 0 : 1), 'middle')); });
    sv.push('</svg>');
    box.innerHTML = sv.join('');
    const mf = s.multi_factor || {};
    $('enWeights').innerHTML = `<div class="table-scroll"><table class="v2-table sg-weights"><caption>How the score is built (lib/cron_models.py)</caption>`
      + '<thead><tr><th scope="col">Part</th><th scope="col">Reading today</th><th scope="col">Weight</th><th scope="col">Contribution</th></tr></thead><tbody>'
      + `<tr><td>Revenue z-score<small>daily F&amp;O revenue against its last 60 days</small></td><td>${signed(mf.revenue_z, 2)}</td><td>30%<small>70% × 3/7</small></td><td>${signed(c.parts[0].value, 2)}</td></tr>`
      + `<tr><td>Turnover z-score<small>futures notional + option premium</small></td><td>${signed(mf.turnover_z, 2)}</td><td>40%<small>70% × 4/7</small></td><td>${signed(c.parts[1].value, 2)}</td></tr>`
      + `<tr><td>Price gap z-score, sign flipped<small>a gap below its norm counts in favour</small></td><td>${signed(-s.ecm.z_score, 2)}</td><td>30%</td><td>${signed(c.parts[2].value, 2)}</td></tr>`
      + `<tr class="total"><td>Ensemble score</td><td></td><td>100%</td><td>${signed(c.total, 2)}</td></tr></tbody></table></div>`
      + (Math.abs(c.total - s.ensemble.score) > 0.01 ? `<p class="note">The job stores ${signed(s.ensemble.score, 2)}; the parts above are rounded.</p>` : '');
  }

  function factorsHtml(mf) {
    const bar = z => {
      if (z === null || z === undefined) return '<span class="sg-zbar"></span>';
      const c = Math.max(-3, Math.min(3, z)), wd = Math.abs(c) / 3 * 50;
      return `<span class="sg-zbar" aria-hidden="true"><i style="left:${(c >= 0 ? 50 : 50 - wd).toFixed(1)}%;width:${wd.toFixed(1)}%"></i></span>`;
    };
    const row = (lab, sub, z, ref) => `<div class="sg-factor${ref ? ' sg-factor--ref' : ''}"><span class="sg-fname">${lab}<small>${sub}</small></span>${bar(z)}`
      + `<span class="sg-fval">${z === null || z === undefined ? '—' : signed(z, 2)}</span></div>`;
    return `<div class="sg-factors"><div class="sg-factor sg-factor--head" aria-hidden="true"><span></span><span class="sg-zscale"><span>−3</span><span>0</span><span>+3</span></span><span></span></div>`
      + row('Revenue', 'weight 3/7 of this model', mf.revenue_z)
      + row('Turnover', 'weight 4/7 of this model', mf.turnover_z)
      + `<div class="sg-factor sg-factor--total"><span class="sg-fname">Composite</span>${bar(mf.composite_z)}<span class="sg-fval">${mf.composite_z === null || mf.composite_z === undefined ? '—' : signed(mf.composite_z, 2)}</span></div>`
      + row('Share volume', 'computed, not in the score', mf.volume_z, true)
      + row('Intraday calm', 'inverse of the high–low range; computed, not in the score', mf.volatility_z, true)
      + '</div><p class="note">Each factor is today’s value in standard deviations from its last 60 trading days. Volume and intraday calm were dropped from the score after a factor review found they added nothing; they stay here for reference.</p>';
  }

  // Score (upper panel) and price gap (lower panel) on one time axis
  function renderEnHist() {
    const d = EN.data, box = $('enHist'), rows = d.history || [];
    if (rows.length < 2) { box.innerHTML = '<p class="note">Not enough history for a chart.</p>'; return; }
    const w = Math.round(box.clientWidth) || 900, compact = w < 520;
    const long = ['1Y', '2Y', 'Max'].includes(rangeState.ecmChart) && rows.length > 200;
    const pl = compact ? 40 : 52, pr = compact ? 10 : 118, aTop = 10, aH = compact ? 130 : 170, gap = 24, bH = compact ? 110 : 140;
    const bTop = aTop + aH + gap, h = bTop + bH + 26, iw = w - pl - pr, X = i => pl + iw * i / (rows.length - 1);
    const [aLo0, aHi0] = span(rows.map(r => r.ensemble_score), 0.1);
    const aLo = Math.min(aLo0, -1.8), aHi = Math.max(aHi0, 1.8);
    const YA = v => aTop + aH * (1 - (v - aLo) / (aHi - aLo));
    const [bLo, bHi] = span(rows.flatMap(r => [r.ecm_spread_pct, r.ecm_band_1dn, r.ecm_band_1up]), 0.08);
    const YB = v => bTop + bH * (1 - (v - bLo) / (bHi - bLo));
    const last = rows[rows.length - 1];
    const s = [V.open(w, h, `Ensemble score and price gap to fair value, ${fmt.span(rows[0].date, last.date)}`)];
    s.push(`<rect x="${pl}" y="${YA(0.5).toFixed(1)}" width="${iw}" height="${(YA(-0.5) - YA(0.5)).toFixed(1)}" class="c-band"/>`);
    [-1.5, 1.5].forEach(v => s.push(`<line x1="${pl}" x2="${w - pr}" y1="${YA(v).toFixed(1)}" y2="${YA(v).toFixed(1)}" class="c-typical"/>`));
    s.push(`<line x1="${pl}" x2="${w - pr}" y1="${YA(0).toFixed(1)}" y2="${YA(0).toFixed(1)}" class="c-axis"/>`);
    [-1.5, -0.5, 0, 0.5, 1.5].forEach(v => s.push(txt(pl - 8, YA(v) + 4, 'c-tick', v === 0 ? '0' : signed(v, 1), 'end')));
    s.push(`<path d="${path(rows, X, YA, 'ensemble_score')}" class="c-line"/>`);
    V.niceTicks(bHi, compact ? 3 : 4, bLo).forEach(v => s.push(`<line x1="${pl}" x2="${w - pr}" y1="${YB(v).toFixed(1)}" y2="${YB(v).toFixed(1)}" class="${Math.abs(v) < 1e-9 ? 'c-axis' : 'c-grid'}"/>`,
      txt(pl - 8, YB(v) + 4, 'c-tick', `${num(v, 0)}%`, 'end')));
    s.push(`<path d="${bandPath(rows, X, YB, r => r.ecm_band_1dn ?? null, r => r.ecm_band_1up ?? null)}" class="c-band"/>`);
    s.push(`<path d="${path(rows, X, YB, 'ecm_band_mean')}" class="c-typical"/>`, `<path d="${path(rows, X, YB, 'ecm_spread_pct')}" class="c-line"/>`);
    if (!compact) {
      const xl = w - pr + 10;
      const ia = [{ y: YA(last.ensemble_score) + 4, s: `Score ${signed(last.ensemble_score, 2)}`, cls: 'c-label' },
                  { y: YA(1.5) + 4, s: 'Strong buy', cls: 'c-label3' }, { y: YA(-1.5) + 4, s: 'Strong sell', cls: 'c-label3' }];
      V.spread(ia, 15, aTop, aTop + aH).forEach(it => s.push(txt(xl, it.y, it.cls, it.s)));
      const ib = [{ y: YB(last.ecm_spread_pct) + 4, s: `Gap ${signed(last.ecm_spread_pct)}%`, cls: 'c-label' }];
      if (last.ecm_band_mean !== null && last.ecm_band_mean !== undefined) ib.push({ y: YB(last.ecm_band_mean) + 4, s: `60-day avg ${signed(last.ecm_band_mean)}%`, cls: 'c-label2' });
      V.spread(ib, 15, bTop, bTop + bH).forEach(it => s.push(txt(xl, it.y, it.cls, it.s)));
    }
    s.push(txt(pl + 4, aTop + 12, 'c-label3', 'Ensemble score', null, true), txt(pl + 4, bTop + 12, 'c-label3', 'Price gap to fair value, %', null, true));
    s.push(dateTicks(rows, X, h - 6, long, compact), '</svg>');
    box.innerHTML = s.join('');
    V.hover(box, { pl, pt: aTop, iw, ih: bTop + bH - aTop, h, pb: h - bTop - bH }, rows.map((_, i) => X(i)), i => {
      const r = rows[i];
      return `<strong>${fmt.day(r.date)}</strong> · ${esc(label(COMPOSITE, r.ensemble_signal))}<br>Score ${r.ensemble_score === null ? '—' : signed(r.ensemble_score, 2)}`
        + `<br><span class="c-tip-sub">Gap ${signed(r.ecm_spread_pct)}%${r.ecm_band_mean !== null && r.ecm_band_mean !== undefined ? ` · 60-day avg ${signed(r.ecm_band_mean)}%` : ''}</span>`;
    });
    $('enLegend').innerHTML = '<span><i class="sw sw--line"></i>Score (upper) and gap (lower)</span><span><i class="sw sw--band"></i>Neutral band, ±0.5 (upper); 60-day average ±1 standard deviation (lower)</span>'
      + '<span><i class="sw sw--typical"></i>Strong buy / sell at ±1.5 (upper); 60-day average gap (lower)</span>';
  }

  makeRangeToggle({ key: 'ecmChart', containerId: 'enRange', ranges: RANGES, defaultRange: '60D', labelIds: ['enRangeLabel'],
                    onChange: () => loadModels() });
  lazyEvidence('enEvidence', 'enEvBtn', () => fetchRanged('/api/models?range=Max').then(d => {
    if (!d.success) throw new Error(d.error || 'No data');
    const ev = forward(d.history || [], 'ensemble_signal', ['STRONG_BUY', 'BUY', 'NEUTRAL', 'SELL', 'STRONG_SELL'], 10, 'price');
    $('enEvidence').innerHTML = evidenceHtml(ev, COMPOSITE, 10);
  }).catch(e => MCX.ui.error($('enEvidence'), 'Could not load the full history: ' + (e.message || e))));

  // ── Resizing ─────────────────────────────────────────────────────────────
  if (window.ResizeObserver) {
    const watch = (id, st, redraw) => {
      let queued = false;
      new ResizeObserver(() => {
        if (queued) return;
        queued = true;
        requestAnimationFrame(() => { queued = false; const el = $(id); if (st.data && el.clientWidth && Math.abs(el.clientWidth - st.width) > 2) redraw(); });
      }).observe($(id));
    };
    watch('momentum', MO, renderMomentum);
    watch('ensemble', EN, renderEnsemble);
  }

  MCX.signals = { momentum: { mount: loadMomentum }, ensemble: { mount: loadModels } };
})();
