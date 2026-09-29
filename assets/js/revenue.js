/* MCX Revenue Monitor: the Revenue pages — Trends, Seasonality and Commodities.
   Data: /api/exchange_dashboard (daily revenue by segment and its period summaries),
   /api/commodity_dashboard (revenue by commodity), /api/commodities?view=signals and
   ?view=icomdex. Charts are SVG coloured by CSS variables (v2.css). */
(function () {
  'use strict';
  const fmt = MCX.fmt, num = fmt.num, V = MCX.svg, esc = V.esc;

  // ════════════════════════════════════════════════════════════════════════
  //  Model: pure functions (scripts/test_revenue_js.js)
  // ════════════════════════════════════════════════════════════════════════
  const pctText = v => `${Math.abs(v).toFixed(0)}%`;
  const share = r => (r.avg_total ? r.avg_opt / r.avg_total * 100 : null);
  const lastYear = q => q.replace(/FY(\d+)/, (_, y) => 'FY' + String(+y - 1).padStart(2, '0'));

  // "Revenue is averaging ₹11.25 Cr a day this quarter, more than double a year ago."
  // Early in a quarter (under 5 sessions) it speaks about the last full quarter instead.
  function trendsLede(quarterly) {
    if (!quarterly || quarterly.length < 2) return null;
    const young = quarterly[0].trading_days < 5;
    const q = young ? quarterly[1] : quarterly[0];
    const prev = quarterly[quarterly.indexOf(q) + 1];
    const ly = quarterly.find(x => x.quarter === lastYear(q.quarter));
    const yoy = q.yoy_total, qoq = q.qoq_total;
    let comp = '';
    if (yoy !== null && yoy !== undefined) {
      const mult = 1 + yoy / 100;
      comp = mult >= 3 ? ', more than three times a year ago' : mult > 2 ? ', more than double a year ago' : mult === 2 ? ', double a year ago'
        : yoy >= 0.5 ? `, ${pctText(yoy)} more than a year ago` : yoy <= -0.5 ? `, ${pctText(yoy)} less than a year ago` : ', level with a year ago';
    }
    const head = young ? `${q.quarter} averaged ₹${num(q.avg_total, 2)} Cr a day${comp}.`
                       : `Revenue is averaging ₹${num(q.avg_total, 2)} Cr a day this quarter${comp}.`;
    const parts = [];
    const vs = (v, label) => Math.abs(v) < 0.5 ? `level with ${label}` : `${pctText(v)} ${v > 0 ? 'above' : 'below'} ${label}`;
    if (ly && yoy !== null && yoy !== undefined && prev && qoq !== null && qoq !== undefined) parts.push(`That is ${vs(yoy, ly.quarter)} and ${vs(qoq, prev.quarter)}.`);
    else if (prev && qoq !== null && qoq !== undefined) parts.push(`That is ${vs(qoq, prev.quarter)}.`);
    if (ly) {
      const sh = share(q), shLy = share(ly);
      const when = young ? `in ${q.quarter}` : 'this quarter';
      if (sh - shLy >= 2 && q.yoy_opt > q.yoy_fut) parts.push(`Options drive the growth: ${sh.toFixed(0)}% of revenue ${when}, up from ${shLy.toFixed(0)}% a year ago.`);
      else parts.push(`Options were ${sh.toFixed(0)}% of revenue ${when}, against ${shLy.toFixed(0)}% a year ago.`);
    }
    return { head, deck: parts.join(' '), subject: q.quarter };
  }

  // Rolling n-session mean of futures and options (null until n sessions exist)
  function rollingSplit(rows, n) {
    let f = 0, o = 0;
    return rows.map((r, i) => {
      f += r.fut; o += r.opt;
      if (i >= n) { f -= rows[i - n].fut; o -= rows[i - n].opt; }
      return i >= n - 1 ? { date: r.date, fut: f / n, opt: o / n } : null;
    });
  }

  // Busiest and quietest weekday this quarter, and the last 10 weeks
  function seasonLede(qdow, dow) {
    const rows = (qdow || []).filter(r => r.cur_total);
    if (rows.length < 5) return null;
    const hi = rows.reduce((a, b) => (b.cur_total > a.cur_total ? b : a));
    const lo = rows.reduce((a, b) => (b.cur_total < a.cur_total ? b : a));
    const spread = (hi.cur_total / lo.cur_total - 1) * 100;
    const q = rows[0].cur_q;
    const head = spread < 5
      ? `Revenue has been spread evenly across the week in ${q}: every weekday averaged between ₹${num(lo.cur_total, 2)} and ₹${num(hi.cur_total, 2)} Cr.`
      : `${hi.day}s have been the busiest day in ${q}, averaging ₹${num(hi.cur_total, 2)} Cr, ${spread.toFixed(0)}% more than ${lo.day}s.`;
    const parts = [];
    const d10 = (dow || []).filter(r => r.avg10_total);
    if (d10.length === 5 && spread >= 5) {
      const top = d10.reduce((a, b) => (b.avg10_total > a.avg10_total ? b : a));
      parts.push(top.day === hi.day ? `${top.day}s have also led over the last 10 weeks, at ₹${num(top.avg10_total, 2)} Cr.`
                                    : `Over the last 10 weeks ${top.day}s have led instead, at ₹${num(top.avg10_total, 2)} Cr.`);
    }
    const yoy = rows.map(r => r.yoy_total_pct).filter(v => v !== null && v !== undefined);
    if (yoy.length === 5) {
      const mn = Math.min(...yoy), mx = Math.max(...yoy), yq = rows[0].yoy_q;
      if (mn > 0) parts.push(`Every weekday is above ${yq}, by ${mn.toFixed(0)}% to ${mx.toFixed(0)}%.`);
      else if (mx < 0) parts.push(`Every weekday is below ${yq}, by ${Math.abs(mx).toFixed(0)}% to ${Math.abs(mn).toFixed(0)}%.`);
    }
    return { head, deck: parts.join(' ') };
  }

  const NAMES = { CRUDEOIL: 'Crude oil', NATURALGAS: 'Natural gas', SILVER100: 'Silver 100', OTHERS: 'Others', TOTAL: 'All commodities' };
  const cname = c => esc(NAMES[c] || (String(c).charAt(0) + String(c).slice(1).toLowerCase()));   // escaped: used in HTML

  // The biggest commodity by the last 45 days, the fastest grower, and the biggest recent shift
  function cmdLede(matrix) {
    const tot = (matrix || []).find(r => r.commodity === 'TOTAL');
    const rows = (matrix || []).filter(r => r.commodity !== 'TOTAL' && r.commodity !== 'OTHERS' && r.avg_45d);
    if (!tot || !tot.avg_45d || !rows.length) return null;
    const top = rows.reduce((a, b) => (b.avg_45d > a.avg_45d ? b : a));
    const sh = top.avg_45d / tot.avg_45d * 100;
    const shWord = sh >= 60 ? `${sh.toFixed(0)}%` : sh >= 45 && sh < 50 ? 'nearly half' : sh >= 50 ? 'more than half' : `${sh.toFixed(0)}%`;
    const head = `${cname(top.commodity)} brings in ${shWord} of MCX’s revenue: ₹${num(top.avg_45d, 2)} Cr a day over the last 45 days.`;
    const parts = [];
    const grow = rows.filter(r => r.yoy_pct !== null && r.yoy_pct !== undefined && r.avg_45d / tot.avg_45d >= 0.05);
    if (grow.length) {
      const g = grow.reduce((a, b) => (b.yoy_pct > a.yoy_pct ? b : a));
      if (g.yoy_pct > 0) parts.push(`${cname(g.commodity)} is growing fastest this year, ${g.yoy_pct >= 100 ? `${(1 + g.yoy_pct / 100).toFixed(1)} times its level` : `up ${g.yoy_pct.toFixed(0)}% on`} a year ago.`);
    }
    const shift = rows.filter(r => r.avg_5d && r.avg_45d / tot.avg_45d >= 0.05).map(r => ({ r, x: (r.avg_5d / r.avg_45d - 1) * 100 }));
    if (shift.length) {
      const s = shift.reduce((a, b) => (Math.abs(b.x) > Math.abs(a.x) ? b : a));
      if (Math.abs(s.x) >= 10) parts.push(`Over the last five days ${cname(s.r.commodity).toLowerCase()} ran ${Math.abs(s.x).toFixed(0)}% ${s.x > 0 ? 'above' : 'below'} its 45-day average.`);
    }
    return { head, deck: parts.join(' ') };
  }

  // One scale for every signal: text label, arrow, and a state class
  function signalPill(sig) {
    const M = { STRONG_BUY: ['strong-up', '▲▲', 'Strong buy'], BUY: ['up', '▲', 'Buy'], NEUTRAL: ['', '', 'Neutral'],
                SELL: ['down', '▼', 'Sell'], STRONG_SELL: ['strong-down', '▼▼', 'Strong sell'] };
    const m = M[sig] || ['none', '', 'No data'];
    return `<span class="pill${m[0] ? ' pill--' + m[0] : ''}">${m[1] ? `<span aria-hidden="true">${m[1]}</span>` : ''}${m[2]}</span>`;
  }

  MCX.revenueModel = { trendsLede, rollingSplit, seasonLede, cmdLede, signalPill, cname, lastYear };
  if (window.MCX_TEST) return;

  // ════════════════════════════════════════════════════════════════════════
  //  Shared
  // ════════════════════════════════════════════════════════════════════════
  const $ = id => document.getElementById(id);
  const cr = (v, dp = 2) => (v === null || v === undefined ? '—' : `₹${num(v, dp)}`);
  const z = v => (v === null || v === undefined ? '—' : `<span${Math.abs(v) >= 1 ? ' class="strong"' : ''}>${v > 0 ? '+' : ''}${num(v, 2)}</span>`);
  const INFO = '<svg aria-hidden="true" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M12 11v5"/><path d="M12 8h.01"/></svg>';
  function chg(v, suffix) {
    if (v === null || v === undefined) return '<span class="muted">—</span>';
    if (Math.abs(v) < 0.5) return 'level' + (suffix || '');
    return `<span class="${v > 0 ? 'up' : 'down'}">${v > 0 ? '▲' : '▼'} ${Math.abs(v).toFixed(0)}%</span>${suffix || ''}`;
  }
  // Index moves need two decimals: −2.85%, not −3%
  const chg2 = v => (v === null || v === undefined ? '<span class="muted">—</span>'
    : `<span class="${v > 0 ? 'up' : v < 0 ? 'down' : ''}">${v > 0 ? '▲' : v < 0 ? '▼' : ''} ${num(Math.abs(v), 2)}%</span>`);
  function table(head, rows, opts = {}) {
    return `<div class="table-scroll"><table class="v2-table"><thead><tr>${head.map(h => `<th scope="col">${h}</th>`).join('')}</tr></thead><tbody>`
      + rows.map(r => `<tr${r.cls ? ` class="${r.cls}"` : ''}>${(r.cells || r).map(c => `<td>${c}</td>`).join('')}</tr>`).join('')
      + `</tbody></table></div>${opts.after || ''}`;
  }
  function ledeInto(prefix, kicker, l) {
    $(prefix + 'Kicker').textContent = kicker;
    $(prefix + 'Head').textContent = l ? l.head : 'No data yet.';
    $(prefix + 'Deck').textContent = l ? l.deck : '';
  }
  function fail(el, msg, retry) { MCX.ui.error(el, msg, retry); }
  const monthLabel = m => `${['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'][m.month - 1]} ${m.year}`;
  function onResize(el, fn) {
    if (!window.ResizeObserver) return;
    let w = 0, queued = false;
    new ResizeObserver(() => {
      if (queued) return; queued = true;
      requestAnimationFrame(() => { queued = false; if (Math.abs(el.clientWidth - w) > 2) { w = el.clientWidth; fn(); } });
    }).observe(el);
  }

  // Stacked area paths for series (bottom first); xs: pixel x per point
  function stackPaths(f, xs, series, keyOf) {
    const n = xs.length, base = new Array(n).fill(0), out = [];
    series.forEach(sr => {
      const top = base.map((b, i) => b + (sr.vals[i] || 0));
      const up = xs.map((x, i) => `${x.toFixed(1)},${f.y(top[i]).toFixed(1)}`);
      const dn = xs.map((x, i) => `${x.toFixed(1)},${f.y(base[i]).toFixed(1)}`).reverse();
      out.push(`<path d="M${up.join('L')}L${dn.join('L')}Z" class="${keyOf(sr)}"/><path d="M${up.join('L')}" class="c-sep"/>`);
      top.forEach((v, i) => { base[i] = v; });
    });
    return { svg: out.join(''), top: base };
  }
  function quarterTicks(dates, X, f, h) {
    const out = [], seen = new Set();
    // Always mark where the range starts, then each quarter start that has room
    const [y0, m0, d0] = dates[0].split('-').map(Number);
    out.push(V.text(X(0), h - 8, 'c-tick', `${d0} ${['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'][m0 - 1]} ${y0}`, 'start'));
    let lastX = X(0) + 40;
    dates.forEach((d, i) => {
      const [y, m, dd] = d.split('-').map(Number);
      if (![1, 4, 7, 10].includes(m) || dd > 7 || seen.has(y * 100 + m)) return;
      seen.add(y * 100 + m);
      const x = X(i);
      if (x - lastX < 60 || x > f.w - f.pr - 20) return;
      lastX = x;
      out.push(V.text(x, h - 8, 'c-tick', `${['', 'Jan', '', '', 'Apr', '', '', 'Jul', '', '', 'Oct'][m]} ${y}`, x - f.pl < 30 ? 'start' : 'middle'));
    });
    return out.join('');
  }
  function dayTicks(dates, X, f, h) {
    if (!dates.length) return '';
    return V.text(f.pl, h - 8, 'c-tick', fmt.dayMonth(dates[0])) + V.text(f.w - f.pr, h - 8, 'c-tick', fmt.dayMonth(dates[dates.length - 1]), 'end');
  }

  let EX = null, exLoading = null;
  function loadEx() {
    if (EX) return Promise.resolve(EX);
    if (!exLoading) exLoading = fetchRanged('/api/exchange_dashboard').then(d => {
      if (!d.success) throw new Error(d.error || 'No data');
      EX = d; return d;
    }).finally(() => { exLoading = null; });
    return exLoading;
  }
  // A refresh clears the ranged cache (legacy.js); drop ours with it so the next visit refetches
  MCX.store.on('refresh', () => { EX = null; });

  // ════════════════════════════════════════════════════════════════════════
  //  Trends
  // ════════════════════════════════════════════════════════════════════════
  const TR = { period: MCX.storage.get('mcx.trends.period', 'quarter'), all: false };
  const TR_N = { Q: 63, '1Y': 252, '2Y': 504, Max: 1e9 };

  function trendsSvg(w, daily, n) {
    const roll = rollingSplit(daily, 20).filter(Boolean);
    const pts = roll.slice(-Math.min(n, roll.length));
    if (pts.length < 2) return { svg: '' };
    const compact = w < 520;
    const h = compact ? 240 : 340;
    const vmax = Math.max(...pts.map(p => p.fut + p.opt)) * 1.12;
    const f = V.frame(w, h, vmax, [compact ? 36 : 44, compact ? 8 : 128, 16, 30]);
    const X = i => f.pl + f.iw * i / (pts.length - 1);
    const xs = pts.map((_, i) => X(i));
    const st = stackPaths(f, xs, [{ key: 'fut', vals: pts.map(p => p.fut) }, { key: 'opt', vals: pts.map(p => p.opt) }], s => 'c-' + s.key);
    const last = pts[pts.length - 1], tot = last.fut + last.opt;
    const s = [V.open(w, h, `Revenue per day, 20-session average, futures and options stacked: latest ₹${num(tot, 2)} crore (futures ₹${num(last.fut, 2)}, options ₹${num(last.opt, 2)})`)];
    s.push(V.grid(f, V.niceTicks(vmax, compact ? 3 : 5), v => `₹${num(v, 0)}`));
    s.push(st.svg);
    s.push(`<path d="M${pts.map((p, i) => `${xs[i].toFixed(1)},${f.y(p.fut + p.opt).toFixed(1)}`).join('L')}" class="c-line" style="stroke-width:1.5px"/>`);
    if (!compact) {
      const items = V.spread([{ y: f.y(tot) + 4, s: `Total ₹${num(tot, 2)}`, cls: 'c-label' },
                              { y: f.y(last.fut + last.opt / 2) + 4, s: `Options ₹${num(last.opt, 2)}`, cls: 'c-label2' },
                              { y: f.y(last.fut / 2) + 4, s: `Futures ₹${num(last.fut, 2)}`, cls: 'c-label2' }], 15, f.pt + 4, f.y(0));
      items.forEach(it => s.push(V.text(f.w - f.pr + 12, it.y, it.cls, it.s)));
    }
    s.push(pts.length > 90 ? quarterTicks(pts.map(p => p.date), X, f, h) : dayTicks(pts.map(p => p.date), X, f, h));
    s.push('</svg>');
    const tip = i => `<strong>${fmt.day(pts[i].date)}</strong><br>20-session average ₹${num(pts[i].fut + pts[i].opt, 2)} Cr`
      + `<br><span class="c-tip-sub">futures ₹${num(pts[i].fut, 2)} · options ₹${num(pts[i].opt, 2)}</span>`;
    return { svg: s.join(''), f, xs, tip };
  }

  function renderTrendsChart() {
    const box = $('trChart');
    if (!EX) return;
    const c = trendsSvg(Math.round(box.clientWidth) || 900, EX.daily_trend || [], TR_N[rangeState.trends] || 252);
    box.innerHTML = c.svg;
    if (c.f) V.hover(box, c.f, c.xs, c.tip);
  }

  function periodRows() {
    const ex = EX, cur = ex.current_quarter;
    const base = r => [String(r.trading_days), num(r.avg_fut, 2), num(r.avg_opt, 2), `<strong>${num(r.avg_total, 2)}</strong>`, share(r) === null ? '—' : `${share(r).toFixed(0)}%`];
    if (TR.period === 'year') return { head: ['Year', 'Sessions', 'Futures', 'Options', 'Total', 'Options share', 'vs year before'],
      rows: ex.fy_summary.map(r => [esc(r.fy) + (r.fy === ex.current_fy ? ' <span class="muted">to date</span>' : ''), ...base(r), chg(r.yoy_total)]) };
    if (TR.period === 'month') return { head: ['Month', 'Sessions', 'Futures', 'Options', 'Total', 'Options share', 'vs month before', 'vs 6 months'],
      rows: ex.monthly.map((r, i) => [monthLabel(r) + (i === 0 ? ' <span class="muted">to date</span>' : ''), ...base(r), chg(r.mom_total), chg(r.mo6m_total)]) };
    if (TR.period === 'week') return { head: ['Window', 'Sessions', 'Futures', 'Options', 'Total', 'Options share', 'vs week before', 'vs 10 weeks'],
      rows: ex.weekly.map(r => [esc(String(r.label).replace('Trading Days', 'sessions')), ...base(r), chg(r.wow_total), chg(r.wo10w_total)]) };
    return { head: ['Quarter', 'Sessions', 'Futures', 'Options', 'Total', 'Options share', 'vs quarter before', 'vs a year before'],
      rows: ex.quarterly.map(r => [esc(r.quarter) + (r.quarter === cur ? ' <span class="muted">to date</span>' : ''), ...base(r), chg(r.qoq_total), chg(r.yoy_total)]) };
  }

  function renderPeriod() {
    const box = $('trPeriod');
    if (!EX) return;
    const { head, rows } = periodRows();
    const cap = { quarter: 8, month: 12 }[TR.period];
    const shown = cap && !TR.all ? rows.slice(0, cap) : rows;
    const more = cap && rows.length > cap ? `<button type="button" class="more-btn" id="trMore">${TR.all ? 'Show fewer' : `Show all ${rows.length}`}</button>` : '';
    box.innerHTML = table(head, shown, { after: more });
    document.querySelectorAll('#trSeg button').forEach(b => { b.setAttribute('aria-pressed', String(b.dataset.period === TR.period)); });
    const m = $('trMore'); if (m) m.addEventListener('click', () => { TR.all = !TR.all; renderPeriod(); });
  }

  function renderTrends() {
    const ex = EX;
    const l = trendsLede(ex.quarterly);
    ledeInto('tr', `Revenue trends · data to ${fmt.dayMonth(ex.latest_day.date)} ${ex.latest_day.date.slice(0, 4)}`, l);
    renderTrendsChart();
    renderPeriod();
    $('trTrail').innerHTML = (ex.trailing || []).map(r => `<div class="avg"><div class="avg-label">Last ${r.trading_days} sessions</div>`
      + `<div class="avg-value">${cr(r.avg_total)}<small>Cr</small></div><div class="avg-meta">${chg(r.chg_total, ` vs the ${r.trading_days} before`)}</div>`
      + `<div class="avg-base">Options ${share(r).toFixed(0)}% of revenue</div></div>`).join('');
  }

  function mountTrends() {
    loadEx().then(renderTrends).catch(e => fail($('trChart'), 'Could not load revenue trends: ' + e.message, mountTrends));
  }

  // ════════════════════════════════════════════════════════════════════════
  //  Seasonality
  // ════════════════════════════════════════════════════════════════════════
  function weekdaySvg(w, rows) {
    const compact = w < 520;
    const h = compact ? 220 : 280;
    const vmax = Math.max(...rows.map(r => Math.max(r.cur_total || 0, r.prev_total || 0))) * 1.18;
    const f = V.frame(w, h, vmax, [compact ? 36 : 44, 8, 22, 40]);
    const bw = f.iw / rows.length, bwid = Math.min(bw * 0.5, 80);
    const s = [V.open(w, h, 'Average revenue by weekday this quarter, futures and options stacked, with last quarter marked: '
      + rows.map(r => `${r.day} ₹${num(r.cur_total, 2)} crore (last quarter ₹${num(r.prev_total, 2)})`).join(', '))];
    s.push(V.grid(f, V.niceTicks(vmax, compact ? 3 : 4), v => `₹${num(v, 0)}`));
    rows.forEach((r, i) => {
      const x = f.pl + bw * i + (bw - bwid) / 2, cx = x + bwid / 2, yf = f.y(r.cur_fut);
      s.push(`<g><title>${esc(r.day)}, ${esc(r.cur_q)}: ₹${num(r.cur_total, 2)} Cr (futures ₹${num(r.cur_fut, 2)}, options ₹${num(r.cur_opt, 2)}); ${esc(r.prev_q)} ₹${num(r.prev_total, 2)}</title>`
        + `<rect x="${x.toFixed(1)}" y="${yf.toFixed(1)}" width="${bwid.toFixed(1)}" height="${(f.y(0) - yf).toFixed(1)}" class="c-fut"/>`
        + `<path d="${V.barPath(x, f.y(r.cur_total), yf - 2, bwid, 3)}" class="c-opt"/></g>`);
      if (r.prev_total) { const yp = f.y(r.prev_total); s.push(`<line x1="${(x - 6).toFixed(1)}" x2="${(x + bwid + 6).toFixed(1)}" y1="${yp.toFixed(1)}" y2="${yp.toFixed(1)}" class="c-typical" style="stroke-width:2px"/>`); }
      s.push(V.text(cx, f.y(r.cur_total) - 7, 'c-label', `₹${num(r.cur_total, 2)}`, 'middle', true));
      s.push(V.text(cx, f.y(0) + 18, 'c-label', esc(compact ? String(r.day).slice(0, 3) : r.day), 'middle'));
    });
    s.push('</svg>');
    return s.join('');
  }

  function renderSeason() {
    const ex = EX, q = ex.quarter_dow || [], d = ex.day_of_week || [];
    ledeInto('se', `Revenue by weekday · data to ${fmt.dayMonth(ex.latest_day.date)} ${ex.latest_day.date.slice(0, 4)}`, seasonLede(q, d));
    const box = $('seChart');
    if (q.length) {
      box.innerHTML = weekdaySvg(Math.round(box.clientWidth) || 800, q);
      $('seLegend').innerHTML = `<span><i class="sw sw--fut"></i>Futures, ${esc(q[0].cur_q)}</span><span><i class="sw sw--opt"></i>Options, ${esc(q[0].cur_q)}</span>`
        + `<span><i class="sw sw--typical"></i>${esc(q[0].prev_q)} average</span>`;
    }
    $('seRecent').innerHTML = table(['Weekday', 'Latest', '3-week average', 'Latest vs 3 weeks', '10-week average', 'Latest vs 10 weeks', 'Options share'],
      d.map(r => [esc(r.day), `${cr(r.latest_total)}<small>${fmt.dayMonth(r.latest_date)}</small>`, cr(r.avg3_total), chg(r.var3_total), cr(r.avg10_total), chg(r.var10_total),
                  r.avg10_total ? `${(r.avg10_opt / r.avg10_total * 100).toFixed(0)}%` : '—']));
    if (q.length) {
      $('seQuarterSub').textContent = `Average revenue per session on each weekday: ${q[0].cur_q} so far, ${q[0].prev_q}, and ${q[0].yoy_q} a year earlier`;
      $('seQuarter').innerHTML = table(['Weekday', esc(q[0].cur_q), esc(q[0].prev_q), esc(q[0].yoy_q), `vs ${esc(q[0].prev_q)}`, `vs ${esc(q[0].yoy_q)}`],
        q.map(r => [esc(r.day), `<strong>${cr(r.cur_total)}</strong>`, cr(r.prev_total), cr(r.yoy_total), chg(r.qoq_total), chg(r.yoy_total_pct)]));
    }
  }

  function mountSeason() {
    loadEx().then(renderSeason).catch(e => fail($('seChart'), 'Could not load weekday figures: ' + e.message, mountSeason));
  }

  // ════════════════════════════════════════════════════════════════════════
  //  Commodities
  // ════════════════════════════════════════════════════════════════════════
  const STACK = ['GOLD', 'SILVER', 'CRUDEOIL', 'NATURALGAS', 'COPPER', 'OTHERS'];
  const SECTORS = [['bullion_pct', 'BULLION', 'Bullion'], ['energy_pct', 'ENERGY', 'Energy'], ['base_metals_pct', 'BASE', 'Base metals']];
  const INDICES = [['MCXBULLDEX', 'BULLION', 'Bullion'], ['MCXENRGDEX', 'ENERGY', 'Energy'], ['MCXMETLDEX', 'BASE', 'Base metal'], ['MCXCOMPDEX', 'COMPOSITE', 'Composite']];
  const CM = { dash: null, sig: null, mom: null, ico: null, table: MCX.storage.get('mcx.cmd.table', 'quarter') };
  const dashUrl = () => '/api/commodity_dashboard?range=' + encodeURIComponent(rangeState.cmdTrend || '60D');
  const sigUrl = key => '/api/commodities?view=signals&range=' + encodeURIComponent(rangeState[key] || '60D');
  const icoUrl = () => '/api/commodities?view=icomdex&range=' + encodeURIComponent(rangeState.icomdex || '60D');
  const sw = k => `<i class="sw k-${k}"></i>`;

  function cmdSvg(w, data) {
    const trend = data.daily_trend || [];
    const keys = STACK.filter(k => (data.commodities || []).includes(k));
    if (trend.length < 2) return { svg: '' };
    const compact = w < 520;
    const h = compact ? 240 : 320;
    // Long ranges: 5-session average so the stack reads; short ranges: each day
    const smooth = trend.length > 90 ? 5 : 1;
    const pts = trend.map((t, i) => {
      if (i < smooth - 1) return null;
      const win = trend.slice(i - smooth + 1, i + 1), o = { date: t.date };
      keys.forEach(k => { o[k] = win.reduce((a, x) => a + (x[k] || 0), 0) / smooth; });
      return o;
    }).filter(Boolean);
    const tot = pts.map(p => keys.reduce((a, k) => a + p[k], 0));
    const vmax = Math.max(...tot) * 1.1;
    const f = V.frame(w, h, vmax, [compact ? 36 : 44, compact ? 8 : 118, 14, 30]);
    const X = i => f.pl + f.iw * i / (pts.length - 1);
    const xs = pts.map((_, i) => X(i));
    const st = stackPaths(f, xs, keys.map(k => ({ key: k, vals: pts.map(p => p[k]) })), s => 'k-' + s.key);
    const s = [V.open(w, h, `Revenue per day by commodity${smooth > 1 ? ', 5-session average' : ''}: latest `
      + keys.map(k => `${cname(k)} ₹${num(pts[pts.length - 1][k], 2)} crore`).join(', '))];
    s.push(V.grid(f, V.niceTicks(vmax, compact ? 3 : 5), v => `₹${num(v, 0)}`), st.svg);
    if (!compact) {
      let base = 0;
      const last = pts[pts.length - 1];
      const items = keys.map(k => { const mid = base + last[k] / 2; base += last[k]; return { y: f.y(mid) + 4, s: `${cname(k)} ₹${num(last[k], 2)}`, cls: 'c-label2' }; });
      V.spread(items, 14, f.pt + 4, f.y(0)).forEach(it => s.push(V.text(f.w - f.pr + 12, it.y, it.cls, it.s)));
    }
    s.push(pts.length > 90 ? quarterTicks(pts.map(p => p.date), X, f, h) : dayTicks(pts.map(p => p.date), X, f, h));
    s.push('</svg>');
    const tip = i => `<strong>${fmt.day(pts[i].date)}</strong> · ₹${num(tot[i], 2)} Cr` + keys.slice().reverse()
      .map(k => `<br>${sw(k)} <span class="c-tip-sub">${cname(k)}</span> ₹${num(pts[i][k], 2)}`).join('');
    return { svg: s.join(''), f, xs, tip, smooth };
  }

  function renderCmdChart() {
    const box = $('cmChart');
    if (!CM.dash) return;
    const c = cmdSvg(Math.round(box.clientWidth) || 900, CM.dash);
    box.innerHTML = c.svg;
    if (c.f) V.hover(box, c.f, c.xs, c.tip);
    $('cmChartSub').textContent = `₹ crore per day, ${c.smooth > 1 ? '5-session average, ' : ''}stacked by commodity`;
    $('cmLegend').innerHTML = STACK.filter(k => (CM.dash.commodities || []).includes(k)).map(k => `<span>${sw(k)}${cname(k)}</span>`).join('');
  }

  function renderCmdTables() {
    const d = CM.dash, sm = d.summary_matrix || [];
    $('cmSummary').innerHTML = table(['Commodity', 'Last day', '5-day average', '45-day average', 'This month', 'This quarter', `${esc(d.current_fy)} average`, 'vs a year ago', 'Share'],
      sm.map(r => ({ cls: r.commodity === 'TOTAL' ? 'total' : '', cells: [
        (r.commodity === 'TOTAL' ? '' : sw(r.commodity) + ' ') + cname(r.commodity), cr(r.last_day), cr(r.avg_5d), cr(r.avg_45d), cr(r.avg_month), cr(r.avg_quarter), cr(r.avg_fy),
        chg(r.yoy_pct), r.share_pct === null || r.share_pct === undefined ? '—' : `${num(r.share_pct, 1)}%`] })));
    renderPeriodByCommodity();
  }

  function renderPeriodByCommodity() {
    const d = CM.dash;
    const isQ = CM.table === 'quarter';
    const n = isQ ? ({ '4Q': 4, '8Q': 8 }[rangeState.cmdQtr] || 8) : ({ '3M': 3, '6M': 6, '12M': 12, '24M': 24 }[rangeState.cmdMonth] || 6);
    const periods = (isQ ? d.quarterly : d.monthly || []).slice(-n);
    const lab = p => isQ ? p.quarter : p.month.replace(/^FY\d+ /, '').slice(0, 3) + ' ' + p.month.slice(2, 4);
    const rows = (d.commodities || []).map(k => [sw(k) + ' ' + cname(k), ...periods.map(p => {
      const c = (p.commodities || {})[k] || {};
      return c.avg_rev === undefined ? '—' : `${num(c.avg_rev, 2)}<small>${c.share_pct === null || c.share_pct === undefined ? '' : num(c.share_pct, 0) + '%'}</small>`;
    })]);
    rows.push({ cls: 'total', cells: ['All commodities', ...periods.map(p => num(p.total, 2))] });
    $('cmPeriods').innerHTML = table([isQ ? 'Quarter' : 'Month', ...periods.map(p => esc(lab(p)))], rows);
    document.querySelectorAll('#cmTableSeg button').forEach(b => b.setAttribute('aria-pressed', String(b.dataset.table === CM.table)));
    $('cmQtrRange').hidden = !isQ; $('cmMonthRange').hidden = isQ;
  }

  function renderSignals() {
    const d = CM.sig;
    if (!d) return;
    const t = d.today || {};
    const dq = d.data_quality || {};
    const date = t.date || dq.latest_date;
    $('cmSigSub').textContent = date ? `Turnover and activity on ${fmt.day(date)}; exchange turnover ₹${num(t.exchange_turnover_cr || 0, 0)} Cr across ${(t.commodities || []).length} commodities` : '';
    $('cmSignals').innerHTML = table(['Commodity', 'Sector', 'Turnover ₹ Cr', 'Share', 'Signal', 'Composite z', 'Turnover z', 'Open interest z', 'Volume z'],
      (t.commodities || []).map(c => [cname(c.commodity), esc(String(c.head).charAt(0) + String(c.head).slice(1).toLowerCase()), num(c.turnover_cr || 0, 0),
        `${num((c.weight || 0) * 100, 1)}%`, signalPill(c.signal), z(c.composite_z), z(c.turnover_z), z(c.oi_z), z(c.volume_z)]));
    $('cmMovers').innerHTML = table(['Commodity', 'Sector', 'z before', 'z now', 'Change', 'Signal'],
      (d.top_movers || []).map(m => [cname(m.commodity), esc(String(m.head).charAt(0) + String(m.head).slice(1).toLowerCase()), z(m.prev_z), z(m.curr_z),
        m.delta_z === null || m.delta_z === undefined ? '—' : `<span class="${m.delta_z > 0 ? 'up' : 'down'}">${m.delta_z > 0 ? '▲' : '▼'} ${num(Math.abs(m.delta_z), 2)}</span>`, signalPill(m.signal)]));
    renderRotation();
  }

  function rotationSvg(w, rot) {
    if (rot.length < 2) return { svg: '' };
    const compact = w < 520, h = compact ? 200 : 240;
    const f = V.frame(w, h, 100, [compact ? 36 : 44, compact ? 8 : 110, 10, 30]);
    const X = i => f.pl + f.iw * i / (rot.length - 1);
    const xs = rot.map((_, i) => X(i));
    const series = SECTORS.map(([k, key]) => ({ key, vals: rot.map(r => r[k] || 0) }));
    const other = rot.map(r => Math.max(0, 100 - SECTORS.reduce((a, [k]) => a + (r[k] || 0), 0)));
    if (other.some(v => v > 0.5)) series.push({ key: 'OTHER', vals: other });
    const st = stackPaths(f, xs, series, s => 'k-' + s.key);
    const last = rot[rot.length - 1];
    const s = [V.open(w, h, 'Share of turnover by sector: latest ' + SECTORS.map(([k, , n]) => `${n} ${num(last[k] || 0, 0)}%`).join(', '))];
    s.push(V.grid(f, [0, 25, 50, 75, 100], v => `${v}%`), st.svg);
    if (!compact) {
      let base = 0;
      const items = SECTORS.map(([k, , n]) => { const mid = base + (last[k] || 0) / 2; base += last[k] || 0; return { y: f.y(mid) + 4, s: `${n} ${num(last[k] || 0, 0)}%`, cls: 'c-label2' }; });
      V.spread(items, 14, f.pt + 4, f.y(0)).forEach(it => s.push(V.text(f.w - f.pr + 12, it.y, it.cls, it.s)));
    }
    s.push(rot.length > 90 ? quarterTicks(rot.map(r => r.date), X, f, h) : dayTicks(rot.map(r => r.date), X, f, h));
    s.push('</svg>');
    const tip = i => `<strong>${fmt.day(rot[i].date)}</strong>` + SECTORS.map(([k, key, n]) => `<br>${sw(key)} <span class="c-tip-sub">${n}</span> ${num(rot[i][k] || 0, 1)}%`).join('');
    return { svg: s.join(''), f, xs, tip };
  }

  function renderRotation() {
    const box = $('cmRotation');
    if (!CM.sig) return;
    const c = rotationSvg(Math.round(box.clientWidth) || 800, CM.sig.sector_rotation || []);
    box.innerHTML = c.svg;
    if (c.f) V.hover(box, c.f, c.xs, c.tip);
  }

  function renderMomentum() {
    const d = CM.mom;
    if (!d) return;
    $('cmMomentum').innerHTML = table(['Commodity', 'Sector', 'Average composite z', 'Days above zero', 'Latest z', 'Signal'],
      (d.commodity_momentum || []).map(m => [cname(m.commodity), esc(String(m.head).charAt(0) + String(m.head).slice(1).toLowerCase()), z(m.avg_composite_z),
        `${num(m.positive_day_pct, 0)}%`, z(m.latest_z), signalPill(m.signal)]));
  }

  function icoSvg(w, series) {
    const dates = series.dates || [];
    if (dates.length < 2) return { svg: '' };
    const compact = w < 520, h = compact ? 200 : 240;
    const lines = INDICES.map(([code, key, name]) => {
      const col = series[code] || [];
      const base = col.find(v => v !== null && v > 0);
      return base ? { key, name, vals: col.map(v => (v !== null && v > 0 ? v / base * 100 : null)) } : null;
    }).filter(Boolean);
    const all = lines.flatMap(l => l.vals.filter(v => v !== null));
    const lo = Math.floor(Math.min(...all) / 5) * 5, hi = Math.ceil(Math.max(...all) / 5) * 5;
    const f = V.frame(w, h, hi, [compact ? 36 : 44, compact ? 8 : 110, 10, 30], lo);
    const X = i => f.pl + f.iw * i / (dates.length - 1);
    const s = [V.open(w, h, 'MCX iCOMDEX indices rebased to 100 at the start of the range: latest '
      + lines.map(l => `${l.name} ${num(l.vals.filter(v => v !== null).slice(-1)[0], 1)}`).join(', '))];
    s.push(V.grid(f, V.niceTicks(hi, compact ? 3 : 4, lo), v => num(v, 0)));
    const y100 = f.y(100);
    s.push(`<line x1="${f.pl}" x2="${f.w - f.pr}" y1="${y100.toFixed(1)}" y2="${y100.toFixed(1)}" class="c-typical"/>`);
    lines.forEach(l => {
      const d = l.vals.map((v, i) => (v === null ? null : `${X(i).toFixed(1)},${f.y(v).toFixed(1)}`)).filter(Boolean);
      s.push(`<path d="M${d.join('L')}" class="ks-${l.key}"/>`);
    });
    if (!compact) {
      const items = lines.map(l => { const v = l.vals.filter(x => x !== null).slice(-1)[0]; return { y: f.y(v) + 4, s: `${l.name} ${num(v, 1)}`, cls: l.key === 'COMPOSITE' ? 'c-label' : 'c-label2' }; });
      V.spread(items, 14, f.pt + 4, f.h - f.pb).forEach(it => s.push(V.text(f.w - f.pr + 12, it.y, it.cls, it.s)));
    }
    s.push(dates.length > 90 ? quarterTicks(dates, X, f, h) : dayTicks(dates, X, f, h));
    s.push('</svg>');
    const xs = dates.map((_, i) => X(i));
    const tip = i => `<strong>${fmt.day(dates[i])}</strong>` + lines.map(l => `<br>${sw(l.key)} <span class="c-tip-sub">${l.name}</span> ${l.vals[i] === null ? '—' : num(l.vals[i], 1)}`).join('');
    return { svg: s.join(''), f, xs, tip };
  }

  function renderIco() {
    const d = CM.ico;
    if (!d) return;
    const box = $('cmIco');
    const c = icoSvg(Math.round(box.clientWidth) || 800, d.series || {});
    box.innerHTML = c.svg;
    if (c.f) V.hover(box, c.f, c.xs, c.tip);
    $('cmIcoLegend').innerHTML = INDICES.map(([, key, name]) => `<span>${sw(key)}${name}</span>`).join('') + '<span><i class="sw sw--typical"></i>Start of range = 100</span>';
    $('cmIcoTable').innerHTML = table(['Index', 'Level', '1 day', 'Over the range', 'Date'],
      (d.latest || []).map(x => [`${esc(String(x.name || x.code).replace('MCX iCOMDEX ', ''))} <span class="muted">${esc(x.code)}</span>`,
        x.close === null || x.close === undefined ? '—' : num(x.close, 2), chg2(x.change_pct), chg2(x.range_change_pct), x.date ? fmt.dayMonth(x.date) : '—']));
  }

  function loadDash() {
    fetchRanged(dashUrl()).then(d => {
      if (!d.success) throw new Error(d.error || 'No data');
      CM.dash = d;
      const lastDay = d.daily_trend && d.daily_trend.length ? d.daily_trend[d.daily_trend.length - 1].date : null;
      $('cmKicker').textContent = `Revenue by commodity · data to ${lastDay ? fmt.dayMonth(lastDay) + ' ' + lastDay.slice(0, 4) : '—'}`;
      const l = cmdLede(d.summary_matrix);
      $('cmHead').textContent = l ? l.head : 'No commodity data yet.';
      $('cmDeck').textContent = l ? l.deck : '';
      renderCmdChart(); renderCmdTables();
    }).catch(e => fail($('cmChart'), 'Could not load revenue by commodity: ' + e.message, loadDash));
  }
  function loadSig() {
    fetchRanged(sigUrl('sectorRot')).then(d => { if (!d.success) throw new Error(d.error || 'No data'); CM.sig = d; renderSignals(); })
      .catch(e => fail($('cmSignals'), 'Could not load activity signals: ' + e.message, loadSig));
  }
  function loadMom() {
    fetchRanged(sigUrl('cmdMomentum')).then(d => { if (!d.success) throw new Error(d.error || 'No data'); CM.mom = d; renderMomentum(); })
      .catch(e => fail($('cmMomentum'), 'Could not load momentum: ' + e.message, loadMom));
  }
  function loadIco() {
    fetchRanged(icoUrl()).then(d => { if (!d.success) throw new Error(d.error || 'No data'); CM.ico = d; renderIco(); })
      .catch(e => fail($('cmIco'), 'Could not load iCOMDEX levels: ' + e.message, loadIco));
  }
  function mountCmd() { loadDash(); loadSig(); loadMom(); loadIco(); }

  // ════════════════════════════════════════════════════════════════════════
  //  Wiring
  // ════════════════════════════════════════════════════════════════════════
  makeRangeToggle({ key: 'trends', containerId: 'trRange', ranges: ['Q', '1Y', '2Y', 'Max'], defaultRange: '1Y', labelIds: [],
                    onChange: () => { renderTrendsChart(); $('trRangeLabel').textContent = { Q: 'last quarter', '1Y': 'last 12 months', '2Y': 'last 2 years', Max: 'full history' }[rangeState.trends]; } });
  $('trRangeLabel').textContent = { Q: 'last quarter', '1Y': 'last 12 months', '2Y': 'last 2 years', Max: 'full history' }[rangeState.trends];
  $('trSeg').addEventListener('click', e => {
    const b = e.target.closest('button[data-period]');
    if (!b || b.dataset.period === TR.period) return;
    TR.period = b.dataset.period; TR.all = false;
    MCX.storage.set('mcx.trends.period', TR.period);
    renderPeriod();
  });
  makeRangeToggle({ key: 'cmdTrend', containerId: 'cmRange', ranges: ['30D', '60D', 'Q', '1Y', '2Y'], defaultRange: '60D', labelIds: ['cmRangeLabel'], onChange: loadDash });
  makeRangeToggle({ key: 'cmdQtr', containerId: 'cmQtrRange', ranges: ['4Q', '8Q'], defaultRange: '8Q', labelIds: [], onChange: () => CM.dash && renderPeriodByCommodity() });
  makeRangeToggle({ key: 'cmdMonth', containerId: 'cmMonthRange', ranges: ['3M', '6M', '12M', '24M'], defaultRange: '6M', labelIds: [], onChange: () => CM.dash && renderPeriodByCommodity() });
  makeRangeToggle({ key: 'sectorRot', containerId: 'cmRotRange', ranges: ['30D', '60D', 'Q', '1Y', '2Y'], defaultRange: '60D', labelIds: ['cmRotRangeLabel'], onChange: loadSig });
  makeRangeToggle({ key: 'cmdMomentum', containerId: 'cmMomRange', ranges: ['30D', '60D', 'Q', '1Y', '2Y'], defaultRange: '60D', labelIds: ['cmMomRangeLabel'], onChange: loadMom });
  makeRangeToggle({ key: 'icomdex', containerId: 'cmIcoRange', ranges: ['30D', '60D', 'Q', '1Y', '2Y'], defaultRange: '60D', labelIds: ['cmIcoRangeLabel'], onChange: loadIco });
  $('cmTableSeg').addEventListener('click', e => {
    const b = e.target.closest('button[data-table]');
    if (!b || b.dataset.table === CM.table) return;
    CM.table = b.dataset.table;
    MCX.storage.set('mcx.cmd.table', CM.table);
    renderPeriodByCommodity();
  });
  onResize($('trends'), () => EX && renderTrendsChart());
  onResize($('season'), () => EX && renderSeason());
  onResize($('commodities'), () => { renderCmdChart(); renderRotation(); renderIco(); });

  MCX.revenue = { trends: { mount: mountTrends }, season: { mount: mountSeason }, cmd: { mount: mountCmd } };
})();
