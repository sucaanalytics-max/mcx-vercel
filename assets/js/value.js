/* MCX Revenue Monitor: Earnings & value pages — Quarter P&L (this file), Fair value, Scenarios.
   Data: /api/quarterly (reported quarters, the current quarter's projection on both revenue
   bases, and a walk-forward backtest of each basis). Charts are SVG coloured by CSS variables. */
(function () {
  'use strict';
  const fmt = MCX.fmt, num = fmt.num, V = MCX.svg, esc = V.esc;

  // ════════════════════════════════════════════════════════════════════════
  //  Model: pure functions (scripts/test_value_js.js)
  // ════════════════════════════════════════════════════════════════════════
  const WORDS = ['no', 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten'];
  const words = n => (n >= 0 && n < WORDS.length ? WORDS[n] : String(n));
  const cr1 = v => `₹${num(v, 1)} Cr`;

  // How many past quarters the basis landed above / below reported profit
  function missStory(rows, key) {
    const n = rows.length, below = rows.filter(r => r[key] < 0).length, above = n - below;
    return { n, below, above };
  }

  // Headline and deck for one revenue basis
  function quarterLede(d, basis) {
    const cq = d.current_quarter, bt = d.backtest, nf = d.non_fo;
    const acts = d.actuals || [];
    const prev = acts[acts.length - 1];
    const all = basis === 'all' && nf;
    const pat = all ? nf.pat_projected_cr : cq.pat_projected_cr;
    const qShort = cq.quarter.split(' ')[0];
    const left = cq.trading_days_total - cq.trading_days_elapsed;
    const togo = left <= 0 ? 'with the quarter’s trading complete'
      : `with ${words(left)} session${left === 1 ? '' : 's'} to go${cq.today_in_remaining ? ', including today' : ''}`;
    const head = all ? `Including non-F&O revenue, the model projects ${qShort} profit of ${cr1(pat)}, ${togo}.`
                     : `On F&O revenue alone, the model projects ${qShort} profit of ${cr1(pat)}, ${togo}.`;
    const parts = [];
    if (all) parts.push(`Non-F&O revenue is estimated at ${cr1(nf.estimate_cr)}, ${num(nf.share * 100, 1)}% of F&O revenue as in ${nf.share_from}.`);
    if (prev && prev.pat_cr) {
      const q = (pat / prev.pat_cr - 1) * 100;
      parts.push(`That would ${all ? 'put profit' : 'be'} ${Math.abs(q).toFixed(0)}% ${q >= 0 ? 'above' : 'below'} ${prev.quarter.split(' ')[0]}’s reported ₹${num(prev.pat_cr, 0)} Cr.`);
    }
    if (bt && bt.rows && bt.rows.length) {
      const key = all ? 'miss_all_cr' : 'miss_fo_cr';
      const m = missStory(bt.rows, key);
      const avg = all ? bt.abs_miss_all_avg_cr : bt.abs_miss_fo_avg_cr;
      if (!all && m.below === m.n) parts.push(`This basis leaves out non-F&O revenue, and it has come in below reported profit in each of the last ${words(m.n)} quarters, by ₹${num(avg, 0)} Cr on average.`);
      else if (!all) parts.push(`This basis leaves out non-F&O revenue; over the last ${words(m.n)} quarters it missed reported profit by ₹${num(avg, 0)} Cr on average, landing below it in ${words(m.below)}.`);
      else parts.push(`On this basis the model has missed reported profit by ₹${num(avg, 0)} Cr on average over the last ${words(m.n)} quarters, landing above it in ${words(m.above)} of them.`);
    }
    return { head, deck: parts.join(' ') };
  }

  // Both bases, adjusted for their average past miss
  function adjusted(d) {
    const bt = d.backtest, nf = d.non_fo;
    if (!bt || !nf || bt.miss_fo_avg_cr === null) return null;
    const a = d.current_quarter.pat_projected_cr - bt.miss_fo_avg_cr, b = nf.pat_projected_cr - bt.miss_all_avg_cr;
    return { fo: a, all: b, close: Math.abs(a - b) / ((a + b) / 2) < 0.05 };
  }

  // Data-driven signal from a price and the P/E band (same rules as api/valuation.py)
  function ddSignal(price, fv) {
    if (!price || !fv || !fv.base) return 'NO_DATA';
    if (price < fv.bear) return 'DEEP_VALUE';
    if (price < fv.base * 0.95) return 'UNDERVALUED';
    if (price <= fv.base * 1.05) return 'FAIR';
    if (price <= fv.bull) return 'OVERVALUED';
    return 'STRETCHED';
  }
  // The Tusk sheet, line by line (mirrors house_calc in lib/house_model.py).
  // inp: adr_fy28, days_fy28, non_fo_fy26, other_income_fy26, growth_fy27, growth_fy28, margin,
  // pe {bear, base, bull}, disc_fy28, disc_today, method ('fixed' | 'prorata'). todayIso: the valuation date.
  function houseCalc(inp, shares, todayIso, fy27End = '2027-03-31') {
    const grow = (1 + inp.growth_fy27) * (1 + inp.growth_fy28);
    const op = inp.adr_fy28 * inp.days_fy28, nonFo = inp.non_fo_fy26 * grow, other = inp.other_income_fy26 * grow;
    const total = op + nonFo + other, pat = total * inp.margin, eps = pat / shares;
    const daysLeft = Math.max(Math.round((Date.parse(fy27End) - Date.parse(todayIso)) / 86400000), 0);
    const prorata = inp.disc_fy28 * daysLeft / 365;
    const step2 = inp.method === 'prorata' ? prorata : inp.disc_today;
    const map = f => ({ bear: f(inp.pe.bear), base: f(inp.pe.base), bull: f(inp.pe.bull) });
    const fy28 = map(pe => pe * eps), fy27 = map(pe => pe * eps / (1 + inp.disc_fy28));
    const today = map(pe => pe * eps / (1 + inp.disc_fy28) / (1 + step2));
    return { op, nonFo, other, total, pat, eps, fy28, fy27, today, step2, daysLeft, prorata };
  }
  // Daily revenue the price implies at the data-driven multiple
  function revenuePricedIn(price, pe, c) {
    return ((price / pe) * c.diluted_shares_cr / c.pat_margin - c.non_fo_rev_cr) / c.trading_days;
  }
  // FY28 revenue per day at which the base case equals today's price
  function breakevenAdr(price, inp, shares, c) {
    const eps = price * (1 + inp.disc_fy28) * (1 + c.step2) / inp.pe.base;
    return (eps * shares / inp.margin - c.nonFo - c.other) / inp.days_fy28;
  }
  // Where the price sits against the house cases: within 5% of the base counts as near it
  function houseState(price, t) {
    if (Math.abs(price / t.base - 1) <= 0.05) return 'near';
    if (price < t.bear) return 'deep';
    if (price < t.base) return 'under';
    return price <= t.bull ? 'over' : 'stretched';
  }
  const HOUSE_STATE = { near: ['', 'Near the base case'], deep: ['strong-up', 'Below the bear case'], under: ['up', 'Below the base case'],
                        over: ['down', 'Above the base case'], stretched: ['strong-down', 'Above the bull case'] };
  function fvLede(price, t, inp, v) {
    const fv = v.snapshot.fair_value, pe = v.pe_bands.mean, ma45 = v.snapshot.eps_chain.ma45_rev_cr;
    const st = houseState(price, t);
    const gap = (t.base / price - 1) * 100, prem = (price / fv.base - 1) * 100;
    const r = x => `₹${num(x, 0)}`;
    const house = st === 'near' ? `values MCX at ${r(t.base)} a share, close to the price`
      : `values MCX at ${r(t.base)} a share, ${Math.round(Math.abs(gap))}% ${gap > 0 ? 'above' : 'below'} the price`;
    const dd = Math.abs(prem) < 5 ? 'the data-driven view has it near fair value' : `the data-driven view has it about ${Math.round(Math.abs(prem))}% ${prem > 0 ? 'overvalued' : 'undervalued'}`;
    const cs = k => `${k} case (${r(t[k])} at ${inp.pe[k]}×)`;
    const where = { near: `close to the house ${cs('base')}`, deep: `below even the house ${cs('bear')}`,
                    under: `between the house ${cs('bear')} and ${cs('base')}`, over: `between the house ${cs('base')} and ${cs('bull')}`,
                    stretched: `above even the house ${cs('bull')}` }[st];
    const deck = `At ${r(price)}, the price sits ${where}, and ${Math.abs(prem).toFixed(0)}% ${prem >= 0 ? 'above' : 'below'} the data-driven base of ${r(fv.base)}. `
      + `The house case rests on ₹${num(inp.adr_fy28, 2)} Cr a day in FY28${ma45 ? `, against ₹${num(ma45, 2)} Cr over the last 45 days,` : ''} and on ${inp.pe.bear}–${inp.pe.bull}× FY28 earnings, against the stock’s own median of ${num(pe, 1)}× run-rate earnings.`;
    return { head: `The Tusk house model ${house}; ${dd}.`, deck, state: st, prem };
  }

  // Trailing twelve months: the last four reported quarters' profit over diluted shares
  function ttm(actuals, shares) {
    if (!actuals || actuals.length < 4 || !shares) return null;
    const last = actuals.slice(-4);
    return { eps: last.reduce((a, q) => a + q.pat_cr, 0) / shares, from: last[0].quarter, to: last[3].quarter };
  }

  // ── Scenarios (moved from legacy.js, same arithmetic) ────────────────────
  // One year at a revenue per day: revenue − costs = EBITDA, + other income = PBT, − tax = PAT,
  // ÷ shares = EPS, × P/E = price. a: { days, opex, other, tax (%), shares }
  function yearModel(dailyRev, pe, a) {
    const annualRev = dailyRev * a.days, ebitda = annualRev - a.opex, pbt = ebitda + a.other;
    const tax = pbt > 0 ? pbt * a.tax / 100 : 0, pat = pbt > 0 ? pbt - tax : 0;
    const eps = a.shares > 0 ? pat / a.shares : 0, price = eps * pe;
    return { annualRev, ebitda, pbt, tax, pat, eps, price, mcap: price * a.shares };
  }
  // What the price implies: the revenue per day at your P/E, and the P/E at your revenue per day
  function impliedBy(price, dailyRev, pe, a) {
    const eps = pe > 0 ? price / pe : 0, pat = eps * a.shares, pbt = a.tax < 100 ? pat / (1 - a.tax / 100) : 0;
    const annualRev = pbt - a.other + a.opex, m = yearModel(dailyRev, pe, a);
    return { eps, pat, pbt, annualRev, rev: a.days > 0 ? annualRev / a.days : 0, pe: m.eps > 0 ? price / m.eps : null };
  }
  // A year in the FY table: revenue per day × sessions + other revenue, × margin = PAT, ÷ shares, × P/E
  function trendRow(r, shares) {
    const op = r.adr * r.days, tot = op + r.other, pat = tot * r.margin / 100, eps = shares > 0 ? pat / shares : 0;
    return { op, tot, pat, eps, px: eps * r.pe };
  }
  // An FY27 case: revenue per day grown by `growth`, × FY27 sessions + other income, × margin → EPS → price,
  // and that price discounted by `disc` %
  function caseRow(c, days, shares, disc, cmp) {
    const tot = c.adr * (1 + c.growth / 100) * days + c.other, pat = tot * c.margin / 100, eps = shares > 0 ? pat / shares : 0, px = eps * c.pe;
    return { tot, pat, eps, px, upside: cmp > 0 ? (px / cmp - 1) * 100 : null, target: px / (1 + disc / 100) };
  }

  MCX.valueModel = { yearModel, impliedBy, trendRow, caseRow, quarterLede, adjusted, missStory, words, ddSignal, houseCalc, breakevenAdr, houseState, HOUSE_STATE, revenuePricedIn, fvLede, ttm };
  if (window.MCX_TEST) return;

  // ════════════════════════════════════════════════════════════════════════
  //  Shared
  // ════════════════════════════════════════════════════════════════════════
  const $ = id => document.getElementById(id);
  const INFO = '<svg aria-hidden="true" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M12 11v5"/><path d="M12 8h.01"/></svg>';
  const sgn = (v, dp = 0) => (v === null || v === undefined ? '—' : `${v > 0 ? '+' : v < 0 ? '−' : ''}₹${num(Math.abs(v), dp)}`);
  function stat(label, value, sub, lead) {
    return `<div class="stat${lead ? ' stat--lead' : ''}"><div class="stat-label">${label}</div><div class="stat-value">${value}</div>${sub ? `<div class="stat-sub">${sub}</div>` : ''}</div>`;
  }
  function table(head, rows, cls) {
    return `<div class="table-scroll"><table class="v2-table${cls ? ' ' + cls : ''}"><thead><tr>${head.map(h => `<th scope="col">${h}</th>`).join('')}</tr></thead><tbody>`
      + rows.map(r => `<tr${r.cls ? ` class="${r.cls}"` : ''}>${(r.cells || r).map(c => `<td>${c}</td>`).join('')}</tr>`).join('') + '</tbody></table></div>';
  }
  function onResize(el, fn) {
    if (!window.ResizeObserver) return;
    let w = 0, queued = false;
    new ResizeObserver(() => {
      if (queued) return; queued = true;
      requestAnimationFrame(() => { queued = false; if (Math.abs(el.clientWidth - w) > 2) { w = el.clientWidth; fn(); } });
    }).observe(el);
  }

  // ════════════════════════════════════════════════════════════════════════
  //  Quarter P&L
  // ════════════════════════════════════════════════════════════════════════
  const QS = { data: null, at: 0, basis: MCX.storage.get('mcx.quarter.basis', 'fo') };

  // Cumulative revenue this quarter, the projection to the end, and (all basis) the non-F&O block
  function buildupSvg(w, d, all) {
    const cq = d.current_quarter, nf = d.non_fo, ds = cq.daily_series || [];
    const nt = Math.max(cq.trading_days_total, ds.length, 1);
    const compact = w < 520, h = compact ? 240 : 320;
    const hiRev = all && nf ? nf.revenue_high_cr : cq.revenue_high_cr;
    const vmax = hiRev * 1.08;
    const f = V.frame(w, h, vmax, [compact ? 40 : 48, compact ? 8 : 132, 16, 30]);
    const X = i => f.pl + f.iw * (nt === 1 ? 0 : i / (nt - 1));
    const s = [V.open(w, h, `Cumulative ${cq.quarter} revenue: ₹${num(cq.revenue_actual_cr, 1)} crore booked, ₹${num(cq.revenue_projected_cr, 1)} crore F&O projected`
      + (all && nf ? `, ₹${num(nf.revenue_projected_cr, 1)} crore including non-F&O` : '')), `<defs>${V.hatch('qh')}</defs>`];
    s.push(V.grid(f, V.niceTicks(vmax, compact ? 3 : 4), v => `₹${num(v, 0)}`));
    const endX = X(nt - 1);
    // Range of the projection at the quarter's end (grey band = range)
    const lo = all && nf ? nf.revenue_low_cr : cq.revenue_low_cr, hi = hiRev;
    s.push(`<rect x="${(endX - 7).toFixed(1)}" y="${f.y(hi).toFixed(1)}" width="14" height="${(f.y(lo) - f.y(hi)).toFixed(1)}" rx="3" class="c-band"/>`);
    if (ds.length) {
      const i0 = ds.length - 1, last = ds[i0].cumul_cr;
      s.push(`<line x1="${X(i0).toFixed(1)}" y1="${f.y(last).toFixed(1)}" x2="${endX.toFixed(1)}" y2="${f.y(cq.revenue_projected_cr).toFixed(1)}" class="c-line" style="stroke-dasharray:2 4;stroke-linecap:round"/>`);
      s.push(`<path d="M${ds.map((p, i) => `${X(i).toFixed(1)},${f.y(p.cumul_cr).toFixed(1)}`).join('L')}" class="c-line" style="stroke-width:2.5px"/>`);
      s.push(`<circle cx="${X(i0).toFixed(1)}" cy="${f.y(last).toFixed(1)}" r="4.5" class="c-dot c-dot--now"/>`);
      if (!compact) s.push(V.text(X(i0) - 8, f.y(last) - 10, 'c-label', `Booked ₹${num(last, 1)}`, 'end', true));
    }
    if (all && nf) {
      const yb = f.y(cq.revenue_projected_cr), yt = f.y(nf.revenue_projected_cr);
      s.push(`<rect x="${(endX - 7).toFixed(1)}" y="${yt.toFixed(1)}" width="14" height="${(yb - yt).toFixed(1)}" fill="url(#qh)" class="c-proj"/>`);
    }
    if (!compact) {
      const xe = f.w - f.pr + 16, items = [];
      if (all && nf) {
        items.push({ y: f.y(nf.revenue_projected_cr) + 4, s: `Total ₹${num(nf.revenue_projected_cr, 0)}`, cls: 'c-label' });
        items.push({ y: f.y(nf.revenue_projected_cr) + 19, s: `incl. non-F&O ₹${num(nf.estimate_cr, 0)}`, cls: 'c-label2' });
        items.push({ y: f.y(cq.revenue_projected_cr) + 18, s: `F&O ₹${num(cq.revenue_projected_cr, 0)}`, cls: 'c-label2' });
      } else {
        items.push({ y: f.y(cq.revenue_projected_cr) + 4, s: `F&O ₹${num(cq.revenue_projected_cr, 0)}`, cls: 'c-label' });
        items.push({ y: f.y(cq.revenue_projected_cr) + 19, s: 'projected', cls: 'c-label2' });
      }
      V.spread(items, 14, f.pt + 4, f.y(0)).forEach(it => s.push(V.text(xe, it.y, it.cls, it.s)));
    }
    // Month starts along the bottom, and the quarter's end
    let lastM = '';
    ds.forEach((p, i) => { const m = p.date.slice(0, 7); if (m !== lastM) { lastM = m; s.push(V.text(X(i), h - 8, 'c-tick', fmt.dayMonth(p.date), i === 0 ? 'start' : 'middle')); } });
    s.push(V.text(endX, h - 8, 'c-tick', 'Quarter end', 'end'));
    s.push('</svg>');
    return s.join('');
  }

  // Revenue and profit for the last seven reported quarters, then this quarter's model (hatched)
  function panelSvg(w, labels, vals, uid, title) {
    const h = 170, n = vals.length;
    const vmax = Math.max(...vals.filter(v => v !== null && v !== undefined)) * 1.2;
    const f = V.frame(w, h, vmax, [4, 4, 22, 24]);
    const bw = f.iw / n, wid = Math.min(bw * 0.55, 40);
    const s = [V.open(w, h, `${title} by quarter: ` + labels.map((l, i) => `${l} ${vals[i] === null ? 'n/a' : '₹' + num(vals[i], 0) + ' crore'}`).join(', ')), `<defs>${V.hatch(uid)}</defs>`];
    s.push(`<line x1="${f.pl}" x2="${f.w - f.pr}" y1="${f.y(0).toFixed(1)}" y2="${f.y(0).toFixed(1)}" class="c-axis"/>`);
    vals.forEach((v, i) => {
      const cx = f.pl + bw * i + bw / 2, x0 = cx - wid / 2, proj = i === n - 1;
      if (v !== null && v !== undefined) {
        s.push(`<path d="${V.barPath(x0, f.y(v), f.y(0), wid, 3)}" ${proj ? `fill="url(#${uid})" class="c-proj"` : 'class="c-bar"'}><title>${esc(labels[i])}: ₹${num(v, 1)} Cr${proj ? ' (model)' : ''}</title></path>`);
        s.push(V.text(cx, f.y(v) - 6, proj ? 'c-label' : 'c-label2', num(v, 0), 'middle'));
      } else {
        s.push(V.text(cx, f.y(0) - 6, 'c-label3', 'n/a', 'middle'));
      }
      s.push(V.text(cx, h - 6, 'c-tick', esc(labels[i]), 'middle'));
    });
    s.push('</svg>');
    return s.join('');
  }

  function renderQuarter() {
    const d = QS.data;
    if (!d) return;
    const cq = d.current_quarter, nf = d.non_fo, bt = d.backtest, em = d.expense_model;
    const all = QS.basis === 'all' && !!nf;
    document.querySelectorAll('#qpBasis button').forEach(b => b.setAttribute('aria-pressed', String(b.dataset.basis === (all ? 'all' : 'fo'))));
    $('qpBasisAllSub').textContent = nf ? `Adds ${cr1(nf.estimate_cr)} estimated` : 'Estimate unavailable';
    $('qpBasis').querySelector('[data-basis="all"]').disabled = !nf;

    const l = quarterLede(d, all ? 'all' : 'fo');
    $('qpKicker').textContent = `${cq.quarter} · ${all ? 'F&O plus estimated non-F&O revenue' : 'F&O transaction revenue only'} · data to ${cq.rows_through ? fmt.dayMonth(cq.rows_through) : '—'}, ${cq.trading_days_elapsed} of ${cq.trading_days_total} sessions`;
    $('qpHead').textContent = l.head;
    $('qpDeck').textContent = l.deck;

    const pat = all ? nf.pat_projected_cr : cq.pat_projected_cr;
    const miss = bt ? (all ? bt.miss_all_avg_cr : bt.miss_fo_avg_cr) : null;
    $('qpStats').innerHTML =
      stat('Profit after tax, model', `₹${num(pat, 1)}<small>Cr</small>`,
           `Model range ₹${num(all ? nf.pat_low_cr : cq.pat_low_cr, 0)} – ${num(all ? nf.pat_high_cr : cq.pat_high_cr, 0)} Cr · margin ${num(all ? nf.pat_margin_pct : cq.pat_margin_pct, 1)}%`, true)
      + stat('Revenue, projected', `₹${num(all ? nf.revenue_projected_cr : cq.revenue_projected_cr, 1)}<small>Cr</small>`,
             all ? `F&O ₹${num(cq.revenue_projected_cr, 1)} + non-F&O ₹${num(nf.estimate_cr, 1)} Cr` : `F&O only · ₹${num(cq.revenue_actual_cr, 1)} Cr booked`)
      + stat('Expenses, model', `₹${num(all ? nf.expenses_projected_cr : cq.expenses_projected_cr, 1)}<small>Cr</small>`,
             `₹${num(em.fixed_cr, 1)} Cr fixed + ${num(em.variable_pct, 1)}% of revenue${em.seasonal_adj_cr ? ` + ₹${em.seasonal_adj_cr} Cr (Q4)` : ''}`)
      + stat('Past miss on this basis', miss === null ? '—' : `${sgn(miss, 0)}<small>Cr</small>`, bt && bt.rows.length ? `Average over the last ${words(bt.rows.length)} quarters, walk-forward` : '');

    const box = $('qpChart');
    box.innerHTML = buildupSvg(Math.round(box.clientWidth) || 800, d, all);
    $('qpChartCap').innerHTML = `<strong>Revenue build-up</strong> · cumulative F&amp;O revenue, ₹ crore${all ? ', with estimated non-F&amp;O revenue added at the end' : ''}; the grey bar is the model’s range for the quarter`;

    // Quarter by quarter
    const acts = (d.actuals || []).slice(-7);
    const labels = acts.map(a => a.quarter).concat(cq.quarter);
    const revVals = acts.map(a => (all ? a.revenue_cr : a.fo_revenue_cr)).concat(all ? nf.revenue_projected_cr : cq.revenue_projected_cr);
    const patVals = acts.map(a => a.pat_cr).concat(pat);
    const pw = Math.round(($('qpPanels').clientWidth - 48) / 2) || 500;
    const narrow = $('qpPanels').clientWidth < 700;
    const w2 = narrow ? $('qpPanels').clientWidth : pw;
    $('qpPanels').innerHTML =
      `<div><div class="panel-title">${all ? 'Revenue' : 'F&amp;O transaction revenue'} <span class="muted">₹ crore</span></div>${panelSvg(w2, labels, revVals, 'qpr', all ? 'Revenue' : 'F&O revenue')}</div>`
      + `<div><div class="panel-title">Profit after tax <span class="muted">₹ crore</span></div>${panelSvg(w2, labels, patVals, 'qpp', 'Profit after tax')}</div>`;
    $('qpPanelsSub').textContent = all ? 'Like for like: reported revenue for every quarter; the hatched bars are this quarter’s model'
                                       : 'Like for like: F&O revenue for every quarter; the hatched bars are this quarter’s model';

    // Year so far
    const fy = d.fy_projection;
    if (fy) {
      const parts = fy.quarters_actual.map(q => `${esc(q.quarter)} reported ₹${num(q.pat_cr, 0)} Cr`);
      if (fy.projected_quarter) parts.push(`${esc(fy.projected_quarter.quarter)} projected ₹${num(fy.projected_quarter.pat_cr, 1)} Cr (F&amp;O only)`);
      $('qpYear').innerHTML = `<div class="qtr-line"><span><strong>₹${num(fy.fy_pat_cr, 1)} Cr</strong> <span class="muted">profit after tax in ${esc(fy.fy)} ${fy.is_complete ? '' : `so far, ${fy.quarters_actual.length + (fy.projected_quarter ? 1 : 0)} of 4 quarters`}</span></span>`
        + `<span class="muted">EPS ₹${num(fy.fy_eps, 2)} on ${num(fy.diluted_shares_cr, 3)} Cr shares</span></div><p class="note">${parts.join(' · ')}.</p>`;
    }

    // How each basis has tracked reported profit
    if (bt && bt.rows.length) {
      const rows = bt.rows.map(r => [esc(r.quarter), num(r.fo_revenue_cr, 0), num(r.reported_revenue_cr, 0),
        `${num(r.non_fo_cr, 0)} <span class="muted">(${num(r.non_fo_cr / r.fo_revenue_cr * 100, 1)}%)</span>`, num(r.reported_pat_cr, 0),
        `${num(r.pat_fo_cr, 0)} <span class="muted">${sgn(r.miss_fo_cr, 0)}</span>`, `${num(r.pat_all_cr, 0)} <span class="muted">${sgn(r.miss_all_cr, 0)}</span>`]);
      rows.push({ cls: 'total', cells: ['Average miss', '', '', '', '', sgn(bt.miss_fo_avg_cr, 0), sgn(bt.miss_all_avg_cr, 0)] });
      $('qpBacktest').innerHTML = table(['Quarter', 'F&amp;O revenue', 'Reported revenue', 'Non-F&amp;O (share)', 'Reported PAT', 'F&amp;O-only model (miss)', 'Incl. non-F&amp;O model (miss)'], rows);
      const fo = missStory(bt.rows, 'miss_fo_cr'), al = missStory(bt.rows, 'miss_all_cr'), adj = adjusted(d);
      let p = `F&amp;O-only came in below reported profit in ${fo.below === fo.n ? 'every quarter' : `${words(fo.below)} of ${words(fo.n)} quarters`}; including non-F&amp;O came in above it in ${words(al.above)} of ${words(al.n)}, `
        + `with an average miss of ₹${num(bt.abs_miss_all_avg_cr, 0)} Cr against ₹${num(bt.abs_miss_fo_avg_cr, 0)} Cr.`;
      if (adj) p += adj.close ? ` Adjusted for their average past misses, both point to about <strong>₹${num((adj.fo + adj.all) / 2, 0)} Cr</strong> for ${esc(cq.quarter)} (₹${num(adj.fo, 0)} Cr and ₹${num(adj.all, 0)} Cr).`
                              : ` Adjusted for their average past misses they point to ₹${num(adj.fo, 0)} Cr and ₹${num(adj.all, 0)} Cr for ${esc(cq.quarter)}.`;
      $('qpBacktestNote').innerHTML = p;
      const ex = (bt.excluded || []).map(e => `${esc(e.quarter)} (${esc(e.reason)})`);
      $('qpBacktestBasis').innerHTML = INFO + `<span>Walk-forward: for each quarter, the expense model, tax rate and non-F&amp;O share come only from earlier quarters. `
        + `Non-F&amp;O revenue is the gap between reported revenue and the sum of daily F&amp;O revenue.${ex.length ? ' Left out: ' + ex.join('; ') + '.' : ''}</span>`;
    }

    // Every reported quarter
    const allActs = (d.actuals || []).slice().reverse();
    $('qpActuals').innerHTML = table(['Quarter', 'Revenue', 'Expenses', 'Profit after tax', 'Margin', 'F&amp;O revenue', 'Revenue vs a year before'],
      allActs.map((a, i) => {
        const ly = allActs[i + 4];
        const yoy = ly ? (a.revenue_cr / ly.revenue_cr - 1) * 100 : null;
        return [esc(a.quarter) + ` <span class="muted">${esc(a.label)}</span>`, num(a.revenue_cr, 0), num(a.expenses_cr, 0), `<strong>${num(a.pat_cr, 0)}</strong>`,
                `${num(a.pat_margin_pct, 1)}%`, a.fo_revenue_cr ? num(a.fo_revenue_cr, 0) : '—',
                yoy === null ? '<span class="muted">—</span>' : `<span class="${yoy >= 0 ? 'up' : 'down'}">${yoy >= 0 ? '▲' : '▼'} ${Math.abs(yoy).toFixed(0)}%</span>`];
      }));
  }

  function mountQuarter() {
    if (QS.data && Date.now() - QS.at < 5 * 60 * 1000) { renderQuarter(); return; }
    fetch('/api/quarterly').then(r => r.json()).then(d => {
      if (!d.success) throw new Error(d.error || 'No data');
      QS.data = d; QS.at = Date.now(); renderQuarter();
    }).catch(e => MCX.ui.error($('qpChart'), 'Could not load the quarter: ' + e.message, mountQuarter));
  }

  $('qpBasis').addEventListener('click', e => {
    const b = e.target.closest('button[data-basis]');
    if (!b || b.disabled || b.dataset.basis === QS.basis) return;
    QS.basis = b.dataset.basis;
    MCX.storage.set('mcx.quarter.basis', QS.basis);
    renderQuarter();
  });
  onResize($('quarter'), () => QS.data && renderQuarter());

  // ════════════════════════════════════════════════════════════════════════
  //  Fair value
  // ════════════════════════════════════════════════════════════════════════
  const FV = { data: null, price: null, inp: null, calc: null };
  const valUrl = () => '/api/valuation?range=' + encodeURIComponent(rangeState.valChart || '60D');
  const SIGNALS = { DEEP_VALUE: ['strong-up', '▲▲', 'Deep value'], UNDERVALUED: ['up', '▲', 'Undervalued'], FAIR: ['', '', 'Fair'],
                    OVERVALUED: ['down', '▼', 'Overvalued'], STRETCHED: ['strong-down', '▼▼', 'Stretched'] };
  const sigPill = sig => { const m = SIGNALS[sig] || ['none', '', 'No data']; return `<span class="pill${m[0] ? ' pill--' + m[0] : ''}">${m[1] ? `<span aria-hidden="true">${m[1]}</span>` : ''}${m[2]}</span>`; };
  const pct = v => `${v >= 0 ? '+' : '−'}${num(Math.abs(v), 1)}%`;
  const rs = v => `₹${num(v, 0)}`;

  function currentPrice() {
    const live = MCX.store.get('price');
    const snap = FV.data && FV.data.snapshot;
    if (live && live.price) return { price: live.price, change: live.change_pct, label: live.fetched_at ? `live, ${new Date(live.fetched_at).toLocaleTimeString('en-GB', { timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit' })} IST` : 'live' };
    return snap && snap.latest_price ? { price: snap.latest_price, change: null, label: `close ${fmt.dayMonth(snap.latest_price_date)}` } : null;
  }

  // Every method on one scale: bars are ranges, ticks are central values, the dashed line is the price
  function footballSvg(w, rows, price) {
    const compact = w < 640;
    const vals = rows.flatMap(r => (r.group ? [] : [r.lo, r.hi, r.mark].filter(v => v !== null && v !== undefined))).concat(price);
    const step = 400, lo = Math.floor(Math.min(...vals) * 0.95 / step) * step, hi = Math.ceil(Math.max(...vals) * 1.03 / step) * step;
    const labW = compact ? 0 : 280, valW = compact ? 0 : 190;
    const X = v => labW + (w - labW - valW) * (v - lo) / (hi - lo);
    let y = 34;
    const body = [];
    rows.forEach(r => {
      if (r.group) { y += 8; body.push(V.text(0, y + 4, 'c-label3', esc(r.group))); y += 20; return; }
      if (compact) { body.push(V.text(0, y + 2, 'c-label', esc(r.name)) + V.text(w, y + 2, 'c-label', r.value, 'end')); y += 12; }
      else { body.push(V.text(0, y + 9, r.kind === 'blend' ? 'c-label' : 'c-label', esc(r.name)) + V.text(0, y + 25, 'c-label2', r.sub)); }
      const hgt = r.kind === 'blend' ? 18 : 14, cy = y + 8;
      if (r.kind === 'whisker') {
        body.push(`<line x1="${X(r.lo).toFixed(1)}" x2="${X(r.hi).toFixed(1)}" y1="${cy}" y2="${cy}" class="c-whisker"/>`
          + [r.lo, r.hi].map(v => `<line x1="${X(v).toFixed(1)}" x2="${X(v).toFixed(1)}" y1="${cy - 6}" y2="${cy + 6}" class="c-whisker"/>`).join(''));
      } else {
        body.push(`<rect x="${X(r.lo).toFixed(1)}" y="${(cy - hgt / 2).toFixed(1)}" width="${Math.max(X(r.hi) - X(r.lo), 3).toFixed(1)}" height="${hgt}" rx="3" class="${r.kind === 'blend' ? 'c-ff-blend' : r.kind === 'dd' ? 'c-ff-dd' : 'c-ff-leg'}"><title>${esc(r.name)}: ${rs(r.lo)} – ${rs(r.hi)}</title></rect>`);
      }
      if (r.mark !== null && r.mark !== undefined) body.push(`<rect x="${(X(r.mark) - 2).toFixed(1)}" y="${cy - 11}" width="4" height="22" rx="1" class="c-dot"/>`);
      if (!compact) body.push(V.text(w, y + 13, 'c-label', r.value, 'end'));
      y += compact ? 30 : 48;
    });
    const h = y + 18;
    const s = [V.open(w, h, 'Valuation methods on one scale: ' + rows.filter(r => !r.group).map(r => `${r.name} ${rs(r.lo)} to ${rs(r.hi)}`).join('; ') + `; price ${rs(price)}`)];
    for (let v = lo; v <= hi; v += step) {
      s.push(`<line x1="${X(v).toFixed(1)}" x2="${X(v).toFixed(1)}" y1="26" y2="${h - 18}" class="c-grid"/>`);
      s.push(V.text(X(v), h - 2, 'c-tick', `₹${num(v, 0)}`, 'middle'));
    }
    s.push(...body);
    const px = X(price);
    s.push(`<line x1="${px.toFixed(1)}" x2="${px.toFixed(1)}" y1="20" y2="${h - 18}" class="c-price"/>`);
    s.push(V.text(px, 13, 'c-label', `Price ${rs(price)}`, 'middle', true));
    s.push('</svg>');
    return s.join('');
  }

  // Price against the data-driven band over the chosen range
  function bandSvg(w, hist) {
    const pts = hist.filter(x => x.fair_base);
    if (pts.length < 2) return { svg: '' };
    const compact = w < 520, h = compact ? 220 : 280;
    const all = pts.flatMap(x => [x.fair_bear, x.fair_bull, x.price].filter(Boolean));
    const vmin = Math.floor(Math.min(...all) * 0.95 / 100) * 100, vmax = Math.ceil(Math.max(...all) * 1.03 / 100) * 100;
    const f = V.frame(w, h, vmax, [compact ? 44 : 56, compact ? 8 : 140, 12, 30], vmin);
    const X = i => f.pl + f.iw * i / (pts.length - 1);
    const s = [V.open(w, h, `Share price against the data-driven fair value band, ${pts.length} sessions`)];
    s.push(V.grid(f, V.niceTicks(vmax, compact ? 3 : 4, vmin), v => `₹${num(v, 0)}`));
    const up = pts.map((p, i) => `${X(i).toFixed(1)},${f.y(p.fair_bull).toFixed(1)}`), dn = pts.map((p, i) => `${X(i).toFixed(1)},${f.y(p.fair_bear).toFixed(1)}`).reverse();
    s.push(`<path d="M${up.join('L')}L${dn.join('L')}Z" class="c-band"/>`);
    s.push(`<path d="M${pts.map((p, i) => `${X(i).toFixed(1)},${f.y(p.fair_base).toFixed(1)}`).join('L')}" class="c-typical"/>`);
    const pp = pts.map((p, i) => (p.price ? `${X(i).toFixed(1)},${f.y(p.price).toFixed(1)}` : null)).filter(Boolean);
    if (pp.length > 1) s.push(`<path d="M${pp.join('L')}" class="c-line"/>`);
    if (!compact) {
      const L = pts[pts.length - 1];
      V.spread([L.price ? { y: f.y(L.price) + 4, s: `Price ₹${num(L.price, 0)}`, cls: 'c-label' } : null,
                { y: f.y(L.fair_base) + 4, s: `Base ₹${num(L.fair_base, 0)}`, cls: 'c-label2' },
                { y: f.y(L.fair_bull) + 4, s: `Bull ₹${num(L.fair_bull, 0)}`, cls: 'c-label3' },
                { y: f.y(L.fair_bear) + 4, s: `Bear ₹${num(L.fair_bear, 0)}`, cls: 'c-label3' }].filter(Boolean), 14, f.pt + 4, f.h - f.pb)
        .forEach(it => s.push(V.text(f.w - f.pr + 12, it.y, it.cls, it.s)));
    }
    s.push(V.text(f.pl, h - 8, 'c-tick', fmt.dayMonth(pts[0].date)) + V.text(f.w - f.pr, h - 8, 'c-tick', fmt.dayMonth(pts[pts.length - 1].date), 'end'));
    s.push('</svg>');
    const xs = pts.map((_, i) => X(i));
    const tip = i => `<strong>${fmt.day(pts[i].date)}</strong><br>Price ${pts[i].price ? '₹' + num(pts[i].price, 0) : '—'}<br><span class="c-tip-sub">Base ₹${num(pts[i].fair_base, 0)} · bear ₹${num(pts[i].fair_bear, 0)} · bull ₹${num(pts[i].fair_bull, 0)}</span>`;
    return { svg: s.join(''), f, xs, tip };
  }

  function chain(steps) {
    return '<ol class="chain">' + steps.map(([k, v, note], i) => `<li${i === steps.length - 1 ? ' class="last"' : ''}><span><span class="chain-k">${k}</span>${note ? `<span class="chain-n">${note}</span>` : ''}</span><span class="chain-v">${v}</span></li>`).join('') + '</ol>';
  }

  // ── House inputs: start from the backend's house values; edits stay in this browser ──
  const HI_KEY = 'mcx.house.inputs';
  const clone = o => JSON.parse(JSON.stringify(o));
  function houseInputs(h) {
    const base = h.inputs;
    try {
      const saved = JSON.parse(MCX.storage.get(HI_KEY, 'null'));
      // Edits made against older house values are dropped, so a house update is never masked
      if (saved && JSON.stringify(saved.base) === JSON.stringify(base)) return saved.edits;
    } catch (e) { /* unreadable: start from the house values */ }
    return clone(base);
  }
  const edited = h => JSON.stringify(FV.inp) !== JSON.stringify(h.inputs);
  function saveInputs(h) {
    MCX.storage.set(HI_KEY, edited(h) ? JSON.stringify({ base: h.inputs, edits: FV.inp }) : 'null');
  }
  // [key, label, unit, scale (shown = stored × scale), step, decimals]
  const FIELDS = [
    ['adr_fy28', 'Revenue per day, FY28', '₹ Cr', 1, 0.25, 2],
    ['days_fy28', 'Trading days, FY28', 'days', 1, 1, 0],
    ['non_fo_fy26', 'Non-F&amp;O revenue, FY26', '₹ Cr', 1, 1, 2],
    ['other_income_fy26', 'Other income, FY26', '₹ Cr', 1, 1, 2],
    ['growth_fy27', 'Growth into FY27', '%', 100, 1, 1],
    ['growth_fy28', 'Growth into FY28', '%', 100, 1, 1],
    ['margin', 'PAT margin, of total revenue', '%', 100, 0.5, 1],
    ['pe.bear', 'P/E, bear', '×', 1, 1, 1], ['pe.base', 'P/E, base', '×', 1, 1, 1], ['pe.bull', 'P/E, bull', '×', 1, 1, 1],
    ['disc_fy28', 'Discount, FY28 to FY27', '%', 100, 0.5, 1],
    ['disc_today', 'Discount, FY27 to today', '%', 100, 0.5, 2],
  ];
  const getK = (o, k) => k.split('.').reduce((a, p) => a[p], o);
  const setK = (o, k, v) => { const ps = k.split('.'); ps.slice(0, -1).reduce((a, p) => a[p], o)[ps[ps.length - 1]] = v; };
  const fieldVal = (k, sc, dp) => +(getK(FV.inp, k) * sc).toFixed(dp);

  function renderInputs(h) {
    const form = $('fvInputs');
    const row = ([k, lab, unit, sc, step, dp]) => `<label class="hin"><span>${lab}</span><span class="hin-box">`
      + `<input type="number" inputmode="decimal" step="${step}" min="0" data-k="${k}" data-sc="${sc}" data-dp="${dp}" value="${fieldVal(k, sc, dp)}">`
      + `<small>${unit}</small></span></label>`;
    form.innerHTML = `<fieldset><legend>FY28 earnings</legend>${FIELDS.slice(0, 7).map(row).join('')}</fieldset>`
      + `<fieldset><legend>Multiples</legend>${FIELDS.slice(7, 10).map(row).join('')}</fieldset>`
      + `<fieldset><legend>Discounting</legend>${row(FIELDS[10])}`
      + '<div class="hin hin--method"><span id="fvMethodLab">Second step</span><span class="seg" role="group" aria-labelledby="fvMethodLab">'
      + '<button type="button" data-method="fixed">Fixed</button><button type="button" data-method="prorata">Pro-rata to 31 Mar</button></span></div>'
      + `${row(FIELDS[11])}<p class="hin-note" id="fvMethodNote"></p></fieldset>`
      + '<div class="hin-foot"><button type="button" class="more-btn" id="fvReset">Reset to house inputs</button><span id="fvEditNote"></span></div>';
    syncInputs(h);
  }
  function syncInputs(h) {
    const c = FV.calc, pro = FV.inp.method === 'prorata';
    document.querySelectorAll('#fvInputs [data-method]').forEach(b => b.setAttribute('aria-pressed', String(b.dataset.method === FV.inp.method)));
    const d2 = document.querySelector('#fvInputs input[data-k="disc_today"]');
    d2.disabled = pro;
    if (pro) d2.value = +(c.prorata * 100).toFixed(2);
    $('fvMethodNote').textContent = pro
      ? `${num(FV.inp.disc_fy28 * 100, 1)}% × ${c.daysLeft} days left to 31 Mar 2027 ÷ 365 = ${num(c.prorata * 100, 2)}%. It falls to zero by 31 March.`
      : `A fixed step, as in the Tusk sheet. Pro-rata would be ${num(c.prorata * 100, 2)}% today (${c.daysLeft} days to 31 Mar 2027).`;
    const ed = edited(h);
    $('fvEditNote').textContent = ed ? 'Your edits, kept in this browser only.' : 'The house inputs.';
    $('fvReset').disabled = !ed;
    $('fvHouseEdited').innerHTML = ed ? '<span class="pill pill--caution"><span aria-hidden="true">!</span>Edited inputs</span>' : '';
  }
  function onInput(e) {
    const el = e.target.closest('input[data-k]');
    if (!el) return;
    const v = parseFloat(el.value), sc = +el.dataset.sc;
    const ok = isFinite(v) && v >= 0 && !(el.dataset.k === 'days_fy28' && v < 1) && !(el.dataset.k.startsWith('pe.') && v <= 0);
    el.setAttribute('aria-invalid', String(!ok));
    if (!ok) return;
    setK(FV.inp, el.dataset.k, v / sc);
    saveInputs(FV.data.house);
    renderHouse();
  }

  function renderFairValue() {
    const v = FV.data;
    if (!v) return;
    const h = v.house;
    if (!h) { MCX.ui.error($('fvStats'), v.house_error ? 'House model unavailable: ' + v.house_error : 'No house model.', mountFairValue); return; }
    if (!FV.inp) { FV.inp = houseInputs(h); FV.calc = houseCalc(FV.inp, h.shares_cr, MCX.market.ist().iso); renderInputs(h); }
    renderHouse();
    renderBand();
  }

  // Everything that depends on the house inputs or the price
  function renderHouse() {
    const v = FV.data, h = v.house, snap = v.snapshot, c = snap.eps_chain, pb = v.pe_bands, fv = snap.fair_value;
    const cp = currentPrice();
    if (!cp) { MCX.ui.error($('fvStats'), 'No share price yet.', mountFairValue); return; }
    const inp = FV.inp, price = cp.price, pe = pb.mean;
    const hc = FV.calc = houseCalc(inp, h.shares_cr, MCX.market.ist().iso), t = hc.today;
    syncInputs(h);
    const l = fvLede(price, t, inp, v);
    $('fvKicker').textContent = `Fair value · two views · ADR to ${fmt.dayMonth(v.data_quality.latest_valuation_date)}, price ${cp.label}`;
    $('fvHead').textContent = l.head;
    $('fvDeck').textContent = l.deck;
    const implied = revenuePricedIn(price, pe, c);
    const hs = HOUSE_STATE[l.state];
    $('fvStats').innerHTML =
      stat('Share price', `₹${num(price, 0)}`, `${cp.change !== null && cp.change !== undefined ? `<span class="${cp.change >= 0 ? 'up' : 'down'}">${cp.change >= 0 ? '▲' : '▼'} ${num(Math.abs(cp.change), 2)}%</span> today · ` : ''}NSE, ${esc(cp.label)}`, true)
      + stat('Tusk house view', `₹${num(t.base, 0)}`, `<div class="stat-pill"><span class="pill${hs[0] ? ' pill--' + hs[0] : ''}">${hs[1]}</span></div>bear ₹${num(t.bear, 0)} · bull ₹${num(t.bull, 0)} · base ${pct((t.base / price - 1) * 100)} vs price`)
      + stat('Data-driven view', `₹${num(fv.base, 0)}`, `<div class="stat-pill">${sigPill(ddSignal(price, fv))}</div>price ${Math.abs(l.prem).toFixed(1)}% ${l.prem >= 0 ? 'above' : 'below'}`)
      + stat('Revenue priced in', `₹${num(implied, 2)}<small>Cr/day</small>`, `at the data-driven multiple · 45-day average ₹${num(c.ma45_rev_cr, 2)} Cr`);

    const reg = h.regression, pvs = h.analysts.map(a => a.present_value);
    const rows = [
      { group: 'Tusk house model' },
      { name: 'House view', sub: `${inp.pe.bear} / ${inp.pe.base} / ${inp.pe.bull}× FY28 EPS, discounted to today`, lo: t.bear, hi: t.bull, mark: t.base, kind: 'blend',
        value: `${rs(t.base)} <tspan class="c-label2">(${num(t.bear, 0)}–${num(t.bull, 0)})</tspan>` },
      { group: 'Data-driven' },
      { name: 'P/E band on run-rate EPS', sub: `${num(pb.bear_pe, 1)}–${num(pb.bull_pe, 1)}× · base ${num(pe, 1)}×`, lo: fv.bear, hi: fv.bull, mark: fv.base, kind: 'dd',
        value: `${rs(fv.base)} <tspan class="c-label2">(${num(fv.bear, 0)}–${num(fv.bull, 0)})</tspan>` },
      { group: 'Cross-checks, not in the house view' },
      reg ? { name: 'Regression', sub: 'price on 45-day ADR · 95% band', lo: reg.low, hi: reg.high, mark: reg.value, kind: 'whisker', value: rs(reg.value) } : null,
      { name: 'Analyst targets', sub: `${h.analysts.length} brokers, discounted at ${Math.round(h.assumptions.hurdle * 100)}%`, lo: Math.min(...pvs), hi: Math.max(...pvs), mark: h.analyst_leg, kind: 'leg',
        value: `${rs(h.analyst_leg)} <tspan class="c-label2">(${num(Math.min(...pvs), 0)}–${num(Math.max(...pvs), 0)})</tspan>` },
    ].filter(Boolean);
    const box = $('fvFootball');
    box.innerHTML = footballSvg(Math.round(box.clientWidth) || 1000, rows, price);
    $('fvFootballBasis').innerHTML = INFO + `<span>The regression is refitted on ${reg ? reg.n : '—'} sessions since ${reg ? fmt.dayMonth(reg.since) + ' ' + reg.since.slice(0, 4) : '—'} (price ≈ ${reg ? num(reg.a, 0) : '—'} + ${reg ? num(reg.b, 1) : '—'} × ADR${reg && reg.r_squared ? `, R² ${num(reg.r_squared, 2)}` : ''}). `
      + `Analyst targets are from the Tusk workbook (reports of ${esc([...new Set(h.analysts.map(a => fmt.dayMonth(a.report_date).split(' ')[1]))].join(' and '))} 2026), each discounted from its 12-month target date.</span>`;

    // The sheet
    const three = f => ['bear', 'base', 'bull'].map(f);
    const same = x => three(() => x);
    const grow = `FY26 ₹${num(inp.non_fo_fy26, 0)} Cr, +${num(inp.growth_fy27 * 100, 0)}% then +${num(inp.growth_fy28 * 100, 0)}%`;
    const sheetRows = [
      ['Revenue per day, FY28', same(`₹${num(inp.adr_fy28, 2)}`)],
      ['Trading days', same(num(inp.days_fy28))],
      ['Operating revenue (F&amp;O)', same(num(hc.op, 0))],
      [`Other operating revenue<small>${grow}</small>`, same(num(hc.nonFo, 0))],
      [`Other income<small>FY26 ₹${num(inp.other_income_fy26, 0)} Cr, same growth</small>`, same(num(hc.other, 0))],
      ['Total revenue', same(num(hc.total, 0)), 'sub'],
      [`PAT<small>${num(inp.margin * 100, 1)}% of total revenue</small>`, same(num(hc.pat, 0))],
      [`EPS<small>÷ ${num(h.shares_cr, 3)} Cr shares</small>`, same(`₹${num(hc.eps, 2)}`)],
      ['P/E', three(k => `${num(inp.pe[k], 1)}×`)],
      ['Target price, FY28', three(k => `₹${num(hc.fy28[k], 0)}`), 'sub'],
      ['Discount to FY27', same(`${num(inp.disc_fy28 * 100, 1)}%`)],
      ['Target price, FY27', three(k => `₹${num(hc.fy27[k], 0)}`)],
      [`Discount to today<small>${inp.method === 'prorata' ? `pro-rata, ${hc.daysLeft} days to 31 Mar 2027` : 'fixed'}</small>`, same(`${num(hc.step2 * 100, 2)}%`)],
      ['Target price today', three(k => `₹${num(t[k], 0)}`), 'total'],
      ['Against the price', three(k => `<span class="${t[k] >= price ? 'up' : 'down'}">${pct((t[k] / price - 1) * 100)}</span>`)],
    ];
    $('fvSheet').innerHTML = `<div class="table-scroll"><table class="v2-table house-sheet"><thead><tr><th scope="col">Based on the latest trend, FY28</th>`
      + three(k => `<th scope="col">${k[0].toUpperCase() + k.slice(1)}</th>`).join('') + '</tr></thead><tbody>'
      + sheetRows.map(([lab, cells, cls]) => `<tr${cls ? ` class="${cls}"` : ''}><td>${lab}</td>${cells.map(x => `<td>${x}</td>`).join('')}</tr>`).join('')
      + '</tbody></table></div>';
    $('fvSheetBasis').innerHTML = INFO + `<span>From the Tusk sheet “Based on latest trend, FY2028”, with the FY28 calendar of ${h.assumptions.days.FY28} trading days. `
      + 'The margin is applied to total revenue including other income, which is how the sheet’s figures are computed. The target is P/E on FY28 earnings, brought back to FY27 and then to today.</span>';

    $('fvDdChain').innerHTML = chain([
      ['Run-rate EPS', `₹${num(c.eps, 2)}`, `ADR ₹${num(c.ma45_rev_cr, 2)} Cr × ${c.trading_days} days + non-F&amp;O ₹${num(c.non_fo_rev_cr, 1)} Cr, × ${Math.round(c.pat_margin * 100)}%, ÷ ${num(c.diluted_shares_cr, 3)} Cr shares`],
      ['Multiple', `${num(pe, 1)}×`, `Median of the stock’s own P/E on run-rate EPS; band ±1 SD = ${num(pb.bear_pe, 1)}–${num(pb.bull_pe, 1)}×`],
      ['Fair value today', `₹${num(fv.base, 0)}`, `Range ₹${num(fv.bear, 0)} – ${num(fv.bull, 0)} · no discounting: a spot multiple on current earnings`],
      ['Revenue priced in', `₹${num(implied, 2)} Cr/day`, `What today’s price needs at ${num(pe, 1)}×; each ₹1 Cr a day is worth about ₹${num(c.trading_days * c.pat_margin / c.diluted_shares_cr * pe, 0)} a share`],
    ]);

    $('fvWhy').innerHTML = table(['', 'Tusk house', 'Data-driven'], [
      ['Revenue per day', `₹${num(inp.adr_fy28, 2)} Cr in FY28`, `₹${num(c.ma45_rev_cr, 2)} Cr, the 45-day average`],
      ['Earnings used', `FY28 EPS ₹${num(hc.eps, 2)}`, `Run-rate EPS ₹${num(c.eps, 2)}`],
      ['Multiple', `${inp.pe.bear} / ${inp.pe.base} / ${inp.pe.bull}×`, `${num(pe, 1)}×, median of own history`],
      ['Discounting', `${num(inp.disc_fy28 * 100, 1)}% to FY27, then ${num(hc.step2 * 100, 2)}% to today`, 'Not applied'],
      { cls: 'total', cells: ['Base value', `₹${num(t.base, 0)}`, `₹${num(fv.base, 0)}`] },
    ]);
    $('fvWhyBasis').innerHTML = INFO + `<span>The house earnings are ${num(Math.abs(hc.eps / c.eps - 1) * 100, 0)}% ${hc.eps >= c.eps ? 'above' : 'below'} run-rate: a higher revenue per day, other income counted, and a full FY28.</span>`;

    // What moves the house number: revenue per day × multiple
    const adrs = [-2, -1, 0, 1, 2].map(d => inp.adr_fy28 + d).filter(x => x > 0);
    $('fvSens').innerHTML = table(['Revenue per day, FY28', `Bear ${inp.pe.bear}×`, `Base ${inp.pe.base}×`, `Bull ${inp.pe.bull}×`], adrs.map(a => {
      const r = houseCalc(Object.assign({}, inp, { adr_fy28: a }), h.shares_cr, MCX.market.ist().iso).today;
      const cur = Math.abs(a - inp.adr_fy28) < 1e-9;
      return { cls: cur ? 'total' : '', cells: [`₹${num(a, 2)} Cr${cur ? ' <span class="muted">house</span>' : ''}`, ...three(k => `₹${num(r[k], 0)}`)] };
    }));
    const be = breakevenAdr(price, inp, h.shares_cr, hc);
    $('fvSensBasis').innerHTML = INFO + `<span>Today’s price equals the house base case at ₹${num(be, 2)} Cr a day in FY28, against the house’s ₹${num(inp.adr_fy28, 2)} Cr and the last 45 days’ ₹${num(c.ma45_rev_cr, 2)} Cr. `
      + `For comparison, the ${h.street.brokers} brokers average ${num(h.street.pe, 0)}× on FY28E EPS of ₹${num(h.street.eps28, 1)}.</span>`;

    $('fvBrokers').innerHTML = table(['Broker', 'Report', 'Rating', 'Target', 'Target date', 'Worth today', 'FY28E EPS', 'P/E on FY28E'], h.analysts.map(a => [
      esc(a.broker), fmt.dayMonth(a.report_date) + ' ' + a.report_date.slice(0, 4), esc(a.rating), `₹${num(a.target, 0)}`,
      fmt.dayMonth(a.target_date) + ' ' + a.target_date.slice(0, 4) + (a.expired ? ' <span class="muted">passed</span>' : ''), `₹${num(a.present_value, 0)}`, `₹${num(a.eps28, 1)}`, `${num(a.pe, 0)}×`]));
  }

  function renderBand() {
    const v = FV.data, box = $('fvBand');
    if (!v) return;
    const c = bandSvg(Math.round(box.clientWidth) || 900, v.history || []);
    box.innerHTML = c.svg;
    if (c.f) V.hover(box, c.f, c.xs, c.tip);
  }

  function mountFairValue() {
    fetchRanged(valUrl()).then(d => {
      if (!d.success) throw new Error(d.error || 'No data');
      FV.data = d; renderFairValue();
    }).catch(e => MCX.ui.error($('fvStats'), 'Could not load fair value: ' + e.message, mountFairValue));
    MCX.poll.kick('cmp');
  }
  MCX.store.on('price', () => { if (FV.data && MCX.router.current() === 'val-fv') renderFairValue(); });
  $('fvInputs').addEventListener('input', onInput);
  $('fvInputs').addEventListener('submit', e => e.preventDefault());
  $('fvInputs').addEventListener('click', e => {
    const m = e.target.closest('[data-method]'), r = e.target.closest('#fvReset');
    if (m && FV.inp && m.dataset.method !== FV.inp.method) { FV.inp.method = m.dataset.method; saveInputs(FV.data.house); renderHouse(); }
    if (r && FV.data) {
      FV.inp = clone(FV.data.house.inputs); saveInputs(FV.data.house);
      FV.calc = houseCalc(FV.inp, FV.data.house.shares_cr, MCX.market.ist().iso); renderInputs(FV.data.house); renderHouse();
    }
  });
  makeRangeToggle({ key: 'valChart', containerId: 'fvBandRange', ranges: ['30D', '60D', 'Q', '1Y', '2Y', 'Max'], defaultRange: '60D', labelIds: ['fvBandRangeLabel'],
                    onChange: () => fetchRanged(valUrl()).then(d => { if (d.success) { FV.data = d; renderBand(); } }) });
  onResize($('fairvalue'), () => FV.data && renderFairValue());

  // ════════════════════════════════════════════════════════════════════════
  //  Scenarios
  // ════════════════════════════════════════════════════════════════════════
  // Editable house defaults; shares, sessions and the price come from the backend unless the user edits them
  const SC = { rev: 12.62, pe: 42, opex: 700, other: 126, tax: 20.3, shares: null, days: null, cmp: null,
               adj: { bearRev: -15, bearPe: -10, bullRev: 15, bullPe: 10 }, touched: new Set(), ready: false,
               trend: { 26: { adr: 9.1, days: 257, other: 280, margin: 51.5, pe: 55 }, 27: { adr: 11.2, days: 256, other: 336, margin: 60, pe: 42 }, 28: { adr: 13.5, days: 260, other: 403, margin: 60, pe: 55 } },
               cases: { bear: { growth: 0, adr: 11.2, other: 336, margin: 60, pe: 36 }, base: { growth: 0, adr: 11.2, other: 336, margin: 60, pe: 42 }, bull: { growth: 0, adr: 11.2, other: 336, margin: 60, pe: 48 } },
               disc: 18 };
  const ASSUMP = [['opex', 'Operating costs, a year', '₹ Cr', 10], ['other', 'Other income, a year', '₹ Cr', 10], ['tax', 'Tax rate', '%', 0.1],
                  ['shares', 'Diluted shares', 'Cr', 0.001], ['days', 'Trading days in the year', 'days', 1], ['cmp', 'Share price', '₹', 1]];
  const inputBox = (attrs, val, unit, step, label) => `<span class="hin-box"><input type="number" inputmode="decimal" step="${step}" ${attrs} value="${val ?? ''}" aria-label="${esc(label)}"><small>${unit}</small></span>`;
  const cellInput = (attrs, val, step, label) => `<input type="number" inputmode="decimal" class="cell-input" step="${step}" ${attrs} value="${val}" aria-label="${esc(label)}">`;
  const scA = () => ({ days: SC.days, opex: SC.opex, other: SC.other, tax: SC.tax, shares: SC.shares });

  // Built once, so typing never loses focus; outputs update in place
  function buildScenarios() {
    $('scAssumpForm').innerHTML = ASSUMP.map(([k, lab, unit, step]) => `<label class="hin"><span>${lab}</span>${inputBox(`data-sc="${k}"`, SC[k], unit, step, lab)}</label>`).join('');
    $('scAdjust').innerHTML = `<span>Bear: revenue ${cellInput('data-adj="bearRev"', SC.adj.bearRev, 1, 'Bear case: change in revenue per day, %')}% and P/E ${cellInput('data-adj="bearPe"', SC.adj.bearPe, 1, 'Bear case: change in P/E, %')}%</span>`
      + `<span>Bull: revenue ${cellInput('data-adj="bullRev"', SC.adj.bullRev, 1, 'Bull case: change in revenue per day, %')}% and P/E ${cellInput('data-adj="bullPe"', SC.adj.bullPe, 1, 'Bull case: change in P/E, %')}%</span>`;
    const yrs = ['26', '27', '28'];
    const tRow = (lab, k, step) => `<tr><td>${lab}</td>${yrs.map(y => `<td>${cellInput(`data-pt="${k}" data-yr="${y}"`, SC.trend[y][k], step, `FY${y} ${lab}`)}</td>`).join('')}</tr>`;
    const tOut = (lab, k, cls) => `<tr${cls ? ` class="${cls}"` : ''}><td>${lab}</td>${yrs.map(y => `<td data-pt-out="${k}" data-yr="${y}">—</td>`).join('')}</tr>`;
    $('patTrend').innerHTML = `<div class="table-scroll"><table class="v2-table house-sheet sc-table"><caption>Based on the latest trend</caption><thead><tr><th scope="col"></th>${yrs.map(y => `<th scope="col">FY${y}</th>`).join('')}</tr></thead><tbody>`
      + tRow('Revenue per day, ₹ Cr', 'adr', 0.1) + tRow('Trading days', 'days', 1) + tOut('Operating revenue', 'op') + tRow('Other revenue, ₹ Cr', 'other', 1)
      + tOut('Total revenue', 'tot', 'sub') + tRow('PAT margin, %', 'margin', 0.1) + tOut('PAT', 'pat') + tOut('EPS', 'eps') + tRow('P/E', 'pe', 0.5) + tOut('Price target', 'px', 'total')
      + '</tbody></table></div>';
    const cs = ['bear', 'base', 'bull'];
    const cRow = (lab, k, step) => `<tr><td>${lab}</td>${cs.map(c => `<td>${cellInput(`data-ps="${k}" data-case="${c}"`, SC.cases[c][k], step, `${c} case ${lab}`)}</td>`).join('')}</tr>`;
    const cOut = (lab, k, cls) => `<tr${cls ? ` class="${cls}"` : ''}><td>${lab}</td>${cs.map(c => `<td data-ps-out="${k}" data-case="${c}">—</td>`).join('')}</tr>`;
    $('patCases').innerHTML = `<div class="table-scroll"><table class="v2-table house-sheet sc-table"><caption>FY27: bear, base and bull</caption><thead><tr><th scope="col"></th>${cs.map(c => `<th scope="col">${c[0].toUpperCase() + c.slice(1)}</th>`).join('')}</tr></thead><tbody>`
      + cRow('Growth in revenue per day, %', 'growth', 1) + cRow('Revenue per day, ₹ Cr', 'adr', 0.1) + cRow('Other income, ₹ Cr', 'other', 1) + cOut('Total income', 'tot', 'sub')
      + cRow('PAT margin, %', 'margin', 0.1) + cOut('EPS', 'eps') + cRow('P/E', 'pe', 0.5) + cOut('Price, FY27', 'px') + cOut('Against the price', 'upside')
      + `<tr><td>Discount, % ${cellInput('id="patDisc"', SC.disc, 0.5, 'Discount, %')}</td><td></td><td></td><td></td></tr>` + cOut('Target price', 'target', 'total')
      + '</tbody></table></div>';
  }

  function syncSliders() {
    $('fcRevSlider').value = SC.rev; $('fcRevInput').value = SC.rev;
    $('fcPeSlider').value = SC.pe; $('fcPeInput').value = SC.pe;
  }

  function renderScenarios() {
    if (!SC.shares || !SC.days || !SC.cmp) return;             // waiting for the backend figures
    const a = scA(), base = yearModel(SC.rev, SC.pe, a), cmp = SC.cmp, gap = (base.price / cmp - 1) * 100;
    const cp = currentPrice(), t = MCX.store.get('ttmEps');
    $('scKicker').textContent = `Scenarios · price ₹${num(cmp, 0)}${cp ? `, ${cp.label}` : ''} · revenue per day starts at today’s projection`;
    $('scHead').textContent = `At ₹${num(SC.rev, 2)} Cr a day and ${num(SC.pe, 1)}×, a year of earnings supports ₹${num(base.price, 0)} a share, `
      + (Math.abs(gap) < 0.5 ? 'level with the price.' : `${Math.abs(gap).toFixed(0)}% ${gap > 0 ? 'above' : 'below'} the price.`);
    $('scDeck').textContent = `That is EPS of ₹${num(base.eps, 2)} on ${num(SC.shares, 3)} Cr shares and ${Math.round(SC.days)} sessions, after ₹${num(SC.opex, 0)} Cr of costs and ${num(SC.tax, 1)}% tax. `
      + (t ? `Reported EPS over the last four quarters (${t.from} to ${t.to}) is ₹${num(t.eps, 2)}. ` : '')
      + 'Move the sliders or open the assumptions; the FY27 tables below work from revenue per day and margin instead.';
    $('fcRevAnnual').textContent = `₹${num(base.annualRev, 0)} Cr over ${Math.round(SC.days)} sessions`;
    const im = impliedBy(cmp, SC.rev, SC.pe, a);
    $('scStats').innerHTML = stat('Share price', `₹${num(cmp, 0)}`, `${t ? `EPS (last four quarters) ₹${num(t.eps, 2)} · P/E ${num(cmp / t.eps, 1)}× · ` : ''}market cap ₹${num(cmp * SC.shares, 0)} Cr`, true)
      + stat('A year of earnings supports', `₹${num(base.price, 0)}`, `${pct(gap)} against the price`)
      + stat('Revenue the price implies', `₹${num(im.rev, 2)}<small>Cr/day</small>`, `at ${num(SC.pe, 1)}×; ${pct((im.rev / SC.rev - 1) * 100)} against your ₹${num(SC.rev, 2)} Cr`)
      + stat('P/E the price implies', im.pe ? `${num(im.pe, 1)}×` : '—', `at ₹${num(SC.rev, 2)} Cr a day; your P/E is ${num(SC.pe, 1)}×`);
    $('scChain').innerHTML = chain([
      ['Revenue', `₹${num(base.annualRev, 0)} Cr`, `₹${num(SC.rev, 2)} Cr a day × ${Math.round(SC.days)} sessions`],
      ['Less operating costs', `₹${num(SC.opex, 0)} Cr`, ''],
      ['EBITDA', `₹${num(base.ebitda, 0)} Cr`, ''],
      ['Plus other income', `₹${num(SC.other, 0)} Cr`, ''],
      ['Profit before tax', `₹${num(base.pbt, 0)} Cr`, ''],
      ['Less tax', `₹${num(base.tax, 0)} Cr`, `${num(SC.tax, 2)}%`],
      ['Profit after tax', `₹${num(base.pat, 0)} Cr`, ''],
      ['EPS', `₹${num(base.eps, 2)}`, `÷ ${num(SC.shares, 3)} Cr shares`],
      ['Price', `₹${num(base.price, 0)}`, `EPS × ${num(SC.pe, 1)}`],
    ]);
    const cases = [['Bear', SC.adj.bearRev, SC.adj.bearPe], ['Base', 0, 0], ['Bull', SC.adj.bullRev, SC.adj.bullPe]].map(([n, dr, dp]) => {
      const rv = SC.rev * (1 + dr / 100), p = SC.pe * (1 + dp / 100);
      return { n, rv, p, m: yearModel(rv, p, a) };
    });
    $('scCases').innerHTML = table(['', ...cases.map(c => c.n)], [
      ['Revenue per day', ...cases.map(c => `₹${num(c.rv, 2)} Cr`)], ['Revenue, a year', ...cases.map(c => `₹${num(c.m.annualRev, 0)} Cr`)],
      ['P/E', ...cases.map(c => `${num(c.p, 1)}×`)], ['EPS', ...cases.map(c => `₹${num(c.m.eps, 2)}`)], ['Profit after tax', ...cases.map(c => `₹${num(c.m.pat, 0)} Cr`)],
      { cls: 'total', cells: ['Price', ...cases.map(c => `₹${num(c.m.price, 0)}`)] },
      ['Against the price', ...cases.map(c => { const g = (c.m.price / cmp - 1) * 100; return `<span class="${g >= 0 ? 'up' : 'down'}">${pct(g)}</span>`; })],
    ], 'dg-fit');
    renderEarningsTables();
  }

  function renderEarningsTables() {
    const sh = SC.shares, cmp = SC.cmp;
    const set = (sel, v) => { const el = document.querySelector(sel); if (el) el.innerHTML = v; };
    ['26', '27', '28'].forEach(y => {
      const r = trendRow(SC.trend[y], sh);
      set(`[data-pt-out="op"][data-yr="${y}"]`, num(r.op, 0)); set(`[data-pt-out="tot"][data-yr="${y}"]`, num(r.tot, 0));
      set(`[data-pt-out="pat"][data-yr="${y}"]`, num(r.pat, 0)); set(`[data-pt-out="eps"][data-yr="${y}"]`, sh ? `₹${num(r.eps, 2)}` : '—');
      set(`[data-pt-out="px"][data-yr="${y}"]`, sh ? `₹${num(r.px, 0)}` : '—');
    });
    ['bear', 'base', 'bull'].forEach(c => {
      const r = caseRow(SC.cases[c], SC.trend[27].days, sh, SC.disc, cmp);
      set(`[data-ps-out="tot"][data-case="${c}"]`, num(r.tot, 0)); set(`[data-ps-out="eps"][data-case="${c}"]`, sh ? `₹${num(r.eps, 2)}` : '—');
      set(`[data-ps-out="px"][data-case="${c}"]`, sh ? `₹${num(r.px, 0)}` : '—');
      set(`[data-ps-out="upside"][data-case="${c}"]`, r.upside === null || !sh ? '—' : `<span class="${r.upside >= 0 ? 'up' : 'down'}">${pct(r.upside)}</span>`);
      set(`[data-ps-out="target"][data-case="${c}"]`, sh ? `₹${num(r.target, 0)}` : '—');
    });
  }

  function fillScenarioInputs(v, q) {
    const c = v.snapshot.eps_chain;
    const put = (k, val) => { if (!SC.touched.has(k) && val !== null && val !== undefined) { SC[k] = val; const el = document.querySelector(`[data-sc="${k}"]`); if (el) el.value = val; } };
    put('shares', c.diluted_shares_cr); put('days', c.trading_days);
    const cp = currentPrice();
    if (cp) put('cmp', Math.round(cp.price));
    const t = ttm(q && q.actuals, c.diluted_shares_cr);
    if (t) MCX.store.set('ttmEps', t);
    if (v.house) ['27', '28'].forEach(y => {           // the FY table's sessions: the calendar, as in the house model
      const d = v.house.assumptions.days['FY' + y], k = 'days' + y;
      if (d && !SC.touched.has(k)) { SC.trend[y].days = d; const el = document.querySelector(`[data-pt="days"][data-yr="${y}"]`); if (el) el.value = d; }
    });
    SC.ready = true;
  }

  function mountScenarios() {
    const q = QS.data ? Promise.resolve(QS.data) : fetch('/api/quarterly').then(r => r.json()).then(d => { if (d.success) { QS.data = d; QS.at = Date.now(); } return d; });
    Promise.all([fetchRanged(valUrl()), q]).then(([v, qd]) => {
      if (!v.success) throw new Error(v.error || 'No valuation data');
      if (!FV.data) FV.data = v;
      fillScenarioInputs(v, qd);
      renderScenarios();
      renderEarningsTables();
    }).catch(e => MCX.ui.error($('scDeck'), 'Could not load the backend figures: ' + e.message, mountScenarios));
    MCX.poll.kick('cmp');
  }

  buildScenarios();
  const num0 = el => { const v = parseFloat(el.value); return isFinite(v) ? v : null; };
  $('scenarios').addEventListener('input', e => {
    const el = e.target;
    if (el.id === 'fcRevSlider' || el.id === 'fcRevInput') { const v = num0(el); if (v !== null && v >= 0) { SC.rev = v; SC.touched.add('rev'); (el.id === 'fcRevSlider' ? $('fcRevInput') : $('fcRevSlider')).value = v; } }
    else if (el.id === 'fcPeSlider' || el.id === 'fcPeInput') { const v = num0(el); if (v !== null && v > 0) { SC.pe = v; (el.id === 'fcPeSlider' ? $('fcPeInput') : $('fcPeSlider')).value = v; } }
    else if (el.dataset.sc) { const v = num0(el); if (v !== null && v >= 0) { SC[el.dataset.sc] = v; SC.touched.add(el.dataset.sc); } }
    else if (el.dataset.adj) { const v = num0(el); if (v !== null) SC.adj[el.dataset.adj] = v; }
    else if (el.dataset.pt) { const v = num0(el); if (v !== null) { SC.trend[el.dataset.yr][el.dataset.pt] = v; if (el.dataset.pt === 'days') SC.touched.add('days' + el.dataset.yr); } }
    else if (el.dataset.ps) { const v = num0(el); if (v !== null) SC.cases[el.dataset.case][el.dataset.ps] = v; }
    else if (el.id === 'patDisc') { const v = num0(el); if (v !== null) SC.disc = v; }
    else return;
    renderScenarios();
    if (!SC.shares) renderEarningsTables();
  });
  // Revenue per day starts at today's projection until the user moves it
  MCX.store.on('refresh', r => {
    if (!r || !r.success || !(r.proj_rev_cr > 0) || SC.touched.has('rev')) return;
    SC.rev = +r.proj_rev_cr.toFixed(2); syncSliders();
    if (MCX.router.current() === 'val-scen') renderScenarios();
  });
  MCX.store.on('price', p => {
    if (!p || !p.price) return;
    if (!SC.touched.has('cmp')) { SC.cmp = Math.round(p.price); const el = document.querySelector('[data-sc="cmp"]'); if (el) el.value = SC.cmp; }
    if (MCX.router.current() === 'val-scen') renderScenarios();
  });

  MCX.value = { quarter: { mount: mountQuarter }, fairValue: { mount: mountFairValue }, scenarios: { mount: mountScenarios } };
})();
