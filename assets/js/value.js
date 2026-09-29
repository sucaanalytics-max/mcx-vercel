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
  // Where the price sits against the house range: within 5% of it counts as close to fair value
  function houseState(price, lo, hi) {
    if (lo <= price * 1.05 && hi >= price * 0.95) return 'near';
    return lo > price ? 'below' : 'above';
  }
  // Daily revenue the price implies at the data-driven multiple
  function revenuePricedIn(price, pe, c) {
    return ((price / pe) * c.diluted_shares_cr / c.pat_margin - c.non_fo_rev_cr) / c.trading_days;
  }
  function fvLede(price, h, v) {
    const fv = v.snapshot.fair_value, pe = v.pe_bands.mean;
    const lo = h.blend['48'], hi = h.blend['52'];
    const st = houseState(price, lo, hi);
    const prem = (price / fv.base - 1) * 100;
    const house = st === 'near' ? 'close to fair value' : st === 'below' ? `below fair value: the house range starts ${Math.round((lo / price - 1) * 100)}% above the price`
                                                        : `above fair value: the house range tops out ${Math.round((1 - hi / price) * 100)}% below the price`;
    const dd = Math.abs(prem) < 5 ? 'the data-driven view has it near fair value' : `the data-driven view has it about ${Math.round(Math.abs(prem))}% ${prem > 0 ? 'overvalued' : 'undervalued'}`;
    const head = `The Tusk house view puts MCX ${house}; ${dd}.`;
    const inside = price >= lo && price <= hi ? 'inside' : price < lo ? 'below' : 'above';
    const deck = `At ₹${num(price, 0)}, the price sits ${inside} the house range of ₹${num(lo, 0)} – ${num(hi, 0)} and ${Math.abs(prem).toFixed(0)}% ${prem >= 0 ? 'above' : 'below'} the data-driven base of ₹${num(fv.base, 0)}. `
      + `The gap is the multiple (${h.assumptions.pe[0]}–${h.assumptions.pe[1]}× forward against the stock’s own median of ${num(pe, 1)}×) and the house’s ${Math.round(h.assumptions.fy28_growth * 100)}% FY28 growth assumption.`;
    return { head, deck, state: st, prem };
  }

  MCX.valueModel = { quarterLede, adjusted, missStory, words, ddSignal, houseState, revenuePricedIn, fvLede };
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
  function table(head, rows) {
    return `<div class="table-scroll"><table class="v2-table"><thead><tr>${head.map(h => `<th scope="col">${h}</th>`).join('')}</tr></thead><tbody>`
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
  const FV = { data: null, price: null };
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

  function renderFairValue() {
    const v = FV.data;
    if (!v) return;
    const h = v.house, snap = v.snapshot, c = snap.eps_chain, pb = v.pe_bands, fv = snap.fair_value;
    const cp = currentPrice();
    if (!cp || !h) { MCX.ui.error($('fvStats'), v.house_error ? 'House model unavailable: ' + v.house_error : 'No share price yet.', mountFairValue); return; }
    const price = cp.price, pe = pb.mean;
    const l = fvLede(price, h, v);
    $('fvKicker').textContent = `Fair value · two views · ADR to ${fmt.dayMonth(v.data_quality.latest_valuation_date)}, price ${cp.label}`;
    $('fvHead').textContent = l.head;
    $('fvDeck').textContent = l.deck;
    const lo = h.blend['48'], hi = h.blend['52'];
    const implied = revenuePricedIn(price, pe, c);
    const perCr = c.trading_days * c.pat_margin / c.diluted_shares_cr * pe;
    const stateLabel = { near: ['', 'Near fair value'], below: ['up', 'Below the house range'], above: ['down', 'Above the house range'] }[l.state];
    $('fvStats').innerHTML =
      stat('Share price', `₹${num(price, 0)}`, `${cp.change !== null && cp.change !== undefined ? `<span class="${cp.change >= 0 ? 'up' : 'down'}">${cp.change >= 0 ? '▲' : '▼'} ${num(Math.abs(cp.change), 2)}%</span> today · ` : ''}NSE, ${esc(cp.label)}`, true)
      + stat('Tusk house view', `₹${num(lo, 0)} – ${num(hi, 0)}`, `<div class="stat-pill"><span class="pill${stateLabel[0] ? ' pill--' + stateLabel[0] : ''}">${stateLabel[1]}</span></div>${pct((lo / price - 1) * 100)} to ${pct((hi / price - 1) * 100)} vs price`)
      + stat('Data-driven view', `₹${num(fv.base, 0)}`, `<div class="stat-pill">${sigPill(ddSignal(price, fv))}</div>price ${Math.abs(l.prem).toFixed(1)}% ${l.prem >= 0 ? 'above' : 'below'}`)
      + stat('Revenue priced in', `₹${num(implied, 2)}<small>Cr/day</small>`, `at the data-driven multiple · 45-day average ₹${num(c.ma45_rev_cr, 2)} Cr`);

    const reg = h.regression, pvs = h.analysts.map(a => a.present_value);
    const rows = [
      { group: 'Data-driven' },
      { name: 'P/E band on run-rate EPS', sub: `${num(pb.bear_pe, 1)}–${num(pb.bull_pe, 1)}× · base ${num(pe, 1)}×`, lo: fv.bear, hi: fv.bull, mark: fv.base, kind: 'dd',
        value: `${rs(fv.base)} <tspan class="c-label2">(${num(fv.bear, 0)}–${num(fv.bull, 0)})</tspan>` },
      { group: 'Tusk house model' },
      { name: 'ADR model', sub: `${h.assumptions.pe[0]}–${h.assumptions.pe[1]}× FY28E EPS, ${Math.round(h.assumptions.hurdle * 100)}% hurdle`, lo: h.adr_leg['48'], hi: h.adr_leg['52'], mark: null, kind: 'leg', value: `${rs(h.adr_leg['48'])} – ${num(h.adr_leg['52'], 0)}` },
      reg ? { name: 'Regression', sub: 'price on 45-day ADR · 95% band', lo: reg.low, hi: reg.high, mark: reg.value, kind: 'whisker', value: rs(reg.value) } : null,
      { name: 'Analyst targets', sub: `${h.analysts.length} brokers, discounted at ${Math.round(h.assumptions.hurdle * 100)}%`, lo: Math.min(...pvs), hi: Math.max(...pvs), mark: h.analyst_leg, kind: 'leg',
        value: `${rs(h.analyst_leg)} <tspan class="c-label2">(${num(Math.min(...pvs), 0)}–${num(Math.max(...pvs), 0)})</tspan>` },
      { name: 'House blend', sub: `average of the ${reg ? 'three' : 'two'} legs`, lo, hi, mark: null, kind: 'blend', value: `${rs(lo)} – ${num(hi, 0)}` },
    ].filter(Boolean);
    const box = $('fvFootball');
    box.innerHTML = footballSvg(Math.round(box.clientWidth) || 1000, rows, price);
    $('fvFootballBasis').innerHTML = INFO + `<span>The regression is refitted on ${reg ? reg.n : '—'} sessions since ${reg ? fmt.dayMonth(reg.since) + ' ' + reg.since.slice(0, 4) : '—'} (price ≈ ${reg ? num(reg.a, 0) : '—'} + ${reg ? num(reg.b, 1) : '—'} × ADR${reg && reg.r_squared ? `, R² ${num(reg.r_squared, 2)}` : ''}). `
      + `Analyst targets are from the Tusk workbook (reports of ${esc([...new Set(h.analysts.map(a => fmt.dayMonth(a.report_date).split(' ')[1]))].join(' and '))} 2026), each discounted from its 12-month target date.</span>`;

    $('fvHouseChain').innerHTML = chain([
      ['FY27E EPS', `₹${num(h.eps27, 2)}`, `ADR ₹${num(h.adr_cr, 2)} Cr × ${h.assumptions.days.FY27} days + non-F&amp;O ₹${num(h.non_fo_fy27_cr, 0)} Cr, × ${Math.round(h.assumptions.margin * 100)}% margin, ÷ ${num(h.shares_cr, 3)} Cr shares`],
      ['FY28E EPS', `₹${num(h.eps28, 2)}`, `House assumption: ADR and non-F&amp;O both +${Math.round(h.assumptions.fy28_growth * 100)}%, over FY28’s ${h.assumptions.days.FY28} trading days`],
      [`Target price at ${fmt.dayMonth(h.assumptions.target_date)} ${h.assumptions.target_date.slice(0, 4)}`, `₹${num(h.target['48'], 0)} – ${num(h.target['52'], 0)}`, `${h.assumptions.pe[0]}–${h.assumptions.pe[1]}× forward P/E on FY28E EPS`],
      ['ADR model today', `₹${num(h.adr_leg['48'], 0)} – ${num(h.adr_leg['52'], 0)}`, `Discounted at the ${Math.round(h.assumptions.hurdle * 100)}% hurdle for ${num(h.years_to_target, 2)} years`],
      ['House blend', `₹${num(lo, 0)} – ${num(hi, 0)}`, `Average of ADR model${reg ? `, regression ₹${num(reg.value, 0)}` : ''} and analysts ₹${num(h.analyst_leg, 0)}`],
    ]);
    $('fvDdChain').innerHTML = chain([
      ['Run-rate EPS', `₹${num(c.eps, 2)}`, `ADR ₹${num(c.ma45_rev_cr, 2)} Cr × ${c.trading_days} days + non-F&amp;O ₹${num(c.non_fo_rev_cr, 1)} Cr, × ${Math.round(c.pat_margin * 100)}%, ÷ ${num(c.diluted_shares_cr, 3)} Cr shares`],
      ['Multiple', `${num(pe, 1)}×`, `Median of the stock’s own P/E on run-rate EPS; band ±1 SD = ${num(pb.bear_pe, 1)}–${num(pb.bull_pe, 1)}×`],
      ['Fair value today', `₹${num(fv.base, 0)}`, `Range ₹${num(fv.bear, 0)} – ${num(fv.bull, 0)} · no discounting: a spot multiple on current earnings`],
      ['Revenue priced in', `₹${num(implied, 2)} Cr/day`, `What today’s price needs at ${num(pe, 1)}×; each ₹1 Cr a day is worth about ₹${num(perCr, 0)} a share`],
    ]);

    $('fvWhy').innerHTML = table(['', 'Tusk house', 'Data-driven'], [
      ['Earnings used', `FY28E ₹${num(h.eps28, 2)}`, `Run-rate ₹${num(c.eps, 2)}`],
      ['Multiple', `${h.assumptions.pe[0]}–${h.assumptions.pe[1]}× forward`, `${num(pe, 1)}× median of own history`],
      ['Discounting', `${Math.round(h.assumptions.hurdle * 100)}% hurdle, ${num(h.years_to_target, 2)} years`, 'Not applied'],
      ['Other legs', reg ? 'Regression and analysts, equal weight' : 'Analysts, equal weight', 'Not used'],
      { cls: 'total', cells: ['Result', `₹${num(lo, 0)} – ${num(hi, 0)}`, `₹${num(fv.base, 0)}`] },
    ]);
    const epsGap = (h.eps27 / c.eps - 1) * 100;
    $('fvWhyBasis').innerHTML = INFO + `<span>On the same year the two methods’ EPS differ by ${num(Math.abs(epsGap), 0)}% (house FY27E ₹${num(h.eps27, 2)} against run-rate ₹${num(c.eps, 2)}); most of the gap is the growth year, the multiple and the blend.</span>`;

    $('fvSens').innerHTML = table(['FY28 growth', 'FY28E EPS', 'ADR model today', 'House blend'], h.sensitivity.map(r => ({
      cls: Math.abs(r.growth - h.assumptions.fy28_growth) < 1e-9 ? 'total' : '',
      cells: [`${Math.round(r.growth * 100)}%${Math.abs(r.growth - h.assumptions.fy28_growth) < 1e-9 ? ' <span class="muted">house</span>' : ''}`, `₹${num(r.eps28, 2)}`,
              `₹${num(r.adr_leg_48, 0)} – ${num(r.adr_leg_52, 0)}`, `₹${num(r.blend_48, 0)} – ${num(r.blend_52, 0)}`] })));
    $('fvSensBasis').innerHTML = INFO + `<span>For comparison, the ${h.street.brokers} brokers average ${num(h.street.pe, 0)}× on FY28E EPS of ₹${num(h.street.eps28, 1)}; the house uses ${h.assumptions.pe[0]}–${h.assumptions.pe[1]}× on ₹${num(h.eps28, 2)}.</span>`;

    $('fvBrokers').innerHTML = table(['Broker', 'Report', 'Rating', 'Target', 'Target date', 'Worth today', 'FY28E EPS', 'P/E on FY28E'], h.analysts.map(a => [
      esc(a.broker), fmt.dayMonth(a.report_date) + ' ' + a.report_date.slice(0, 4), esc(a.rating), `₹${num(a.target, 0)}`,
      fmt.dayMonth(a.target_date) + ' ' + a.target_date.slice(0, 4) + (a.expired ? ' <span class="muted">passed</span>' : ''), `₹${num(a.present_value, 0)}`, `₹${num(a.eps28, 1)}`, `${num(a.pe, 0)}×`]));

    renderBand();
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
  makeRangeToggle({ key: 'valChart', containerId: 'fvBandRange', ranges: ['30D', '60D', 'Q', '1Y', '2Y', 'Max'], defaultRange: '60D', labelIds: ['fvBandRangeLabel'],
                    onChange: () => fetchRanged(valUrl()).then(d => { if (d.success) { FV.data = d; renderBand(); } }) });
  onResize($('fairvalue'), () => FV.data && renderFairValue());

  MCX.value = { quarter: { mount: mountQuarter }, fairValue: { mount: mountFairValue } };
})();
