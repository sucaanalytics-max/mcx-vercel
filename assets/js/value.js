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

  MCX.valueModel = { quarterLede, adjusted, missStory, words };
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

  MCX.value = { quarter: { mount: mountQuarter } };
})();
