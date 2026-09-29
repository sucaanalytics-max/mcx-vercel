/* MCX Revenue Monitor: the Lab pages — Diagnostics, Positioning, Margins.
   Data: /api/exchange_dashboard?view=home (the live projection's measured error, as on Today),
   /api/analytics (the ensemble score against later share prices) and its hourly_accuracy section,
   /api/commodity_dashboard?view=oi_participants and ?view=margins.
   Charts are SVG coloured by CSS variables, so they follow the theme without redrawing. */
(function () {
  'use strict';
  const fmt = MCX.fmt, num = fmt.num, V = MCX.svg, esc = V.esc;

  // ════════════════════════════════════════════════════════════════════════
  //  Model: pure functions (scripts/test_lab_js.js)
  // ════════════════════════════════════════════════════════════════════════
  const signed = (v, dp = 1) => `${v >= 0 ? '+' : '−'}${num(Math.abs(v), dp)}`;
  const NAMES = { CRUDEOIL: 'Crude oil', 'CRUDE OIL': 'Crude oil', NATURALGAS: 'Natural gas', SILVER100: 'Silver 100', MENTHAOIL: 'Mentha oil',
                  COTTONCNDY: 'Cotton candy', COTTONOIL: 'Cotton oil', ELECDMBL: 'Electricity', ELECTRCITY: 'Electricity',
                  MCXBULLDEX: 'Bulldex index', MCXMETLDEX: 'Metldex index', ALL: 'All contracts' };
  const pretty = c => NAMES[c] || (String(c).charAt(0) + String(c).slice(1).toLowerCase());
  const strength = ic => Math.abs(ic) < 0.05 ? 'no useful' : Math.abs(ic) < 0.1 ? 'a weak' : Math.abs(ic) < 0.2 ? 'a modest' : 'a fair';

  // Diagnostics: how far to trust the live projection, and whether the ensemble score has been any use
  function diagLede(acc, an) {
    const grid = (acc && acc.grid) || [];
    const at = m => grid.find(g => g.min === m && g.p90_abs_pct !== null);
    const g21 = at(720), g12 = at(180), g0 = grid.find(g => g.median_pct !== null);
    const head = g21 && g12
      ? `By 21:00 the live projection has landed within ${Math.round(g21.p90_abs_pct)}% of the final on 9 days in 10; at noon a miss of ${Math.round(g12.p90_abs_pct)}% is still possible.`
      : 'Diagnostics: how far to trust the projection and the signals.';
    const parts = [];
    if (g0 && g0.median_pct > 10) parts.push(`The first projection of the day (${g0.time}) has run high: on the median day it was ${Math.round(g0.median_pct)}% above the final.`);
    const later = grid.filter(g => g.min >= 360 && g.median_pct !== null);
    if (later.length) {
      const mx = Math.max(...later.map(g => Math.abs(g.median_pct)));
      if (mx < 6) parts.push(`From 15:00 the median miss stays within ${Math.ceil(mx)}% either way.`);
    }
    const cur = an && (an.weight_sensitivity || []).find(w => w.is_current);
    const ics = an ? (an.rolling_ic || []).filter(r => r.ensemble_ic !== null) : [];
    const last = ics.length ? ics[ics.length - 1].ensemble_ic : null;
    if (cur && cur.ic !== null) {
      let s = `The ensemble score has had ${strength(cur.ic)} link to the share price over the following week (correlation ${signed(cur.ic, 2)} over the full history)`;
      if (last !== null) s += last < cur.ic - 0.05 ? `, weaker over the last 60 signal days (${signed(last, 2)})` : last > cur.ic + 0.05 ? `, stronger over the last 60 signal days (${signed(last, 2)})` : `, much the same over the last 60 signal days (${signed(last, 2)})`;
      parts.push(s + '.');
    }
    return { head, deck: parts.join(' ') };
  }

  // Pearson correlation, ignoring pairs with a missing value
  function corr(a, b) {
    const xs = [], ys = [];
    a.forEach((x, i) => { if (x !== null && x !== undefined && b[i] !== null && b[i] !== undefined) { xs.push(x); ys.push(b[i]); } });
    const n = xs.length;
    if (n < 10) return null;
    const mx = xs.reduce((p, q) => p + q, 0) / n, my = ys.reduce((p, q) => p + q, 0) / n;
    let sxy = 0, sxx = 0, syy = 0;
    for (let i = 0; i < n; i++) { sxy += (xs[i] - mx) * (ys[i] - my); sxx += (xs[i] - mx) ** 2; syy += (ys[i] - my) ** 2; }
    return sxx && syy ? sxy / Math.sqrt(sxx * syy) : null;
  }

  // Positioning: participation against a year ago, futures and options
  function posLede(g, asOf) {
    const o = g.Overall, f = g.Futures, p = g.Options;
    const head = `Participation in MCX contracts is ${Math.round(Math.abs(o.yoy_pct))}% ${o.yoy_pct >= 0 ? 'higher' : 'lower'} than a year ago, `
      + `${f.yoy_pct >= p.yoy_pct ? 'led by futures' : 'led by options'} (${signed(f.yoy_pct, 0)}% against ${signed(p.yoy_pct, 0)}% for ${f.yoy_pct >= p.yoy_pct ? 'options' : 'futures'}).`;
    const parts = [`${num(o.current)} participants counted across contracts on ${fmt.dayMonth(asOf)}: ${signed(o.wow_pct)}% on the week and ${signed(o.mom_pct)}% on the month.`];
    const big = [['Futures', f], ['Options', p]].filter(([, x]) => Math.abs(x.wow_pct) >= 10);
    big.forEach(([k, x]) => parts.push(`${k} participation ${x.wow_pct > 0 ? 'rose' : 'fell'} ${Math.round(Math.abs(x.wow_pct))}% in the week.`));
    return { head, deck: parts.join(' ') };
  }

  // Category mix of one contract: each category's long + short counts as a share of all of them.
  // MCX reports counts under 10 as −1; those count as zero here.
  const CATS = [['hedgers', 'Hedgers', ['fpo', 'vcp']], ['prop', 'Prop traders', ['prop']], ['inst', 'Institutions', ['dfi', 'foreign']], ['others', 'Others', ['others']]];
  function mix(p) {
    const v = k => Math.max(0, p[k + '_long'] || 0) + Math.max(0, p[k + '_short'] || 0);
    const parts = CATS.map(([key, lab, ks]) => ({ key, label: lab, n: ks.reduce((a, k) => a + v(k), 0) }));
    const tot = parts.reduce((a, x) => a + x.n, 0);
    parts.forEach(x => { x.share = tot ? x.n / tot * 100 : 0; });
    return { parts, suppressed: Object.keys(p).some(k => /_(long|short)$/.test(k) && p[k] === -1) };
  }

  // Margins: the latest snapshot's changes. Total margin includes the tender margin MCX adds as a
  // contract nears expiry, so a rise in a contract now carrying one is the expiry cycle, not a risk call.
  const WORDS = ['No', 'One', 'Two', 'Three', 'Four', 'Five', 'Six', 'Seven', 'Eight', 'Nine', 'Ten', 'Eleven', 'Twelve'];
  const nWord = (n, lower) => { const w = n < WORDS.length ? WORDS[n] : String(n); return lower ? w.toLowerCase() : w; };
  function marginLede(changes, current) {
    if (!changes || !changes.length) return { head: 'No margin changes on record.', deck: '' };
    const d = changes[0].date, day = changes.filter(c => c.date === d);
    const tenderNow = new Set((current || []).filter(r => r.tender_margin_pct > 0).map(r => r.symbol));
    const tender = day.filter(c => c.change > 0 && tenderNow.has(c.symbol)), other = day.filter(c => !tender.includes(c));
    const pts = xs => { const a = Math.min(...xs.map(c => Math.abs(c.change))), b = Math.max(...xs.map(c => Math.abs(c.change))); return Math.abs(a - b) < 0.05 ? `${num(a, 1)} points` : `${num(a, 1)} to ${num(b, 1)} points`; };
    const cs = n => `contract${n === 1 ? '' : 's'}`;
    let head;
    if (tender.length) {
      head = `${nWord(tender.length)} ${cs(tender.length)} entered ${tender.length === 1 ? 'its' : 'their'} tender period in the ${fmt.dayMonth(d)} snapshot, adding ${pts(tender)} of margin`
        + (other.length ? `; ${nWord(other.length, true)} other margin${other.length === 1 ? '' : 's'} moved by ${pts(other)}.` : '.');
    } else {
      const up = day.filter(c => c.change > 0), dn = day.filter(c => c.change < 0);
      head = up.length && !dn.length ? `MCX raised margins on ${nWord(up.length, true)} ${cs(up.length)} in the ${fmt.dayMonth(d)} snapshot, by ${pts(up)}.`
        : dn.length && !up.length ? `MCX cut margins on ${nWord(dn.length, true)} ${cs(dn.length)} in the ${fmt.dayMonth(d)} snapshot, by ${pts(dn)}.`
        : `MCX changed margins on ${nWord(day.length, true)} ${cs(day.length)} in the ${fmt.dayMonth(d)} snapshot: ${up.length} up, ${dn.length} down.`;
    }
    const parts = ['A tender margin is charged as a contract nears expiry and falls away after it, so most swings in total margin follow the expiry cycle rather than a change in MCX’s risk settings.'];
    const top = (current || []).reduce((a, r) => { const x = r.total_margin_pct - (r.tender_margin_pct || 0); return !a || x > a.x ? { r, x } : a; }, null);
    if (top) parts.push(`Leaving tender margins out, the highest now is ${pretty(top.r.symbol)} at ${num(top.x, 1)}%.`);
    const cut = Date.parse(d) - 30 * 86400000;
    const recent = changes.filter(c => Date.parse(c.date) >= cut);
    parts.push(`Total margins changed ${recent.length} time${recent.length === 1 ? '' : 's'} in the last 30 days, tender margins included.`);
    return { head, deck: parts.join(' '), tender: tender.length };
  }

  MCX.labModel = { diagLede, corr, posLede, mix, marginLede, pretty, signed };
  if (window.MCX_TEST) return;

  // ════════════════════════════════════════════════════════════════════════
  //  Shared page pieces
  // ════════════════════════════════════════════════════════════════════════
  const $ = id => document.getElementById(id);
  const txt = V.text;
  const INFO = '<svg aria-hidden="true" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M12 11v5"/><path d="M12 8h.01"/></svg>';
  const stat = (lab, value, sub, lead) => `<div class="stat${lead ? ' stat--lead' : ''}"><div class="stat-label">${lab}</div><div class="stat-value">${value}</div>${sub ? `<div class="stat-sub">${sub}</div>` : ''}</div>`;
  const pctCell = (v, dp = 1) => v === null || v === undefined ? '—' : `<span class="${Math.abs(v) < 0.05 ? '' : v > 0 ? 'up' : 'down'}">${signed(v, dp)}%</span>`;
  const table = (head, rows, cls) => `<div class="table-scroll"><table class="v2-table${cls ? ' ' + cls : ''}"><thead><tr>${head.map(h => `<th scope="col">${h}</th>`).join('')}</tr></thead><tbody>`
    + rows.map(r => Array.isArray(r) ? `<tr>${r.map(c => `<td>${c}</td>`).join('')}</tr>` : `<tr class="${r.cls || ''}">${r.cells.map(c => `<td>${c}</td>`).join('')}</tr>`).join('') + '</tbody></table></div>';
  const path = (vals, X, Y) => {
    let d = '', pen = false;
    vals.forEach((v, i) => { if (v === null || v === undefined) { pen = false; return; } d += `${pen ? 'L' : 'M'}${X(i).toFixed(1)},${Y(v).toFixed(1)}`; pen = true; });
    return d;
  };
  const xLabel = (iso, long) => long ? `${fmt.dayMonth(iso)} ’${iso.slice(2, 4)}` : fmt.dayMonth(iso);
  function dateTicks(dates, X, y, compact) {
    const n = dates.length, k = compact ? 3 : 5, long = Date.parse(dates[n - 1]) - Date.parse(dates[0]) > 300 * 86400000, out = [];
    for (let j = 0; j < k; j++) {
      const i = Math.round(j * (n - 1) / (k - 1));
      out.push(txt(X(i), y, 'c-tick', xLabel(dates[i], long), j === 0 ? 'start' : j === k - 1 ? 'end' : 'middle'));
    }
    return out.join('');
  }
  const sliceDays = (dates, key) => {
    const n = RANGE_TRADING_DAYS[key];
    return n ? Math.max(0, dates.length - n) : 0;
  };
  // Cut by calendar days from the last date, for irregular snapshot series
  const CAL_DAYS = { '30D': 30, '60D': 60, 'Q': 92, '1Y': 366, '2Y': 731, 'Max': null };
  const sliceCal = (dates, key) => {
    const d = CAL_DAYS[key];
    if (!d || !dates.length) return 0;
    const cut = Date.parse(dates[dates.length - 1]) - d * 86400000;
    const i = dates.findIndex(x => Date.parse(x) >= cut);
    return i < 0 ? 0 : i;
  };

  // ════════════════════════════════════════════════════════════════════════
  //  Diagnostics
  // ════════════════════════════════════════════════════════════════════════
  const DG = { home: null, an: null, width: 0, hourly: null };
  const TM = MCX.todayModel;

  function loadDiagnostics() {
    const a = fetchRanged('/api/exchange_dashboard?view=home').then(d => { if (d.success) DG.home = d; });
    const b = fetchRanged('/api/analytics').then(d => { if (!d.success) throw new Error(d.error || 'No data'); DG.an = d; });
    return Promise.allSettled([a, b]).then(rs => {
      DG.err = rs.map(r => r.status === 'rejected' ? (r.reason && r.reason.message) || String(r.reason) : null).filter(Boolean).join('; ');
      renderDiagnostics();
    });
  }

  function renderDiagnostics() {
    const acc = DG.home && DG.home.accuracy, an = DG.an;
    if (!acc && !an) { if (DG.err) MCX.ui.error($('dgTrust'), 'Could not load diagnostics: ' + DG.err, loadDiagnostics); return; }
    const l = diagLede(acc, an);
    $('dgKicker').textContent = acc ? `Diagnostics · measured on ${acc.sessions} normal trading days, ${fmt.span(acc.first, acc.last)}` : 'Diagnostics';
    $('dgHead').textContent = l.head;
    $('dgDeck').textContent = l.deck;
    if (acc) renderTrust(acc);
    if (an) { renderIc(an); renderWeights(an); renderCorr(an); renderState(an); }
    DG.width = $('diagnostics').clientWidth;
  }

  function renderTrust(acc) {
    const box = $('dgTrust'), w = Math.round(box.clientWidth) || 800;
    box.innerHTML = MCX.today.trustSvg(w, acc.grid, null, null);
    const fb = g => { const b = TM.finalBand(g.q10_pct, g.q90_pct); return b.above === null ? '—' : TM.rangePct(-b.below, b.above); };
    // Below the projection is not a loss, so no direction colour
    const medFinal = g => g.median_pct === null ? '—' : `${signed((1 / (1 + g.median_pct / 100) - 1) * 100, 0)}%`;
    const rows = acc.grid.filter(g => g.q10_pct !== null && (g.min % 60 === 30 || g.min === acc.grid[acc.grid.length - 1].min));
    $('dgTrustTable').innerHTML = table(['Projection made at', 'Days', 'Final against the projection, 8 days in 10', 'Median final', 'Within, 9 days in 10'],
      rows.map(g => [esc(g.time), num(g.n), fb(g), medFinal(g), `±${num(g.p90_abs_pct, 0)}%`]), 'dg-trust');
    const left = [acc.excluded_part_day ? `${acc.excluded_part_day} part-day session${acc.excluded_part_day === 1 ? '' : 's'}` : '',
                  acc.excluded_us_holiday ? `${acc.excluded_us_holiday} US market holiday${acc.excluded_us_holiday === 1 ? '' : 's'}` : ''].filter(Boolean);
    $('dgTrustBasis').innerHTML = INFO + `<span>The projections are the ones the page showed live, stored every 15 minutes, compared with each day’s final revenue. `
      + `Error = projection ÷ final − 1; “final against the projection” turns that round, so −20% means the day closed 20% below what was projected${left.length ? `. Left out: ${left.join(' and ')}` : ''}. This is the range Today uses.</span>`;
  }

  // Rolling IC (upper) and the 60-day Sharpe of a long/short rule (lower), one time axis
  function renderIc(an) {
    const box = $('dgIc'), w = Math.round(box.clientWidth) || 900, compact = w < 520;
    const ic = an.rolling_ic || [], pm = an.rolling_metrics || [];
    const byDate = {}; pm.forEach(r => { byDate[r.date] = r; });
    const k0 = sliceDays(ic.map(r => r.date), rangeState.icChart || '1Y');
    const rows = ic.slice(k0);
    if (rows.length < 2) { box.innerHTML = '<p class="note">Not enough history.</p>'; return; }
    const dates = rows.map(r => r.date), icv = rows.map(r => r.ensemble_ic), sh = rows.map(r => (byDate[r.date] || {}).sharpe_ratio ?? null);
    const pl = compact ? 40 : 52, pr = compact ? 10 : 118, aTop = 10, aH = compact ? 120 : 150, gap = 26, bH = compact ? 110 : 140;
    const bTop = aTop + aH + gap, h = bTop + bH + 26, iw = w - pl - pr, X = i => pl + iw * i / (rows.length - 1);
    const lim = (vals, floor) => { const xs = vals.filter(v => v !== null); const m = Math.max(floor, ...xs.map(Math.abs)) * 1.1; return [-m, m]; };
    const [aLo, aHi] = lim(icv, 0.3), [bLo, bHi] = lim(sh, 2);
    const YA = v => aTop + aH * (1 - (v - aLo) / (aHi - aLo)), YB = v => bTop + bH * (1 - (v - bLo) / (bHi - bLo));
    const s = [V.open(w, h, `Rolling correlation of the ensemble score with the following 5-day return, and the 60-day Sharpe ratio of a long/short rule, ${fmt.span(dates[0], dates[dates.length - 1])}`)];
    V.niceTicks(aHi, 4, aLo).forEach(v => s.push(`<line x1="${pl}" x2="${w - pr}" y1="${YA(v).toFixed(1)}" y2="${YA(v).toFixed(1)}" class="${Math.abs(v) < 1e-9 ? 'c-axis' : 'c-grid'}"/>`, txt(pl - 8, YA(v) + 4, 'c-tick', signed(v, 1), 'end')));
    V.niceTicks(bHi, 4, bLo).forEach(v => s.push(`<line x1="${pl}" x2="${w - pr}" y1="${YB(v).toFixed(1)}" y2="${YB(v).toFixed(1)}" class="${Math.abs(v) < 1e-9 ? 'c-axis' : 'c-grid'}"/>`, txt(pl - 8, YB(v) + 4, 'c-tick', signed(v, 0), 'end')));
    s.push(`<path d="${path(icv, X, YA)}" class="c-line"/>`, `<path d="${path(sh, X, YB)}" class="c-line"/>`);
    s.push(txt(pl + 4, aTop + 12, 'c-label3', 'Correlation with the next 5 days’ return, 60-day window', null, true));
    s.push(txt(pl + 4, bTop + 12, 'c-label3', 'Sharpe ratio of long-if-positive, short-if-negative, 60 days', null, true));
    if (!compact) {
      const li = icv[icv.length - 1], ls = sh[sh.length - 1];
      if (li !== null) s.push(txt(w - pr + 10, YA(li) + 4, 'c-label', `IC ${signed(li, 2)}`));
      if (ls !== null) s.push(txt(w - pr + 10, YB(ls) + 4, 'c-label', `Sharpe ${signed(ls, 1)}`));
    }
    s.push(dateTicks(dates, X, h - 6, compact), '</svg>');
    box.innerHTML = s.join('');
    V.hover(box, { pl, pt: aTop, iw, ih: bTop + bH - aTop, h, pb: h - bTop - bH }, rows.map((_, i) => X(i)), i => {
      const m = byDate[dates[i]] || {};
      return `<strong>${fmt.day(dates[i])}</strong><br>Correlation ${icv[i] === null ? '—' : signed(icv[i], 2)}`
        + `<br><span class="c-tip-sub">Sharpe ${m.sharpe_ratio === undefined ? '—' : signed(m.sharpe_ratio, 2)} · win rate ${m.win_rate === undefined ? '—' : num(m.win_rate * 100, 0) + '%'}</span>`;
    });
    const all = ic.filter(r => r.ensemble_ic !== null), pos = all.filter(r => r.ensemble_ic > 0).length;
    const lm = pm[pm.length - 1], cur = (an.weight_sensitivity || []).find(x => x.is_current);
    $('dgIcStats').innerHTML = stat('Full history', cur ? signed(cur.ic, 2) : '—', 'correlation of the score with the next 5 days’ return', true)
      + stat('Windows with a positive link', all.length ? `${num(pos / all.length * 100, 0)}%` : '—', `of ${num(all.length)} rolling 60-day windows`)
      + stat('Latest 60 days', all.length ? signed(all[all.length - 1].ensemble_ic, 2) : '—', 'correlation, using returns already known')
      + stat('Long/short rule, latest 60 days', lm ? signed(lm.sharpe_ratio, 1) : '—', lm ? `Sharpe; right on ${num(lm.win_rate * 100, 0)}% of days, before costs` : '');
  }

  function renderWeights(an) {
    const ws = an.weight_sensitivity || [];
    $('dgWeights').innerHTML = !ws.length ? '<p class="note">Not enough history.</p>' : table(['Price gap', 'Activity', 'Correlation, next 5 days', 'Direction right'],
      ws.map(r => ({ cls: r.is_current ? 'total' : '', cells: [`${num(r.ecm_weight * 100, 0)}%${r.is_current ? ' <span class="muted">current</span>' : ''}`, `${num(r.mf_weight * 100, 0)}%`,
        r.ic === null ? '—' : signed(r.ic, 3), `${num(r.hit_rate * 100, 1)}%`] })), 'dg-fit');
  }

  function renderCorr(an) {
    const fs = an.factor_series || {}, dates = fs.dates || [];
    const k0 = sliceDays(dates, rangeState.corrTable || '1Y');
    const S = { ecm: (fs.ecm_z || []).slice(k0).map(v => (v === null ? null : -v)), rev: (fs.rev_z || []).slice(k0), turn: (fs.turn_z || []).slice(k0), pos: (fs.position_score || []).slice(k0) };
    const labs = { ecm: 'Price gap, flipped', rev: 'Revenue', turn: 'Turnover', pos: 'Position' };
    const short = { ecm: 'Gap', rev: 'Revenue', turn: 'Turnover', pos: 'Position' };
    const keys = Object.keys(S);
    const cell = (a, b) => {
      if (a === b) return '<span class="muted">—</span>';
      const r = corr(S[a], S[b]);
      return r === null ? '—' : `<span class="dg-r" style="--r:${Math.abs(r).toFixed(2)}">${signed(r, 2)}</span>`;
    };
    $('dgCorr').innerHTML = table([''].concat(keys.map(k => short[k])), keys.map(a => [labs[a]].concat(keys.map(b => cell(a, b)))), 'dg-corr dg-fit');
    const rt = corr(S.rev, S.turn);
    $('dgCorrBasis').innerHTML = INFO + `<span>${dates.length ? `${fmt.span(dates[k0], dates[dates.length - 1])}. ` : ''}`
      + (rt !== null && rt > 0.7 ? `Revenue and turnover move closely together (${signed(rt, 2)}), so the exchange-activity model is close to a single factor. ` : '')
      + 'Shading grows with the size of the correlation, whichever its sign.</span>';
  }

  function renderState(an) {
    const r = an.regime || {}, hm = (an.hmm_regime || {}).current || {}, vol = r.volatility || {};
    const band = { BULL: 'Long', BEAR: 'Short', NEUTRAL: 'Neutral' }[r.current] || '—';
    const state = { BULL: 'Steady long', BEAR: 'Steady short', TRANSITION: 'Choppy', NEUTRAL: 'Flat' }[hm.state] || '—';
    $('dgState').innerHTML = stat('Ensemble position', band, `${r.duration_days ? `for ${r.duration_days} trading day${r.duration_days === 1 ? '' : 's'}; ` : ''}long above +0.25, short below −0.25 · history: ${num(r.bull_days || 0)} long, ${num(r.neutral_days || 0)} neutral, ${num(r.bear_days || 0)} short days`, true)
      + stat('Last 20 days of the signal', state, hm.avg_position !== undefined ? `average position ${signed(hm.avg_position, 2)}, swinging ${num(hm.position_vol, 2)} (choppy above 0.30)` : '')
      + stat('Share price volatility', vol.annualized_pct ? `${num(vol.annualized_pct, 1)}%` : '—', vol.regime ? `annualised over 20 days; ${vol.regime.toLowerCase()} (below 20% low, above 35% high)` : '');
  }

  function renderHourly(d) {
    const ss = d.signal_stability || [], fa = d.forward_accuracy || [];
    const byLab = {}; fa.forEach(r => { byLab[r.label] = r; });
    $('dgHourly').innerHTML = !ss.length ? '<p class="note">Not enough history.</p>' : table(['Time', 'Days', 'Intraday signal matches the close', 'Score error against the close', 'Direction right next day', 'Direction right over 5 days'],
      ss.map(r => { const f = byLab[r.label] || {}; return [esc(r.label), num(r.n), `${num(r.signal_match_rate * 100, 0)}%`, num(r.ensemble_score_mae, 2),
        f.hit_rate_1d === undefined ? '—' : `${num(f.hit_rate_1d * 100, 0)}%`, f.hit_rate_5d === undefined ? '—' : `${num(f.hit_rate_5d * 100, 0)}%`]; }), 'dg-hourly');
    const cv = d.convergence || {};
    $('dgHourlyBasis').innerHTML = INFO + `<span>Re-runs the ensemble at each hour of the last ${(d.data_quality || {}).lookback_days || 90} days, feeding it that hour’s projected revenue instead of the final. `
      + (cv.signal_90pct ? `The intraday signal first matches the close on 9 days in 10 at ${esc(cv.signal_90pct.label)}. ` : '')
      + 'Direction right compares the signal’s sign with the share price over the next day and 5 days, from that day’s close; readings after 15:30, when the NSE has shut, could only be acted on a day later.</span>';
  }

  makeRangeToggle({ key: 'icChart', containerId: 'dgIcRange', ranges: ['Q', '1Y', '2Y', 'Max'], defaultRange: '1Y', labelIds: ['dgIcRangeLabel'], onChange: () => DG.an && renderIc(DG.an) });
  makeRangeToggle({ key: 'corrTable', containerId: 'dgCorrRange', ranges: ['Q', '1Y', '2Y', 'Max'], defaultRange: '1Y', labelIds: ['dgCorrRangeLabel'], onChange: () => DG.an && renderCorr(DG.an) });
  (function lazyHourly() {
    const box = $('dgHourly');
    let started = false;
    const go = () => {
      if (started) return; started = true;
      box.innerHTML = '<span class="skel"></span> <span class="note">Re-running 90 days of snapshots, about 15 seconds…</span>';
      fetchRanged('/api/analytics?section=hourly_accuracy').then(d => { if (!d.success) throw new Error(d.error || 'No data'); renderHourly(d); })
        .catch(e => MCX.ui.error(box, 'Could not load the intraday check: ' + (e.message || e), () => { started = false; go(); }));
    };
    $('dgHourlyBtn').addEventListener('click', go);
    if (window.IntersectionObserver) {
      const io = new IntersectionObserver(es => { if (es.some(e => e.isIntersecting) && !$('tabAnalytics').hidden) { io.disconnect(); go(); } }, { rootMargin: '200px' });
      io.observe(box);
    }
  })();

  // ════════════════════════════════════════════════════════════════════════
  //  Positioning
  // ════════════════════════════════════════════════════════════════════════
  const PS = { data: null, inst: 'Futures', width: 0 };
  function loadPositioning() {
    return fetchRanged('/api/commodity_dashboard?view=oi_participants').then(d => {
      if (!d.success) throw new Error(d.error || 'No data');
      PS.data = d; renderPositioning();
    }).catch(e => MCX.ui.error($('psChart'), 'Could not load participation: ' + (e.message || e), loadPositioning));
  }

  function renderPositioning() {
    const d = PS.data, g = d.growth_overall;
    const l = posLede(g, d.as_of);
    $('psKicker').textContent = `Positioning · MCX participant disclosure, ${fmt.dayMonth(d.as_of)}`;
    $('psHead').textContent = l.head;
    $('psDeck').textContent = l.deck;
    const top = (d.participants || []).reduce((a, p) => (!a || p.total_participation > a.total_participation ? p : a), null);
    $('psStats').innerHTML = stat('All contracts', num(g.Overall.current), `${pctCell(g.Overall.yoy_pct, 0)} on a year ago · ${pctCell(g.Overall.wow_pct)} on the week`, true)
      + stat('Futures', num(g.Futures.current), `${pctCell(g.Futures.yoy_pct, 0)} on a year ago · ${pctCell(g.Futures.wow_pct)} on the week`)
      + stat('Options', num(g.Options.current), `${pctCell(g.Options.yoy_pct, 0)} on a year ago · ${pctCell(g.Options.wow_pct)} on the week`)
      + stat('Most participants', top ? esc(pretty(top.commodity)) : '—', top ? `${esc(top.instrument.toLowerCase())}: ${num(top.total_participation)}` : '');
    renderPsChart();
    renderPsTables();
    PS.width = $('positioning').clientWidth;
  }

  // Futures and options participants, stacked (they add up to all contracts)
  function renderPsChart() {
    const t = PS.data.trend, box = $('psChart');
    const all = t.dates || [], k0 = sliceDays(all, rangeState.oipHero || '1Y');
    const dates = all.slice(k0), fu = (t.ALL_Futures || {}).total || [], op = (t.ALL_Options || {}).total || [];
    const F = fu.slice(k0), O = op.slice(k0);
    if (dates.length < 2) { box.innerHTML = '<p class="note">Not enough history.</p>'; return; }
    const w = Math.round(box.clientWidth) || 900, compact = w < 520, h = compact ? 220 : 280;
    const top = Math.max(...dates.map((_, i) => (F[i] || 0) + (O[i] || 0))) * 1.08;
    const f = V.frame(w, h, top, [compact ? 48 : 60, compact ? 10 : 120, 10, 26]);
    const X = i => f.pl + f.iw * i / (dates.length - 1);
    const area = (lo, hi) => {
      const up = dates.map((_, i) => `${X(i).toFixed(1)},${f.y(hi(i)).toFixed(1)}`), dn = dates.map((_, i) => `${X(i).toFixed(1)},${f.y(lo(i)).toFixed(1)}`).reverse();
      return `M${up.join('L')}L${dn.join('L')}Z`;
    };
    const s = [V.open(w, h, `Participants in futures and options, ${fmt.span(dates[0], dates[dates.length - 1])}`)];
    s.push(V.grid(f, V.niceTicks(top, compact ? 3 : 4), v => v >= 1000 ? `${num(v / 1000, 0)}k` : num(v, 0)));
    s.push(`<path d="${area(() => 0, i => F[i] || 0)}" class="c-fut"/>`, `<path d="${area(i => F[i] || 0, i => (F[i] || 0) + (O[i] || 0))}" class="c-opt"/>`);
    s.push(`<path d="${path(dates.map((_, i) => F[i] || 0), X, f.y)}" class="c-sep"/>`);
    if (!compact) {
      const n = dates.length - 1, items = [{ y: f.y((F[n] || 0) / 2) + 4, s: `Futures ${num(F[n] || 0)}`, cls: 'c-label2' },
        { y: f.y((F[n] || 0) + (O[n] || 0) / 2) + 4, s: `Options ${num(O[n] || 0)}`, cls: 'c-label2' },
        { y: f.y((F[n] || 0) + (O[n] || 0)) + 4, s: `All ${num((F[n] || 0) + (O[n] || 0))}`, cls: 'c-label' }];
      V.spread(items, 15, f.pt, f.h - f.pb).forEach(it => s.push(txt(f.w - f.pr + 10, it.y, it.cls, it.s)));
    }
    s.push(dateTicks(dates, X, h - 6, compact), '</svg>');
    box.innerHTML = s.join('');
    V.hover(box, f, dates.map((_, i) => X(i)), i => `<strong>${fmt.day(dates[i])}</strong><br>All ${num((F[i] || 0) + (O[i] || 0))}<br><span class="c-tip-sub">Futures ${num(F[i] || 0)} · options ${num(O[i] || 0)}</span>`);
  }

  function renderPsTables() {
    const d = PS.data, inst = PS.inst;
    document.querySelectorAll('#psInst button').forEach(b => b.setAttribute('aria-pressed', String(b.dataset.inst === inst)));
    const ps = (d.participants || []).filter(p => p.instrument === inst).sort((a, b) => b.total_participation - a.total_participation);
    let supp = false;
    $('psMix').innerHTML = !ps.length ? '<p class="note">No contracts.</p>' : table(['Contract', 'Participants', 'Mix of long and short positions', ...CATS.map(c => c[1])], ps.map(p => {
      const m = mix(p); supp = supp || m.suppressed;
      const bar = `<span class="ps-mix" role="img" aria-label="${esc(m.parts.map(x => `${x.label} ${num(x.share, 1)}%`).join(', '))}">${m.parts.map(x => `<i class="ps-${x.key}" style="width:${x.share.toFixed(2)}%"></i>`).join('')}</span>`;
      if (!m.parts.some(x => x.n)) return [esc(pretty(p.commodity)), num(p.total_participation), '<span class="muted">every category under 10, withheld</span>', ...m.parts.map(() => '—')];
      return [esc(pretty(p.commodity)), num(p.total_participation), bar, ...m.parts.map(x => `${num(x.share, x.share < 10 ? 1 : 0)}%`)];
    }), 'ps-table');
    $('psMixLegend').innerHTML = CATS.map(([k, lab]) => `<span><i class="sw ps-${k}"></i>${lab}</span>`).join('');
    $('psMixBasis').innerHTML = INFO + '<span>Each category’s long and short participants as a share of all of them in the contract. Hedgers are value-chain participants and farmer producer organisations; institutions are domestic financial institutions and foreign participants. '
      + (supp ? 'MCX withholds counts under 10; those count as zero here. ' : '') + 'A participant holding both long and short positions appears on both sides.</span>';
    const gr = (d.growth || []).filter(r => r.instrument === inst).sort((a, b) => b.current - a.current);
    $('psGrowth').innerHTML = table(['Contract', 'Participants', 'Week', 'Month', 'Quarter', 'Year'],
      gr.map(r => [esc(pretty(r.commodity)), num(r.current), pctCell(r.wow_pct), pctCell(r.mom_pct), pctCell(r.qoq_pct), pctCell(r.yoy_pct)]), 'ps-table');
  }

  makeRangeToggle({ key: 'oipHero', containerId: 'psRange', ranges: ['Q', '1Y', '2Y', 'Max'], defaultRange: '1Y', labelIds: ['psRangeLabel'], onChange: () => PS.data && renderPsChart() });
  $('psInst').addEventListener('click', e => { const b = e.target.closest('button[data-inst]'); if (b && PS.data && b.dataset.inst !== PS.inst) { PS.inst = b.dataset.inst; renderPsTables(); } });

  // ════════════════════════════════════════════════════════════════════════
  //  Margins
  // ════════════════════════════════════════════════════════════════════════
  const MG = { data: null, width: 0, all: false };
  function loadMargins() {
    return fetchRanged('/api/commodity_dashboard?view=margins').then(d => {
      if (!d.success) throw new Error(d.error || 'No data');
      MG.data = d; renderMargins();
    }).catch(e => MCX.ui.error($('mgTable'), 'Could not load margins: ' + (e.message || e), loadMargins));
  }

  function renderMargins() {
    const d = MG.data, cur = (d.current_margins || []).slice().sort((a, b) => b.total_margin_pct - a.total_margin_pct);
    const l = marginLede(d.margin_changes, cur);
    $('mgKicker').textContent = `Margins · MCX clearing margins, snapshot of ${fmt.dayMonth(d.as_of)}`;
    $('mgHead').textContent = l.head;
    $('mgDeck').textContent = l.deck;
    const ex = r => r.total_margin_pct - (r.tender_margin_pct || 0);
    const byEx = cur.slice().sort((a, b) => ex(b) - ex(a));
    const inTender = cur.filter(r => r.tender_margin_pct > 0);
    const ch = d.margin_changes || [], cut = ch.length ? Date.parse(ch[0].date) - 30 * 86400000 : 0, recent = ch.filter(c => Date.parse(c.date) >= cut);
    $('mgStats').innerHTML = stat('Highest, leaving out tender', byEx.length ? `${num(ex(byEx[0]), 1)}%` : '—', byEx.length ? esc(pretty(byEx[0].symbol)) : '', true)
      + stat('In their tender period', `${inTender.length} of ${cur.length}`, inTender.length ? esc(inTender.map(r => pretty(r.symbol)).join(', ')) : 'none')
      + stat('Changes, last 30 days', num(recent.length), `${recent.filter(c => c.change > 0).length} up, ${recent.filter(c => c.change < 0).length} down, tender margins included`);
    const lc = r => r.change_pct ? `${pctCellPts(r.change_pct)}<small>${r.last_change_date ? fmt.dayMonth(r.last_change_date) : ''}</small>` : `<span class="muted">no change</span>${r.last_change_date ? `<small>since ${fmt.dayMonth(r.last_change_date)}</small>` : ''}`;
    $('mgTable').innerHTML = table(['Contract', 'Total', 'Tender', 'Excluding tender', 'Initial', 'ELM long / short', 'Additional long / short', 'Special long / short', 'Last change'],
      cur.map(r => [esc(pretty(r.symbol)), `<strong>${num(r.total_margin_pct, 2)}%</strong>`, r.tender_margin_pct ? `${num(r.tender_margin_pct, 2)}%` : '<span class="muted">—</span>', `${num(ex(r), 2)}%`,
        `${num(r.initial_margin_pct, 2)}%`, `${num(r.elm_long_pct, 2)} / ${num(r.elm_short_pct, 2)}`,
        `${num(r.additional_long_pct, 2)} / ${num(r.additional_short_pct, 2)}`, `${num(r.special_long_pct, 2)} / ${num(r.special_short_pct, 2)}`, lc(r)]), 'mg-table');
    renderMgHistory();
    renderMgChanges();
    MG.width = $('margins').clientWidth;
  }
  // A margin rise is not good or bad in itself: the arrow gives the direction, in plain ink
  const pctCellPts = v => `<span class="mg-chg">${v > 0 ? '▲' : '▼'} ${signed(v, 2)} pts</span>`;

  // One small panel per contract, on a shared scale
  function renderMgHistory() {
    const d = MG.data, hs = d.margin_history || {}, all = hs.dates || [], box = $('mgHistory');
    const k0 = sliceCal(all, rangeState.marginHist || '1Y'), dates = all.slice(k0);
    const syms = (d.current_margins || []).slice().sort((a, b) => b.total_margin_pct - a.total_margin_pct).map(r => r.symbol).filter(s => hs[s]);
    if (dates.length < 2 || !syms.length) { box.innerHTML = '<p class="note">Not enough history.</p>'; return; }
    const vmax = Math.max(...syms.flatMap(s => hs[s].slice(k0).filter(v => v !== null))) * 1.08;
    const cw = Math.round(box.clientWidth) || 900, cols = cw < 520 ? 2 : cw < 900 ? 3 : 4, gap = 20;
    const w = Math.floor((cw - gap * (cols - 1)) / cols), h = 96;
    const t0 = Date.parse(dates[0]), t1 = Date.parse(dates[dates.length - 1]);
    const X = iso => 4 + (w - 8) * (Date.parse(iso) - t0) / Math.max(t1 - t0, 1);
    const Y = v => 30 + (h - 38) * (1 - v / vmax);
    box.style.setProperty('--mg-cols', cols);
    box.innerHTML = syms.map(sym => {
      const vals = hs[sym].slice(k0);
      let dd = '', pen = false, last = null;
      vals.forEach((v, i) => {
        if (v === null || v === undefined) { pen = false; return; }
        const x = X(dates[i]).toFixed(1), y = Y(v).toFixed(1);
        dd += pen ? `H${x}V${y}` : `M${x},${y}`; pen = true; last = v;   // step line: margins change on a date
      });
      const first = vals.find(v => v !== null && v !== undefined);
      const chg = first !== undefined && last !== null ? last - first : null;
      return `<figure class="mg-cell"><svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}" role="img" aria-label="${esc(pretty(sym))}: total margin ${last === null ? 'not available' : num(last, 2) + '%'}${chg !== null ? `, ${signed(chg, 2)} points over the range` : ''}">`
        + `<line x1="4" x2="${w - 4}" y1="${Y(0).toFixed(1)}" y2="${Y(0).toFixed(1)}" class="c-axis"/>`
        + `<path d="${dd}" class="c-line mg-line"/>`
        + txt(4, 13, 'c-label', esc(pretty(sym))) + txt(w - 4, 13, 'c-label', last === null ? '—' : `${num(last, 1)}%`, 'end')
        + (chg !== null && Math.abs(chg) >= 0.05 ? txt(w - 4, 26, 'c-label2', `${chg > 0 ? '▲' : '▼'} ${signed(chg, 1)} pts`, 'end') : '')
        + '</svg></figure>';
    }).join('');
    $('mgHistBasis').innerHTML = INFO + `<span>The short spikes are tender periods before each expiry. Total margin, % of contract value, at each snapshot from ${fmt.dayMonth(dates[0])} ${dates[0].slice(0, 4)} to ${fmt.dayMonth(dates[dates.length - 1])}; every panel runs from 0 to ${num(vmax, 0)}%, so heights compare across contracts. The change is over the range shown.</span>`;
  }

  function renderMgChanges() {
    const d = MG.data, all = d.margin_changes || [];
    const days = CAL_DAYS[rangeState.marginChanges || '60D'];
    const cut = days && all.length ? Date.parse(all[0].date) - days * 86400000 : -Infinity;
    const rows = all.filter(c => Date.parse(c.date) >= cut);
    const shown = MG.all ? rows : rows.slice(0, 30);
    $('mgChanges').innerHTML = !rows.length ? '<p class="note">No changes in this range.</p>' : table(['Snapshot', 'Contract', 'From', 'To', 'Change'],
      shown.map(c => [fmt.day(c.date), esc(pretty(c.symbol)), `${num(c.old_total, 2)}%`, `${num(c.new_total, 2)}%`, pctCellPts(c.change)]), 'mg-changes')
      + (rows.length > 30 ? `<button type="button" class="more-btn" id="mgMore">${MG.all ? 'Show the latest 30' : `Show all ${rows.length}`}</button>` : '');
    const b = $('mgMore');
    if (b) b.addEventListener('click', () => { MG.all = !MG.all; renderMgChanges(); });
  }

  makeRangeToggle({ key: 'marginHist', containerId: 'mgHistRange', ranges: ['60D', 'Q', '1Y', 'Max'], defaultRange: '1Y', labelIds: ['mgHistRangeLabel'], onChange: () => MG.data && renderMgHistory() });
  makeRangeToggle({ key: 'marginChanges', containerId: 'mgChangesRange', ranges: ['30D', '60D', 'Q', '1Y', 'Max'], defaultRange: '60D', labelIds: ['mgChangesRangeLabel'], onChange: () => MG.data && renderMgChanges() });

  // ── Resizing ─────────────────────────────────────────────────────────────
  if (window.ResizeObserver) {
    const watch = (id, st, has, redraw) => {
      let queued = false;
      new ResizeObserver(() => {
        if (queued) return;
        queued = true;
        requestAnimationFrame(() => { queued = false; const el = $(id); if (has() && el.clientWidth && Math.abs(el.clientWidth - st.width) > 2) redraw(); });
      }).observe($(id));
    };
    watch('diagnostics', DG, () => DG.home || DG.an, renderDiagnostics);
    watch('positioning', PS, () => PS.data, renderPositioning);
    watch('margins', MG, () => MG.data, renderMargins);
  }

  MCX.lab = { diagnostics: { mount: loadDiagnostics }, positioning: { mount: loadPositioning }, margins: { mount: loadMargins } };
})();
