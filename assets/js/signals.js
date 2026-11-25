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
  // Multi-factor: described as activity, so the panel does not show a second "Buy" beside the ensemble's
  const ACTIVITY = { STRONG_BUY: ['strong-up', '▲▲', 'Activity well above its norm'], BUY: ['up', '▲', 'Activity above its norm'],
                     NEUTRAL: ['', '■', 'Activity near its norm'], SELL: ['down', '▼', 'Activity below its norm'],
                     STRONG_SELL: ['strong-down', '▼▼', 'Activity well below its norm'] };

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
  // MCX revenue, known only after the evening session (23:30, or 23:55 in US winter), so it is first tradeable at the
  // next day's close: the change runs from row i + lag to row i + lag + h. Overlapping windows;
  // days without an outcome yet are left out. priceKey: 'close_price' (momentum) or 'price' (models).
  // keep(date), when given, limits the signal days counted; prices still come from all rows, so a
  // window that starts before a cutoff can end after it.
  function forward(rows, key, order, h = 10, priceKey = 'close_price', lag = 1, keep = null) {
    const g = {}, all = [];
    let first = null, last = null;
    for (let i = 0; i + lag + h < rows.length; i++) {
      if (keep && !keep(rows[i].date)) continue;
      const a = rows[i + lag][priceKey], b = rows[i + lag + h][priceKey], k = rows[i][key];
      if (!(a > 0) || !(b > 0) || !k || k === 'NO_DATA') continue;
      const r = (b / a - 1) * 100;
      (g[k] = g[k] || []).push(r); all.push(r);
      if (first === null) first = rows[i].date;
      last = rows[i].date;
    }
    const stat = xs => xs.length ? { n: xs.length, avg: xs.reduce((p, q) => p + q, 0) / xs.length, up: xs.filter(x => x > 0).length / xs.length * 100 } : null;
    if (!keep) { first = rows.length ? rows[0].date : null; last = rows.length > h + lag ? rows[rows.length - 1 - h - lag].date : null; }
    return { rows: order.filter(k => g[k]).map(k => ({ key: k, ...stat(g[k]) })), all: stat(all), first, last };
  }

  // Last day of data in the study that set the momentum rules ("MCX Momentum Strategy: 10D/45D F&O
  // Revenue Ratio, ADR Divergence Overlay, Entry & Exit Framework", data Sep 2024 – 11 Mar 2026).
  // Signal days after it are out of sample.
  const MOM_RULES_FIXED = '2026-03-11', MOM_RULES_FIXED_TEXT = '11 March 2026';
  const afterRules = d => d > MOM_RULES_FIXED, inStudy = d => d <= MOM_RULES_FIXED;

  // The study's trade rule, entering and exiting at the next row's close (a day's signal is known
  // only after MCX's evening session): buy after a Buy or Strong-buy day; sell after the first Sell
  // or Cold day, or once the position has been held maxHold rows. One position at a time. keep(date)
  // limits the entry days; exits follow the rule wherever they fall. A trade still open at the end
  // is left out. Before costs.
  function tradeBacktest(rows, lag = 1, keep = null, maxHold = 15) {
    const trades = [];
    let k0 = -1, k1 = -1;
    rows.forEach((r, i) => { if (!keep || keep(r.date)) { if (k0 < 0) k0 = i; k1 = i; } });
    let i = 0;
    while (i < rows.length) {
      const r = rows[i];
      if ((keep && !keep(r.date)) || (r.composite_signal !== 'BUY' && r.composite_signal !== 'STRONG_BUY')) { i++; continue; }
      const e = i + lag;
      if (e >= rows.length) break;
      let j = i + 1, why = null;
      for (; j < rows.length; j++) {
        if (rows[j].composite_signal === 'SELL' || rows[j].regime === 'COLD') { why = 'sell'; break; }
        if (j + lag - e >= maxHold) { why = 'time'; break; }
      }
      const x = j + lag;
      if (!why || x >= rows.length) break;
      const a = rows[e].close_price, b = rows[x].close_price;
      if (a > 0 && b > 0) trades.push({ entry: rows[e].date, exit: rows[x].date, signal: r.composite_signal, why, days: x - e, ret: (b / a - 1) * 100 });
      i = j + 1;
    }
    if (k0 < 0 || k0 + lag >= rows.length) return null;
    const s0 = k0 + lag, s1 = Math.min(k1 + lag, rows.length - 1);
    const rets = trades.map(t => t.ret), n = rets.length;
    const sorted = rets.slice().sort((p, q) => p - q);
    const bh = rows[s0].close_price > 0 && rows[s1].close_price > 0 ? (rows[s1].close_price / rows[s0].close_price - 1) * 100 : null;
    return {
      trades, n,
      win: n ? rets.filter(v => v > 0).length / n * 100 : null,
      avg: n ? rets.reduce((p, q) => p + q, 0) / n : null,
      med: n ? (n % 2 ? sorted[(n - 1) / 2] : (sorted[n / 2 - 1] + sorted[n / 2]) / 2) : null,
      compounded: n ? (rets.reduce((p, v) => p * (1 + v / 100), 1) - 1) * 100 : null,
      inMarket: s1 > s0 ? Math.min(100, trades.reduce((p, t) => p + t.days, 0) / (s1 - s0) * 100) : null,
      bh, from: rows[s0].date, to: rows[s1].date,
    };
  }

  // The out-of-sample line under the momentum headline: Strong-buy days after the rules were set
  // against all days. Calls a breakdown only when Strong buy has done no better than all days.
  function momCaveat(rows, h = 10) {
    if (!rows.length) return null;
    const fromStart = rows[0].date <= MOM_RULES_FIXED;
    const ev = forward(rows, 'composite_signal', ['STRONG_BUY'], h, 'close_price', 1, fromStart ? afterRules : null);
    const sb = ev.rows[0];
    if (!sb || !ev.all) return null;
    const since = fromStart ? `Since the rules were set on ${MOM_RULES_FIXED_TEXT}` : `Over the loaded history (from ${fmt.dateLong(ev.first)})`;
    const broken = sb.avg <= ev.all.avg;
    const body = `${since}, Strong-buy days have been followed by ${signed(sb.avg)}% over ${h} trading days (${num(sb.up, 0)}% of them up), against ${signed(ev.all.avg)}% for all days.`;
    return { broken, sb, all: ev.all,
             text: broken ? `${body} The signal has not held up out of sample: read it as a description of revenue activity, not a trade.`
                          : `${body} ${num(sb.n)} Strong-buy days so far; the windows overlap, so this is thin evidence.` };
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
      : z < 0 ? ' The model expects the gap to rise back toward its norm, which reads as room for the price to gain on fair value.'
      : ' The model expects the gap to fall back toward its norm, which reads as a drag on the price relative to fair value.';
    return t;
  }

  // ── Options feed (scripts/options_paper.py; rules in Docs/Options_Paper_Trading_Rules.md) ──
  const BOOK = { PCS: 'Put credit spread', CDS: 'Call debit spread', EVT_IFLY: 'Iron butterfly', EVT_STRADDLE: 'Long straddle' };
  const VARIANT = { v45_20: '45-day trend', v60_40: '60-day trend' };
  const TREND = { BULL: ['up', '▲', 'Bullish'], FLAT: ['', '■', 'Not bullish'] };
  const legsText = legs => (legs || []).map(l => `${l.qty > 0 ? 'Buy' : 'Sell'} ${num(l.strike)} ${l.type === 'CE' ? 'call' : 'put'}`).join(' · ');
  const isBull = s => !!s && s.signal > 0;

  // Headline and deck for the latest feed row
  function optLede(f) {
    const sig = f.signals || {}, rec = f.recommendation || {}, ph = rec.phase;
    const bulls = Object.keys(VARIANT).filter(k => isBull(sig[k])).length;
    const live = (rec.trades || []).filter(t => !t.shadow && t.status === 'open');
    const held = (f.positions || []).filter(p => !p.shadow).length;
    let head;
    if (ph === 'before_entry') head = bulls ? `Bullish: next paper entry ${fmt.day(f.entry_date)}` : 'No trade lined up: the revenue trend is not rising';
    else if (ph === 'entry_day') head = live.length ? 'Paper spreads opened today' : 'No paper trade this series: the trend is not rising';
    else if (ph === 'in_trade') head = held ? `Holding ${held} paper spread${held === 1 ? '' : 's'} until ${fmt.day(f.exit_date)}` : `No paper position this series; it ends ${fmt.day(f.exit_date)}`;
    else if (ph === 'exit_day') head = 'Paper spreads closed today';
    else head = rec.next_entry ? `Between series: next paper entry ${fmt.day(rec.next_entry)}` : 'Between series';
    const trend = bulls === 2 ? 'Both revenue-trend signals are bullish'
      : bulls === 1 ? 'One of the two revenue-trend signals is bullish' : 'Neither revenue-trend signal is bullish';
    let deck = `${trend}, so the rule ${bulls ? 'takes' : 'skips'} the bullish spreads. Bearish readings are logged, never traded.`;
    const ev = f.event, r = ev && ev.reading;
    if (ev && ev.status === 'pending' && r)
      deck += ` Results on ${fmt.day(ev.results_date)}: the options price a ${num(r.implied_move * 100, 1)}% move, against a ${num(r.threshold_rich * 100, 2)}% threshold.`;
    return { head, deck, bulls };
  }

  // Closed-trade summary rows next to the backtest's reference for the same structure
  function trackRows(track, backtest) {
    return (track || []).filter(t => !t.shadow).map(t => ({ ...t, ref: (backtest || {})[t.book] || null }));
  }

  // ── ADR bands (lib/strategy_bands.py; verdict in the private tusk-strategy-research repo) ──
  // [pill state, glyph, label]. Turning up is the live signal (long only); the others are context.
  const BAND = { 'TURNING UP': ['strong-up', '▲', 'Turning up'], 'AT LOWER': ['up', '▲', 'At lower band'],
                 'IN BAND': ['', '■', 'In band'], 'AT UPPER': ['caution', '!', 'At upper band'],
                 'TURNING DOWN': ['caution', '▼', 'Turning down'], 'NO DATA': ['none', '', 'No data'] };

  // Headline and deck for the bands page from the stocks the API could compute
  function bandLede(stocks) {
    const ok = (stocks || []).filter(s => s.available && s.today);
    if (!ok.length) return { head: 'No stock could be computed.', deck: '' };
    const flagged = ok.filter(s => s.today.state === 'TURNING UP' || s.today.state === 'AT LOWER');
    const z = s => `${s.today.z >= 0 ? '+' : '−'}${num(Math.abs(s.today.z), 1)} SD`;
    const where = s => `${s.name} ${s.today.z >= 0 ? 'above' : 'below'} its line (${z(s)})`;
    // Exit flags (trim longs, not shorts): the verdict found below-average returns follow these
    const exits = ok.filter(s => s.today.state === 'TURNING DOWN' || s.today.state === 'AT UPPER');
    const exitTxt = exits.length ? ` Exit flag: ${exits.map(s => `${s.name} (${label(BAND, s.today.state).toLowerCase()})`).join(', ')}.` : '';
    const head = flagged.length
      ? `${flagged.map(s => s.name).join(' and ')} ${flagged.length === 1 ? 'is' : 'are'} flagged: ${flagged.map(s => label(BAND, s.today.state).toLowerCase()).join(', ')}.${exitTxt}`
      : exits.length ? `No buy flag today.${exitTxt}` : `No buy flag today: ${ok.map(where).join('; ')}.`;
    const parts = ok.map(s => {
      const t = s.today, trend = t.ratio > 1 ? 'above' : 'below';
      return `${s.name}: ₹${num(t.close, 0)} against a line of ₹${num(t.line, 0)}; the 10-day revenue average is ${trend} the 45-day (${num(t.ratio, 2)}×).`;
    });
    return { head, deck: parts.join(' ') };
  }

  // What it would take to turn up: price below line − 1 SD, with the 10-day above the 45-day
  function turnUpText(t) {
    if (t.state === 'TURNING UP') return 'Turning up now.';
    const px = t.lo1, needPx = t.close > px, needTrend = !(t.ratio > 1);
    if (needPx && needTrend) return `Needs the price below ₹${num(px, 0)} and the 10-day revenue average back above the 45-day.`;
    if (needPx) return `Needs the price below ₹${num(px, 0)} (${num((1 - px / t.close) * 100, 1)}% lower).`;
    return 'Needs the 10-day revenue average back above the 45-day.';
  }

  // The card's "what flips it" line: exit flags read as an action, the rest as the turn-up condition
  function flipText(t) {
    if (t.state === 'TURNING DOWN' || t.state === 'AT UPPER') return 'Exit flag: trim longs, don’t short.';
    if (t.state === 'AT LOWER') return `Buy-watch. ${turnUpText(t)}`;
    return turnUpText(t);
  }

  // Band gauge: z clamped to −3…+3 SD, mapped across the gauge width
  const GAUGE_LIM = 3;
  const gaugeX = (z, w) => (Math.max(-GAUGE_LIM, Math.min(GAUGE_LIM, z)) + GAUGE_LIM) / (2 * GAUGE_LIM) * w;

  // Ribbon colour group for a day's state
  const RIBBON = { 'TURNING UP': 'up', 'AT LOWER': 'lower', 'IN BAND': 'band', 'AT UPPER': 'upper', 'TURNING DOWN': 'down' };
  // Consecutive days in the same ribbon group: [{key, from, to}] as row indexes
  function stateRuns(rows) {
    const out = [];
    rows.forEach((r, i) => {
      const k = RIBBON[r.state] || 'band', last = out[out.length - 1];
      if (last && last.key === k) last.to = i; else out.push({ key: k, from: i, to: i });
    });
    return out;
  }

  // Revenue in, price out: the latest line at a chosen 45-day ADR, with the ±2 SD band
  function calcLine(t, adr) {
    const a = Number(adr);
    if (!(a > 0) || !isFinite(a)) return { error: 'Enter a 45-day revenue above ₹0 Cr.' };
    if (a > t.adr45 * 5) return { error: `Enter a figure within 5× today’s ₹${num(t.adr45, 2)} Cr.` };
    const line = t.a + t.b * a;
    return { line, buyWatch: line - 2 * t.sd, trim: line + 2 * t.sd };
  }

  // The count line under the headline
  function bandCount(stocks) {
    const ok = (stocks || []).filter(s => s.available && s.today);
    const buy = ok.filter(s => s.today.state === 'TURNING UP' || s.today.state === 'AT LOWER').length;
    const exit = ok.filter(s => s.today.state === 'TURNING DOWN' || s.today.state === 'AT UPPER').length;
    const pl = (n, w) => `${n} ${w}${n === 1 ? '' : 's'}`;
    return `${pl(ok.length, 'stock')} · ${pl(buy, 'buy flag')} · ${pl(exit, 'exit flag')}`;
  }

  // "data through …" for the kicker. The stocks' latest rows can differ (Angel One's MTF revenue
  // lands a day after its other segments; NSE holidays), so name each stock's date when they do.
  const nbsp = s => s.replace(/ /g, ' ');     // keep "24 Nov" on one line on a phone
  function bandThrough(stocks) {
    const ok = (stocks || []).filter(s => s.available && s.today);
    if (!ok.length) return '';
    const dates = ok.map(s => s.today.date);
    if (dates.every(d => d === dates[0])) return `data through ${nbsp(fmt.day(dates[0]))}`;
    return 'data through ' + ok.map(s => nbsp(`${s.name} ${fmt.dayMonth(s.today.date)}`)).join(', ');
  }

  // A card's date; on an NSE holiday the close is the last one before it, so say which day it is from
  function bandDateText(t) {
    const d = nbsp(fmt.dayMonth(t.date));
    return t.close_date && t.close_date !== t.date ? `${d} (price ${nbsp(fmt.dayMonth(t.close_date))})` : d;
  }

  MCX.signalsModel = { HOT, COLD, W, streak, cushion, changes, momLede, overlayText, forward, contributions, ensLede, ecmText, pill,
                       MOM_RULES_FIXED, afterRules, inStudy, tradeBacktest, momCaveat,
                       COMPOSITE, REGIME, OVERLAY, ECM, ACTIVITY, optLede, legsText, trackRows, BOOK, BAND, bandLede, turnUpText,
                       flipText, gaugeX, stateRuns, calcLine, bandCount, bandThrough, bandDateText };
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
  const SAME_HISTORY = 'The rules were chosen on this same history, which flatters them. Compare each row with all days: MCX’s shares rose over most of this period.';
  function evidenceHtml(ev, map, h, note = SAME_HISTORY) {
    if (!ev || !ev.all) return '<p class="note">Not enough history yet.</p>';
    const row = (lab, s, cls) => `<tr${cls ? ` class="${cls}"` : ''}><td>${lab}</td><td>${num(s.n)}</td><td>${pctCell(s.avg)}</td><td>${num(s.up, 0)}%</td></tr>`;
    return `<div class="table-scroll"><table class="v2-table sg-ev"><thead><tr><th scope="col">Signal that day</th><th scope="col">Days</th>`
      + `<th scope="col">Avg. change, ${h} days</th><th scope="col">Days up</th></tr></thead><tbody>`
      + ev.rows.map(r => row(pill(map, r.key), r)).join('')
      + `${row('All days', ev.all, 'total')}</tbody></table></div>`
      + `<p class="note">Signal days ${fmt.span(ev.first, ev.last)}. Each change runs from the next day’s close, the first chance to act, since a day’s signal needs MCX’s revenue from its evening session. It covers ${h} trading days, so the windows overlap and the counts overstate how much independent evidence there is. `
      + note + '</p>';
  }
  // One line for the study's trade rule against holding the shares over the same span
  function tradeHtml(bt) {
    if (!bt) return '';
    const rule = '<strong>The study’s trade rule</strong> (buy at the close after a Buy or Strong-buy day; sell at the close after the first Sell or Cold day, or after 15 trading days)';
    const hold = bt.bh === null ? '' : ` Holding the shares over the same span (${fmt.span(bt.from, bt.to)}): ${pctCell(bt.bh)}.`;
    if (!bt.n) return `<p class="note">${rule}: no completed trades yet.${hold}</p>`;
    return `<p class="note">${rule}: ${num(bt.n)} completed trade${bt.n === 1 ? '' : 's'}, ${num(bt.win, 0)}% won, average ${pctCell(bt.avg)} (median ${pctCell(bt.med)}), `
      + `${pctCell(bt.compounded)} compounded, in the market ${num(bt.inMarket, 0)}% of days.${hold} Before costs.</p>`;
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
    const cv = momCaveat(rows);
    const cvBox = $('moCaveat');
    cvBox.hidden = !cv;
    cvBox.className = 'note mo-caveat' + (cv && cv.broken ? ' mo-caveat--warn' : '');
    cvBox.textContent = cv ? cv.text : '';
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
    $('moFoot').textContent = `Computed each morning, once the previous evening’s MCX revenue is in, from MCX’s daily revenue and the NSE share price; data through ${fmt.day(s.date)}.`;
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
      + '<li><strong>Sharp fall</strong>: down more than 1%, with the 5-day average move above 1.3× the 20-day. The study reads this as a sell-off likely to bounce, so it softens the signal rather than deepening it</li>'
      + '<li><strong>Neutral</strong>: none of these</li></ul>'
      + `<div class="table-scroll"><table class="v2-table sg-matrix"><caption>The composite: price overlay × regime</caption><thead><tr><th scope="col">Overlay</th>${['HOT', 'NEUTRAL', 'COLD'].map(r => `<th scope="col">${esc(label(REGIME, r))}</th>`).join('')}</tr></thead><tbody>`
      + ov.map(o => `<tr><th scope="row">${esc(label(OVERLAY, o))}</th>${['HOT', 'NEUTRAL', 'COLD'].map(r => `<td>${cell(r, o)}</td>`).join('')}</tr>`).join('')
      + '</tbody></table></div>';
  }

  makeRangeToggle({ key: 'momRegime', containerId: 'moRange', ranges: RANGES, defaultRange: '60D', labelIds: ['moRangeLabel'],
                    onChange: () => loadMomentum() });
  lazyEvidence('moEvidence', 'moEvBtn', () => fetchRanged('/api/models?view=momentum&range=Max').then(d => {
    if (!d.success) throw new Error(d.error || 'No data');
    const h = d.history || [], order = ['STRONG_BUY', 'BUY', 'HOLD', 'WATCH', 'SELL'];
    const study = forward(h, 'composite_signal', order, 10, 'close_price', 1, inStudy);
    const live = forward(h, 'composite_signal', order, 10, 'close_price', 1, afterRules);
    $('moEvidence').innerHTML = `<h3 class="mo-ev-h">Since the rules were set (signal days after ${MOM_RULES_FIXED_TEXT})</h3>`
      + evidenceHtml(live, COMPOSITE, 10, 'These days come after the rules were fixed, so this is the out-of-sample record. Compare each row with all days.')
      + tradeHtml(tradeBacktest(h, 1, afterRules))
      + `<h3 class="mo-ev-h">Before the rules were set (signal days to ${MOM_RULES_FIXED_TEXT}; the study itself used September 2024 on)</h3>`
      + evidenceHtml(study, COMPOSITE, 10)
      + tradeHtml(tradeBacktest(h, 1, inStudy));
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
        'Neutral from −0.5 to +0.5; strong beyond ±1.5', true)
      + stat('Position the model implies', val(pos.score === null || pos.score === undefined ? '—' : `${num(Math.abs(pos.score) * 100, 0)}%<small>${pos.score >= 0 ? 'long' : 'short'}</small>`),
        `of a full position: the score ÷ 2, capped at 100%${pos.velocity_1d !== null && pos.velocity_1d !== undefined ? `; ${signed(pos.velocity_1d * 100, 1)} points on the day` : ''}`)
      + stat('Price gap to fair value', val(ecm.spread_pct === null || ecm.spread_pct === undefined ? '—' : `${signed(ecm.spread_pct)}%`),
        last.ecm_band_mean !== undefined && last.ecm_band_mean !== null ? `60-day average ${signed(last.ecm_band_mean)}%; z ${signed(ecm.z_score, 2)}` : '')
      + stat('Exchange activity', val(mf.composite_z === null || mf.composite_z === undefined ? '—' : signed(mf.composite_z, 2)),
        'z-score: revenue and turnover against their 60-day norm, blended 3 : 4');
    renderBuild(s);
    renderEnHist();
    $('enEcmPill').innerHTML = pill(ECM, ecm.signal);
    $('enEcm').innerHTML = `<p class="sg-para">${esc(ecmText(s, last.ecm_band_mean))}</p>`
      + '<p class="note">Fair value here is the data-driven base on the Fair value page. The gap’s z-score uses the last 60 trading days (at least 30).</p>';
    $('enMfPill').innerHTML = pill(ACTIVITY, mf.signal);
    $('enMf').innerHTML = factorsHtml(mf);
    $('enFoot').textContent = `Computed after each close by the model job, after the fair-value job; data through ${fmt.day(s.date)}.`;
    EN.width = $('ensemble').clientWidth;
  }

  // Waterfall of contributions on the −3…+3 scale, with the signal bands behind it
  function renderBuild(s) {
    const box = $('enBuild'), c = contributions(s);
    if (!c) { box.innerHTML = '<p class="note">Not enough data for today’s score.</p>'; $('enWeights').innerHTML = ''; return; }
    const w = Math.round(box.clientWidth) || 700, compact = w < 520;
    const lw = compact ? 104 : 176, rw = compact ? 44 : 64, top = 26, rowH = 42;
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
      // Name, then reading × weight underneath: the arithmetic behind the bar
      sv.push(txt(0, y - 3, 'c-label2', esc(compact ? p.label.replace('Price gap to fair value', 'Price gap') : p.label)));
      sv.push(txt(0, y + 13, 'c-label3', `${signed(p.reading, 2)} × ${num(p.weight * 100, 0)}%`));
      sv.push(txt(w, y + 4, 'c-label', signed(p.value, 2), 'end'));
      run += p.value;
    });
    const y = top + rowH * 3 + rowH / 2;
    sv.push(`<rect x="${Math.min(X(0), X(c.total)).toFixed(1)}" y="${(y - 8).toFixed(1)}" width="${Math.max(Math.abs(X(c.total) - X(0)), 2).toFixed(1)}" height="16" rx="2" class="c-ink"/>`);
    sv.push(txt(0, y + 4, 'c-label', 'Score'), txt(w, y + 4, 'c-label', signed(c.total, 2), 'end'));
    [-lim, -1.5, -0.5, 0, 0.5, 1.5, lim].forEach(v => { if (!compact || Number.isInteger(v)) sv.push(txt(X(v), h - 6, 'c-tick', v === 0 ? '0' : signed(v, Number.isInteger(v) ? 0 : 1), 'middle')); });
    sv.push('</svg>');
    box.innerHTML = sv.join('');
    $('enWeights').innerHTML = '<p class="note">Each reading is a z-score: today against its last 60 trading days. '
      + 'Revenue is daily F&amp;O revenue; turnover is futures notional plus option premium. The price gap’s sign is flipped, so a gap below its norm counts in favour.'
      + (Math.abs(c.total - s.ensemble.score) > 0.01 ? ` The job stores ${signed(s.ensemble.score, 2)}; the parts above are rounded.` : '') + '</p>';
  }

  function factorsHtml(mf) {
    const bar = z => {
      if (z === null || z === undefined) return '<span class="sg-zbar"></span>';
      const c = Math.max(-3, Math.min(3, z)), wd = Math.abs(c) / 3 * 50;
      return `<span class="sg-zbar" aria-hidden="true"><i style="left:${(c >= 0 ? 50 : 50 - wd).toFixed(1)}%;width:${wd.toFixed(1)}%"></i></span>`;
    };
    const zs = z => (z === null || z === undefined ? '—' : signed(z, 2));
    const row = (lab, sub, z, cls) => `<div class="sg-factor${cls ? ' ' + cls : ''}"><span class="sg-fname">${lab}${sub ? `<small>${sub}</small>` : ''}</span>${bar(z)}`
      + `<span class="sg-fval">${zs(z)}</span></div>`;
    return `<div class="sg-factors"><div class="sg-factor sg-factor--head" aria-hidden="true"><span></span><span class="sg-zscale"><span>−3</span><span>0</span><span>+3</span></span><span></span></div>`
      + row('Revenue', 'weight 3/7', mf.revenue_z)
      + row('Turnover', 'weight 4/7', mf.turnover_z)
      + row('Composite', '', mf.composite_z, 'sg-factor--total')
      + `</div><p class="note">Each factor is today’s value in standard deviations from its last 60 trading days. Share volume (${zs(mf.volume_z)}) and intraday calm (${zs(mf.volatility_z)}) are computed too, but were dropped from the score after a factor review found they added nothing.</p>`;
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

  // ════════════════════════════════════════════════════════════════════════
  //  Options (paper-trading feed)
  // ════════════════════════════════════════════════════════════════════════
  const OP = { data: null, err: null };
  const rs = (v, dp = 2) => (v === null || v === undefined ? '—' : `₹${num(v, dp)}`);
  const pctOf = v => (v === null || v === undefined ? '—' : `${signed(v * 100, 1)}%`);

  function loadOptions() {
    return fetchRanged('/api/models?view=options').then(d => {
      OP.data = d; OP.err = d.success || d.setup ? null : (d.error || 'No data'); renderOptions();
    }).catch(e => { OP.err = e.message || String(e); renderOptions(); });
  }

  function renderOptions() {
    const d = OP.data;
    if (OP.err || !d) {
      $('opHead').textContent = 'The options feed could not be loaded.';
      MCX.ui.error($('opRec'), 'Could not load the options feed: ' + (OP.err || 'no data'), loadOptions);
      return;
    }
    const later = ['opPos', 'opEvent', 'opTrack', 'opNote'].map(id => $(id).closest('.sec'));
    later.forEach(s => { s.hidden = !d.success; });
    if (!d.success) {
      $('opKicker').textContent = 'Options';
      $('opHead').textContent = 'The options feed is not running yet';
      $('opDeck').textContent = '';
      $('opStats').innerHTML = '';
      $('opRec').innerHTML = '<p class="note">The paper trader publishes here after its 15:10 run, once the feed tables exist in Supabase and the Mac running it has the service key.</p>'
        + `<p class="note">${esc(d.error || 'No data yet.')}</p>`;
      return;
    }
    const f = d.latest, sig = f.signals || {}, l = optLede(f);
    const runAt = (f.run_ts || '').slice(11, 16);
    $('opKicker').textContent = `Options · paper trades · ${fmt.day(f.trading_date)}${runAt ? `, ${runAt} IST` : ''}`;
    $('opHead').textContent = l.head;
    $('opDeck').textContent = l.deck;

    const trendStat = (k, lead) => {
      const s = sig[k];
      if (!s) return stat(VARIANT[k], val('—'));
      return stat(VARIANT[k], pillRow(pill(TREND, isBull(s) ? 'BULL' : 'FLAT')),
        `${signed(s.signal * 100, 1)}% vs ${k === 'v45_20' ? 20 : 40} days ago${s.z !== null && s.z !== undefined ? ` · z ${num(s.z, 2)}` : ''}`, lead);
    };
    const ev = f.event, r = ev && ev.reading;
    $('opStats').innerHTML = trendStat('v45_20', true) + trendStat('v60_40')
      + stat('This series', val(fmt.dayMonth(f.series_expiry)), `expiry · entry ${fmt.day(f.entry_date)}, exit ${fmt.day(f.exit_date)}`)
      + (ev ? stat('Results-day move priced', val(r ? `${num(r.implied_move * 100, 1)}%` : '—'),
          `${fmt.day(ev.results_date)} · ${r ? (r.vol_view === 'rich' ? 'rich' : r.vol_view) + ` vs ${num(r.threshold_rich * 100, 2)}%` : (ev.status || '').replace('_', ' ')}`)
        : stat('Results day', val('—'), 'no date set'));

    renderOpRec(f);
    renderOpPos(f);
    renderOpEvent(ev);
    renderOpTrack(d, f);
    $('opNote').innerHTML = `<p class="op-note">${esc(f.note || '')}</p>`;
    $('opFoot').textContent = `Paper trades only: no orders are placed. From the paper trader's run on ${fmt.day(f.trading_date)}${runAt ? ` at ${runAt} IST` : ''}; fills at the live bid and ask, costs included.`;
  }

  function tradeTable(rows, cols) {
    return `<div class="table-scroll"><table class="v2-table"><thead><tr>${cols.map(c => `<th scope="col">${c}</th>`).join('')}</tr></thead><tbody>`
      + rows.join('') + '</tbody></table></div>';
  }

  function renderOpRec(f) {
    const rec = f.recommendation || {}, box = $('opRec');
    const trades = (rec.trades || []).filter(t => t.legs);
    $('opRecSub').textContent = rec.phase === 'entry_day' ? 'Opened today at the live bid and ask, one lot each'
      : rec.phase === 'before_entry' ? 'What the rule would open if today were the entry day, at the live bid and ask, one lot each'
      : 'New trades open on the entry day of each monthly series';
    if (!['before_entry', 'entry_day'].includes(rec.phase)) {
      box.innerHTML = `<p class="note">${rec.phase === 'in_trade' || rec.phase === 'exit_day' ? 'This series is under way: see the open positions.'
        : `No new trade until the next series${rec.next_entry ? `, entering ${esc(fmt.day(rec.next_entry))}` : ''}.`}</p>`;
      return;
    }
    if (!trades.length) { box.innerHTML = '<p class="note">No trade: neither revenue-trend signal is bullish.</p>'; return; }
    const rows = trades.map(t => `<tr${t.shadow ? ' class="muted"' : ''}><td>${esc(BOOK[t.book] || t.book)}<small>${esc(VARIANT[t.variant] || t.variant)}${t.shadow ? ' · shadow (bearish, not traded)' : ''}</small></td>`
      + `<td>${esc(legsText(t.legs))}<small>${t.legs.map(x => `${num(x.strike)} ${x.type === 'CE' ? 'call' : 'put'} ${num(x.bid, 2)}/${num(x.ask, 2)}`).join(' · ')}</small></td>`
      + `<td>${rs(Math.abs(t.net_entry))}<small>${t.net_entry < 0 ? 'credit' : 'debit'} per share</small></td>`
      + `<td>${rs(t.risk_per_lot, 0)}</td><td>${t.max_gain != null && t.lot ? rs(t.max_gain * t.lot, 0) : '—'}</td></tr>`);
    box.innerHTML = tradeTable(rows, ['Trade', 'Legs (bid/ask)', 'Net', 'At risk per lot', 'Most it can make per lot'])
      + '<p class="note">Both variants pick the same strikes when both are bullish; they are tracked separately so the paper record can tell them apart. Held to two trading days before expiry, no stops.</p>';
  }

  function renderOpPos(f) {
    const ps = (f.positions || []).filter(p => !p.shadow), box = $('opPos');
    if (!ps.length) { box.innerHTML = '<p class="note">No open paper positions.</p>'; return; }
    box.innerHTML = tradeTable(ps.map(p => `<tr><td>${esc(BOOK[p.book] || p.book)}<small>${esc(p.event ? 'Results day' : VARIANT[p.variant] || p.variant)} · ${esc(legsText(p.legs))}</small></td>`
      + `<td>${p.entry_date ? fmt.dayMonth(p.entry_date) : '—'}</td><td>${p.mtm_per_lot != null ? `${p.mtm_per_lot >= 0 ? '+' : '−'}₹${num(Math.abs(p.mtm_per_lot))}` : '—'}</td>`
      + `<td>${pctOf(p.mtm_pct)}</td></tr>`), ['Position', 'Opened', 'Marked per lot', 'Of capital at risk']);
  }

  function renderOpEvent(ev) {
    const box = $('opEvent');
    if (!ev) { box.innerHTML = '<p class="note">No results date set. Once MCX files its board-meeting notice, set it with <code>options_paper.py set-results-date</code>.</p>'; return; }
    const r = ev.reading;
    let h = `<p class="note">Results ${esc(fmt.day(ev.results_date))}. The trade is decided at 15:10 on ${esc(fmt.day(ev.entry))} and closed at 15:10 on ${esc(fmt.day(ev.exit))}.</p>`;
    if (r) {
      const act = r.book === 'EVT_IFLY' ? 'an iron butterfly (sell the at-the-money call and put, buy wings about 7% away)'
        : r.book === 'EVT_STRADDLE' ? 'a long straddle' : 'no trade';
      h += `<p class="note">${ev.status === 'pending' ? 'Measured today' : 'Measured on the day'}: the options price a <strong>${num(r.implied_move * 100, 1)}%</strong> results-day move. `
        + `MCX's median move on results days since 2022 is 3.9%, so anything above ${num(r.threshold_rich * 100, 2)}% counts as rich. `
        + `The study gives no view on direction, so the rule ${ev.status === 'pending' ? 'would sell' : 'chose'} ${act}.</p>`;
    }
    if (ev.trade && ev.trade.legs) h += `<p class="note">${esc(legsText(ev.trade.legs))} · ${rs(Math.abs(ev.trade.net_entry))} ${ev.trade.net_entry < 0 ? 'credit' : 'debit'}, ${rs(ev.trade.risk_per_lot, 0)} at risk per lot.</p>`;
    if (ev.status && ev.status !== 'pending') h += `<p class="note">Status: ${esc(ev.status.replace('_', ' '))}.</p>`;
    box.innerHTML = h;
  }

  function renderOpTrack(d, f) {
    const rows = trackRows(d.track, d.backtest), box = $('opTrack');
    const ref = Object.entries(d.backtest || {}).map(([k, b]) => `${esc(b.label)} ${signed(b.mean * 100, 0)}% per trade, ${num(b.hit * 100, 0)}% winners (${b.n} trades)`).join('; ');
    if (!rows.length) {
      box.innerHTML = `<p class="note">No closed paper trades yet. The first series closes ${esc(fmt.day(f.exit_date))}. Backtest for comparison (2022–2026, bullish signal only): ${ref}. The review comes after six series.</p>`;
      return;
    }
    box.innerHTML = tradeTable(rows.map(t => `<tr><td>${esc(BOOK[t.book] || t.book)}<small>${esc(VARIANT[t.variant] || t.variant)}</small></td>`
      + `<td>${num(t.n)}</td><td>${pctOf(t.mean_ret)}</td><td>${num(t.hit * 100, 0)}%</td><td>${t.pnl_per_lot >= 0 ? '+' : '−'}₹${num(Math.abs(t.pnl_per_lot))}</td>`
      + `<td>${t.ref ? `${signed(t.ref.mean * 100, 0)}%<small>${num(t.ref.hit * 100, 0)}% winners</small>` : '—'}</td></tr>`),
      ['Book', 'Trades', 'Mean return', 'Winners', 'P&amp;L, one lot', 'Backtest'])
      + '<p class="note">Too few trades to judge yet; the review after six series checks for a clear failure, not for proof.</p>';
  }

  // ════════════════════════════════════════════════════════════════════════
  //  ADR bands (/api/models?view=bands; lib/strategy_bands.py)
  //  Action cards per stock, then one focus chart with a state ribbon, a revenue-in
  //  calculator and the recent flags for the selected stock.
  // ════════════════════════════════════════════════════════════════════════
  const BD = { data: null, err: null, width: 0, sel: null, calc: {} };
  const BAND_RANGES = ['Q', '1Y', '2Y', 'Max'];
  const bdUrl = () => '/api/models?view=bands&range=' + (rangeState.bands || '1Y');
  const sdTxt = v => `${v >= 0 ? '+' : '−'}${num(Math.abs(v), 2)}`;
  const pctTxt = v => `${v >= 0 ? '+' : '−'}${num(Math.abs(v * 100), 1)}%`;
  const FLAG_STATE = { turning_up: 'TURNING UP', lower_band: 'AT LOWER', upper_band: 'AT UPPER', turning_down: 'TURNING DOWN' };
  const bdOk = () => (BD.data ? BD.data.stocks.filter(s => s.available && s.today) : []);

  function loadBands() {
    return fetchRanged(bdUrl()).then(d => {
      BD.data = d; BD.err = d.success ? null : (d.error || 'No data'); renderBands();
    }).catch(e => { BD.err = e.message || String(e); renderBands(); });
  }

  // The selected stock: last choice if still available, else the first stock with a flag, else the first
  function bdSelected() {
    const ok = bdOk();
    if (!ok.length) return null;
    const want = BD.sel || MCX.storage.get('mcx.bands.stock');
    const flagged = ok.find(s => s.today.state !== 'IN BAND');
    return ok.find(s => s.name === want) || flagged || ok[0];
  }
  function bdSelect(name) {
    BD.sel = name;
    MCX.storage.set('mcx.bands.stock', name);
    renderBandCards(BD.data);
    renderBandFocus();
  }

  function renderBands() {
    const d = BD.data;
    if (BD.err || !d) {
      $('bdHead').textContent = 'The ADR bands could not be loaded.';
      $('bdDeck').textContent = '';
      MCX.ui.error($('bdCards'), 'Could not load the ADR bands: ' + (BD.err || 'no data'), loadBands);
      return;
    }
    const thru = bandThrough(d.stocks);
    $('bdKicker').textContent = `ADR bands · under validation${thru ? ` · ${thru}` : ''}`;
    $('bdHead').textContent = bandLede(d.stocks).head;
    $('bdDeck').textContent = bandCount(d.stocks);
    renderBandCards(d);
    renderBandFocus();
    renderVerdict(d.verdict);
    $('bdFoot').textContent = `Line refitted each day on the days before it (at least ${d.min_obs}); ${d.window}-day and ${d.fast}-day revenue averages; computed on request from the stored revenue and share price, ${d.as_of}.`;
    BD.width = $('bands').clientWidth;
  }

  // −3…+3 SD strip: buy-watch, turn zone, in band, caution, trim; dot = today, dashed tick = 5-day average
  // HTML, not SVG, so the dot stays round at any card width (positions are % of the track)
  function gaugeHtml(t) {
    const zones = [[-3, -2, 'buy'], [-2, -1, 'turn'], [-1, 1, 'band'], [1, 2, 'caution'], [2, 3, 'trim']];
    return `<div class="bd-gauge" role="img" aria-label="z ${sdTxt(t.z)} SD from the line; 5-day average ${sdTxt(t.z_week)}">`
      + '<div class="bd-gauge-track">' + zones.map(([a, b, k]) => `<span class="bd-g-${k}" style="width:${(gaugeX(b, 100) - gaugeX(a, 100)).toFixed(2)}%"></span>`).join('') + '</div>'
      + `<span class="bd-g-avg" style="left:${gaugeX(t.z_week, 100).toFixed(2)}%"></span>`
      + `<span class="bd-g-dot" style="left:${gaugeX(t.z, 100).toFixed(2)}%"></span></div>`;
  }

  function renderBandCards(d) {
    const sel = bdSelected();
    $('bdCards').innerHTML = d.stocks.map(s => {
      if (!s.available || !s.today) {
        return `<div class="bd-card bd-card--na"><div class="bd-card-head"><span class="bd-card-name">${esc(s.name)}</span>${pill(BAND, 'NO DATA')}</div>`
          + `<p class="bd-card-line">${esc(s.reason || 'Not available')}</p></div>`;
      }
      const t = s.today, gap = t.close / t.line - 1, on = sel && sel.name === s.name;
      return `<button type="button" class="bd-card" data-stock="${esc(s.name)}" aria-pressed="${on}">`
        + `<div class="bd-card-head"><span class="bd-card-name">${esc(s.name)}</span>${pill(BAND, t.state)}</div>`
        + gaugeHtml(t) + '<div class="bd-gauge-ends" aria-hidden="true"><span>buy-watch</span><span>in band</span><span>trim</span></div>'
        + `<div class="bd-card-price">₹${num(t.close, 0)}<small>${pctTxt(gap)} vs line ₹${num(t.line, 0)}</small></div>`
        + `<div class="bd-card-line">z ${sdTxt(t.z)} (5-day ${sdTxt(t.z_week)}) · revenue trend ${t.ratio > 1 ? '↑' : '↓'} ${num(t.ratio, 2)}× · ${bandDateText(t)}</div>`
        + `<div class="bd-card-flip">${esc(flipText(t))}</div></button>`;
    }).join('');
  }

  function renderBandFocus() {
    const s = bdSelected(), ok = bdOk();
    if (!s) {
      $('bdTabs').innerHTML = ''; $('bdChart').innerHTML = '<p class="note">No stock could be computed.</p>';
      $('bdLegend').innerHTML = ''; $('bdCalc').innerHTML = ''; $('bdFlags').innerHTML = '';
      return;
    }
    $('bdTabs').innerHTML = ok.map(x => `<button type="button" class="bd-tab" data-stock="${esc(x.name)}" aria-pressed="${x.name === s.name}">${esc(x.name)}</button>`).join('');
    if ((s.history || []).length > 1) bandChart($('bdChart'), s);
    else $('bdChart').innerHTML = '<p class="note">Not enough history for a chart.</p>';
    $('bdLegend').innerHTML = '<span><i class="sw sw--line"></i>Share price</span><span><i class="sw sw--typical"></i>Revenue line</span>'
      + '<span><i class="sw sw--band1"></i>±1 SD</span><span><i class="sw sw--band2"></i>±2 SD</span>'
      + '<span><i class="sw sw--rib-up"></i>Ribbon: turning up (light: at lower band)</span><span><i class="sw sw--rib-down"></i>Turning down (light: at upper band)</span>';
    renderBandCalc(s);
    renderBandFlags(s);
  }

  function bandChart(box, s) {
    const rows = s.history;
    const w = Math.round(box.clientWidth) || 900, compact = w < 520;
    const long = ['1Y', '2Y', 'Max'].includes(rangeState.bands || '1Y') && rows.length > 200;
    const pl = compact ? 46 : 58, pr = compact ? 10 : 112, aTop = 10, aH = compact ? 200 : 280;
    const ribY = aTop + aH + 10, ribH = 8, h = ribY + ribH + 28, iw = w - pl - pr;
    const X = i => pl + iw * i / (rows.length - 1), step = iw / Math.max(1, rows.length - 1);
    const [aLo, aHi] = span(rows.flatMap(r => [r.close, r.lo2, r.hi2]), 0.04);
    const YA = v => aTop + aH * (1 - (v - aLo) / (aHi - aLo));
    const last = rows[rows.length - 1];
    const sv = [V.open(w, h, `${s.name}: price ₹${num(last.close, 0)} against the revenue line ₹${num(last.line, 0)}, z ${sdTxt(last.z)}, ${fmt.span(rows[0].date, last.date)}`)];
    V.niceTicks(aHi, compact ? 3 : 5, aLo).forEach(v => sv.push(`<line x1="${pl}" x2="${w - pr}" y1="${YA(v).toFixed(1)}" y2="${YA(v).toFixed(1)}" class="c-grid"/>`,
      txt(pl - 8, YA(v) + 4, 'c-tick', `₹${num(v, 0)}`, 'end')));
    sv.push(`<path d="${bandPath(rows, X, YA, r => r.lo2, r => r.hi2)}" class="c-band-2"/>`,
            `<path d="${bandPath(rows, X, YA, r => r.lo1, r => r.hi1)}" class="c-band-1"/>`,
            `<path d="${path(rows, X, YA, 'line')}" class="c-typical"/>`, `<path d="${path(rows, X, YA, 'close')}" class="c-line"/>`);
    // Turning-up starts as small up-triangles under the price
    rows.forEach((r, i) => {
      if (r.state === 'TURNING UP' && (i === 0 || rows[i - 1].state !== 'TURNING UP')) {
        const x = X(i), y = YA(r.close) + 10;
        sv.push(`<path d="M${(x - 5).toFixed(1)},${(y + 7).toFixed(1)}L${(x + 5).toFixed(1)},${(y + 7).toFixed(1)}L${x.toFixed(1)},${y.toFixed(1)}Z" class="c-turn-up"/>`);
      }
    });
    // State ribbon: one coloured run per stretch of the same state
    stateRuns(rows).forEach(run => {
      const x0 = Math.max(pl, X(run.from) - step / 2), x1 = Math.min(w - pr, X(run.to) + step / 2);
      sv.push(`<rect x="${x0.toFixed(1)}" y="${ribY}" width="${Math.max(1, x1 - x0).toFixed(1)}" height="${ribH}" class="bd-rib-${run.key}"/>`);
    });
    if (!compact) {
      const xl = w - pr + 10;
      const ia = [{ y: YA(last.close) + 4, s: `₹${num(last.close, 0)}`, cls: 'c-label' }, { y: YA(last.line) + 4, s: `Line ₹${num(last.line, 0)}`, cls: 'c-label2' },
                  { y: YA(last.hi2) + 4, s: `+2 SD ₹${num(last.hi2, 0)}`, cls: 'c-label3' }, { y: YA(last.lo2) + 4, s: `−2 SD ₹${num(last.lo2, 0)}`, cls: 'c-label3' }];
      V.spread(ia, 15, aTop, aTop + aH).forEach(it => sv.push(txt(xl, it.y, it.cls, it.s)));
      sv.push(txt(xl, ribY + ribH - 1, 'c-label3', 'State'));
    }
    sv.push(dateTicks(rows, X, h - 6, long, compact), '</svg>');
    box.innerHTML = sv.join('');
    V.hover(box, { pl, pt: aTop, iw, ih: ribY + ribH - aTop, h, pb: h - ribY - ribH }, rows.map((_, i) => X(i)), i => {
      const r = rows[i];
      return `<strong>${fmt.day(r.date)}</strong> · ${esc(label(BAND, r.state))}<br>₹${num(r.close, 0)} vs line ₹${num(r.line, 0)} · z ${sdTxt(r.z)}`
        + `<br><span class="c-tip-sub">45-day revenue ₹${num(r.adr45, 2)} Cr · 10d/45d ${r.ratio === null ? '—' : num(r.ratio, 2)}</span>`;
    });
  }

  // Revenue in, price out (the CIO's "inputs in, answer out"), for the selected stock
  function renderBandCalc(s) {
    const t = s.today, v = BD.calc[s.name] !== undefined ? BD.calc[s.name] : t.adr45;
    $('bdCalcSub').textContent = `${s.name}: today’s line at a 45-day average revenue you choose`;
    $('bdCalc').innerHTML = `<div class="bd-calc-row"><label for="bdAdr">45-day revenue, ₹ Cr</label>`
      + `<input id="bdAdr" type="number" inputmode="decimal" min="0" step="0.1" value="${esc(String(v))}">`
      + '<button type="button" data-step="-0.1">−10%</button><button type="button" data-step="0.1">+10%</button><button type="button" data-step="0">Today</button></div>'
      + '<div id="bdCalcOut"></div>'
      + `<p class="note">Line: price = ${num(t.a, 0)} + ${num(t.b, 1)} × 45-day revenue, fitted on ${num(t.n)} days before today; SD ₹${num(t.sd, 0)}. Line values, not targets.</p>`;
    bandCalcOut(s);
  }
  function bandCalcOut(s) {
    const r = calcLine(s.today, $('bdAdr').value);
    $('bdCalcOut').innerHTML = r.error ? `<p class="bd-calc-err" role="alert">${esc(r.error)}</p>`
      : '<div class="bd-calc-out">' + stat('On the line', val(`₹${num(r.line, 0)}`)) + stat('Buy-watch below', val(`₹${num(r.buyWatch, 0)}`), '−2 SD')
        + stat('Trim above', val(`₹${num(r.trim, 0)}`), '+2 SD') + '</div>';
  }

  function renderBandFlags(s) {
    const ep = s.episodes || [];
    if (!ep.length) { $('bdFlags').innerHTML = '<p class="note">No flags in this stock’s history yet.</p>'; return; }
    $('bdFlags').innerHTML = '<ul class="bd-flags">' + ep.map(e => `<li>${pill(BAND, FLAG_STATE[e.flag])}`
      + `<span>${fmt.day(e.date)}<small class="bd-card-line"> · z ${sdTxt(e.z)}</small></span>`
      + `<span class="num">${e.ret === null ? '<span class="bd-card-line">open</span>' : `<span class="${e.ret >= 0 ? 'up' : 'down'}">${pctTxt(e.ret)}</span>`}</span></li>`).join('')
      + '</ul><p class="note">Close to close over the next 20 revenue days, for the record; not a backtest.</p>';
  }

  function renderVerdict(v) {
    if (!v) { $('bdVerdict').innerHTML = ''; return; }
    $('bdVerdict').innerHTML = tradeTable(v.rules.map(r => `<tr><td>${esc(r.rule)}</td><td><strong>${esc(r.verdict)}</strong></td><td>${esc(r.detail)}</td></tr>`),
      ['Rule', 'Verdict', 'Out of sample, after costs'])
      + `<p class="note">Pre-registered test on MCX (2022–2026), BSE (2024–2026) and Angel One (2025–2026; its revenue is a model), ${esc(fmt.day(v.as_of))}. Full memo: ${esc(v.doc)}.</p>`;
  }

  // Card and tab clicks select a stock; the calculator recomputes as you type
  $('bdCards').addEventListener('click', e => { const b = e.target.closest('button[data-stock]'); if (b) bdSelect(b.dataset.stock); });
  $('bdTabs').addEventListener('click', e => { const b = e.target.closest('button[data-stock]'); if (b) bdSelect(b.dataset.stock); });
  $('bdCalc').addEventListener('input', e => {
    if (e.target.id !== 'bdAdr') return;
    const s = bdSelected(); if (!s) return;
    BD.calc[s.name] = e.target.value; bandCalcOut(s);
  });
  $('bdCalc').addEventListener('click', e => {
    const b = e.target.closest('button[data-step]'), s = bdSelected();
    if (!b || !s) return;
    const st = Number(b.dataset.step), cur = Number($('bdAdr').value) || s.today.adr45;
    const nv = st === 0 ? s.today.adr45 : cur * (1 + st);
    $('bdAdr').value = (st === 0 ? nv : Math.round(nv * 100) / 100).toString();
    BD.calc[s.name] = $('bdAdr').value; bandCalcOut(s);
  });
  makeRangeToggle({ key: 'bands', containerId: 'bdRange', ranges: BAND_RANGES, defaultRange: '1Y', labelIds: ['bdRangeLabel'],
                    onChange: () => loadBands() });
  if (window.ResizeObserver) {
    let queued = false;
    new ResizeObserver(() => {
      if (queued) return;
      queued = true;
      requestAnimationFrame(() => {
        queued = false;
        const el = $('bands'), s = bdSelected();
        if (BD.data && s && el.clientWidth && Math.abs(el.clientWidth - BD.width) > 2) { BD.width = el.clientWidth; if ((s.history || []).length > 1) bandChart($('bdChart'), s); }
      });
    }).observe($('bands'));
  }

  MCX.signals = { momentum: { mount: loadMomentum }, ensemble: { mount: loadModels }, options: { mount: loadOptions }, bands: { mount: loadBands } };
})();
