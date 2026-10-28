#!/usr/bin/env node
/* Unit tests for the Signals pages' wording and calculations (MCX.signalsModel in assets/js/signals.js).
   Fixtures: /api/models?view=momentum and /api/models on 29 Sep 2026 (data through 28 Sep).
   Run:  node scripts/test_signals_js.js */
const fs = require('fs');
const path = require('path');
const vm = require('vm');

global.window = global;
global.MCX_TEST = true;
global.document = { hidden: false, addEventListener() {}, documentElement: { dataset: {}, classList: { contains: () => false, toggle() {} } } };
global.localStorage = { getItem: () => null, setItem() {} };
for (const f of ['core.js', 'signals.js']) {
  const file = path.join(__dirname, '..', 'assets', 'js', f);   // our own sources, run in this context
  vm.runInThisContext(fs.readFileSync(file, 'utf8'), { filename: file });
}
const M = MCX.signalsModel;

let failed = 0;
function check(name, got, want) {
  const ok = JSON.stringify(got) === JSON.stringify(want);
  console.log((ok ? 'PASS ' : 'FAIL ') + name + (ok ? '' : `: got ${JSON.stringify(got)}, want ${JSON.stringify(want)}`));
  if (!ok) failed++;
}
const r2 = v => Math.round(v * 100) / 100;

// ── Weights are the ones in lib/cron_models.py ─────────────────────────────
const src = fs.readFileSync(path.join(__dirname, '..', 'lib', 'cron_models.py'), 'utf8');
check('cron_models.py still blends ECM 0.30 and MF 0.70', [/W_ECM\s*=\s*0\.30/.test(src), /W_MF\s*=\s*0\.70/.test(src)], [true, true]);
check('cron_models.py still weights revenue 3/7 and turnover 4/7', [/W_REV\s*=\s*3\s*\/\s*7/.test(src), /W_TURN\s*=\s*4\s*\/\s*7/.test(src)], [true, true]);
check('so the page shows revenue 30%, turnover 40%, price gap 30%', [r2(M.W.rev * 100), r2(M.W.turn * 100), r2(M.W.ecm * 100)], [30, 40, 30]);
const momSrc = fs.readFileSync(path.join(__dirname, '..', 'lib', 'cron_momentum.py'), 'utf8');
check('cron_momentum.py thresholds match the page (1.05 / 0.95)',
  [/REGIME_HOT_THRESHOLD\s*=\s*1\.05/.test(momSrc), /REGIME_COLD_THRESHOLD\s*=\s*0\.95/.test(momSrc), M.HOT, M.COLD], [true, true, 1.05, 0.95]);

// ── Momentum ───────────────────────────────────────────────────────────────
const snap = { date: '2026-09-28', close_price: 3263.5, fno_rev_cr: 14.5299, regime: 'HOT', ratio_10d_45d: 1.2113, ma10_rev_cr: 14.5181,
               ma45_rev_cr: 11.9856, adr_signal: 'NEUTRAL', adr_ratio: 1.3713, price_mom_5d: 0.017, composite_signal: 'STRONG_BUY' };
const c = M.cushion(snap);
check('hot: the 10-day can fall ₹1.93 Cr to ₹12.58 Cr (13%) before it cools', [c.to, r2(c.gap), r2(c.level), Math.round(c.pct)], ['cool', 1.93, 12.58, 13]);
check('cold: gap to warm', (x => [x.to, r2(x.level), r2(x.gap)])(M.cushion({ regime: 'COLD', ma10_rev_cr: 10, ma45_rev_cr: 12 })), ['warm', 11.4, 1.4]);
check('neutral: the nearer boundary', (x => [x.to, r2(x.level), r2(x.other)])(M.cushion({ regime: 'NEUTRAL', ma10_rev_cr: 12.4, ma45_rev_cr: 12 })), ['hot', 12.6, 11.4]);
check('no cushion without averages', M.cushion({ regime: 'HOT', ma10_rev_cr: null, ma45_rev_cr: 12 }), null);

const days = (regimes, closes) => regimes.map((g, i) => ({ date: `2026-08-${String(i + 1).padStart(2, '0')}`, regime: g, close_price: closes[i],
  composite_signal: g === 'HOT' ? 'STRONG_BUY' : g === 'COLD' ? 'SELL' : 'HOLD' }));
const rows = days(['COLD', 'NEUTRAL', 'NEUTRAL', 'HOT', 'HOT', 'HOT'], [100, 101, 102, 110, 112, 121]);
check('streak: three hot days from 4 Aug', M.streak(rows, 'regime'), { value: 'HOT', n: 3, start: '2026-08-04', startIdx: 3, capped: false });
check('streak that fills the history is capped', M.streak(days(['HOT', 'HOT'], [1, 2]), 'regime').capped, true);

const l = M.momLede(Object.assign({}, snap, { close_price: 121 }), M.streak(rows, 'regime'), rows);
check('momentum headline', l.head, 'Momentum reads strong buy: revenue has run hot for 3 trading days.');
check('momentum deck: ratio, cushion, price since the regime turned',
  l.deck, 'The 10-day average of ₹14.52 Cr is 1.21× the 45-day average of ₹11.99 Cr. At today’s 45-day average, it would have to fall below ₹12.58 Cr, a 13% drop, for the regime to cool. The share price is up 10.0% since the regime turned hot on 4 August.');
check('neutral headline says the neutral zone', M.momLede(Object.assign({}, snap, { regime: 'NEUTRAL', composite_signal: 'HOLD' }), { n: 2, capped: false, startIdx: 4 }, rows).head,
  'Momentum reads hold: revenue has been in the neutral zone for 2 trading days.');
check('a capped run says "more than"', M.momLede(snap, { n: 252, capped: true, startIdx: 0 }, rows).head, 'Momentum reads strong buy: revenue has run hot for more than 252 trading days.');
check('price overlay text', M.overlayText(snap), '5-day average daily move is 1.37× the 20-day; price +1.7% over 5 days');

const ch = M.changes(rows, 'composite_signal');
check('changes, most recent first', ch.map(x => [x.date, x.from, x.to]), [['2026-08-04', 'HOLD', 'STRONG_BUY'], ['2026-08-02', 'SELL', 'HOLD']]);
check('each run: length and price change to the next change (or to now)', ch.map(x => [x.days, r2(x.runPct), x.ongoing]), [[3, 10, true], [2, 8.91, false]]);

const fw = M.forward(days(Array(14).fill('HOT').map((g, i) => (i % 2 ? 'HOT' : 'COLD')), [100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 110, 90, 110, 90]),
                     'composite_signal', ['STRONG_BUY', 'SELL'], 10);
check('forward 10-day returns from the next day’s close, grouped by the day’s signal',
  fw.rows.map(x => [x.key, x.n, r2(x.avg), x.up]), [['STRONG_BUY', 1, 10, 100], ['SELL', 2, -10, 0]]);
check('all days row: the last 11 days have no outcome yet', [fw.all.n, r2(fw.all.avg)], [3, -3.33]);
const fw0 = M.forward(days(Array(14).fill('HOT').map((g, i) => (i % 2 ? 'HOT' : 'COLD')), [100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 110, 90, 110, 90]),
                      'composite_signal', ['STRONG_BUY', 'SELL'], 10, 'close_price', 0);
check('with no lag (same-day entry, not tradeable) the old figures come back', fw0.rows.map(x => [x.key, x.n, r2(x.avg)]), [['STRONG_BUY', 2, -10], ['SELL', 2, 10]]);
const priced = [100, 100, 100, 110].map((price, i) => ({ date: `2026-08-0${i + 1}`, ensemble_signal: 'BUY', price }));
check('model rows carry the price as "price"', (x => [x.all.n, r2(x.all.avg)])(M.forward(priced, 'ensemble_signal', ['BUY'], 2, 'price')), [1, 10]);

// ── Model ensemble ─────────────────────────────────────────────────────────
const es = { date: '2026-09-28', price: 3263.5, fair_value_base: 2940.65,
  ecm: { spread: 322.85, spread_pct: 10.98, z_score: -0.727, half_life_days: 34.7, signal: 'MILD_REVERT_UP' },
  multi_factor: { revenue_z: 1.131, turnover_z: 1.232, volume_z: -0.916, volatility_z: 0.843, composite_z: 1.189, signal: 'BUY' },
  ensemble: { score: 1.05, signal: 'BUY' }, position: { score: 0.525, conviction: 0.525, momentum: 1.05, velocity_1d: -0.026, conviction_2d_ma: 0.538 } };
const k = M.contributions(es);
check('contributions: revenue +0.34, turnover +0.49, price gap +0.22, total +1.05',
  [...k.parts.map(p => r2(p.value)), r2(k.total)], [0.34, 0.49, 0.22, 1.05]);
check('the parts add up to the stored score', Math.abs(k.total - es.ensemble.score) < 0.01, true);
const el = M.ensLede(es);
check('ensemble headline names the biggest part', el.head, 'The model ensemble reads buy, led by exchange turnover running well above its 60-day norm.');
check('ensemble deck', el.deck, 'The score of +1.05 adds three parts: revenue +0.34, turnover +0.49 and the price gap to fair value +0.22. It would read neutral below +0.5 and strong buy above +1.5.');
const weak = JSON.parse(JSON.stringify(es));
Object.assign(weak.multi_factor, { revenue_z: 0.2, turnover_z: -0.1 }); weak.ecm.z_score = 0.1; weak.ensemble = { score: 0.0, signal: 'NEUTRAL' };
check('no strong part', M.ensLede(weak).head, 'The model ensemble reads neutral, with no part pulling hard either way.');
check('price gap in the lead', M.ensLede(Object.assign({}, es, { ecm: Object.assign({}, es.ecm, { z_score: -3 }) })).head,
  'The model ensemble reads buy, led by the price’s gap to fair value sitting well below its 60-day norm.');
check('ECM paragraph', M.ecmText(es, 16.32),
  'The price, ₹3,263.5, is 11.0% above the data-driven fair value of ₹2,941. Over the last 60 days the gap has averaged +16.3%, so today it is 0.73 standard deviations below its norm. '
  + 'The model expects the gap to widen back toward its norm, which reads as room for the price to rise relative to fair value.');

console.log(failed ? `\n${failed} FAILED` : '\nAll Signals model tests passed.');
process.exit(failed ? 1 : 0);
