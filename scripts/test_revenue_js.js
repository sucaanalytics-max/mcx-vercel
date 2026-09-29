#!/usr/bin/env node
/* Unit tests for the Revenue pages' calculations (MCX.revenueModel in assets/js/revenue.js).
   Fixtures are taken from /api/exchange_dashboard and /api/commodity_dashboard on 29 Sep 2026.
   Run:  node scripts/test_revenue_js.js */
const fs = require('fs');
const path = require('path');
const vm = require('vm');

global.window = global;
global.MCX_TEST = true;
global.document = { hidden: false, addEventListener() {}, documentElement: { dataset: {}, classList: { contains: () => false, toggle() {} } } };
global.localStorage = { getItem: () => null, setItem() {} };
for (const f of ['core.js', 'revenue.js']) {
  const file = path.join(__dirname, '..', 'assets', 'js', f);   // our own sources, run in this context
  vm.runInThisContext(fs.readFileSync(file, 'utf8'), { filename: file });
}
const M = MCX.revenueModel;

let failed = 0;
function check(name, got, want) {
  const ok = JSON.stringify(got) === JSON.stringify(want);
  console.log((ok ? 'PASS ' : 'FAIL ') + name + (ok ? '' : `: got ${JSON.stringify(got)}, want ${JSON.stringify(want)}`));
  if (!ok) failed++;
}

// Trends
const Q = [
  { quarter: 'Q2 FY27', trading_days: 64, avg_fut: 2.64, avg_opt: 8.61, avg_total: 11.25, qoq_total: 11.0, yoy_total: 117.0, yoy_fut: 51.0, yoy_opt: 152.0 },
  { quarter: 'Q1 FY27', trading_days: 64, avg_fut: 2.5, avg_opt: 7.59, avg_total: 10.09, qoq_total: -8.0, yoy_total: 60.0, yoy_fut: 10.0, yoy_opt: 90.0 },
  { quarter: 'Q4 FY26', trading_days: 61, avg_fut: 3.7, avg_opt: 7.3, avg_total: 11.0 },
  { quarter: 'Q3 FY26', trading_days: 63, avg_fut: 2.9, avg_opt: 5.7, avg_total: 8.6 },
  { quarter: 'Q2 FY26', trading_days: 64, avg_fut: 1.75, avg_opt: 3.42, avg_total: 5.17 },
];
const t = M.trendsLede(Q);
check('trends headline', t.head, 'Revenue is averaging ₹11.25 Cr a day this quarter, more than double a year ago.');
check('trends deck names both comparisons and the options shift', t.deck,
  'That is 117% above Q2 FY26 and 11% above Q1 FY27. Options drive the growth: 77% of revenue this quarter, up from 66% a year ago.');
const young = [Object.assign({}, Q[0], { quarter: 'Q3 FY27', trading_days: 2, yoy_total: null, qoq_total: null }), ...Q];
check('under 5 sessions into a quarter, it speaks about the last full one', M.trendsLede(young).head,
  'Q2 FY27 averaged ₹11.25 Cr a day, more than double a year ago.');
check('growth under 100% is given as a percentage', M.trendsLede([Object.assign({}, Q[0], { yoy_total: 31 }), ...Q.slice(1)]).head,
  'Revenue is averaging ₹11.25 Cr a day this quarter, 31% more than a year ago.');
check('a fall is a fall', M.trendsLede([Object.assign({}, Q[0], { yoy_total: -12 }), ...Q.slice(1)]).head,
  'Revenue is averaging ₹11.25 Cr a day this quarter, 12% less than a year ago.');
check('last year label', M.lastYear('Q1 FY27'), 'Q1 FY26');

// Rolling futures/options averages for the Trends chart
const rows = [1, 2, 3, 4].map(i => ({ date: `2026-09-0${i}`, fut: i, opt: 10 * i }));
check('rolling 2-session split', M.rollingSplit(rows, 2).map(r => r && [r.fut, r.opt]), [null, [1.5, 15], [2.5, 25], [3.5, 35]]);

// Seasonality
const QD = [['Monday', 10.82, 9.35, 116], ['Tuesday', 11.4, 10.1, 101], ['Wednesday', 11.1, 10.0, 118], ['Thursday', 12.3, 10.6, 131], ['Friday', 10.6, 10.3, 96]]
  .map(([day, cur, prev, yoy]) => ({ day, cur_q: 'Q2 FY27', prev_q: 'Q1 FY27', yoy_q: 'Q2 FY26', cur_total: cur, prev_total: prev, yoy_total_pct: yoy }));
const DW = [['Monday', 11.37], ['Tuesday', 12.1], ['Wednesday', 12.0], ['Thursday', 13.4], ['Friday', 12.2]].map(([day, v]) => ({ day, avg10_total: v }));
const se = M.seasonLede(QD, DW);
check('busiest and quietest weekday', se.head, 'Thursdays have been the busiest day in Q2 FY27, averaging ₹12.30 Cr, 16% more than Fridays.');
check('10-week leader and year-on-year range', se.deck, 'Thursdays have also led over the last 10 weeks, at ₹13.40 Cr. Every weekday is above Q2 FY26, by 96% to 131%.');
const DW2 = DW.map(r => (r.day === 'Tuesday' ? Object.assign({}, r, { avg10_total: 14 }) : r));
check('a different 10-week leader is named as such', M.seasonLede(QD, DW2).deck.split('.')[0], 'Over the last 10 weeks Tuesdays have led instead, at ₹14');
const flat = QD.map(r => Object.assign({}, r, { cur_total: 11 + (r.day === 'Monday' ? 0.2 : 0) }));
check('an even week is called even, not ranked', M.seasonLede(flat, DW).head, 'Revenue has been spread evenly across the week in Q2 FY27: every weekday averaged between ₹11.00 and ₹11.20 Cr.');

// Commodities
const SM = [
  { commodity: 'CRUDEOIL', last_day: 8.5, avg_5d: 5.63, avg_45d: 5.46, yoy_pct: 97 },
  { commodity: 'GOLD', last_day: 3.2, avg_5d: 4.01, avg_45d: 3.1, yoy_pct: 250 },
  { commodity: 'SILVER', last_day: 2.0, avg_5d: 2.45, avg_45d: 1.6, yoy_pct: 180 },
  { commodity: 'NATURALGAS', last_day: 1.8, avg_5d: 2.45, avg_45d: 1.5, yoy_pct: 40 },
  { commodity: 'COPPER', last_day: 0.2, avg_5d: 0.26, avg_45d: 0.25, yoy_pct: 300 },
  { commodity: 'OTHERS', last_day: 0.1, avg_5d: 0.09, avg_45d: 0.08, yoy_pct: 10 },
  { commodity: 'TOTAL', last_day: 15.8, avg_5d: 14.9, avg_45d: 11.99, yoy_pct: 110 },
];
const c = M.cmdLede(SM);
check('biggest commodity by the last 45 days', c.head, 'Crude oil brings in nearly half of MCX’s revenue: ₹5.46 Cr a day over the last 45 days.');
check('fastest grower ignores tiny commodities; biggest recent shift', c.deck,
  'Gold is growing fastest this year, 3.5 times its level a year ago. Over the last five days natural gas ran 63% above its 45-day average.');
check('names', ['CRUDEOIL', 'NATURALGAS', 'ZINC', 'SILVER100'].map(M.cname), ['Crude oil', 'Natural gas', 'Zinc', 'Silver 100']);

// Signal pills: text label always present, one scale
check('strong buy pill', M.signalPill('STRONG_BUY'), '<span class="pill pill--strong-up"><span aria-hidden="true">▲▲</span>Strong buy</span>');
check('neutral pill', M.signalPill('NEUTRAL'), '<span class="pill">Neutral</span>');
check('unknown is no data', M.signalPill(undefined), '<span class="pill pill--none">No data</span>');

console.log(failed ? `\n${failed} FAILED` : '\nAll Revenue model tests passed.');
process.exit(failed ? 1 : 0);
