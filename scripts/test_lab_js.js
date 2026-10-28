#!/usr/bin/env node
/* Unit tests for the Lab pages' wording and calculations (MCX.labModel in assets/js/lab.js).
   Fixtures: /api/exchange_dashboard?view=home, /api/analytics and /api/commodity_dashboard on 29 Sep 2026.
   Run:  node scripts/test_lab_js.js */
const fs = require('fs');
const path = require('path');
const vm = require('vm');

global.window = global;
global.MCX_TEST = true;
global.document = { hidden: false, addEventListener() {}, documentElement: { dataset: {}, classList: { contains: () => false, toggle() {} } } };
global.localStorage = { getItem: () => null, setItem() {} };
for (const f of ['core.js', 'lab.js']) {
  const file = path.join(__dirname, '..', 'assets', 'js', f);   // our own sources, run in this context
  vm.runInThisContext(fs.readFileSync(file, 'utf8'), { filename: file });
}
const M = MCX.labModel;

let failed = 0;
function check(name, got, want) {
  const ok = JSON.stringify(got) === JSON.stringify(want);
  console.log((ok ? 'PASS ' : 'FAIL ') + name + (ok ? '' : `: got ${JSON.stringify(got)}, want ${JSON.stringify(want)}`));
  if (!ok) failed++;
}

// Diagnostics
const grid = [
  { min: 30, time: '09:30', n: 64, q10_pct: 11.85, median_pct: 71.97, q90_pct: 151.35, p90_abs_pct: 151.35 },
  { min: 180, time: '12:00', n: 72, q10_pct: -22.1, median_pct: 6.2, q90_pct: 41.0, p90_abs_pct: 41.0 },
  { min: 390, time: '15:30', n: 77, q10_pct: -27.27, median_pct: -3.62, q90_pct: 22.05, p90_abs_pct: 30.31 },
  { min: 630, time: '19:30', n: 76, q10_pct: -10.22, median_pct: 1.08, q90_pct: 8.75, p90_abs_pct: 14.42 },
  { min: 720, time: '21:00', n: 75, q10_pct: -6.8, median_pct: 3.4, q90_pct: 8.1, p90_abs_pct: 9.4 },
];
const an = { weight_sensitivity: [{ ecm_weight: 0.3, ic: 0.1583, is_current: true }], rolling_ic: [{ ensemble_ic: 0.2 }, { ensemble_ic: 0.0624 }] };
const dl = M.diagLede({ grid }, an);
check('diagnostics headline', dl.head, 'By 21:00 the live projection has landed within 9% of the final on 9 days in 10; at noon a miss of 41% is still possible.');
check('diagnostics deck', dl.deck, 'The first projection of the day (09:30) has run high: on the median day it was 72% above the final. '
  + 'From 15:00 the median miss stays within 4% either way. '
  + 'The ensemble score has had a modest link to the share price over the following week (correlation +0.16 over the full history), weaker over the last 60 signal days (+0.06).');
check('without analytics, only the projection', M.diagLede({ grid }, null).deck.includes('ensemble'), false);

check('correlation of a line with itself', Math.round(M.corr([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], [2, 4, 6, 8, 10, 12, 14, 16, 18, 20]) * 1000) / 1000, 1);
check('correlation skips missing pairs', Math.round(M.corr([1, 2, null, 4, 5, 6, 7, 8, 9, 10, 11], [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]) * 1000) / 1000, 1);
check('too few pairs', M.corr([1, 2, 3], [1, 2, 3]), null);

// Positioning
const g = { Futures: { current: 139893, wow_pct: 3.8, mom_pct: 11.6, yoy_pct: 34.1 }, Options: { current: 112850, wow_pct: -18.0, mom_pct: -6.5, yoy_pct: 18.4 },
            Overall: { current: 252743, wow_pct: -7.2, mom_pct: 2.7, yoy_pct: 26.6 } };
const pl = M.posLede(g, '2026-09-25');
check('positioning headline', pl.head, 'Participation in MCX contracts is 27% higher than a year ago, led by futures (+34% against +18% for options).');
check('positioning deck (Indian digit grouping, as everywhere on the site)', pl.deck, '2,52,743 participants counted across contracts on 25 Sep: −7.2% on the week and +2.7% on the month. Options participation fell 18% in the week.');
const silver = { fpo_long: 0, fpo_short: 0, vcp_long: 690, vcp_short: 159, prop_long: 98, prop_short: 66, dfi_long: 14, dfi_short: -1, foreign_long: 0, foreign_short: 0, others_long: 47227, others_short: 7499 };
const mx = M.mix(silver);
check('mix: shares of all long and short participants', mx.parts.map(x => [x.key, Math.round(x.share * 10) / 10]), [['hedgers', 1.5], ['prop', 0.3], ['inst', 0], ['others', 98.2]]);
check('a suppressed count (−1) is flagged and counted as zero', [mx.suppressed, mx.parts[2].n], [true, 14]);

// Margins: rises in contracts now carrying a tender margin are the expiry cycle
const changes = [{ date: '2026-09-29', symbol: 'ALUMINIUM', old_total: 16.34, new_total: 24.68, change: 8.34 },
                 { date: '2026-09-29', symbol: 'CARDAMOM', old_total: 23.68, new_total: 35.36, change: 11.68 },
                 { date: '2026-09-29', symbol: 'NATURALGAS', old_total: 16.11, new_total: 16.54, change: 0.43 },
                 { date: '2026-09-29', symbol: 'ELECDMBL', old_total: 21.64, new_total: 21.06, change: -0.58 },
                 { date: '2026-09-15', symbol: 'CRUDEOIL', old_total: 32, new_total: 30, change: -2 },
                 { date: '2026-07-01', symbol: 'GOLD', old_total: 10, new_total: 11, change: 1 }];
const cur = [{ symbol: 'CARDAMOM', total_margin_pct: 35.36, tender_margin_pct: 23.36 }, { symbol: 'ALUMINIUM', total_margin_pct: 24.68, tender_margin_pct: 16.68 },
             { symbol: 'CRUDEOIL', total_margin_pct: 30, tender_margin_pct: 0 }, { symbol: 'NATURALGAS', total_margin_pct: 16.54, tender_margin_pct: 0 }];
const ml = M.marginLede(changes, cur);
check('margins headline separates the tender period', ml.head, 'Two contracts entered their tender period in the 29 Sep snapshot, adding 8.3 to 11.7 points of margin; two other margins moved by 0.4 to 0.6 points.');
check('margins deck', ml.deck, 'A tender margin is charged as a contract nears expiry and falls away after it, so most swings in total margin follow the expiry cycle rather than a change in MCX’s risk settings. '
  + 'Leaving tender margins out, the highest now is Crude oil at 30.0%. Total margins changed 5 times in the last 30 days, tender margins included.');
check('without tender margins, a plain rise', M.marginLede([{ date: '2026-09-29', symbol: 'GOLD', old_total: 8, new_total: 9, change: 1 }], [{ symbol: 'GOLD', total_margin_pct: 9, tender_margin_pct: 0 }]).head,
  'MCX raised margins on one contract in the 29 Sep snapshot, by 1.0 points.');
check('names', ['CRUDEOIL', 'MCXBULLDEX', 'ELECTRCITY', 'GOLD'].map(M.pretty), ['Crude oil', 'Bulldex index', 'Electricity', 'Gold']);

console.log(failed ? `\n${failed} FAILED` : '\nAll Lab model tests passed.');
process.exit(failed ? 1 : 0);
