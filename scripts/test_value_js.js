#!/usr/bin/env node
/* Unit tests for the Earnings & value pages' wording and calculations (MCX.valueModel in
   assets/js/value.js). Fixture: /api/quarterly on 29 Sep 2026 with the revenue-basis block.
   Run:  node scripts/test_value_js.js */
const fs = require('fs');
const path = require('path');
const vm = require('vm');

global.window = global;
global.MCX_TEST = true;
global.document = { hidden: false, addEventListener() {}, documentElement: { dataset: {}, classList: { contains: () => false, toggle() {} } } };
global.localStorage = { getItem: () => null, setItem() {} };
for (const f of ['core.js', 'value.js']) {
  const file = path.join(__dirname, '..', 'assets', 'js', f);   // our own sources, run in this context
  vm.runInThisContext(fs.readFileSync(file, 'utf8'), { filename: file });
}
const M = MCX.valueModel;

let failed = 0;
function check(name, got, want) {
  const ok = JSON.stringify(got) === JSON.stringify(want);
  console.log((ok ? 'PASS ' : 'FAIL ') + name + (ok ? '' : `: got ${JSON.stringify(got)}, want ${JSON.stringify(want)}`));
  if (!ok) failed++;
}

const D = {
  actuals: [{ quarter: 'Q4 FY26', pat_cr: 530 }, { quarter: 'Q1 FY27', pat_cr: 413 }],
  current_quarter: { quarter: 'Q2 FY27', trading_days_elapsed: 64, trading_days_total: 66, today_in_remaining: true, pat_projected_cr: 456.7 },
  non_fo: { share: 0.0873, share_from: 'Q1 FY27', estimate_cr: 65.3, pat_projected_cr: 500.6 },
  backtest: {
    rows: [-27.9, -19.1, -53.1, -32.1, -19.1].map((m, i) => ({ miss_fo_cr: m, miss_all_cr: [2.0, 3.7, -10.9, 22.3, 23.5][i] })),
    miss_fo_avg_cr: -30.3, miss_all_avg_cr: 8.1, abs_miss_fo_avg_cr: 30.3, abs_miss_all_avg_cr: 12.5,
  },
};

const fo = M.quarterLede(D, 'fo');
check('F&O headline', fo.head, 'On F&O revenue alone, the model projects Q2 profit of ₹456.7 Cr, with two sessions to go, including today.');
check('F&O deck: vs last quarter and the one-sided past miss', fo.deck,
  'That would be 11% above Q1’s reported ₹413 Cr. This basis leaves out non-F&O revenue, and it has come in below reported profit in each of the last five quarters, by ₹30 Cr on average.');
const all = M.quarterLede(D, 'all');
check('including non-F&O headline', all.head, 'Including non-F&O revenue, the model projects Q2 profit of ₹500.6 Cr, with two sessions to go, including today.');
check('including non-F&O deck', all.deck,
  'Non-F&O revenue is estimated at ₹65.3 Cr, 8.7% of F&O revenue as in Q1 FY27. That would put profit 21% above Q1’s reported ₹413 Cr. On this basis the model has missed reported profit by ₹13 Cr on average over the last five quarters, landing above it in four of them.');

const done = JSON.parse(JSON.stringify(D));
done.current_quarter.trading_days_elapsed = 66; done.current_quarter.today_in_remaining = false;
check('quarter over', M.quarterLede(done, 'fo').head, 'On F&O revenue alone, the model projects Q2 profit of ₹456.7 Cr, with the quarter’s trading complete.');
const one = JSON.parse(JSON.stringify(D));
one.current_quarter.trading_days_elapsed = 65; one.current_quarter.today_in_remaining = false;
check('one session, not today', M.quarterLede(one, 'fo').head, 'On F&O revenue alone, the model projects Q2 profit of ₹456.7 Cr, with one session to go.');

const mixed = JSON.parse(JSON.stringify(D));
mixed.backtest.rows[0].miss_fo_cr = 5;
check('when F&O only was not low every time, it says so', M.quarterLede(mixed, 'fo').deck.split('. ').slice(1).join('. '),
  'This basis leaves out non-F&O revenue; over the last five quarters it missed reported profit by ₹30 Cr on average, landing below it in four.');

const adj = M.adjusted(D);
check('both bases adjusted for past misses', [Math.round(adj.fo), Math.round(adj.all), adj.close], [487, 493, true]);
check('no adjustment without the non-F&O basis', M.adjusted(Object.assign({}, D, { non_fo: null })), null);

console.log(failed ? `\n${failed} FAILED` : '\nAll value model tests passed.');
process.exit(failed ? 1 : 0);
