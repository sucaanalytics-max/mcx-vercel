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

// Fair value
const fvd = { bear: 2442.46, base: 2981.27, bull: 3520.07 };
check('data-driven signal rules', [2400, 2800, 3000, 3300, 3600].map(p => M.ddSignal(p, fvd)), ['DEEP_VALUE', 'UNDERVALUED', 'FAIR', 'OVERVALUED', 'STRETCHED']);
const chainD = { diluted_shares_cr: 25.451, pat_margin: 0.55, non_fo_rev_cr: 374.5, trading_days: 256 };
check('revenue priced in at the data-driven multiple', Math.round(M.revenuePricedIn(3337.6, 40.06, chainD) * 100) / 100, 13.6);

// Tusk house model: the sheet, exactly (₹15.00 × 258, 42 / 48 / 54×, 18% then 9%)
const INP = { adr_fy28: 15, days_fy28: 260, non_fo_fy26: 211.06, other_income_fy26: 127.05, growth_fy27: 0.2, growth_fy28: 0.15,
              margin: 0.57, pe: { bear: 42, base: 48, bull: 54 }, disc_fy28: 0.18, disc_today: 0.09, method: 'fixed' };
const R0 = o => ({ bear: Math.round(o.bear), base: Math.round(o.base), bull: Math.round(o.bull) });
const sheet = M.houseCalc(Object.assign({}, INP, { days_fy28: 258 }), 25.451, '2026-09-29');
check('sheet: operating, other operating, other income, total, PAT', [sheet.op, sheet.nonFo, sheet.other, sheet.total, sheet.pat].map(Math.round), [3870, 291, 175, 4337, 2472]);
check('sheet: FY28 targets', R0(sheet.fy28), { bear: 4079, base: 4662, bull: 5245 });
check('sheet: FY27 targets', R0(sheet.fy27), { bear: 3457, base: 3951, bull: 4445 });
check('sheet: today (bear and bull as shown; base 3,624.5)', [Math.round(sheet.today.bear), Math.round(sheet.today.base * 10) / 10, Math.round(sheet.today.bull)], [3171, 3624.5, 4078]);
// The house defaults: 260 sessions
const hc = M.houseCalc(INP, 25.451, '2026-09-29');
check('house defaults today, same as lib/house_model.py', R0(hc.today), { bear: 3193, base: 3650, bull: 4106 });
const pro = M.houseCalc(Object.assign({}, INP, { method: 'prorata' }), 25.451, '2026-09-29');
check('pro-rata: 18% × 183 / 365 = 9.02%', [pro.daysLeft, Math.round(pro.step2 * 10000) / 100], [183, 9.02]);
check('pro-rata after 31 Mar 2027 is zero', M.houseCalc(Object.assign({}, INP, { method: 'prorata' }), 25.451, '2027-04-15').step2, 0);
check('breakeven: the FY28 revenue per day at which the base case equals ₹3,263.5', Math.round(M.breakevenAdr(3263.5, INP, 25.451, hc) * 100) / 100, 13.22);
check('house state bands', [2900, 3300, 3500, 3700, 3900, 4300].map(p => M.houseState(p, hc.today)), ['deep', 'under', 'near', 'near', 'over', 'stretched']);

const V = { snapshot: { fair_value: fvd, eps_chain: { ma45_rev_cr: 11.99 } }, pe_bands: { mean: 40.06 } };
const fl = M.fvLede(3263.5, hc.today, INP, V);
check('fair value headline', fl.head, 'The Tusk house model values MCX at ₹3,650 a share, 12% above the price; the data-driven view has it about 9% overvalued.');
check('fair value deck', fl.deck, 'At ₹3,264, the price sits between the house bear case (₹3,193 at 42×) and base case (₹3,650 at 48×), and 9% above the data-driven base of ₹2,981. '
  + 'The house case rests on ₹15.00 Cr a day in FY28, against ₹11.99 Cr over the last 45 days, and on 42–54× FY28 earnings, against the stock’s own median of 40.1× run-rate earnings.');
check('near the base case', M.fvLede(3600, hc.today, INP, V).head, 'The Tusk house model values MCX at ₹3,650 a share, close to the price; the data-driven view has it about 21% overvalued.');

// Scenarios: trailing EPS from the last four reported quarters
const acts = [{ quarter: 'Q1 FY26', pat_cr: 203 }, { quarter: 'Q2 FY26', pat_cr: 197 }, { quarter: 'Q3 FY26', pat_cr: 401 }, { quarter: 'Q4 FY26', pat_cr: 530 }, { quarter: 'Q1 FY27', pat_cr: 413 }];
const tt = M.ttm(acts, 25.451);
check('TTM EPS = (197 + 401 + 530 + 413) / 25.451', [Math.round(tt.eps * 100) / 100, tt.from, tt.to], [60.55, 'Q2 FY26', 'Q1 FY27']);
check('no TTM with under four quarters', M.ttm(acts.slice(0, 3), 25.451), null);

// Scenarios: the arithmetic moved from legacy.js
const A = { days: 256, opex: 700, other: 126, tax: 20.3, shares: 25.451 };
const y = M.yearModel(12.62, 42, A);
check('a year at ₹12.62 Cr a day: revenue, PAT, EPS, price', [Math.round(y.annualRev * 100) / 100, Math.round(y.pat * 100) / 100, Math.round(y.eps * 100) / 100, Math.round(y.price * 10) / 10],
  [3230.72, 2117.41, 83.2, 3494.2]);
check('a loss year books no tax and no profit', [M.yearModel(1, 42, A).tax, M.yearModel(1, 42, A).pat], [0, 0]);
const im = M.impliedBy(3232, 12.62, 42, A);
check('what ₹3,232 implies: revenue per day at 42×, P/E at ₹12.62 Cr', [Math.round(im.rev * 1000) / 1000, Math.round(im.pe * 100) / 100], [11.841, 38.85]);
const tr = M.trendRow({ adr: 11.2, days: 256, other: 336, margin: 60, pe: 42 }, 25.451);
check('FY27 trend row', [Math.round(tr.op * 10) / 10, Math.round(tr.tot * 10) / 10, Math.round(tr.eps * 100) / 100, Math.round(tr.px)], [2867.2, 3203.2, 75.51, 3172]);
const cr = M.caseRow({ growth: 10, adr: 11.2, other: 336, margin: 60, pe: 48 }, 256, 25.451, 18, 3232);
check('FY27 case with 10% growth, 48×, discounted 18%', [Math.round(cr.tot * 10) / 10, Math.round(cr.eps * 100) / 100, Math.round(cr.px), Math.round(cr.upside * 10) / 10, Math.round(cr.target)],
  [3489.9, 82.27, 3949, 22.2, 3347]);

console.log(failed ? `\n${failed} FAILED` : '\nAll value model tests passed.');
process.exit(failed ? 1 : 0);
