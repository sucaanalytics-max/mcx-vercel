#!/usr/bin/env node
/* Unit tests for the Today page's calculations (MCX.todayModel in assets/js/today.js).
   Run:  node scripts/test_today_js.js */
const fs = require('fs');
const path = require('path');
const vm = require('vm');

global.window = global;
global.MCX_TEST = true;                       // load the model only, no page wiring
global.document = { hidden: false, addEventListener() {}, documentElement: { dataset: {}, classList: { contains: () => false, toggle() {} } } };
global.localStorage = { getItem: () => null, setItem() {} };
for (const f of ['core.js', 'today.js']) {
  const file = path.join(__dirname, '..', 'assets', 'js', f);   // our own sources, run in this context
  vm.runInThisContext(fs.readFileSync(file, 'utf8'), { filename: file });
}
const M = MCX.todayModel;

let failed = 0;
function check(name, got, want) {
  const ok = JSON.stringify(got) === JSON.stringify(want);
  console.log((ok ? 'PASS ' : 'FAIL ') + name + (ok ? '' : `: got ${JSON.stringify(got)}, want ${JSON.stringify(want)}`));
  if (!ok) failed++;
}
const r2 = v => Math.round(v * 100) / 100;

// Session state
const snap = (o) => Object.assign({ success: true, trading_date: '2026-09-29', session_closed: false }, o);
check('no refresh yet is before the open', M.sessionState(undefined, '2026-09-29'), 'pre');
check('a failed refresh (no snapshot today) is before the open', M.sessionState({ success: false }, '2026-09-29'), 'pre');
check('a snapshot from another day is not today', M.sessionState(snap({ trading_date: '2026-09-28' }), '2026-09-29'), 'pre');
check('open session', M.sessionState(snap(), '2026-09-29'), 'live');
check('closed session', M.sessionState(snap({ session_closed: true }), '2026-09-29'), 'closed');

// Measured error at the current minute
const grid = [{ min: 300, p90_abs_pct: 35 }, { min: 330, p90_abs_pct: 32 }, { min: 360, p90_abs_pct: null }, { min: 390, p90_abs_pct: 29 }];
check('interpolates between grid times', r2(M.gridAt(grid, 342, 'p90_abs_pct')), 31.4);
check('skips a time with no figure', r2(M.gridAt(grid, 360, 'p90_abs_pct')), 30.5);
check('holds the first value before the grid', M.gridAt(grid, 100, 'p90_abs_pct'), 35);
check('holds the last value after the grid', M.gridAt(grid, 800, 'p90_abs_pct'), 29);
check('empty grid', M.gridAt([], 300, 'p90_abs_pct'), null);

// Likely range from signed percentiles: the final lies in [P/(1+q90), P/(1+q10)]
const rg = M.likelyRange(13.68, -24.5, 40.9);
check('12:00 today: 10th/90th percentiles −24.5% / +40.9% give ₹9.71–18.12 Cr', [r2(rg.lo), r2(rg.hi)], [9.71, 18.12]);
check('the old size-of-miss method would have topped out at ₹24.8 (for the record)', r2(13.68 / (1 - 0.448)), 24.78);
check('no upper bound when projections have run 95% or more too low', M.likelyRange(15, -96, 20).hi, null);
check('no range without measured percentiles', M.likelyRange(15, null, 20), null);
const fb = M.finalBand(-24.5, 40.9);
check('final landed 29% below to 32% above the projection', [Math.round(fb.below), Math.round(fb.above)], [29, 32]);

// Headline: claims only what the whole range supports
const avgs = [10, 11, 12, 13, 11.5, 10.5].map((value, i) => ({ key: ['d5', 'd10', 'd20', 'd45', 'qtd', 'fytd'][i], value }));
check('above every average only when the low end clears them all',
  M.headlineLive(16, { lo: 13.5, hi: 20, p90: 20 }, avgs, 60), 'Today is on course for about ₹16.0 Cr, above every recent average.');
check('hedged when the low end does not clear them',
  M.headlineLive(16, { lo: 12, hi: 23, p90: 30 }, avgs, 39), 'Today is tracking towards ₹16.0 Cr, but it is early: the likely range is ₹12.0 to ₹23.0 Cr.');
check('no "early" once half the session is gone',
  M.headlineLive(16, { lo: 12, hi: 23, p90: 30 }, avgs, 60), 'Today is tracking towards ₹16.0 Cr; the likely range is ₹12.0 to ₹23.0 Cr.');
check('below every average only when the high end is below them all',
  M.headlineLive(7, { lo: 6, hi: 8.5, p90: 15 }, avgs, 70), 'Today is on course for about ₹7.0 Cr, below every recent average.');
check('while the measured range loads, no claim about it',
  M.headlineLive(16, null, avgs, 30, true), 'Today is tracking towards ₹16.0 Cr.');
check('before 09:30 there is no range to lean on',
  M.headlineLive(16, null, avgs, 2), 'Today is tracking towards ₹16.0 Cr, but it is too early to say how far to trust that.');

// Deck
check('before 17:00 the rest comes mostly in the evening',
  M.deckLive(15.32, 5.28, 342, avgs, 9.7),
  '₹5.28 Cr is booked so far. The projection expects the other ₹10.04 Cr later in the day, mostly in the evening hours (17:00–23:30), when trading is usually heaviest. At ₹15.3 Cr today would beat every average below. By 21:00, the final has landed within 10% of the projection on 8 days in 10.');
check('after 21:00: no evening claim and no 21:00 sentence',
  M.deckLive(12, 11.2, 760, avgs, 9.7), '₹11.20 Cr is booked so far. The projection expects the other ₹0.80 Cr before the close.');

// A day against its previous 45 days
const prev45 = Array.from({ length: 45 }, (_, i) => ({ date: `2026-07-${String((i % 28) + 1).padStart(2, '0')}`, total: 12 }));
check('above the previous 45-day average', M.vsPrev45(14.53, prev45).text, '21% above its previous 45-day average');
check('level within half a percent', M.vsPrev45(12.04, prev45).text, 'level with its previous 45-day average');
check('needs 45 days', M.vsPrev45(14, prev45.slice(1)), null);
check('rank among the last 60', M.rankText(16.9, [10, 12, 17, 16.9, 11].concat(Array(55).fill(9))), 'the second-best day of the last 60');
check('no rank outside the top five', M.rankText(10, [11, 12, 13, 14, 15, 16, 10]), null);

// Takeaway over the six averages
const A = vals => vals.map((value, i) => ({ key: ['d5', 'd10', 'd20', 'd45', 'qtd', 'fytd'][i], n: [5, 10, 20, 45, 64, 128][i], value, period: i === 5 ? 'FY27' : 'Q2 FY27' }));
check('shorter windows higher', M.takeaway(A([14.88, 14.52, 13.12, 11.99, 11.25, 10.67])),
  'The shorter the window, the higher the average: the last 5 days averaged ₹14.88 Cr, 39% above the FY27 average so far.');
check('shorter windows lower', M.takeaway(A([8, 9, 10, 11, 11.5, 12])),
  'The shorter the window, the lower the average: the last 5 days averaged ₹8.00 Cr, 33% below the FY27 average so far.');
check('mixed order names the strongest window, not an ordering', M.takeaway(A([14.5, 14.9, 13, 12, 11, 10.67])),
  'Recent days are running well above the longer averages: the last 10 averaged ₹14.90 Cr, 40% above the FY27 average so far.');
check('close to the year', M.takeaway(A([10.7, 10.6, 10.75, 10.65, 10.7, 10.67])), 'Recent days are running close to the FY27 average so far of ₹10.67 Cr.');
check('no sentence without the year', M.takeaway(A([10, 11, 12, 13, 14, null])), '');

// What each average is compared with
check('N-day window', M.change({ key: 'd5', n: 5, value: 14.88, chg_pct: 5.04, prev: { value: 14.16 } }), { dir: 'up', pct: 5.04, vs: '5 days before' });
check('quarter vs the whole previous quarter', M.change({ key: 'qtd', n: 64, chg_pct: -2, prev: { label: 'Q1 FY27' } }), { dir: 'down', pct: 2, vs: 'all of Q1 FY27' });
check('level', M.change({ key: 'd10', n: 10, chg_pct: 0.3, prev: {} }).dir, 'level');
check('no change shown under 5 days', M.change({ key: 'qtd', n: 3, chg_pct: 12, prev: { label: 'Q2 FY27' } }), null);

// Label spacing
const sp = M.spread([{ y: 100, s: 'a' }, { y: 104, s: 'b' }, { y: 106, s: 'c' }], 15, 0, 400).map(x => x.y);
check('labels pushed at least 15 px apart', sp, [100, 115, 130]);
check('pulled back inside the bottom edge', M.spread([{ y: 390, s: 'a' }, { y: 395, s: 'b' }], 15, 0, 400).map(x => x.y), [385, 400]);
check('nice ticks', M.niceTicks(21.7, 6), [0, 5, 10, 15, 20]);
check('nice ticks across zero', M.niceTicks(40, 6, -30), [-20, 0, 20, 40]);

// Rolling averages move with the data (the old 45-day line was one flat value)
const rows = [1, 2, 3, 4, 5, 6].map(t => ({ total: t }));
check('rolling 3-day mean, empty until 3 rows exist', M.rolling(rows, 3), [null, null, 2, 3, 4, 5]);

console.log(failed ? `\n${failed} FAILED` : '\nAll Today model tests passed.');
process.exit(failed ? 1 : 0);
