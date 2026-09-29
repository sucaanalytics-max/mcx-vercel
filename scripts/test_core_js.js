#!/usr/bin/env node
/* Unit tests for assets/js/core.js (formatting, timestamps, market hours, polling).
   Run:  node scripts/test_core_js.js */
const fs = require('fs');
const path = require('path');

const listeners = {};
global.window = global;
global.document = { hidden: false, addEventListener: (ev, fn) => { listeners[ev] = fn; } };
global.localStorage = { getItem: () => { throw new Error('blocked'); }, setItem: () => { throw new Error('blocked'); } };
const coreFile = path.join(__dirname, '..', 'assets', 'js', 'core.js');   // our own source, run in this context
require('vm').runInThisContext(fs.readFileSync(coreFile, 'utf8'), { filename: coreFile });

let failed = 0;
function check(name, got, want) {
  const ok = JSON.stringify(got) === JSON.stringify(want);
  console.log((ok ? 'PASS ' : 'FAIL ') + name + (ok ? '' : `: got ${JSON.stringify(got)}, want ${JSON.stringify(want)}`));
  if (!ok) failed++;
}

// Timestamps: both /api/refresh formats give the same instant
const post = MCX.parseTs('14:42 IST, 28 Sep 2026');
check('POST format parses', post && post.toISOString(), '2026-09-28T09:12:00.000Z');
check('ISO format parses', MCX.parseTs('2026-09-28T09:12:00+00:00').toISOString(), '2026-09-28T09:12:00.000Z');
check('single-digit day', MCX.parseTs('09:05 IST, 1 Oct 2026').toISOString(), '2026-10-01T03:35:00.000Z');
check('garbage is null', MCX.parseTs('not a time'), null);
check('empty is null', MCX.parseTs(''), null);

// Formatting
check('EPS two decimals', MCX.fmt.eps(73.756), '73.76');
check('Indian grouping', MCX.fmt.num(127669.56, 0), '1,27,670');
check('negative uses a true minus', MCX.fmt.num(-1.5, 1), '−1.5');
check('negative zero shows no sign', MCX.fmt.num(-0.001, 2), '0.00');
check('missing is a dash', [MCX.fmt.num(null), MCX.fmt.num(undefined), MCX.fmt.num(NaN), MCX.fmt.num('')], ['—', '—', '—', '—']);

// Storage never throws
check('storage get falls back when blocked', MCX.storage.get('mcxTheme', 'light'), 'light');
check('storage set reports failure', MCX.storage.set('mcxTheme', 'dark'), false);

// NSE hours in IST, whatever the machine's time zone
const at = (iso) => new Date(iso);
check('open Tue 10:00 IST', MCX.market.nseOpen(at('2026-09-29T04:30:00Z')), true);
check('closed Tue 09:00 IST', MCX.market.nseOpen(at('2026-09-29T03:30:00Z')), false);
check('closed Tue 15:31 IST', MCX.market.nseOpen(at('2026-09-29T10:01:00Z')), false);
check('closed Sat 11:00 IST', MCX.market.nseOpen(at('2026-10-03T05:30:00Z')), false);

// Poll scheduler: runs only while the condition holds, and not more than once per interval
let runs = 0, allowed = false;
MCX.poll.every('t', () => runs++, 60000, () => allowed);
MCX.poll.kick('t');
check('skips while condition is false', runs, 0);
allowed = true;
MCX.poll.kick('t'); MCX.poll.kick('t');
check('runs once when allowed, not twice in the same interval', runs, 1);
listeners.visibilitychange();
check('becoming visible does not double-run within the interval', runs, 1);

console.log(failed ? `\n${failed} FAILED` : '\nAll core.js tests passed.');
process.exit(failed ? 1 : 0);
