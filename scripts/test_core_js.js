#!/usr/bin/env node
/* Unit tests for assets/js/core.js (formatting, timestamps, market hours, polling, store, theme, router).
   Run:  node scripts/test_core_js.js */
const fs = require('fs');
const path = require('path');

const listeners = {}, winListeners = {};
global.window = global;
const classes = new Set();
const pages = ['tabPredictor', 'tabValuation', 'pageSigEnsemble', 'tabOIP'].map(id => ({ id, hidden: false }));
global.document = {
  hidden: false,
  addEventListener: (ev, fn) => { listeners[ev] = fn; },
  documentElement: { dataset: {}, classList: { contains: c => classes.has(c), toggle: (c, on) => { if (on) classes.add(c); else classes.delete(c); } } },
  querySelectorAll: sel => (sel === '.page' ? pages : []),
  getElementById: () => null,
};
global.addEventListener = (ev, fn) => { winListeners[ev] = fn; };
global.scrollTo = () => {};
global.location = { hash: '', pathname: '/', search: '' };
global.history = { state: null, replaceState: (st, t, url) => { location.hash = url.slice(url.indexOf('#')); } };
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

// Store: late subscribers get the current value; later sets notify
const seen = [];
MCX.store.set('liveRevenue', { value: 14.5 });
MCX.store.on('liveRevenue', v => seen.push(v.value));
MCX.store.set('liveRevenue', { value: 15.3 });
check('store replays current value, then notifies', seen, [14.5, 15.3]);

// Theme: three states, kept in memory when storage is blocked; html.dark follows
const flips = [];
MCX.theme.onChange(d => flips.push(d));
check('default theme is system', MCX.theme.mode(), 'system');
check('system without a dark preference is light', classes.has('dark'), false);
MCX.theme.cycle();
check('cycle: system -> light', MCX.theme.mode(), 'light');
MCX.theme.cycle();
check('cycle: light -> dark, even with storage blocked', [MCX.theme.mode(), classes.has('dark'), document.documentElement.dataset.theme], ['dark', true, 'dark']);
MCX.theme.set('system');
check('back to system clears dark', classes.has('dark'), false);
check('change listeners fire only when light/dark flips', flips, [true, false]);
MCX.theme.set('sepia');
check('unknown theme falls back to system', MCX.theme.mode(), 'system');

// Router
const mounts = [], rethemes = [];
const reg = (id, path, pagesIds, withRetheme) => MCX.router.register({
  id, path, pages: pagesIds, mount: () => mounts.push(id), retheme: withRetheme ? () => rethemes.push(id) : undefined });
reg('today', '/today', ['tabPredictor'], true);
reg('val-fv', '/value/fair-value', ['tabValuation'], false);
reg('sig-ens', '/signals/ensemble', ['pageSigEnsemble'], false);
reg('lab-pos', '/lab/positioning', ['tabOIP'], false);
location.hash = '#tabOIP';
MCX.router.start({ fallback: 'today', legacy: { tabPredictor: '/today', tabValuation: '/value/fair-value', tabOIP: '/lab/positioning' } });
check('old tab id deep-links and is rewritten', [location.hash, MCX.router.current()], ['#/lab/positioning', 'lab-pos']);
check('only the route\'s page is visible', pages.filter(p => !p.hidden).map(p => p.id), ['tabOIP']);
check('mount runs on show', mounts, ['lab-pos']);
const R = h => MCX.router.resolve(h);
check('empty hash opens Today without rewriting', R(''), { id: 'today', replace: null });
check('route path', R('#/value/fair-value'), { id: 'val-fv', replace: null });
check('query after the path is ignored', R('#/value/fair-value?m=pe').id, 'val-fv');
check('trailing slash', R('#/signals/ensemble/').id, 'sig-ens');
check('old tab id', R('#tabValuation'), { id: 'val-fv', replace: '#/value/fair-value' });
check('deleted Backtest tab opens Today', R('#tabBacktest'), { id: 'today', replace: '#/today' });
check('unknown route opens Today', R('#/nope'), { id: 'today', replace: '#/today' });
location.hash = '#/value/fair-value'; winListeners.hashchange();
check('hashchange shows the new route', [MCX.router.current(), pages.filter(p => !p.hidden).map(p => p.id)], ['val-fv', ['tabValuation']]);
MCX.router.retheme();
check('retheme redraws the current page through mount when it has no retheme', mounts.slice(-1), ['val-fv']);
location.hash = '#/today'; winListeners.hashchange();
check('a page left during a theme change is rethemed when shown, then mounted', [rethemes, mounts.slice(-1)], [['today'], ['today']]);
location.hash = '#/value/fair-value'; winListeners.hashchange();
location.hash = '#/today'; winListeners.hashchange();
check('retheme happens once, not on every visit', rethemes, ['today']);

console.log(failed ? `\n${failed} FAILED` : '\nAll core.js tests passed.');
process.exit(failed ? 1 : 0);
