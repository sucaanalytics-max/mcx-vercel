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
// MCX close follows the US clock; dates from MCX circulars 491/2025 and 068/2026, then 2026-27 expected
check('MCX close by date',
  ['2025-10-31', '2025-11-03', '2026-03-06', '2026-03-09', '2026-10-30', '2026-11-02', '2027-03-12', '2027-03-15'].map(MCX.market.closeMin),
  [1410, 1435, 1435, 1410, 1410, 1435, 1435, 1410]);
check('MCX session length and label in winter', [MCX.market.sessionMin('2026-11-02'), MCX.market.closeLabel('2026-11-02')], [895, '23:55']);
check('MCX close label in summer', MCX.market.closeLabel('2026-10-28'), '23:30');
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

// Dates: bad input prints a dash, never "NaN undefined"
check('date formatters on null', [MCX.fmt.dayMonth(null), MCX.fmt.day(undefined), MCX.fmt.weekday(''), MCX.fmt.dateLong('nope')], ['—', '—', '—', '—']);
check('span with a missing end', MCX.fmt.span('2026-11-02', null), '—');
check('valid dates unchanged', [MCX.fmt.dayMonth('2026-09-28'), MCX.fmt.day('2026-09-28'), MCX.fmt.span('2026-09-22', '2026-09-28')], ['28 Sep', 'Mon 28 Sep', '22–28 Sep']);

// Fetching: errors are readable and are not cached
(async () => {
  const res = (body, { status = 200, type = 'application/json' } = {}) => ({
    ok: status < 400, status, headers: { get: k => (k.toLowerCase() === 'content-type' ? type : null) },
    json: () => (typeof body === 'string' ? Promise.reject(new SyntaxError('bad')) : Promise.resolve(body)),
  });
  const queue = [];
  let calls = 0;
  global.fetch = () => { calls++; return Promise.resolve(queue.shift()); };

  queue.push(res('<!DOCTYPE html>', { status: 504, type: 'text/html' }));
  let msg = null; try { await MCX.getJSON('/api/x'); } catch (e) { msg = e.message; }
  check('HTML error page becomes a readable error', msg, 'Service unavailable (HTTP 504)');
  queue.push(res({ success: false, error: 'boom' }, { status: 500 }));
  check('JSON error body is returned to the caller', await MCX.getJSON('/api/x'), { success: false, error: 'boom' });

  calls = 0;
  queue.push(res({ success: false, error: 'supabase down' }), res({ success: true, v: 1 }));
  check('first answer is the error', (await fetchRanged('/api/r1')).success, false);
  check('retry fetches again and gets the good answer', (await fetchRanged('/api/r1')).v, 1);
  check('good answer is cached', [(await fetchRanged('/api/r1')).v, calls], [1, 2]);
  queue.push(res('<html>', { status: 502, type: 'text/html' }), res({ success: true, v: 2 }));
  let threw = false; try { await fetchRanged('/api/r2'); } catch (e) { threw = true; }
  check('a thrown fetch is not cached either', [threw, (await fetchRanged('/api/r2')).v], [true, 2]);

  console.log(failed ? `\n${failed} FAILED` : '\nAll core.js tests passed.');
  process.exit(failed ? 1 : 0);
})();
