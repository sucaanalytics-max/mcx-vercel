-- ============================================================================
-- Remove sentinel / write-test rows dated before 2000
-- ============================================================================
-- Old write tests and probes left rows dated 1900-01-01 and 1999-01-0x in the
-- MCX tables (checked 24 Oct 2026, 10 rows in all):
--   mcx_daily_revenue     1900-01-01 (source 'probe'), 1999-01-01 (all values null)
--   mcx_share_price       1900-01-01
--   mcx_valuation         1900-01-01
--   mcx_commodity_daily   1999-01-01 .. 1999-01-04 (sources 'writetest', 't', 'before')
--   mcx_snapshots         1900-01-01 (source 'test')
--   mcx_margin_daily      1999-01-01
-- No real MCX data predates 2000, but ascending reads see these rows first,
-- which is why api/analytics.py and lib/margin_dashboard.py filter them out.
--
-- Run in the MCX project's Supabase SQL editor (the anon key has no DELETE
-- grant). Safe to run more than once. It deletes only rows dated before
-- 2000-01-01, skips tables that do not exist, and rolls everything back if any
-- one table holds more than 10 such rows (that would not be test debris).
-- ============================================================================

begin;

do $$
declare
  t record;
  n bigint;
begin
  for t in select * from (values
      ('mcx_daily_revenue',   'trading_date'),
      ('mcx_share_price',     'trading_date'),
      ('mcx_valuation',       'trading_date'),
      ('mcx_model_signals',   'trading_date'),
      ('mcx_momentum_signals','trading_date'),
      ('mcx_commodity_signals','trading_date'),
      ('mcx_commodity_daily', 'trading_date'),
      ('mcx_icomdex_daily',   'trading_date'),
      ('mcx_snapshots',       'trading_date'),
      ('mcx_margin_daily',    'snapshot_date'),
      ('mcx_oi_participants', 'report_date'),
      ('mcx_commodity_prices','price_date')
    ) as v(tbl, col)
  loop
    if to_regclass(format('public.%I', t.tbl)) is null then
      raise notice 'skip %: no such table', t.tbl;
      continue;
    end if;
    execute format('select count(*) from public.%I where %I < date ''2000-01-01''', t.tbl, t.col) into n;
    if n > 10 then
      raise exception '% has % rows before 2000 — not test debris, nothing deleted', t.tbl, n;
    end if;
    execute format('delete from public.%I where %I < date ''2000-01-01''', t.tbl, t.col);
    raise notice '%: deleted % row(s)', t.tbl, n;
  end loop;
end $$;

commit;

-- Check: every count should be 0
--   select 'mcx_daily_revenue' t, count(*) from mcx_daily_revenue where trading_date < '2000-01-01'
--   union all select 'mcx_share_price', count(*) from mcx_share_price where trading_date < '2000-01-01'
--   union all select 'mcx_valuation', count(*) from mcx_valuation where trading_date < '2000-01-01'
--   union all select 'mcx_commodity_daily', count(*) from mcx_commodity_daily where trading_date < '2000-01-01'
--   union all select 'mcx_snapshots', count(*) from mcx_snapshots where trading_date < '2000-01-01'
--   union all select 'mcx_margin_daily', count(*) from mcx_margin_daily where snapshot_date < '2000-01-01';
