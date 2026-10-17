-- ============================================================================
-- Lock the other MCX tables: public read, service-role write
-- ============================================================================
-- Same exposure as mcx_daily_revenue (see lock_mcx_daily_revenue.sql): these
-- tables let the public anon key INSERT/UPDATE, and that key is committed in
-- this repo. After this script anon / authenticated can only SELECT; every
-- writer must use the service-role key (SUPABASE_WRITE_KEY in lib/mcx_config.py,
-- which every writer now goes through).
--
-- PREREQUISITES — all three, or the writers below start failing with 42501:
--   1. Vercel → mcx-vercel → Settings → Environment Variables:
--        SUPABASE_SERVICE_KEY = <Supabase → Project Settings → API → service_role>
--      for Production (and Preview), then redeploy. Covers the crons
--      (valuation, models, momentum, commodity_signals, margins,
--      oi_participants) and the public endpoints that write
--      (/api/mcxlive → mcx_snapshots, /api/refresh → mcx_snapshots,
--       /api/mcxprice → mcx_cmp_cache).
--   2. Relay Mac: SUPABASE_SERVICE_KEY in .env.local (same as for
--      lock_mcx_daily_revenue.sql). Covers the relay, daily_verify,
--      bhav_refresh, icomdex/commodity/price/valuation refresh, the MCXCCL
--      scraper and the freshness agents' run_cron.py.
--   3. One trading day with both in place and no write errors in the relay /
--      daily_verify logs or the Vercel cron logs.
-- Then run this in the MCX project's Supabase SQL editor. Tables that do not
-- exist are skipped. Safe to run more than once. Rollback: section R.
-- ============================================================================

begin;

do $$
declare
  t text;
  p record;
begin
  foreach t in array array[
    'mcx_snapshots', 'mcx_cmp_cache', 'relay_heartbeat',
    'mcx_margin_daily', 'mcx_oi_participants',
    'mcx_share_price', 'mcx_valuation',
    'mcx_model_signals', 'mcx_momentum_signals', 'mcx_commodity_signals',
    'mcx_commodity_daily', 'mcx_icomdex_daily', 'mcx_commodity_prices'
  ]
  loop
    if to_regclass(format('public.%I', t)) is null then
      raise notice 'skip %: no such table', t;
      continue;
    end if;
    execute format('alter table public.%I enable row level security;', t);
    for p in select policyname from pg_policies where schemaname = 'public' and tablename = t loop
      execute format('drop policy %I on public.%I;', p.policyname, t);
    end loop;
    execute format('create policy %I on public.%I for select to anon, authenticated using (true);',
                   'public_read_' || t, t);
    execute format('revoke insert, update, delete, truncate on public.%I from anon, authenticated;', t);
    raise notice 'locked %', t;
  end loop;
end $$;

commit;

-- Checks
--   select tablename, policyname, cmd, roles from pg_policies
--   where tablename like 'mcx_%' or tablename = 'relay_heartbeat' order by 1;
--   select table_name, grantee, string_agg(privilege_type, ',') from information_schema.role_table_grants
--   where grantee in ('anon','authenticated') and (table_name like 'mcx_%' or table_name = 'relay_heartbeat')
--   group by 1, 2 order by 1;   -- expect SELECT (plus REFERENCES/TRIGGER) only

-- R. Rollback for one table <t> (restores anon writes):
--   grant insert, update on public.<t> to anon;
--   create policy "anon_insert_<t>" on public.<t> for insert to anon with check (true);
--   create policy "anon_update_<t>" on public.<t> for update to anon using (true) with check (true);
