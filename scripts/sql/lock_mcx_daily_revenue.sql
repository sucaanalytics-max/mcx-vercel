-- ============================================================================
-- Lock mcx_daily_revenue: public read, service-role write, relay EOD protected
-- ============================================================================
-- Why: the anon key is public (committed in this repo and in ExchangesDash), and
-- mcx_daily_revenue lets anon INSERT/UPDATE, so anyone can rewrite MCX revenue
-- history. That history feeds mcx_valuation, the model / momentum signals and
-- the ExchangesDash MCX views. Writes are last-writer-wins: until this change the
-- public /api/mcxlive endpoint upserted a bhav proxy over the relay's
-- authoritative EOD row. (Checked 17 Oct 2026: no row carries that proxy yet.)
--
-- After this script:
--   * anon / authenticated can only SELECT (the dashboards and ExchangesDash
--     read with the anon key, unchanged);
--   * writes need the service_role key (bypasses RLS);
--   * a row written by the relay (source = 'mcx_relay_eod') is kept when another
--     source tries to replace it, whatever the key (the update is skipped with
--     a WARNING, never an error), unless it is daily_verify's reconciliation
--     (source 'mcx_historical', data_source 'mcx_historical_reconciled') or an
--     operator says so in the same SQL session:
--       set local mcx.allow_eod_override = 'on';
--   * updated_at records when each row last changed.
--
-- If guard_mcx_daily_revenue.sql already ran, sections 1–2 below just repeat it.
--
-- ORDER MATTERS — do steps 1–2 before running this, or the relay's EOD write
-- and the 07:00 daily_verify will fail with 42501:
--   1. Relay Mac: add the service-role key to the repo's .env.local
--        SUPABASE_SERVICE_KEY=<Supabase → Project Settings → API → service_role>
--      then restart the relay (launchctl kickstart -k gui/$(id -u)/com.mcx.relay).
--      Its log must start with "Supabase writes as: service_role".
--   2. Merge the PR that stops /api/mcxlive writing (Vercel never writes this
--      table after it).
--   3. Run this script in the MCX project's Supabase SQL editor.
--   4. Check: the queries at the bottom.
-- Rollback: section R at the end.
-- ============================================================================

begin;

-- 1. Audit column (already exists in production: then this is a no-op) -------
alter table public.mcx_daily_revenue
  add column if not exists updated_at timestamptz not null default now();

-- 2. Guard trigger: stamp updated_at; never let another source replace the
--    relay's authoritative EOD row (the overwrite is skipped, not an error) ----
create or replace function public.mcx_daily_revenue_guard()
returns trigger
language plpgsql
as $$
begin
  -- Never an error: a writer whose upsert is refused here carries on as if it
  -- had succeeded (the relay's upsert raises on any error and is not caught,
  -- so an error would crash it). The row simply keeps the relay's values.
  if tg_op = 'UPDATE'
     and old.source = 'mcx_relay_eod'
     and new.source is distinct from 'mcx_relay_eod'
     -- The one admitted replacement: daily_verify's morning reconciliation,
     -- which writes MCX's settled full-day report only when it is more than 1%
     -- from the relay row (e.g. a capture that missed the 23:55 winter close).
     -- It alone sets this data_source; the relay's own same-night fallback
     -- (source 'mcx_historical', often missing the options leg) stays refused.
     and not (new.source = 'mcx_historical'
              and new.data_source = 'mcx_historical_reconciled')
     and coalesce(current_setting('mcx.allow_eod_override', true), '') <> 'on' then
    raise warning 'mcx_daily_revenue %: kept the relay EOD row, skipped an overwrite by source %',
      old.trading_date, new.source;
    return null;   -- skip this row's update
  end if;
  new.updated_at := now();
  return new;
end;
$$;

drop trigger if exists mcx_daily_revenue_guard on public.mcx_daily_revenue;
create trigger mcx_daily_revenue_guard
  before insert or update on public.mcx_daily_revenue
  for each row execute function public.mcx_daily_revenue_guard();

-- 3. Row-level security: replace whatever policies exist with read-only ------
alter table public.mcx_daily_revenue enable row level security;

do $$
declare
  p record;
begin
  for p in select policyname from pg_policies
           where schemaname = 'public' and tablename = 'mcx_daily_revenue'
  loop
    execute format('drop policy %I on public.mcx_daily_revenue;', p.policyname);
  end loop;
end $$;

create policy "public_read_mcx_daily_revenue" on public.mcx_daily_revenue
  for select to anon, authenticated using (true);

-- Belt and braces: no table privileges for the public roles either
revoke insert, update, delete, truncate on public.mcx_daily_revenue from anon, authenticated;

commit;

-- 4. Checks (run after commit) -------------------------------------------------
-- Policies: expect exactly one, SELECT for {anon,authenticated}
--   select policyname, cmd, roles from pg_policies where tablename = 'mcx_daily_revenue';
-- Privileges: expect only SELECT for anon / authenticated
--   select grantee, privilege_type from information_schema.role_table_grants
--   where table_name = 'mcx_daily_revenue' and grantee in ('anon', 'authenticated');
-- From any machine, an anon write must now fail (42501 / permission denied).
-- The date matches no row, so before the lock (or if it did not take) the
-- request changes nothing; 1900-01-01 was a real junk row, removed by
-- cleanup_sentinel_rows.sql:
--   curl -X PATCH "$SUPABASE_URL/rest/v1/mcx_daily_revenue?trading_date=eq.1800-01-01" \
--        -H "apikey: $ANON" -H "Authorization: Bearer $ANON" \
--        -H "Content-Type: application/json" -d '{"source":"probe"}'

-- R. Rollback (restores anon writes; only if something breaks) ----------------
--   begin;
--   grant insert, update on public.mcx_daily_revenue to anon;
--   create policy "anon_insert_mcx_daily_revenue" on public.mcx_daily_revenue
--     for insert to anon with check (true);
--   create policy "anon_update_mcx_daily_revenue" on public.mcx_daily_revenue
--     for update to anon using (true) with check (true);
--   drop trigger if exists mcx_daily_revenue_guard on public.mcx_daily_revenue;
--   commit;
