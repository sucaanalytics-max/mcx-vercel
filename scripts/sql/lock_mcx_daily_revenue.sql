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
--   * a row written by the relay (source = 'mcx_relay_eod') cannot be replaced
--     by another source, whatever the key, unless an operator says so in the
--     same SQL session:  set local mcx.allow_eod_override = 'on';
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

-- 1. Audit column ------------------------------------------------------------
alter table public.mcx_daily_revenue
  add column if not exists updated_at timestamptz not null default now();

-- 2. Guard trigger: stamp updated_at; never let another source replace the
--    relay's authoritative EOD row --------------------------------------------
create or replace function public.mcx_daily_revenue_guard()
returns trigger
language plpgsql
as $$
begin
  if tg_op = 'UPDATE'
     and old.source = 'mcx_relay_eod'
     and new.source is distinct from 'mcx_relay_eod'
     and coalesce(current_setting('mcx.allow_eod_override', true), '') <> 'on' then
    raise exception 'mcx_daily_revenue %: relay EOD row; refusing overwrite by source %',
      old.trading_date, new.source
      using errcode = 'P0001',
            hint = 'Intentional correction? set local mcx.allow_eod_override = ''on'' first.';
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
-- From any machine, an anon write must now fail (42501 / permission denied):
--   curl -X PATCH "$SUPABASE_URL/rest/v1/mcx_daily_revenue?trading_date=eq.1900-01-01" \
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
