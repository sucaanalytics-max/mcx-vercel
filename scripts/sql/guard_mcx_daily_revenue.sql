-- ============================================================================
-- Guard mcx_daily_revenue now, before the full lock (safe with anon writes)
-- ============================================================================
-- Part one of lock_mcx_daily_revenue.sql, for the days before the relay Mac has
-- SUPABASE_SERVICE_KEY. It changes no permission, so the relay and the 07:00
-- jobs keep writing with the anon key exactly as today. It adds:
--   * a trigger that keeps a relay EOD row (source = 'mcx_relay_eod') when
--     any other source tries to replace it, whatever the key: that row's
--     update is skipped with a WARNING in the Postgres log, never an error,
--     so no writer breaks. An operator correction goes through after
--       set local mcx.allow_eod_override = 'on';
--   * updated_at stamped on every insert and update (the column already
--     exists; until now it only held the insert time), so any change to the
--     history from now on is visible.
-- Writers today: the relay writes only 'mcx_relay_eod' rows; catchup_missing
-- and daily_verify only fill missing dates; bhav_refresh already skips relay
-- rows. None of them is blocked.
--
-- Run in the MCX project's Supabase SQL editor. Safe to run more than once;
-- lock_mcx_daily_revenue.sql repeats it, so running both is fine.
-- Undo: drop trigger mcx_daily_revenue_guard on public.mcx_daily_revenue;
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

commit;

-- Check: select tgname from pg_trigger where tgname = 'mcx_daily_revenue_guard';
