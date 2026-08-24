-- ============================================================================
-- Create mcx_icomdex_daily (iCOMDEX index levels — BULLDEX & siblings)
-- ============================================================================
-- Stores daily OHLC of the MCX iCOMDEX indices (Bullion/BULLDEX, Base Metal,
-- Energy, Composite, Gold, Silver, Aluminium, Copper), fetched locally by
-- scripts/icomdex_refresh.py from:
--   GET /market-data/mcx-icomdex-indices/GetMCXIComdexIndicesHistoryFilter
-- History is available from 2015-12-31.
--
-- Why a new table: index LEVELS are prices (points), not turnover — they fit
-- none of the existing volume/turnover tables. The BULLDEX futures contract
-- itself has had zero volume since Jun 2026; the index value is the only live
-- BULLDEX series MCX still publishes.
--
-- Composite primary key (no id column) so PostgREST upserts with
-- Prefer: resolution=merge-duplicates merge on (trading_date, index_code)
-- without an on_conflict hint — same convention as mcx_commodity_daily.
--
-- Writes use the anon key (no service key on the relay machine), so anon
-- insert/update policies are required — same as enable_commodity_prices_writes.sql.
--
-- Run ONCE in the Supabase SQL editor (Project → SQL Editor → New query → Run).
-- Idempotent and safe to re-run.
-- ============================================================================

create table if not exists public.mcx_icomdex_daily (
  trading_date date        not null,
  index_code   text        not null,   -- e.g. MCXBULLDEX, MCXMETLDEX, MCXCOMPDEX
  index_name   text,                   -- e.g. "MCX iCOMDEX Bullion"
  open         numeric,
  high         numeric,
  low          numeric,
  close        numeric     not null,
  change_pct   numeric,                -- day-over-day % as published by MCX
  source       text        default 'mcx_icomdex_api',
  created_at   timestamptz default now(),
  primary key (trading_date, index_code)
);

-- Anon read + write policies (idempotent)
do $$
begin
  execute 'alter table public.mcx_icomdex_daily enable row level security;';

  execute 'drop policy if exists "anon_read_mcx_icomdex_daily" on public.mcx_icomdex_daily;';
  execute 'create policy "anon_read_mcx_icomdex_daily" on public.mcx_icomdex_daily
             for select to anon using (true);';

  execute 'drop policy if exists "anon_insert_mcx_icomdex_daily" on public.mcx_icomdex_daily;';
  execute 'create policy "anon_insert_mcx_icomdex_daily" on public.mcx_icomdex_daily
             for insert to anon with check (true);';

  execute 'drop policy if exists "anon_update_mcx_icomdex_daily" on public.mcx_icomdex_daily;';
  execute 'create policy "anon_update_mcx_icomdex_daily" on public.mcx_icomdex_daily
             for update to anon using (true) with check (true);';
end $$;

-- ============================================================================
-- After running this, backfill locally (chunks the range automatically):
--   /opt/homebrew/bin/python3 scripts/icomdex_refresh.py --backfill 2015-12-31 today
-- ============================================================================
