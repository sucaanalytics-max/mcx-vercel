-- ============================================================================
-- Enable writes to mcx_commodity_prices (Commodities tab price panel)
-- ============================================================================
-- Symptom: the price panel (WTI / NatGas / USD-INR) was empty because
--   mcx_commodity_prices has RLS enabled but NO policy permitting the anon
--   role to write, so the refresh upsert fails with:
--       42501: new row violates row-level security policy
--   (Same class of bug fixed earlier for mcx_commodity_signals / mcx_model_signals
--    in enable_signal_table_writes.sql.)
--
-- This project's refresh now runs LOCALLY (scripts/commodity_price_refresh.py,
-- keyless via yfinance) and writes with the anon key — matching every other
-- pipeline table — so we allow the anon role to write this table too.
--
-- Run ONCE in the Supabase SQL editor (Project → SQL Editor → New query → Run).
-- Idempotent and safe to re-run.
-- ============================================================================

-- 1. Unique key so upserts merge instead of duplicating (one row per day+commodity)
create unique index if not exists mcx_commodity_prices_date_commodity_uidx
  on public.mcx_commodity_prices (price_date, commodity);

-- 2. Anon read + write policies (idempotent)
do $$
begin
  execute 'alter table public.mcx_commodity_prices enable row level security;';

  execute 'drop policy if exists "anon_read_mcx_commodity_prices" on public.mcx_commodity_prices;';
  execute 'create policy "anon_read_mcx_commodity_prices" on public.mcx_commodity_prices
             for select to anon using (true);';

  execute 'drop policy if exists "anon_insert_mcx_commodity_prices" on public.mcx_commodity_prices;';
  execute 'create policy "anon_insert_mcx_commodity_prices" on public.mcx_commodity_prices
             for insert to anon with check (true);';

  execute 'drop policy if exists "anon_update_mcx_commodity_prices" on public.mcx_commodity_prices;';
  execute 'create policy "anon_update_mcx_commodity_prices" on public.mcx_commodity_prices
             for update to anon using (true) with check (true);';
end $$;

-- ============================================================================
-- After running this, repopulate locally:
--   python3 scripts/commodity_price_refresh.py --days 120
-- ============================================================================
