-- ============================================================================
-- Fix: Commodity-signals and model-signals tables frozen since early March 2026
-- ============================================================================
-- Root cause: `mcx_commodity_signals` and `mcx_model_signals` have RLS enabled
-- but NO policy permitting the anon role to write, so every nightly cron upsert
-- failed silently with:
--     42501: new row violates row-level security policy
-- (Other tables — mcx_commodity_daily, mcx_daily_revenue, mcx_valuation,
--  mcx_momentum_signals, mcx_margin_daily — already allow anon writes, which is
--  why only these two froze.)
--
-- You have TWO ways to fix this. Pick ONE.
--
-- ── OPTION A (RECOMMENDED, most secure) — service-role key, no SQL needed ────
--   The code now prefers a service-role key for all writes (SUPABASE_WRITE_KEY
--   = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY). The service role bypasses RLS,
--   so you do NOT need to loosen any policy. Just set, in BOTH places that run
--   crons/relay:
--       Vercel  →  Project Settings → Environment Variables → SUPABASE_SERVICE_KEY
--       Relay   →  the relay machine's environment / .env  → SUPABASE_SERVICE_KEY
--   (Find the value in Supabase → Project Settings → API → service_role secret.)
--   Then re-run the signal crons (see bottom). Do NOT run the SQL below.
--
-- ── OPTION B — keep using the anon key, allow it to write these two tables ──
--   Consistent with the project's existing design (anon already writes the other
--   tables) but less secure: the anon key is public (committed in the repo), so
--   anyone could write these tables. Only use this if you can't set a service key.
--   Run the statements below in the Supabase SQL editor.
-- ============================================================================

-- OPTION B only — anon write policies (idempotent) ---------------------------
do $$
declare
  t text;
begin
  foreach t in array array['mcx_commodity_signals', 'mcx_model_signals']
  loop
    execute format('alter table public.%I enable row level security;', t);

    execute format('drop policy if exists "anon_read_%1$s" on public.%1$I;', t);
    execute format(
      'create policy "anon_read_%1$s" on public.%1$I for select to anon using (true);', t);

    execute format('drop policy if exists "anon_insert_%1$s" on public.%1$I;', t);
    execute format(
      'create policy "anon_insert_%1$s" on public.%1$I for insert to anon with check (true);', t);

    execute format('drop policy if exists "anon_update_%1$s" on public.%1$I;', t);
    execute format(
      'create policy "anon_update_%1$s" on public.%1$I for update to anon using (true) with check (true);', t);
  end loop;
end $$;

-- ============================================================================
-- After applying EITHER option, repopulate the two tables (run locally):
--   python3 -c "import sys; sys.path.insert(0,'.'); \
--     from lib.cron_commodity_signals import compute_commodity_signals as f; \
--     print(f(mode='backfill')['rows_upserted'])"
--   # then the models cron for mcx_model_signals (api/cron?job=models&mode=backfill)
-- Or just let the scheduled Vercel crons run — they will now succeed.
-- ============================================================================
