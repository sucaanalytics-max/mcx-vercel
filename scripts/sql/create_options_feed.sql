-- Options recommendation feed (Signals → Options page).
-- Written by scripts/options_paper.py on the local Mac after each 15:10 IST run; read by
-- /api/models?view=options. Same exposure rule as lock_mcx_other_tables.sql: anon and
-- authenticated may only SELECT; writes need the service_role key (SUPABASE_SERVICE_KEY).

create table if not exists public.mcx_options_feed (
  trading_date   date primary key,           -- IST date of the run
  run_ts         timestamptz not null,
  spot           numeric,
  series_expiry  date,                       -- near-month MCX Ltd option expiry
  entry_date     date,                       -- Schedule B entry (10 NSE days before expiry)
  exit_date      date,                       -- Schedule B exit (2 NSE days before expiry)
  signals        jsonb,                      -- {v45_20: {signal, z, r2}, v60_40: {...}, last_rev_date}
  recommendation jsonb,                      -- today's trades for each variant, live bid/ask
  positions      jsonb,                      -- open paper positions marked at the run
  event          jsonb,                      -- results-day trade: date, reading, status
  spreads        jsonb,                      -- near-month median half-spread by moneyness
  note           text,                       -- the daily note, plain text
  updated_at     timestamptz not null default now()
);

create table if not exists public.mcx_options_paper (
  id          text primary key,              -- ledger id, e.g. 2026-11-23|v45_20|PCS|bull
  variant     text,
  book        text,
  direction   text,
  shadow      boolean,
  event       boolean not null default false,
  expiry      date,
  entry_date  date,
  exit_date   date,
  status      text,                          -- open / closed / missed / no_quote / no_trade
  signal      numeric,
  z           numeric,
  net_entry   numeric,
  risk        numeric,
  ret         numeric,
  pnl_per_lot numeric,
  record      jsonb not null,                -- the full ledger record (legs, fills, costs, flags)
  updated_at  timestamptz not null default now()
);

do $$
declare t text;
begin
  foreach t in array array['mcx_options_feed', 'mcx_options_paper'] loop
    execute format('alter table public.%I enable row level security;', t);
    execute format('drop policy if exists "read_%s" on public.%I;', t, t);
    execute format('create policy "read_%s" on public.%I for select to anon, authenticated using (true);', t, t);
    execute format('revoke insert, update, delete, truncate on public.%I from anon, authenticated;', t);
    execute format('grant select on public.%I to anon, authenticated;', t);
  end loop;
end $$;
