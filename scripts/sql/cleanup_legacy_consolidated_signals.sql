-- One-time cleanup: remove legacy fragmented rows from mcx_commodity_signals
-- and mcx_commodity_daily that pre-date the COMMODITY_MAP consolidation in
-- lib/cron_commodity_signals.py. The signals cron now writes ONE consolidated
-- row per parent symbol (SILVER aggregates SILVER+SILVERM+SILVERMIC), but the
-- old rows for the child symbols remain until deleted.
--
-- Run once in the Supabase SQL editor. Idempotent: safe to re-run; affects
-- only rows whose `commodity` is a mini/micro/petal CHILD symbol.
--
-- Affected child symbols (from lib/mcx_config.py COMMODITY_MAP):
--   CRUDEOILM, NATGASMINI, ELECDMBL,
--   GOLDM, GOLDGUINEA, GOLDPETAL, GOLDTEN,
--   SILVERM, SILVERMIC,
--   LEADMINI, ZINCMINI, ALUMINI

BEGIN;

-- Snapshot row counts before cleanup (returned by the DELETE if you watch)
-- SELECT commodity, count(*) FROM mcx_commodity_signals
--   WHERE commodity IN ('CRUDEOILM','NATGASMINI','ELECDMBL','GOLDM','GOLDGUINEA',
--                       'GOLDPETAL','GOLDTEN','SILVERM','SILVERMIC',
--                       'LEADMINI','ZINCMINI','ALUMINI')
--   GROUP BY commodity ORDER BY count(*) DESC;

DELETE FROM mcx_commodity_signals
WHERE commodity IN (
  'CRUDEOILM','NATGASMINI','ELECDMBL',
  'GOLDM','GOLDGUINEA','GOLDPETAL','GOLDTEN',
  'SILVERM','SILVERMIC',
  'LEADMINI','ZINCMINI','ALUMINI'
);

COMMIT;

-- Note: we do NOT delete from mcx_commodity_daily — that table is the raw
-- contract-level data per (date, commodity, instrument_type) and the child
-- rows there ARE legitimate (they're what the cron aggregates from).
