-- ============================================================================
-- One-time cleanup: phantom Sunday margin rows (2026-06-07)
-- ============================================================================
-- 149 rows in mcx_margin_daily carry snapshot_date = 2026-06-07 — a SUNDAY,
-- impossible for a SPAN snapshot. Root cause: xlrd/pandas parsed Sharekhan's
-- ambiguous date cell '06-07-2026' (DD-MM = 6 July 2026) month-first into
-- June 7. The correct 2026-07-06 snapshot exists separately (149 rows).
--
-- Write-time guard added 2026-07-07 (cron_margins._fix_snapshot_date) and the
-- dashboard filters weekend dates defensively, so these rows are already
-- invisible in the UI — this is pure data hygiene.
--
-- Anon-key DELETE is blocked by RLS (no delete policy — intentionally), so
-- run this ONCE in the Supabase SQL editor (service role bypasses RLS):

delete from public.mcx_margin_daily where snapshot_date = '2026-06-07';

-- Verify (expect 0):
-- select count(*) from public.mcx_margin_daily where snapshot_date = '2026-06-07';
