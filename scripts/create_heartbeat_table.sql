-- Run this in Supabase Dashboard → SQL Editor
-- Creates the relay_heartbeat table for remote health monitoring

CREATE TABLE IF NOT EXISTS relay_heartbeat (
    relay_id       TEXT PRIMARY KEY DEFAULT 'default',
    heartbeat_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    trading_date   DATE,
    status         TEXT NOT NULL DEFAULT 'running',   -- running | eod_done | error | stopped
    last_rev_cr    NUMERIC(10,4),
    elapsed_min    INT,
    last_error     TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Enable RLS but allow anon insert/update (relay uses anon key)
ALTER TABLE relay_heartbeat ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow anon read"  ON relay_heartbeat FOR SELECT USING (true);
CREATE POLICY "Allow anon upsert" ON relay_heartbeat FOR INSERT WITH CHECK (true);
CREATE POLICY "Allow anon update" ON relay_heartbeat FOR UPDATE USING (true);

COMMENT ON TABLE relay_heartbeat IS 'MCX Relay health monitoring — one row per relay instance, upserted every 15 min';
