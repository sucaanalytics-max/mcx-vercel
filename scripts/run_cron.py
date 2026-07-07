#!/usr/bin/env python3
"""
run_cron.py — local dispatcher for the MCX signal/cron recomputations.

Runs the same logic as the Vercel /api/cron jobs, but locally, writing to
Supabase with the anon key (no CRON_SECRET / HTTP needed). Gives the freshness
fixer agents a SINGLE, narrowly-scoped command to invoke — so the agent
permission allowlist can whitelist `run_cron.py` instead of arbitrary `python -c`.

Usage:
  python3 scripts/run_cron.py <job> [mode]
    job  = valuation | models | momentum | commodity_signals | margins | oi
    mode = recent (default) | latest | backfill   (ignored by margins/oi)
"""
import sys, os, json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _run(job, mode):
    if job == "valuation":
        from lib.cron_valuation import run_refresh
        return run_refresh(mode=mode)
    if job == "models":
        from lib.cron_models import compute_signals
        return compute_signals(mode=mode)
    if job == "momentum":
        from lib.cron_momentum import compute_momentum
        return compute_momentum(mode=mode)
    if job == "commodity_signals":
        from lib.cron_commodity_signals import compute_commodity_signals
        return compute_commodity_signals(mode=mode)
    if job == "margins":
        from lib.cron_margins import refresh_margins
        return refresh_margins()
    if job == "oi":
        from lib.cron_oi_participants import refresh_oi_participants
        return refresh_oi_participants()
    raise SystemExit(f"unknown job: {job} "
                     "(valuation|models|momentum|commodity_signals|margins|oi)")


def main():
    if len(sys.argv) < 2:
        raise SystemExit(__doc__)
    job = sys.argv[1]
    mode = sys.argv[2] if len(sys.argv) > 2 else "recent"
    result = _run(job, mode)
    print(json.dumps(result, default=str)[:600])


if __name__ == "__main__":
    main()
