#!/usr/bin/env python3
"""
run_cron.py — local dispatcher for the MCX signal/cron recomputations.

Runs the same logic as the Vercel /api/cron jobs, but locally, writing to
Supabase with the secret key from .env.local (no CRON_SECRET / HTTP needed).
The signal tables refuse anon writes (HTTP 401, 42501), so run it from the
production checkout, which has .env.local; a worktree has none. Gives the freshness
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
    from lib.supabase import SUPABASE_WRITE_ROLE
    if SUPABASE_WRITE_ROLE == "anon":
        print("warning: no SUPABASE_SECRET_KEY / SUPABASE_SERVICE_KEY (no .env.local here?); "
              "writes will likely be refused (anon)", file=sys.stderr)
    result = _run(job, mode)
    print(json.dumps(result, default=str)[:600])
    # Exit non-zero on a failed run: daily_verify and the freshness fixers read
    # the exit code, and used to print "<job> OK" even when every upsert failed.
    if isinstance(result, dict) and result.get("success") is False:
        sys.exit(1)


if __name__ == "__main__":
    main()
