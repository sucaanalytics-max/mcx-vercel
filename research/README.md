# research/

One-off studies and their data fetchers. Nothing here is scheduled, imported by
the site or deployed (`.vercelignore`). Each script puts the repo root on
`sys.path` and reads Supabase through `lib.mcx_config`.

Run with the `research` extra on top of the project lock:

    scripts/mcx_uv.sh --extra research python -I research/<script>.py …

The write-ups live in `docs/` (event studies, ADR stress test, options agent plan).
