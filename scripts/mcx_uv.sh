#!/bin/bash
# Run Python from the project lock: `scripts/mcx_uv.sh [--extra research] python …`.
# Dependencies come from pyproject.toml + uv.lock (the only manifest) with the
# `local` extra; the venv lives in ~/.cache, never inside the Dropbox checkout.
# Used by run_tests.sh, project_sweep.py, daily_verify's Dhan OI step and the
# uv-based launchd jobs.
root="$(cd -P "$(dirname "$0")/.." && pwd)"
UV="$(command -v uv || { [ -x /opt/homebrew/bin/uv ] && echo /opt/homebrew/bin/uv; } || echo "$HOME/.local/bin/uv")"
[ -x "$UV" ] || { echo "mcx_uv.sh: uv not found" >&2; exit 127; }
export UV_PROJECT_ENVIRONMENT="${UV_PROJECT_ENVIRONMENT:-$HOME/.cache/mcx-vercel/venv}"
exec "$UV" run --project "$root" --frozen --quiet --extra local "$@"
