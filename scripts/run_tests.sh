#!/bin/bash
# Run every self-checking test in scripts/ (test_*.py and test_*_js.js) and exit
# non-zero if any fails. Single entry point for humans, the project steward and
# the .claude/settings.json test-gate hook (blocks `gh pr create` / `git push`
# while anything is red).
#
# Python tests run under `uv run --with-requirements requirements.txt` so they
# see the same deps on every Mac (homebrew python here lacks curl_cffi, the
# system one has it; Pranay's Mac is the other way round). Falls back to
# python3 when uv is absent.
#
# Usage: scripts/run_tests.sh [-q]     (-q: print only failures + the summary)

cd "$(dirname "$0")/.." || exit 2
QUIET=0; [ "$1" = "-q" ] && QUIET=1

UV="$(command -v uv || echo "$HOME/.local/bin/uv")"
if [ -x "$UV" ]; then
  PY=("$UV" run --no-project --quiet --with-requirements requirements.txt python)
else
  PY=(python3)
fi

LOG="$(mktemp -t mcx_tests)"
trap 'rm -f "$LOG"' EXIT
FAILED=(); N=0
# Only git-tracked tests gate: an untracked local test (work in progress) must
# not block every push. project_sweep.py reports untracked tests separately.
for t in $(git ls-files 'scripts/test_*.py' 'scripts/test_*_js.js'); do
  N=$((N + 1))
  case "$t" in
    *.js) node "$t" >"$LOG" 2>&1 ;;
    *)    "${PY[@]}" "$t" >"$LOG" 2>&1 ;;
  esac
  rc=$?
  if [ $rc -ne 0 ]; then
    FAILED+=("$t")
    echo "FAIL $t (exit $rc)"
    tail -15 "$LOG" | sed 's/^/    /'
  elif [ $QUIET -eq 0 ]; then
    echo "ok   $t"
  fi
done

if [ ${#FAILED[@]} -gt 0 ]; then
  echo "${#FAILED[@]} of $N test files FAILED: ${FAILED[*]}"
  exit 1
fi
echo "All $N test files passed."
