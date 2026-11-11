#!/bin/bash
# Print the Python interpreter the launchd jobs should use: the first candidate
# that can import the scripts' third-party packages. Exits 1 if none can.
#
# Not the floating /opt/homebrew/bin/python3 alone: on 10 Nov 2026 a Homebrew
# upgrade repointed it to a fresh 3.15 with no packages (and a Python.app that
# macOS killed for code signing). The relay crash-looped ~2,850 times on
# `import curl_cffi` and the 07:00 daily_verify never ran.
#
# Override with MCX_PYTHON=/path/to/python.

root="$(cd -P "$(dirname "$0")/.." && pwd)"
for cand in "$MCX_PYTHON" "$root/.venv/bin/python" \
            /opt/homebrew/bin/python3.14 /opt/homebrew/opt/python@3.14/bin/python3.14 \
            /opt/homebrew/bin/python3; do
  [ -n "$cand" ] && [ -x "$cand" ] || continue
  if "$cand" -c "import curl_cffi, pandas" >/dev/null 2>&1; then
    echo "$cand"
    exit 0
  fi
done
echo "mcx_python.sh: no Python with curl_cffi+pandas (tried MCX_PYTHON, .venv, python3.14, python3)" >&2
exit 1
