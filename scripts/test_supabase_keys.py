#!/usr/bin/env python3
"""
Supabase API keys: the new sb_ keys and the legacy JWT keys (Supabase retires
the legacy anon / service_role keys by the end of 2026).

  1. supabase_headers: sb_ keys go only in `apikey`; a legacy JWT keeps the
     Bearer copy, so nothing changes until the new keys are configured.
  2. Key precedence: SUPABASE_PUBLISHABLE_KEY > SUPABASE_ANON_KEY > built-in
     legacy anon for reads; SUPABASE_SECRET_KEY > SUPABASE_SERVICE_KEY for writes.
  3. No script carries its own copy of a key any more.

Pure: no network. Key values below are fakes.
"""
import os, sys, subprocess, json

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
FAILED = []


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}" + (f"  ({detail})" if detail and not cond else ""))
    if not cond:
        FAILED.append(name)


def test_headers():
    print("\n[1] supabase_headers")
    from lib.mcx_config import supabase_headers, supabase_key_kind
    pub, sec, jwt = "sb_publishable_FAKE123", "sb_secret_FAKE456", "eyJ0eXAiOiJKV1QifQ.e30.c2ln"
    check("publishable: apikey only", supabase_headers(pub) == {"apikey": pub})
    check("secret: apikey only", supabase_headers(sec) == {"apikey": sec})
    check("legacy JWT: apikey + Bearer (unchanged)",
          supabase_headers(jwt) == {"apikey": jwt, "Authorization": f"Bearer {jwt}"})
    check("kinds", [supabase_key_kind(k) for k in (pub, sec, jwt, "")] ==
          ["publishable", "secret", "legacy-jwt", "unknown"])


def _config_in_subprocess(env):
    """Import lib.mcx_config in a clean process with this env (no .env.local
    influence: setdefault never overrides an explicit env var, and empty-string
    vars are set for every key we don't want)."""
    code = ("import json, lib.mcx_config as c; print(json.dumps({'read': c.SUPABASE_ANON_KEY, "
            "'write': c.SUPABASE_WRITE_KEY, 'role': c.SUPABASE_WRITE_ROLE}))")
    base = {k: v for k, v in os.environ.items()
            if k not in ("SUPABASE_PUBLISHABLE_KEY", "SUPABASE_ANON_KEY", "SUPABASE_SECRET_KEY", "SUPABASE_SERVICE_KEY")}
    blank = {k: "" for k in ("SUPABASE_PUBLISHABLE_KEY", "SUPABASE_ANON_KEY", "SUPABASE_SECRET_KEY", "SUPABASE_SERVICE_KEY")}
    out = subprocess.run([sys.executable, "-c", code], cwd=ROOT, env={**base, **blank, **env},
                         capture_output=True, text=True, timeout=60)
    return json.loads(out.stdout.strip().splitlines()[-1])


def test_precedence():
    print("\n[2] key precedence")
    c = _config_in_subprocess({"SUPABASE_PUBLISHABLE_KEY": "sb_publishable_P", "SUPABASE_ANON_KEY": "eyJx.y.z",
                               "SUPABASE_SECRET_KEY": "sb_secret_S", "SUPABASE_SERVICE_KEY": "eyJa.b.c"})
    check("publishable wins for reads", c["read"] == "sb_publishable_P", c["read"])
    check("secret wins for writes", c["write"] == "sb_secret_S", c["write"])
    check("role is service_role", c["role"] == "service_role", c["role"])
    c = _config_in_subprocess({"SUPABASE_SERVICE_KEY": "eyJa.b.c"})
    check("legacy service key still used", c["write"] == "eyJa.b.c" and c["role"] == "service_role", str(c))
    check("read falls back to the built-in legacy anon key", c["read"].startswith("eyJ"), c["read"][:8])
    c = _config_in_subprocess({})
    check("no write key: writes use the read key as anon", c["write"] == c["read"] and c["role"] == "anon", str(c["role"]))


def test_no_key_copies():
    print("\n[3] one source for the keys")
    hits = []
    for d in ("api", "lib", "scripts"):
        for name in os.listdir(os.path.join(ROOT, d)):
            path = os.path.join(ROOT, d, name)
            if not name.endswith(".py") or name == "mcx_config.py" or not os.path.isfile(path):
                continue
            src = open(path, encoding="utf-8").read()
            if "eyJhbG" + "ciOi" in src:
                hits.append(f"{d}/{name}")
    check("no hardcoded JWT outside lib/mcx_config.py", not hits, ", ".join(hits))


if __name__ == "__main__":
    print("=" * 72)
    print("supabase key tests")
    print("=" * 72)
    test_headers()
    test_precedence()
    test_no_key_copies()
    print("\n" + "=" * 72)
    if FAILED:
        print(f"{len(FAILED)} FAILED: {', '.join(FAILED)}")
        sys.exit(1)
    print("All tests passed.")
