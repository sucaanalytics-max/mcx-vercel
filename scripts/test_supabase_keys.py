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
    for d in ("api", "lib", "scripts", "research"):
        for name in os.listdir(os.path.join(ROOT, d)):
            path = os.path.join(ROOT, d, name)
            if not name.endswith(".py") or name == "supabase.py" or not os.path.isfile(path):
                continue
            src = open(path, encoding="utf-8").read()
            if "eyJhbG" + "ciOi" in src:
                hits.append(f"{d}/{name}")
    check("no hardcoded JWT outside lib/supabase.py", not hits, ", ".join(hits))


def test_raw_helpers():
    print("\n[4] get_rows / post_rows / patch_rows request shape (offline)")
    import io, urllib.error, urllib.request
    import lib.supabase as sb
    seen = []

    class Resp(io.BytesIO):
        def __enter__(self): return self
        def __exit__(self, *a): return False

    def fake(req, timeout=None):
        seen.append((req.get_method(), req.full_url, req.get_header("Prefer"), req.get_header("Apikey"),
                     json.loads(req.data) if req.data else None, timeout))
        if req.get_method() == "POST" and len(seen) == 2:
            raise urllib.error.HTTPError(req.full_url, 409, "Conflict", {}, io.BytesIO(b"dup"))
        return Resp(b'[{"a": 1}]')

    real = urllib.request.urlopen
    urllib.request.urlopen = fake
    try:
        got = sb.get_rows("t", "?x=eq.1", timeout=10)
        try:
            sb.post_rows("t", {"a": 1}, on_conflict="a")
            raised = None
        except urllib.error.HTTPError as e:
            raised = e.code
        sb.patch_rows("t", "?a=eq.1", {"a": 2})
    finally:
        urllib.request.urlopen = real
    check("get_rows: GET with the read key, parsed JSON", seen[0][:2] == ("GET", f"{sb.SUPABASE_URL}/rest/v1/t?x=eq.1")
          and seen[0][3] == sb.SUPABASE_ANON_KEY and seen[0][5] == 10 and got == [{"a": 1}], str(seen[0]))
    check("post_rows: POST merge-duplicates on_conflict, write key", seen[1][:3] == ("POST", f"{sb.SUPABASE_URL}/rest/v1/t?on_conflict=a",
          "resolution=merge-duplicates") and seen[1][3] == sb.SUPABASE_WRITE_KEY, str(seen[1]))
    check("post_rows raises the HTTPError so callers can branch on 409", raised == 409, str(raised))
    check("patch_rows: PATCH on the filter, return=minimal", seen[2][:3] == ("PATCH", f"{sb.SUPABASE_URL}/rest/v1/t?a=eq.1",
          "return=minimal") and seen[2][4] == {"a": 2}, str(seen[2]))


if __name__ == "__main__":
    print("=" * 72)
    print("supabase key tests")
    print("=" * 72)
    test_headers()
    test_precedence()
    test_no_key_copies()
    test_raw_helpers()
    print("\n" + "=" * 72)
    if FAILED:
        print(f"{len(FAILED)} FAILED: {', '.join(FAILED)}")
        sys.exit(1)
    print("All tests passed.")
