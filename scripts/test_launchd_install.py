#!/usr/bin/env python3
"""
scripts/launchd/install.sh: a re-install waits for launchd to finish the
bootout before bootstrapping (18 Nov 2026: bootstrap ran too soon, failed
with error 5 and left the relay unloaded), retries a failed bootstrap, and
exits non-zero unless the job ends up loaded. launchctl is a fake that keeps
state in a temp folder; nothing touches the real launchd.
"""
import os, subprocess, sys, tempfile, textwrap

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INSTALL = os.path.join(ROOT, "scripts", "launchd", "install.sh")
FAILED = []


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}" + (f"  ({detail})" if detail and not cond else ""))
    if not cond:
        FAILED.append(name)


FAKE = textwrap.dedent('''\
    #!/bin/bash
    # fake launchctl: state in $FAKE_DIR. print <domain/label>, bootout <domain/label>,
    # bootstrap <domain> <plist>.
    d="$FAKE_DIR"; echo "$*" >> "$d/calls"
    case "$1" in
      print)
        l="${2##*/}"; [ -f "$d/$l.loaded" ] || exit 113
        if [ -f "$d/$l.lag" ]; then n=$(cat "$d/$l.lag")
          if [ "$n" -gt 0 ]; then echo $((n-1)) > "$d/$l.lag"; exit 0; fi
          rm -f "$d/$l.lag" "$d/$l.loaded"; exit 113; fi
        exit 0 ;;
      bootout)
        l="${2##*/}"; [ -f "$d/$l.loaded" ] || exit 3
        echo "${FAKE_LAG:-0}" > "$d/$l.lag"; exit 0 ;;
      bootstrap)
        l=$(basename "$3" .plist)
        if [ -f "$d/$l.loaded" ]; then echo "Bootstrap failed: 5: Input/output error" >&2; exit 5; fi
        f=$(cat "$d/bfails" 2>/dev/null || echo 0)
        if [ "$f" -gt 0 ]; then echo $((f-1)) > "$d/bfails"; echo "Bootstrap failed: 5" >&2; exit 5; fi
        touch "$d/$l.loaded"; exit 0 ;;
    esac
    exit 0
    ''')


def run(lag=0, bfails=0, loaded=True):
    d = tempfile.mkdtemp()
    agents = os.path.join(d, "agents"); os.mkdir(agents)
    fake = os.path.join(d, "launchctl")
    open(fake, "w").write(FAKE); os.chmod(fake, 0o755)
    if loaded:
        open(os.path.join(d, "com.mcx.relay.loaded"), "w").close()
    if bfails:
        open(os.path.join(d, "bfails"), "w").write(str(bfails))
    env = dict(os.environ, FAKE_DIR=d, FAKE_LAG=str(lag), MCX_LAUNCHCTL=fake,
               MCX_LAUNCH_AGENTS=agents, MCX_WAIT_STEP="0")
    p = subprocess.run(["bash", INSTALL, "relay"], env=env, capture_output=True, text=True)
    calls = open(os.path.join(d, "calls")).read().split("\n")
    return p, calls, d, agents


print("[1] slow bootout: waits, then one clean bootstrap")
p, calls, d, agents = run(lag=5)
boots = [c for c in calls if c.startswith("bootstrap")]
check("exit 0", p.returncode == 0, p.stderr)
check("bootstrap only after the old job is gone (one call, no error 5)", len(boots) == 1, calls)
check("job loaded and plist copied", os.path.exists(os.path.join(d, "com.mcx.relay.loaded"))
      and os.path.exists(os.path.join(agents, "com.mcx.relay.plist")))
check("says installed", "installed com.mcx.relay" in p.stdout)

print("\n[2] transient bootstrap failure is retried")
p, calls, d, _ = run(bfails=2)
check("exit 0 after retries", p.returncode == 0, p.stderr)
check("three bootstrap attempts", sum(c.startswith("bootstrap") for c in calls) == 3, calls)

print("\n[3] bootstrap never succeeds: fails loudly")
p, calls, d, _ = run(bfails=99)
check("exit non-zero", p.returncode != 0)
check("names the job and the manual command", "FAILED: com.mcx.relay is not loaded" in p.stderr
      and "launchctl bootstrap" in p.stderr, p.stderr)

print("\n[4] first install (nothing loaded yet)")
p, calls, d, _ = run(loaded=False)
check("exit 0, loaded", p.returncode == 0 and os.path.exists(os.path.join(d, "com.mcx.relay.loaded")), p.stderr)

print(f"\n{'ALL PASS' if not FAILED else f'{len(FAILED)} FAILED'}")
sys.exit(1 if FAILED else 0)
