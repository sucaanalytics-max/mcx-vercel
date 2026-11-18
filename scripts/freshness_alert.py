#!/usr/bin/env python3
"""
Morning freshness ALERT: run the read-only freshness scan and, when anything is
not OK, show one macOS notification naming it. Writes nothing, fixes nothing,
uses no Claude. (The heavier `com.mcx.freshness` job runs the headless
/freshness-check agent, which also repairs data; this one only tells you.)

Scheduled by scripts/launchd/com.mcx.freshness-alert.plist, Mon–Sat 09:00 IST:
after the 07:00 daily_verify and the overnight Vercel crons, and after the
relay's 08:55 start, so a dead relay or a missed backfill shows up before the
trading day gets going.

  python3 scripts/freshness_alert.py            # scan; notify only if not OK
  python3 scripts/freshness_alert.py --always   # also notify when all OK (test)
  python3 scripts/freshness_alert.py --dry-run  # print the notification, don't show it

Exit code: 0 all OK, 1 something stale/due/erroring, 2 the scan itself failed.
"""
from __future__ import annotations
import os, subprocess, sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

TITLE = "MCX freshness"
MAX_BODY = 230          # macOS truncates longer notification text anyway


def problems(result: dict) -> list[str]:
    """One short line per domain that is not OK, most useful detail first."""
    out = []
    for d in result.get("domains", []):
        status = d.get("status")
        if status == "OK" or (status or "").startswith("INFO_"):
            continue
        name = d.get("domain", "?")
        detail = d.get("message") or (
            f"latest {d.get('latest') or d.get('latest_present') or 'none'}"
            + (f", expected {d['expected']}" if d.get("expected") else ""))
        out.append(f"{name} {status}: {detail}")
    return out


def notification(result: dict) -> tuple[str, str]:
    """(subtitle, body) for the scan result."""
    issues = problems(result)
    day = result.get("expected_trading_day", "?")
    if not issues:
        return f"All OK for {day}", f"{len(result.get('domains', []))} domains current."
    subtitle = f"{len(issues)} issue{'s' if len(issues) != 1 else ''} for {day}"
    body = "; ".join(issues)
    if len(body) > MAX_BODY:
        body = body[:MAX_BODY - 1] + "…"
    return subtitle, body


def _esc(s: str) -> str:
    return s.replace("\\", "\\\\").replace('"', '\\"')


def notify(subtitle: str, body: str, run=subprocess.run) -> None:
    script = (f'display notification "{_esc(body)}" with title "{_esc(TITLE)}" '
              f'subtitle "{_esc(subtitle)}" sound name "Glass"')
    run(["osascript", "-e", script], check=False, timeout=20)


def main(argv=None, scan=None, notify_fn=notify) -> int:
    argv = sys.argv[1:] if argv is None else argv
    always, dry = "--always" in argv, "--dry-run" in argv
    try:
        if scan is None:
            from freshness_scan import scan
        result = scan()
    except Exception as e:   # noqa: BLE001 — a broken scan must still alert
        subtitle, body = "Freshness scan failed", f"{type(e).__name__}: {str(e)[:180]}"
        print(f"{subtitle}: {body}")
        if not dry:
            notify_fn(subtitle, body)
        return 2

    subtitle, body = notification(result)
    ok = not problems(result)
    print(f"{result.get('overall')} — {subtitle}: {body}")
    if (not ok or always) and not dry:
        notify_fn(subtitle, body)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
