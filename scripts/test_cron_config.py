#!/usr/bin/env python3
"""Offline checks on vercel.json crons vs api/cron.py's dispatch table: jobs that can never
work from Vercel (margins, oi_participants) are not scheduled, and every scheduled job exists.
No network. Run:  python3 scripts/test_cron_config.py"""
import ast
import json
import os
import re
import sys
from urllib.parse import parse_qs, urlparse

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
failed = 0


def check(name, got, want):
    global failed
    ok = got == want
    print(("PASS " if ok else "FAIL ") + name + ("" if ok else f": got {got!r}, want {want!r}"))
    if not ok:
        failed += 1


crons = json.load(open(os.path.join(ROOT, "vercel.json")))["crons"]
jobs = [parse_qs(urlparse(c["path"]).query).get("job", [""])[0] for c in crons]

tree = ast.parse(open(os.path.join(ROOT, "api", "cron.py")).read())
table = set()
for node in ast.walk(tree):
    if isinstance(node, ast.Dict) and node.keys and all(isinstance(k, ast.Constant) and isinstance(k.value, str) for k in node.keys):
        keys = {k.value for k in node.keys}
        if "margins" in keys and "oi_participants" in keys:
            table = keys
check("dispatch table found", "oi_participants" in table, True)
check("no oi_participants cron", "oi_participants" in jobs, False)
check("no margins cron", "margins" in jobs, False)
check("every cron job is in the dispatch table", sorted(j for j in jobs if j not in table), [])
check("crons still scheduled", len(jobs) > 0, True)
sys.exit(1 if failed else 0)
