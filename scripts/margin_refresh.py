#!/usr/bin/env python3
"""
MCX Margin Refresh — CLI wrapper for manual runs.

Usage:
  python3 scripts/margin_refresh.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.cron_margins import refresh_margins

if __name__ == "__main__":
    result = refresh_margins()
    print(f"\nSuccess: {result['success']}")
    if result.get("snapshot_date"):
        print(f"Date: {result['snapshot_date']}")
        print(f"Rows: {result['rows_upserted']}")
        print(f"Symbols: {', '.join(result.get('symbols', []))}")
    for line in result.get("log", []):
        print(f"  {line}")
    if result.get("errors"):
        print("\nErrors:")
        for e in result["errors"]:
            print(f"  {e}")
