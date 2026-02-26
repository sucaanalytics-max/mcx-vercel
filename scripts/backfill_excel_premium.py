#!/usr/bin/env python3
"""
One-time backfill: Correct Supabase option premium values using Excel Dashboard actuals.

The BHAV proxy formula (opt_close/undl_close × Value_Lacs) introduces ~8.5% error
on options revenue. The Excel Exchanges Dashboard has direct Premium Value from MCX.
This script patches all overlapping Supabase rows with the correct Excel values.

Usage:
  python3 scripts/backfill_excel_premium.py --dry-run   # preview changes
  python3 scripts/backfill_excel_premium.py              # apply corrections
"""
import sys, os, json, urllib.request

# ── Config ──
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://avqwpebveqetwwzkmtux.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_ANON_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImF2cXdwZWJ2ZXFldHd3emttdHV4Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzE0MDkwMzMsImV4cCI6MjA4Njk4NTAzM30.U_Ug61Fp1NSCesXBkYU7GJGTbuATFtXsz6GTi5948Rw")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ACTUALS_FILE = os.path.join(SCRIPT_DIR, "excel_actuals.json")


def load_excel_actuals():
    """Load pre-extracted Excel actuals from JSON."""
    with open(ACTUALS_FILE) as f:
        return json.load(f)


def fetch_supabase_rows():
    """Fetch all current Supabase rows."""
    url = f"{SUPABASE_URL}/rest/v1/mcx_daily_revenue?order=trading_date.asc&limit=100"
    req = urllib.request.Request(url, headers={
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    })
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode())


def patch_row(date_str, updates):
    """PATCH a single Supabase row by trading_date."""
    url = f"{SUPABASE_URL}/rest/v1/mcx_daily_revenue?trading_date=eq.{date_str}"
    body = json.dumps(updates).encode()
    req = urllib.request.Request(url, data=body, method="PATCH", headers={
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    })
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode())


def main():
    dry_run = "--dry-run" in sys.argv

    print("=" * 70)
    print("MCX Revenue Backfill: Excel Premium Correction")
    print(f"Mode: {'DRY RUN (no writes)' if dry_run else 'LIVE (will update Supabase)'}")
    print("=" * 70)

    # Load data
    excel = load_excel_actuals()
    supa_rows = fetch_supabase_rows()
    supa_dict = {r["trading_date"]: r for r in supa_rows}

    print(f"\nExcel dates: {len(excel)}")
    print(f"Supabase dates: {len(supa_dict)}")

    # Find overlapping dates
    overlap = sorted(set(excel.keys()) & set(supa_dict.keys()))
    print(f"Overlapping dates: {len(overlap)}")

    if not overlap:
        print("ERROR: No overlapping dates found. Aborting.")
        sys.exit(1)

    # Compute and display changes
    total_opt_delta = 0
    total_rev_delta = 0
    changes = []

    print(f"\n{'Date':<12} {'Old Opt':>9} {'New Opt':>9} {'Δ Opt':>9} {'Old Tot':>9} {'New Tot':>9} {'Δ Tot':>9}")
    print("-" * 75)

    for d in overlap:
        old = supa_dict[d]
        xl = excel[d]

        old_opt = old["opt_rev_cr"]
        old_tot = old["total_rev_cr"]
        new_opt = round(xl["opt"], 4)
        new_tot = round(old["fut_rev_cr"] + xl["opt"], 4)  # keep original fut, use Excel opt

        d_opt = new_opt - old_opt
        d_tot = new_tot - old_tot
        total_opt_delta += d_opt
        total_rev_delta += d_tot

        flag = " ⚠" if abs(d_opt) > 1.0 else ""
        print(f"{d:<12} {old_opt:>9.4f} {new_opt:>9.4f} {d_opt:>+9.4f} {old_tot:>9.4f} {new_tot:>9.4f} {d_tot:>+9.4f}{flag}")

        changes.append({
            "date": d,
            "updates": {
                "opt_rev_cr": new_opt,
                "total_rev_cr": new_tot,
                "source": "excel_calibrated",
            }
        })

    print("-" * 75)
    print(f"{'TOTAL':>12} {'':>9} {'':>9} {total_opt_delta:>+9.4f} {'':>9} {'':>9} {total_rev_delta:>+9.4f}")
    print(f"\nCumulative options correction: {total_opt_delta:+.4f} Cr")
    print(f"Cumulative total correction:   {total_rev_delta:+.4f} Cr")

    if dry_run:
        print(f"\n🔍 DRY RUN complete. {len(changes)} rows would be updated.")
        print("Run without --dry-run to apply changes.")
        return

    # Apply changes
    print(f"\n🔧 Applying {len(changes)} corrections to Supabase...")
    success = 0
    errors = 0

    for c in changes:
        try:
            result = patch_row(c["date"], c["updates"])
            if result:
                success += 1
            else:
                print(f"  ⚠ No row returned for {c['date']}")
                errors += 1
        except Exception as e:
            print(f"  ✗ Error patching {c['date']}: {e}")
            errors += 1

    print(f"\n✓ Done: {success} updated, {errors} errors")

    # Verification
    print("\n── Verification ──")
    updated_rows = fetch_supabase_rows()
    xl_sum = sum(excel[d]["opt"] for d in overlap)
    supa_sum = sum(r["opt_rev_cr"] for r in updated_rows if r["trading_date"] in overlap)
    delta = abs(supa_sum - xl_sum)
    print(f"Excel opt_rev sum:    {xl_sum:.4f} Cr")
    print(f"Supabase opt_rev sum: {supa_sum:.4f} Cr")
    print(f"Residual delta:       {delta:.4f} Cr {'✓' if delta < 0.01 else '✗ MISMATCH'}")


if __name__ == "__main__":
    main()
