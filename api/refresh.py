"""
/api/refresh — the latest live snapshot, read from Supabase.

The local relay (scripts/mcx_relay.py) fetches MCX every 15 minutes and writes
mcx_snapshots; this endpoint only reads it and enriches it for the frontend
(share of the session gone, projection band, booked/projected revenue by
segment). ?health=1 reports relay health (last heartbeat + snapshot age).

GET only. The old POST path, which asked Vercel to fetch MCX itself and upsert
the snapshot, was removed: MCX's Akamai blocks cloud IPs, so it only added
12-30 s to every manual refresh before the frontend fell back to GET.
"""
from lib.api_handler import JSONHandler
from lib.mcx_config import (
    TRADING_DAYS, session_shape, traded_minutes,
    DAY_MULTIPLIER, DAY_DESCRIPTION,
    get_day_type, project_full_day,
    calc_revenue, calc_uncertainty, now_ist, is_market_open,
    SUPABASE_ANON_KEY, supabase_read, calendar_status,
)


def _revenue_fields(fut_notl, opt_prem, proj_fut, proj_opt):
    """Revenue booked so far and projected for the day, ₹ Cr, split by segment."""
    bf, bo, _, _ = calc_revenue(fut_notl or 0, opt_prem or 0)
    pf, po, _, _ = calc_revenue(proj_fut or 0, proj_opt or 0)
    return {
        "booked_fut_rev_cr": round(bf, 4), "booked_opt_rev_cr": round(bo, 4), "booked_rev_cr": round(bf + bo, 4),
        "proj_fut_rev_cr": round(pf, 4), "proj_opt_rev_cr": round(po, 4), "proj_rev_cr": round(pf + po, 4),
    }


class handler(JSONHandler):

    def _health_check(self):
        """Return relay health: last heartbeat + last snapshot age."""
        from datetime import datetime, timezone
        try:
            t = now_ist()

            # Try heartbeat table first
            heartbeat = None
            try:
                hb = supabase_read("relay_heartbeat", "?order=heartbeat_at.desc&limit=1")
                if hb:
                    heartbeat = hb[0]
            except Exception:
                pass  # Table may not exist yet

            # Last snapshot (always available)
            snaps = supabase_read("mcx_snapshots", "?order=captured_at.desc&limit=1")
            snap = snaps[0] if snaps else None

            snap_age_min = None
            if snap and snap.get("captured_at"):
                cap = datetime.fromisoformat(snap["captured_at"].replace("Z", "+00:00"))
                # now_ist() is a naive IST wall clock: astimezone() would read it in
                # the host's zone (UTC on Vercel) and add 330 min to every age.
                snap_age_min = round((datetime.now(timezone.utc) - cap.astimezone(timezone.utc)).total_seconds() / 60, 1)

            is_market_hours = is_market_open(t)
            alive = snap_age_min is not None and snap_age_min < 30 if is_market_hours else True
            # A missing holiday calendar makes every session/projection wrong
            # (holidays count as trading days): unhealthy until it is loaded.
            cal = calendar_status(t.date())
            if cal["status"] == "STALE":
                alive = False

            result = {
                "success": True,
                "healthy": alive,
                "checked_at": t.strftime("%Y-%m-%dT%H:%M:%S+05:30"),
                "market_open": is_market_hours,
                "calendar": cal,
                "last_snapshot": {
                    "captured_at": snap.get("captured_at") if snap else None,
                    "trading_date": snap.get("trading_date") if snap else None,
                    "total_rev_cr": snap.get("total_rev_cr") if snap else None,
                    "elapsed_min": snap.get("elapsed_min") if snap else None,
                    "age_min": snap_age_min,
                },
            }
            if heartbeat:
                result["heartbeat"] = heartbeat

            self.send_json(result)
        except Exception as e:
            self.send_json({"success": False, "healthy": False, "error": str(e)[:200]})

    def do_GET(self):
        """F-08: Read latest snapshot from Supabase and enrich for frontend.
        ?health=1 — relay health check (heartbeat + snapshot age)."""
        if not SUPABASE_ANON_KEY:
            self.send_json({"success": False, "error": "Supabase not configured"})
            return

        # ── Health check endpoint ─────────────────────────────────────────
        from urllib.parse import urlparse, parse_qs
        qs = parse_qs(urlparse(self.path).query)
        if "health" in qs:
            return self._health_check()

        try:
            today = now_ist().strftime("%Y-%m-%d")
            # Fetch the top 2 latest rows (by elapsed_min) so we can fall back
            # past a known "stale opening cumulative" artifact (see clamp below).
            rows = supabase_read(
                "mcx_snapshots",
                f"?trading_date=eq.{today}&order=elapsed_min.desc&limit=2"
            )
            if rows:
                row = rows[0]
                # ── Opening-artifact clamp ──────────────────────────────────
                # Early in the session MCX occasionally reports a stale, oversized
                # cumulative value (carry-over from the prior session). Symptom:
                # a very low elapsed_min paired with an implausibly high total_rev_cr
                # (full days are ~8-14 Cr). If the latest row looks like this, fall
                # back to the next-highest-elapsed_min row for the day if one exists;
                # otherwise serve it but flag it so the frontend won't trust the projection.
                opening_artifact = False
                try:
                    _traded = traded_minutes(row.get("elapsed_min") or 0, today)
                    _total = row.get("total_rev_cr") or 0
                    if _traded <= 20 and _total > 30:
                        if len(rows) > 1:
                            # Use the previous good snapshot for the day.
                            row = rows[1]
                        else:
                            # Only one snapshot exists — serve it but mark untrusted.
                            opening_artifact = True
                except Exception:
                    pass  # Never let the clamp crash the serve path.
                result = {
                    "success": True,
                    "source": "supabase_cache",
                    "timestamp": row.get("captured_at", ""),
                    **{k: row[k] for k in row if k not in ("id", "captured_at")},
                }
                # Enrich with computed fields the frontend expects
                elapsed = result.get("elapsed_min") or 0
                shape = session_shape(today)
                total_min = shape["total"]
                traded = traded_minutes(elapsed, today)
                elapsed_pct = round(traded / total_min * 100, 1) if total_min > 0 else 0
                result["elapsed_pct"] = elapsed_pct
                result["session_total_min"] = total_min
                result["session_type"] = shape["type"]
                result["range_min"] = traded   # see process_market_data

                fut_n = result.get("fut_notl_cr") or 0
                opt_n = result.get("opt_notl_cr") or 0
                opt_p = result.get("opt_prem_cr") or 0
                result["total_notl_cr"] = round(fut_n + opt_n, 2)

                # Project full-day if session not closed
                day_type = result.get("day_type") or get_day_type(now_ist())
                result["day_type"] = day_type
                result["day_description"] = DAY_DESCRIPTION.get(day_type, "")
                result["day_multiplier"] = result.get("day_multiplier") or DAY_MULTIPLIER.get(day_type, 1.0)

                if result.get("proj_fut_cr") is None or result.get("proj_opt_cr") is None:
                    # Phase 4: pass today's prior snapshots for regime-drift detection
                    try:
                        today_snaps = supabase_read(
                            "mcx_snapshots",
                            f"?trading_date=eq.{today}&select=elapsed_min,fut_notl_cr,opt_prem_cr&order=elapsed_min.asc"
                        )
                    except Exception:
                        today_snaps = None
                    pf, po, conf = project_full_day(fut_n, opt_p, elapsed, day_type,
                                                    today_snapshots=today_snaps,
                                                    trading_date=today)
                    result["proj_fut_cr"] = round(pf, 2)
                    result["proj_opt_cr"] = round(po, 2)
                    _, _, _, total_rev = calc_revenue(pf, po)
                    result["proj_total_rev"] = round(total_rev, 4)
                    result["confidence"] = conf

                time_pct = traded / total_min if total_min > 0 else 0
                dual = result.get("dual_call", False)
                unc = calc_uncertainty(time_pct, day_type, dual)
                result["uncertainty_pct"] = round(unc * 100, 1)
                proj_total = result.get("proj_total_rev") or result.get("total_rev_cr") or 0
                result["rev_low"] = round(proj_total * (1 - unc), 2)
                result["rev_high"] = round(proj_total * (1 + unc), 2)
                result["trading_days"] = TRADING_DAYS
                result.update(_revenue_fields(fut_n, opt_p, result["proj_fut_cr"], result["proj_opt_cr"]))

                # Surface the opening-artifact flag so the frontend doesn't
                # trust the projection derived from a stale opening cumulative.
                if opening_artifact:
                    result["opening_artifact"] = True
                    result["confidence"] = "untrusted"

                self.send_json(result)
            else:
                self.send_json({"success": False, "error": "No data for today yet. Run the local relay script."})
        except Exception as e:
            self.send_json({"success": False, "error": f"Supabase read failed: {str(e)[:200]}"})
