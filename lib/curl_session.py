"""
curl_cffi sessions with Chrome TLS impersonation, for the MCX / MCXCCL sites
whose Akamai front end rejects plain urllib. Local jobs only (curl_cffi is in
the `local` extra, never on Vercel), so curl_cffi is imported lazily.

One copy of the "try the newest impersonation target, fall back to older ones
the installed curl_cffi knows" rule, which bhav_refresh, icomdex_refresh, the
relay, mcxccl_scraper and backfill_oi_participants each had their own version of.
"""
import os

# Preferred target, then fallbacks for older curl_cffi builds that don't know
# the newest token (e.g. curl_cffi 0.13.0 lacks chrome142). MCX_IMPERSONATE
# overrides the first choice.
CHROME_IMPERSONATE = os.environ.get("MCX_IMPERSONATE", "chrome142")
CHROME_IMPERSONATE_FALLBACKS = ["chrome136", "chrome131", "chrome"]


def impersonation_targets():
    return [CHROME_IMPERSONATE, *CHROME_IMPERSONATE_FALLBACKS]


def _unsupported_target(err) -> bool:
    msg = str(err).lower()
    return "impersonat" in msg or "not supported" in msg


def new_session(warm_url=None, timeout=30, get=None, raise_for_status=False):
    """A curl_cffi Session warmed with one GET on warm_url (Akamai cookies).

    Tries each impersonation target in turn, moving on only when the target is
    unknown to this curl_cffi build (some builds reject it only on the first
    request); any other error is raised. warm_url=None skips the warm-up.
    get(session, url, timeout) replaces session.get for the warm-up, e.g. the
    relay's hard wall-clock guard. raise_for_status: fail on a non-2xx warm-up."""
    from curl_cffi import requests as cfreq
    last_err = None
    for target in impersonation_targets():
        try:
            sess = cfreq.Session(impersonate=target)
            if warm_url:
                resp = (get or (lambda s, u, t: s.get(u, timeout=t)))(sess, warm_url, timeout)
                if raise_for_status:
                    resp.raise_for_status()
            return sess
        except Exception as e:
            if _unsupported_target(e):
                last_err = e
                continue
            raise
    raise last_err
