"""
Dhan API client wrapper and per-commodity live-OI fetcher for MCX.

Wraps `dhanhq` with an auto-renewing token (shared with the Demat project via
~/.dhan/credentials.json, .env as fallback) and a cached scrip master so we
can resolve MCX commodity instruments and fetch live OI in a single call.

Usage:
  from lib.dhan_client import get_client, fetch_mcx_commodity_oi
  oi_rows = fetch_mcx_commodity_oi(["SILVER","SILVERM","GOLD","CRUDEOIL"])

Returns rows shaped for upsert into mcx_commodity_daily:
  {trading_date, commodity, instrument_type, open_interest, oi_value_cr, source}
"""
from __future__ import annotations
import os, time, datetime as dt
from pathlib import Path
from functools import lru_cache
from typing import Iterable, Optional

import pandas as pd

# ── env loading ──────────────────────────────────────────────────────────
def _load_project_env():
    """Load project-local env files into os.environ WITHOUT overriding shell vars.
    Reads .env.local (the project's gitignored secrets file — where
    VERCEL_OIDC_TOKEN and the Dhan creds belong) then .env. Precedence:
    shell > .env.local > .env (first writer wins via setdefault)."""
    root = Path(__file__).resolve().parents[1]
    for name in (".env.local", ".env"):
        env = root / name
        if not env.exists():
            continue
        for line in env.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


_load_project_env()

DHAN_CLIENT_ID = os.environ.get("DHAN_CLIENT_ID", "")
DHAN_ACCESS_TOKEN = os.environ.get("DHAN_ACCESS_TOKEN", "")

# ── token: shared with the Demat project ─────────────────────────────────
# Dhan tokens last 24 h. The Demat project (~/Documents/Demat/dhan_auth.py)
# keeps the live token in ~/.dhan/credentials.json and re-mints it from
# PIN + TOTP. We read and renew the same file, so one renewal serves both.
# Minting a new token does not revoke the old one (both checked 29 Oct 2026),
# so the two projects renewing independently cannot lock each other out.
# Without the file we fall back to DHAN_ACCESS_TOKEN from .env.
DHAN_CREDS_FILE = Path.home() / ".dhan" / "credentials.json"
_AUTH_URL = "https://auth.dhan.co/app/generateAccessToken"
_PROFILE_URL = "https://api.dhan.co/v2/profile"
_TOKEN_LIFETIME_S = 23 * 3600   # Demat's conservative note; Dhan gives ~24 h
_REFRESH_MARGIN_S = 3600        # renew when < 1 h is left


def _totp(secret: str, at: float) -> str:
    """RFC 6238 TOTP (SHA-1, 6 digits, 30 s) — the same codes pyotp makes."""
    import base64, hashlib, hmac, struct
    key = base64.b32decode(secret.upper().replace(" ", "") + "=" * (-len(secret) % 8))
    mac = hmac.new(key, struct.pack(">Q", int(at) // 30), hashlib.sha1).digest()
    o = mac[-1] & 0x0F
    return f"{(struct.unpack('>I', mac[o:o + 4])[0] & 0x7FFFFFFF) % 1_000_000:06d}"


def _token_rejected(client_id: str, token: str) -> bool:
    """True only on an explicit 401 from Dhan. Network errors say nothing about
    the token and must not trigger a re-mint."""
    import urllib.request, urllib.error
    req = urllib.request.Request(_PROFILE_URL, headers={
        "access-token": token, "client-id": client_id, "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10):
            return False
    except urllib.error.HTTPError as e:
        return e.code == 401
    except Exception:
        return False


def _mint_token(creds: dict) -> dict:
    """POST PIN + TOTP to Dhan's auth endpoint; return creds with the new token.
    Retries once across the 30 s boundary on 'Invalid TOTP' (window race)."""
    import json, urllib.parse, urllib.request
    cid, pin, secret = creds.get("client_id"), creds.get("pin"), creds.get("totp_secret")
    if not (cid and pin and secret):
        raise RuntimeError(f"{DHAN_CREDS_FILE} lacks client_id / pin / totp_secret")

    def attempt() -> dict:
        left = 30 - int(time.time()) % 30
        if left <= 3:
            time.sleep(left + 1)
        qs = urllib.parse.urlencode({"dhanClientId": cid, "pin": pin,
                                     "totp": _totp(secret, time.time())})
        with urllib.request.urlopen(urllib.request.Request(
                f"{_AUTH_URL}?{qs}", method="POST"), timeout=15) as r:
            return json.loads(r.read().decode())

    body = attempt()
    token = body.get("accessToken") or body.get("access_token")
    if not token and "TOTP" in str(body.get("message", "")):
        time.sleep(31 - int(time.time()) % 30)
        body = attempt()
        token = body.get("accessToken") or body.get("access_token")
    if not token:
        # Dhan's error body carries no secrets; keep it short for the log.
        raise RuntimeError(f"Dhan token mint failed: {str(body)[:200]}")
    return {**creds, "access_token": token,
            "token_expires_at": int(time.time()) + _TOKEN_LIFETIME_S}


def _save_creds(creds: dict) -> None:
    """Atomic write, 0600 — the file holds the PIN and TOTP secret."""
    import json
    tmp = DHAN_CREDS_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(creds, indent=2))
    os.chmod(tmp, 0o600)
    os.replace(tmp, DHAN_CREDS_FILE)


def _credentials() -> tuple[str, str]:
    """(client_id, access_token), renewing the shared token when it is near
    expiry or Dhan refuses it. Raises if no usable token can be found."""
    import json, sys
    if DHAN_CREDS_FILE.exists():
        creds = json.loads(DHAN_CREDS_FILE.read_text())
        cid, tok = str(creds.get("client_id", "")), creds.get("access_token", "")
        stale = (not tok or
                 time.time() > creds.get("token_expires_at", 0) - _REFRESH_MARGIN_S)
        if stale or _token_rejected(cid, tok):
            try:
                creds = _mint_token(creds)
                _save_creds(creds)
                tok = creds["access_token"]
            except Exception as e:
                # Dhan allows one mint per 2 min, so a failure here often means
                # Demat renewed a moment ago: re-read the file before giving up.
                print(f"[dhan_client] token renewal failed: {e}", file=sys.stderr)
                fresh = json.loads(DHAN_CREDS_FILE.read_text()).get("access_token", "")
                if fresh and fresh != tok and not _token_rejected(cid, fresh):
                    tok = fresh
        if cid and tok:
            return cid, tok
    if DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN:
        return DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN
    raise RuntimeError(
        f"No Dhan token: neither {DHAN_CREDS_FILE} (shared with the Demat "
        "project) nor DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN in .env."
    )


@lru_cache(maxsize=1)
def get_client():
    """Singleton dhanhq client on a token that Dhan currently accepts."""
    from dhanhq import dhanhq
    return dhanhq(*_credentials())


@lru_cache(maxsize=1)
def _scrip_master() -> pd.DataFrame:
    """Detailed MCX scrip master (instrument ids, expiries, lot sizes).

    Cached on disk for a day to avoid re-downloading the ~5MB CSV.
    """
    from dhanhq import dhanhq as _d  # for the URL constant
    cache = Path("/tmp/dhan_scrip_master.csv")
    if cache.exists() and (time.time() - cache.stat().st_mtime) < 86_400:
        df = pd.read_csv(cache, low_memory=False)
    else:
        df = pd.read_csv(_d.DETAILED_CSV_URL, low_memory=False)
        df.to_csv(cache, index=False)
    return df[df["EXCH_ID"] == "MCX"].copy()


def list_front_futures(commodity: str) -> list[dict]:
    """All active FUTCOM contracts for a commodity, sorted by expiry asc.

    `commodity` matches UNDERLYING_SYMBOL exactly (e.g. SILVER, SILVERM).
    """
    df = _scrip_master()
    m = df[(df["UNDERLYING_SYMBOL"].astype(str).str.upper() == commodity.upper())
           & (df["INSTRUMENT"] == "FUTCOM")].copy()
    m["SM_EXPIRY_DATE"] = pd.to_datetime(m["SM_EXPIRY_DATE"])
    m = m.sort_values("SM_EXPIRY_DATE")
    return [
        {"security_id": int(r.SECURITY_ID),
         "commodity": str(r.UNDERLYING_SYMBOL),
         "display": str(r.DISPLAY_NAME),
         "expiry": r.SM_EXPIRY_DATE.date().isoformat(),
         "lot_size": float(r.LOT_SIZE) if pd.notna(r.LOT_SIZE) else None}
        for r in m.itertuples()
    ]


def days_to_next_expiry(commodity: str, as_of: Optional[dt.date] = None) -> Optional[int]:
    """Calendar days from `as_of` to the nearest still-alive futures expiry."""
    if as_of is None:
        as_of = dt.date.today()
    futs = list_front_futures(commodity)
    upcoming = [dt.date.fromisoformat(f["expiry"]) for f in futs
                if dt.date.fromisoformat(f["expiry"]) >= as_of]
    if not upcoming:
        return None
    return (min(upcoming) - as_of).days


def fetch_mcx_commodity_oi(commodities: Iterable[str],
                           trading_date: Optional[str] = None) -> list[dict]:
    """Fetch live aggregate OI per commodity by summing across all FUTCOM expiries.

    Returns rows shaped for `mcx_commodity_daily` upsert (one row per
    (date, commodity, FUTCOM)).
    """
    if trading_date is None:
        # IST date
        now_ist = dt.datetime.utcnow() + dt.timedelta(hours=5, minutes=30)
        trading_date = now_ist.strftime("%Y-%m-%d")
    cli = get_client()
    from dhanhq import dhanhq as _d
    SEG = _d.MCX

    rows = []
    for c in commodities:
        futs = list_front_futures(c)
        if not futs:
            continue
        ids = [f["security_id"] for f in futs]
        # quote_data accepts up to ~50 ids per call. Dhan rate-limit is ~1 req/s
        # and silently corrupts responses if you exceed it — throttle to 1.1s.
        ois = {}
        for i in range(0, len(ids), 50):
            chunk = ids[i:i + 50]
            resp = cli.quote_data({SEG: chunk})
            data = resp.get("data", {}).get("data", {}).get(SEG, {})
            for sid_str, q in data.items():
                ois[int(sid_str)] = {
                    "oi": int(q.get("oi") or 0),
                    "last_price": float(q.get("last_price") or 0),
                    "volume": int(q.get("volume") or 0),
                }
            time.sleep(1.1)

        total_oi = sum(o["oi"] for o in ois.values())
        # OI value: sum over contracts of OI_lots * lot_size * price (₹) -> /1e7 to Cr
        # Lot sizes vary; pull from scrip master per id
        oi_value_cr = 0.0
        for f in futs:
            o = ois.get(f["security_id"])
            if o and f["lot_size"]:
                oi_value_cr += o["oi"] * f["lot_size"] * o["last_price"] / 1e7

        rows.append({
            "trading_date": trading_date,
            "commodity": c,
            "instrument_type": "FUTCOM",
            "open_interest": total_oi,
            "oi_value_cr": round(oi_value_cr, 2),
            "source": "dhan_live",
        })
    return rows


__all__ = [
    "get_client",
    "list_front_futures",
    "days_to_next_expiry",
    "fetch_mcx_commodity_oi",
]
