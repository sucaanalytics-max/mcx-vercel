"""
Base class for the Vercel Python functions in api/: JSON responses, CORS and
the OPTIONS preflight, written once instead of in every api/*.py file.

    class handler(JSONHandler):
        def do_GET(self):
            self.send_json(payload, cache="public, max-age=60, s-maxage=60")

Vercel still finds `handler` in each file; it is a BaseHTTPRequestHandler.
"""
import json
from http.server import BaseHTTPRequestHandler

from lib.mcx_config import make_cors_headers, cache_header


# Cache-Control for a good 200, by how often the data behind it changes.
# s-maxage is the CDN's (Vercel strips it before the browser), max-age the
# browser's; stale-while-revalidate serves the last copy while one request
# refreshes it, so after the first visit nobody waits on a cold function.
# Errors are never cached (send_json).
#   SNAPSHOT  the relay's latest snapshot, every 15 min in trading hours; 30 s
#             at the edge keeps the refresh button honest, none in the browser
#   INTRADAY  rows that change at the close (daily revenue, past days' snapshots)
#   DAILY     written once or twice a day: the 07:00 verify, the 19:30-19:50
#             crons, the 05:30 commodity report, 08:00 momentum, 15:10 options
#             paper. At most 1 h 30 min behind after a write (s-maxage + SWR).
CACHE_SNAPSHOT = "public, max-age=0, s-maxage=30"
CACHE_INTRADAY = "public, max-age=120, s-maxage=120, stale-while-revalidate=600"
CACHE_DAILY = "public, max-age=300, s-maxage=1800, stale-while-revalidate=3600"


class JSONHandler(BaseHTTPRequestHandler):
    allow_methods = "GET, OPTIONS"
    # Cache-Control for a 200 response when a call gives none; None sends no
    # header (the CDN then caches nothing).
    default_cache = None

    def log_message(self, fmt, *args):
        pass

    def _cors(self):
        """Reflect the request's Origin when it is allowed (make_cors_headers)."""
        for k, v in make_cors_headers(self.headers.get("Origin", "")).items():
            self.send_header(k, v)
        self.send_header("Access-Control-Allow-Methods", self.allow_methods)
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def send_json(self, data, status=200, cache=None):
        """Write `data` as JSON. `cache` is the Cache-Control for a good 200
        response; any error status, or a {success: false} body, is sent
        no-store so the CDN never holds a failure (cache_header)."""
        body = json.dumps(data, default=str).encode("utf-8")
        cache = cache if cache is not None else self.default_cache
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        if cache is not None:
            self.send_header("Cache-Control", cache_header(data, cache) if status == 200 else "no-store")
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def send_error_json(self, err, status=500):
        self.send_json({"success": False, "error": str(err)[:200]}, status)

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()
